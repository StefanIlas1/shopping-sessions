use chrono::NaiveDate;
use memchr::memchr;
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::FxBuildHasher;
use std::fs::{self, File};
use std::hash::{BuildHasher, Hash};
use std::io;
use std::path::Path;

pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
const CHUNK: usize = 256 * 1024;
const PARTITIONS: usize = 512;

pub struct Window {
    pub start: i64,
    pub end: i64,
}

impl Window {
    pub fn read(root: &Path) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let manifest: serde_json::Value =
            serde_json::from_reader(File::open(root.join("manifest.json"))?)?;
        let read_field = |key: &str| {
            manifest[key]
                .as_str()
                .and_then(|value| timestamp(value.as_bytes()))
                .ok_or_else(|| format!("invalid manifest field {key}"))
        };
        let start = read_field("window_start")?;
        let end = read_field("window_end")?;
        if start >= end {
            return Err("manifest window_start must precede window_end".into());
        }
        Ok(Self { start, end })
    }
}

#[derive(Default)]
pub struct Files(Vec<Mmap>);

impl Files {
    pub fn open_file(path: &Path) -> io::Result<Self> {
        let mut files = Self::default();
        files.add(path)?;
        Ok(files)
    }

    pub fn open_parts(root: &Path, name: &str) -> io::Result<Self> {
        let mut files = Self::default();
        for entry in fs::read_dir(root.join(name))? {
            let path = entry?.path();
            if path.extension().is_some_and(|extension| extension == "csv") && path.is_file() {
                files.add(&path)?;
            }
        }
        Ok(files)
    }

    fn add(&mut self, path: &Path) -> io::Result<()> {
        let file = File::open(path)?;
        if file.metadata()?.len() != 0 {
            // Input files are immutable throughout a run, including on the scorer's read-only mount.
            self.0.push(unsafe { Mmap::map(&file)? });
        }
        Ok(())
    }

    pub fn bytes(&self) -> usize {
        self.0.iter().map(|file| file.len()).sum()
    }

    pub fn chunks(&self) -> Vec<&[u8]> {
        let mut chunks = Vec::new();
        for mapping in &self.0 {
            let bytes = &mapping[..];
            let mut start = after_newline(bytes, 0);
            while start < bytes.len() {
                let stop = after_newline(bytes, (start + CHUNK).min(bytes.len()));
                chunks.push(&bytes[start..stop]);
                start = stop;
            }
        }
        chunks
    }

    pub fn rows(&self) -> impl Iterator<Item = &[u8]> {
        self.0
            .iter()
            .flat_map(|bytes| lines(&bytes[after_newline(bytes, 0)..]))
    }
}

fn after_newline(bytes: &[u8], at: usize) -> usize {
    memchr(b'\n', &bytes[at..]).map_or(bytes.len(), |offset| at + offset + 1)
}

fn lines(chunk: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut start = 0;
    memchr::memchr_iter(b'\n', chunk)
        .chain((chunk.last().is_some_and(|&last| last != b'\n')).then_some(chunk.len()))
        .map(move |stop| {
            let line = &chunk[start..stop];
            start = stop + 1;
            line
        })
}

pub fn scan(files: &Files) -> usize {
    files
        .chunks()
        .par_iter()
        .map(|chunk| lines(chunk).count())
        .sum()
}

// Quotes only wrap a field. Commas always delimit fields, even in malformed rows.
pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    let mut result = [&[][..]; N];
    let (mut column, mut start) = (0, 0);
    let (mut quoted, mut close) = (false, None);
    for pos in memchr::memchr2_iter(b',', b'"', line).chain(std::iter::once(line.len())) {
        if line.get(pos) == Some(&b'"') {
            if pos == start {
                quoted = true;
            } else if quoted && close.is_none() {
                close = Some(pos);
            } else {
                return None;
            }
            continue;
        }
        *result.get_mut(column)? = if quoted {
            if close != pos.checked_sub(1) || pos - start < 2 {
                return None;
            }
            &line[start + 1..pos - 1]
        } else {
            &line[start..pos]
        };
        column += 1;
        start = pos + 1;
        quoted = false;
        close = None;
    }
    (column == N).then_some(result)
}

// Nine ASCII bytes fit in 63 bits. A length bit keeps prefixes and embedded
// zeroes distinct. IDs outside that representation use their original bytes.
#[inline]
fn packed_id(bytes: &[u8]) -> Option<u64> {
    if bytes.len() > 9 {
        return None;
    }
    let mut key = 1;
    for &byte in bytes {
        if !byte.is_ascii() {
            return None;
        }
        key = (key << 7) | u64::from(byte);
    }
    Some(key)
}

struct Candidate<K, T> {
    id: K,
    ingest: i64,
    value: Option<T>,
}

type CandidatePartitions<K, T> = Vec<Vec<Candidate<K, T>>>;

pub fn unique_rows<'a, T, F, const N: usize>(
    files: &'a Files,
    ingest_col: Option<usize>,
    visit: F,
) -> Vec<Vec<T>>
where
    T: Send,
    F: Fn([&'a [u8]; N], i64) -> Option<T> + Send + Sync,
{
    let chunks = files.chunks();
    let chunks_per_task = chunks
        .len()
        .div_ceil(rayon::current_num_threads() * 3)
        .max(1);
    let batches: Vec<_> = chunks
        .par_chunks(chunks_per_task)
        .map(|batch| {
            let mut packed: CandidatePartitions<u64, T> = empty_partitions();
            let mut wide: CandidatePartitions<&[u8], T> = empty_partitions();
            for line in batch.iter().flat_map(|chunk| lines(chunk)) {
                let Some(row) = fields::<N>(line) else {
                    continue;
                };
                let id = row[0];
                if id.is_empty() {
                    continue;
                }
                let ingest = match ingest_col {
                    Some(col) => match timestamp(row[col]) {
                        Some(time) => time,
                        None => continue,
                    },
                    None => 0,
                };
                // Preserve invalid earlier copies in the minimum-ingest reduction.
                let value = visit(row, ingest);
                if let Some(id) = packed_id(id) {
                    let bucket = id_partition(id);
                    packed[bucket].push(Candidate { id, ingest, value });
                } else {
                    let bucket = id_partition(id);
                    wide[bucket].push(Candidate { id, ingest, value });
                }
            }
            (packed, wide)
        })
        .collect();

    let (packed, wide): (Vec<_>, Vec<_>) = batches.into_iter().unzip();
    let (mut packed, wide) = rayon::join(|| merge_batches(packed), || merge_batches(wide));
    packed.extend(wide);
    packed
}

fn empty_partitions<T>() -> Vec<Vec<T>> {
    (0..PARTITIONS).map(|_| Vec::new()).collect()
}

#[inline]
fn id_partition<K: Hash>(id: K) -> usize {
    (FxBuildHasher.hash_one(id) >> 32) as usize & (PARTITIONS - 1)
}

fn merge_batches<K: Hash + Eq + Send, T: Send>(
    batches: Vec<CandidatePartitions<K, T>>,
) -> Vec<Vec<T>> {
    let mut partitions: Vec<_> = (0..PARTITIONS)
        .map(|_| Vec::with_capacity(batches.len()))
        .collect();
    for batch in batches {
        for (partition, candidates) in partitions.iter_mut().zip(batch) {
            if !candidates.is_empty() {
                partition.push(candidates);
            }
        }
    }
    partitions
        .into_par_iter()
        .filter(|partition| !partition.is_empty())
        .map(deduplicate_partition)
        .collect()
}

fn deduplicate_partition<K: Hash + Eq, T>(mut parts: CandidatePartitions<K, T>) -> Vec<T> {
    let count: usize = parts.iter().map(Vec::len).sum();
    // Parts own every candidate. The probe table only points to the winning record.
    let slots = (count + count.div_ceil(3)).next_power_of_two();
    let mut table: Vec<Option<&mut Candidate<K, T>>> = (0..slots).map(|_| None).collect();
    for candidate in parts.iter_mut().flatten() {
        let mut index = FxBuildHasher.hash_one(&candidate.id) as usize & (slots - 1);
        loop {
            let slot = &mut table[index];
            match slot.as_deref() {
                None => *slot = Some(candidate),
                Some(winner) if winner.id == candidate.id => {
                    if candidate.ingest < winner.ingest {
                        *slot = Some(candidate);
                    }
                }
                Some(_) => {
                    index = (index + 1) & (slots - 1);
                    continue;
                }
            }
            break;
        }
    }
    table
        .into_iter()
        .flatten()
        .filter_map(|winner| winner.value.take())
        .collect()
}

pub fn number(bytes: &[u8], maximum: i64) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let mut value: i64 = 0;
    if bytes.len() <= 18 {
        for &byte in bytes {
            let digit = byte.wrapping_sub(b'0');
            if digit > 9 {
                return None;
            }
            value = value * 10 + i64::from(digit);
        }
    } else {
        // Long digit strings can still fit when they have leading zeroes.
        for &byte in bytes {
            let digit = byte.wrapping_sub(b'0');
            if digit > 9 {
                return None;
            }
            value = value.checked_mul(10)?.checked_add(i64::from(digit))?;
        }
    }
    (value <= maximum).then_some(value)
}

#[inline]
fn two_digits(a: u8, b: u8) -> Option<i64> {
    let a = a.wrapping_sub(b'0');
    let b = b.wrapping_sub(b'0');
    (a <= 9 && b <= 9).then_some(i64::from(a) * 10 + i64::from(b))
}

pub fn timestamp(bytes: &[u8]) -> Option<i64> {
    if bytes.get(10) != Some(&b'T') {
        return number(bytes, i64::MAX);
    }
    let offset = match bytes {
        [.., b'Z'] if bytes.len() == 20 => 0,
        [.., sign @ (b'+' | b'-'), a, b, b':', c, d] if bytes.len() == 25 => {
            let hours = two_digits(*a, *b)?;
            let minutes = two_digits(*c, *d)?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            let direction = if *sign == b'+' { 1 } else { -1 };
            direction * (hours * 60 + minutes) * 60_000
        }
        _ => return None,
    };
    if bytes[4] != b'-' || bytes[7] != b'-' || bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let year = (two_digits(bytes[0], bytes[1])? * 100 + two_digits(bytes[2], bytes[3])?) as i32;
    let month = two_digits(bytes[5], bytes[6])? as u32;
    let day = two_digits(bytes[8], bytes[9])? as u32;
    let hours = two_digits(bytes[11], bytes[12])?;
    let minutes = two_digits(bytes[14], bytes[15])?;
    let seconds = two_digits(bytes[17], bytes[18])?;
    if hours > 23 || minutes > 59 || seconds > 59 {
        return None;
    }
    let days = NaiveDate::from_ymd_opt(year, month, day)?.to_epoch_days();
    Some(i64::from(days) * DAY + (hours * 3600 + minutes * 60 + seconds) * 1000 - offset)
}

#[cfg(test)]
mod tests {
    use super::packed_id;

    #[test]
    fn packed_ids_keep_length_case_and_zero_bytes() {
        let keys = [
            b"abc".as_slice(),
            b"abc\0",
            b"\0abc",
            b"Abc",
            b"123456780",
            b"123456781",
        ];
        let packed: std::collections::HashSet<_> =
            keys.iter().map(|key| packed_id(key).unwrap()).collect();
        assert_eq!(packed.len(), keys.len());
        assert_eq!(packed_id(b"1234567890"), None);
        assert_eq!(packed_id("caf\u{e9}".as_bytes()), None);
    }
}
