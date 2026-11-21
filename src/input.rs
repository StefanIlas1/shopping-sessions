use chrono::NaiveDate;
use memchr::memchr;
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
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
    let mut start = 0;
    let mut stops = memchr::memchr_iter(b',', line).chain(std::iter::once(line.len()));
    for field in &mut result {
        let stop = stops.next()?;
        let raw = &line[start..stop];
        *field = if raw.first() == Some(&b'"') {
            let value = raw.get(1..raw.len() - 1)?;
            if raw.last() != Some(&b'"') || memchr(b'"', value).is_some() {
                return None;
            }
            value
        } else if memchr(b'"', raw).is_some() {
            return None;
        } else {
            raw
        };
        start = stop + 1;
    }
    stops.next().is_none().then_some(result)
}

#[derive(Clone, Copy)]
struct Candidate<'a> {
    id: &'a [u8],
    ingest: &'a [u8],
    row: &'a [u8],
}

type CandidatePartitions<'a> = Vec<Vec<Candidate<'a>>>;

pub fn unique_rows<'a, T, F, const N: usize>(
    files: &'a Files,
    ingest_col: Option<usize>,
    visit: F,
) -> Vec<T>
where
    T: Send,
    F: Fn(&'a [u8], &'a [u8]) -> Option<T> + Send + Sync,
{
    let chunks = files.chunks();
    let chunks_per_task = chunks
        .len()
        .div_ceil(rayon::current_num_threads() * 3)
        .max(1);
    let batches: Vec<CandidatePartitions<'a>> = chunks
        .par_chunks(chunks_per_task)
        .map(|batch| {
            let mut local = vec![Vec::new(); PARTITIONS];
            for line in batch.iter().flat_map(|chunk| lines(chunk)) {
                let Some(row) = fields::<N>(line) else {
                    continue;
                };
                if row[0].is_empty() {
                    continue;
                }
                let mut hasher = FxHasher::default();
                row[0].hash(&mut hasher);
                let bucket = (hasher.finish() >> 32) as usize & (PARTITIONS - 1);
                local[bucket].push(Candidate {
                    id: row[0],
                    ingest: ingest_col.map_or(&[][..], |col| row[col]),
                    row: line,
                });
            }
            local
        })
        .collect();

    let mut partitions = vec![Vec::with_capacity(batches.len()); PARTITIONS];
    for batch in batches {
        for (partition, candidates) in partitions.iter_mut().zip(batch) {
            if !candidates.is_empty() {
                partition.push(candidates);
            }
        }
    }

    let groups: Vec<Vec<T>> = partitions
        .into_par_iter()
        .map(|parts| {
            // All equal IDs land in one partition; slice equality verifies hash collisions.
            let capacity = parts.iter().map(Vec::len).sum();
            let mut winners = FxHashMap::with_capacity_and_hasher(capacity, Default::default());
            for candidates in parts {
                for candidate in candidates {
                    let winner = winners.entry(candidate.id).or_insert(candidate);
                    // F1 ingest timestamps are fixed-width UTC, so byte order equals time order.
                    if candidate.ingest < winner.ingest {
                        *winner = candidate;
                    }
                }
            }
            winners
                .into_values()
                .filter_map(|winner| visit(winner.row, winner.id))
                .collect()
        })
        .collect();
    let mut result = Vec::with_capacity(groups.iter().map(Vec::len).sum());
    for mut group in groups {
        result.append(&mut group);
    }
    result
}

pub fn number(bytes: &[u8], maximum: i64) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let mut value: i64 = 0;
    for &byte in bytes {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(i64::from(digit))?;
        if value > maximum {
            return None;
        }
    }
    Some(value)
}

pub fn timestamp(bytes: &[u8]) -> Option<i64> {
    if !bytes.contains(&b'T') {
        return number(bytes, i64::MAX);
    }
    let offset = match bytes {
        [.., b'Z'] if bytes.len() == 20 => 0,
        [.., sign @ (b'+' | b'-'), a, b, b':', c, d] if bytes.len() == 25 => {
            let hours = number(&[*a, *b], 23)?;
            let minutes = number(&[*c, *d], 59)?;
            (if *sign == b'+' { 1 } else { -1 }) * (hours * 60 + minutes) * 60_000
        }
        _ => return None,
    };
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return None;
    }
    let year = number(&bytes[..4], 9999)? as i32;
    let month = number(&bytes[5..7], 12)? as u32;
    let day = number(&bytes[8..10], 31)? as u32;
    let hours = number(&bytes[11..13], 23)?;
    let minutes = number(&bytes[14..16], 59)?;
    let seconds = number(&bytes[17..19], 59)?;
    let days = NaiveDate::from_ymd_opt(year, month, day)?.to_epoch_days();
    Some(i64::from(days) * DAY + (hours * 3600 + minutes * 60 + seconds) * 1000 - offset)
}
