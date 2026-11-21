use crate::input::{DAY, Files, WEEK, Window, fields, number, timestamp, unique_rows};
use rayon::prelude::*;
use rustc_hash::{FxBuildHasher, FxHashMap};
use std::hash::BuildHasher;

const SESSION_GAP: i64 = 1_800_000;
const ORGANIC: u32 = 0;
const NO_PURCHASE: u32 = u32::MAX;

pub struct Dimensions<'a> {
    people: People<'a>,
    products: FxHashMap<&'a [u8], u32>,
    unknown: u32,
    pub categories: Vec<&'a [u8]>,
    campaigns: FxHashMap<&'a [u8], u32>,
    pub campaign_labels: Vec<(&'a [u8], &'a [u8])>,
}

impl<'a> Dimensions<'a> {
    pub fn load(links: &'a Files, products: &'a Files, campaigns: &'a Files) -> Self {
        let people = link_people(links);
        let product_rows: Vec<_> = products.rows().filter_map(fields::<2>).collect();
        let mut categories: Vec<_> = product_rows.iter().map(|row| row[1]).collect();
        categories.push(b"unknown");
        categories.sort_unstable();
        categories.dedup();
        let unknown = categories.binary_search(&&b"unknown"[..]).unwrap() as u32;
        let products = product_rows
            .into_iter()
            .map(|[id, category]| (id, categories.binary_search(&category).unwrap() as u32))
            .collect();
        let mut campaign_labels = vec![(&b"organic"[..], &b"organic"[..])];
        let mut campaign_ids = FxHashMap::default();
        for [id, channel] in campaigns.rows().filter_map(fields::<2>) {
            campaign_ids.insert(id, campaign_labels.len() as u32);
            campaign_labels.push((id, channel));
        }
        Self {
            people,
            products,
            unknown,
            categories,
            campaigns: campaign_ids,
            campaign_labels,
        }
    }

    fn canonical(&self, id: &'a [u8]) -> &'a [u8] {
        self.people
            .aliases
            .get(id)
            .and_then(|&root| self.people.names[root])
            .unwrap_or(id)
    }
}

#[derive(Default)]
struct DisjointSet {
    parent: Vec<usize>,
    size: Vec<usize>,
}

impl DisjointSet {
    fn add(&mut self) -> usize {
        let id = self.parent.len();
        self.parent.push(id);
        self.size.push(1);
        id
    }

    fn root(&mut self, mut id: usize) -> usize {
        while self.parent[id] != id {
            self.parent[id] = self.parent[self.parent[id]];
            id = self.parent[id];
        }
        id
    }

    fn join(&mut self, a: usize, b: usize) {
        let (mut a, mut b) = (self.root(a), self.root(b));
        if a == b {
            return;
        }
        if self.size[a] < self.size[b] {
            std::mem::swap(&mut a, &mut b);
        }
        self.parent[b] = a;
        self.size[a] += self.size[b];
    }
}

struct People<'a> {
    aliases: FxHashMap<&'a [u8], usize>,
    names: Vec<Option<&'a [u8]>>,
}

fn link_people(files: &Files) -> People<'_> {
    // Estimate capacity from input bytes to reduce resizing on large link files.
    let mut aliases = FxHashMap::with_capacity_and_hasher(files.bytes() / 24, Default::default());
    let mut sets = DisjointSet::default();
    for [a, b] in files.rows().filter_map(fields::<2>) {
        if !a.is_empty() && !b.is_empty() {
            let a = *aliases.entry(a).or_insert_with(|| sets.add());
            let b = *aliases.entry(b).or_insert_with(|| sets.add());
            sets.join(a, b);
        }
    }

    let mut names = vec![None; aliases.len()];
    for (&id, index) in &mut aliases {
        *index = sets.root(*index);
        let name = &mut names[*index];
        *name = Some(name.map_or(id, |old: &[u8]| old.min(id)));
    }
    People { aliases, names }
}

#[derive(Clone, Copy)]
pub struct Event<'a> {
    person: &'a [u8],
    time: i64,
    price: u32,
    category: u32,
}

#[derive(Clone, Copy)]
pub struct Click<'a> {
    person: &'a [u8],
    id: &'a [u8],
    time: i64,
    campaign: u32,
}

pub fn load_events<'a>(
    files: &'a Files,
    dimensions: &Dimensions<'a>,
    window: &Window,
) -> Vec<Event<'a>> {
    unique_rows::<_, _, 7>(
        files,
        Some(3),
        |[_, person, time, _, kind, product, price], ingest| {
            if person.is_empty() || product.is_empty() {
                return None;
            }
            let time = timestamp(time)?;
            if !(window.start..window.end).contains(&time) || ingest - time > DAY {
                return None;
            }
            let (price, category) = match kind {
                b"view" | b"cart" => (0, NO_PURCHASE),
                b"purchase" => {
                    let price = number(price, 10_000_000)?;
                    if price == 0 {
                        return None;
                    }
                    (
                        price as u32,
                        dimensions
                            .products
                            .get(product)
                            .copied()
                            .unwrap_or(dimensions.unknown),
                    )
                }
                _ => return None,
            };
            Some(Event {
                person: dimensions.canonical(person),
                time,
                price,
                category,
            })
        },
    )
}

pub fn load_clicks<'a>(
    files: &'a Files,
    dimensions: &Dimensions<'a>,
    window: &Window,
) -> Vec<Click<'a>> {
    unique_rows::<_, _, 4>(files, None, |[id, person, campaign, time], _| {
        if person.is_empty() {
            return None;
        }
        let campaign = *dimensions.campaigns.get(campaign)?;
        let time = timestamp(time)?;
        if !(window.start - WEEK..window.end).contains(&time) {
            return None;
        }
        Some(Click {
            person: dimensions.canonical(person),
            id,
            time,
            campaign,
        })
    })
}

pub struct UserDay<'a> {
    pub person: &'a [u8],
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top_category: Option<u32>,
}

#[derive(Clone, Copy, Default)]
pub struct CampaignDay {
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assists: i64,
}

pub type CampaignTotals = FxHashMap<(u32, i32), CampaignDay>;

#[derive(Clone, Copy)]
struct Session {
    start: i64,
    day: i32,
    revenue: i64,
}

const PERSON_PARTITIONS: usize = 128;

fn by_person<T: Copy + Send + Sync>(
    rows: &[T],
    hash: impl Fn(&T) -> u64 + Sync,
) -> Vec<Vec<Vec<T>>> {
    let amount = rows.len().div_ceil(rayon::current_num_threads() * 3).max(1);
    let batches: Vec<_> = rows
        .par_chunks(amount)
        .map(|chunk| {
            let mut partitions = vec![Vec::new(); PERSON_PARTITIONS];
            for &row in chunk {
                let bucket = (hash(&row) >> 32) as usize & (PERSON_PARTITIONS - 1);
                partitions[bucket].push(row);
            }
            partitions
        })
        .collect();
    let mut partitions: Vec<_> = (0..PERSON_PARTITIONS)
        .map(|_| Vec::with_capacity(batches.len()))
        .collect();
    for batch in batches {
        for (partition, rows) in partitions.iter_mut().zip(batch) {
            if !rows.is_empty() {
                partition.push(rows);
            }
        }
    }
    partitions
}

pub fn aggregate<'a>(
    events: Vec<Event<'a>>,
    clicks: Vec<Click<'a>>,
) -> (Vec<UserDay<'a>>, CampaignTotals) {
    let (events, clicks) = rayon::join(
        move || by_person(&events, |event| FxBuildHasher.hash_one(event.person)),
        move || by_person(&clicks, |click| FxBuildHasher.hash_one(click.person)),
    );
    let mut all_users = Vec::new();
    let mut all_campaigns = CampaignTotals::default();
    let parts: Vec<_> = events
        .into_par_iter()
        .zip(clicks)
        .map(|(events, clicks)| aggregate_partition(events, clicks))
        .collect();
    for (mut users, campaigns) in parts {
        all_users.append(&mut users);
        for (key, total) in campaigns {
            let merged = all_campaigns.entry(key).or_default();
            merged.sessions += total.sessions;
            merged.purchasing += total.purchasing;
            merged.revenue += total.revenue;
            merged.assists += total.assists;
        }
    }
    (all_users, all_campaigns)
}

#[derive(Default)]
struct Group {
    event_count: usize,
    click_count: usize,
    event_end: usize,
    click_end: usize,
}

fn aggregate_partition<'a>(
    event_batches: Vec<Vec<Event<'a>>>,
    click_batches: Vec<Vec<Click<'a>>>,
) -> (Vec<UserDay<'a>>, CampaignTotals) {
    let mut users = Vec::new();
    let mut campaigns = CampaignTotals::default();
    let Some(&sample) = event_batches.first().and_then(|rows| rows.first()) else {
        return (users, campaigns);
    };

    // Count each person's rows, then scatter into contiguous ranges. Only timestamps
    // within a person need sorting, and click-only people produce no output.
    let count = event_batches.iter().map(Vec::len).sum();
    let mut ids = FxHashMap::default();
    let mut groups = Vec::new();
    let mut event_indices = Vec::with_capacity(count);
    for event in event_batches.iter().flatten() {
        let index = *ids.entry(event.person).or_insert_with(|| {
            let index = groups.len();
            groups.push(Group::default());
            index
        });
        groups[index].event_count += 1;
        event_indices.push(index);
    }
    let mut click_indices = Vec::new();
    for click in click_batches.iter().flatten() {
        let index = ids.get(click.person).copied();
        if let Some(index) = index {
            groups[index].click_count += 1;
        }
        click_indices.push(index);
    }
    drop(ids);

    let (mut event_count, mut click_count) = (0, 0);
    for group in &mut groups {
        group.event_end = event_count;
        group.click_end = click_count;
        event_count += group.event_count;
        click_count += group.click_count;
    }

    let mut events = vec![sample; event_count];
    for (event, index) in event_batches.into_iter().flatten().zip(event_indices) {
        let group = &mut groups[index];
        events[group.event_end] = event;
        group.event_end += 1;
    }
    let mut clicks = vec![
        Click {
            person: b"",
            id: b"",
            time: 0,
            campaign: ORGANIC
        };
        click_count
    ];
    for (click, index) in click_batches.into_iter().flatten().zip(click_indices) {
        if let Some(index) = index {
            let group = &mut groups[index];
            clicks[group.click_end] = click;
            group.click_end += 1;
        }
    }

    let mut sessions = Vec::new();
    let mut categories = FxHashMap::default();
    let mut intervals = FxHashMap::default();
    for group in groups {
        let person_events = &mut events[group.event_end - group.event_count..group.event_end];
        let person_clicks = &mut clicks[group.click_end - group.click_count..group.click_end];
        person_events.sort_unstable_by_key(|event| event.time);
        person_clicks.sort_unstable_by(|a, b| a.time.cmp(&b.time).then_with(|| b.id.cmp(a.id)));

        sessionize(person_events, &mut users, &mut sessions, &mut categories);
        credit(&sessions, person_clicks, &mut campaigns);
        assists(&sessions, person_clicks, &mut intervals, &mut campaigns);
        sessions.clear();
    }
    (users, campaigns)
}

fn sessionize<'a>(
    mut events: &[Event<'a>],
    users: &mut Vec<UserDay<'a>>,
    sessions: &mut Vec<Session>,
    categories: &mut FxHashMap<u32, i64>,
) {
    let person = events[0].person;
    let mut day_stats = UserDay {
        person,
        day: utc_day(events[0].time),
        sessions: 0,
        events: 0,
        revenue: 0,
        top_category: None,
    };
    while let Some(first) = events.first() {
        let start = first.time;
        let day = utc_day(start);
        if day != day_stats.day {
            day_stats.top_category = top_category(categories);
            categories.clear();
            users.push(day_stats);
            day_stats = UserDay {
                person,
                day,
                sessions: 0,
                events: 0,
                revenue: 0,
                top_category: None,
            };
        }
        let mut previous = start;
        let mut end = 0;
        let mut revenue = 0;
        for event in events {
            if event.time - previous > SESSION_GAP {
                break;
            }
            previous = event.time;
            end += 1;
            if event.category != NO_PURCHASE {
                let cents = i64::from(event.price);
                revenue += cents;
                *categories.entry(event.category).or_default() += cents;
            }
        }
        day_stats.sessions += 1;
        day_stats.events += end as i64;
        day_stats.revenue += revenue;
        sessions.push(Session {
            start,
            day,
            revenue,
        });
        events = &events[end..];
    }
    day_stats.top_category = top_category(categories);
    categories.clear();
    users.push(day_stats);
}

fn top_category(categories: &FxHashMap<u32, i64>) -> Option<u32> {
    categories
        .iter()
        .max_by(|a, b| a.1.cmp(b.1).then_with(|| b.0.cmp(a.0)))
        .map(|(&id, _)| id)
}

fn utc_day(time: i64) -> i32 {
    time.div_euclid(DAY) as i32
}

fn credit(sessions: &[Session], clicks: &[Click<'_>], campaigns: &mut CampaignTotals) {
    let mut end: usize = 0;
    for session in sessions {
        while end < clicks.len() && clicks[end].time <= session.start {
            end += 1;
        }
        let campaign = end
            .checked_sub(1)
            .map(|index| &clicks[index])
            .filter(|click| click.time >= session.start - WEEK)
            .map_or(ORGANIC, |click| click.campaign);
        let stats = campaigns.entry((campaign, session.day)).or_default();
        stats.sessions += 1;
        stats.purchasing += i64::from(session.revenue > 0);
        stats.revenue += session.revenue;
    }
}

fn assists(
    sessions: &[Session],
    clicks: &[Click<'_>],
    intervals: &mut FxHashMap<u32, (i64, i64)>,
    campaigns: &mut CampaignTotals,
) {
    let first = sessions.first().unwrap().start;
    let last = sessions.last().unwrap().start;
    let begin = clicks.partition_point(|click| click.time < first - WEEK);
    for click in clicks[begin..]
        .iter()
        .take_while(|click| click.time <= last)
    {
        let interval = intervals
            .entry(click.campaign)
            .or_insert((click.time, click.time + WEEK));
        if click.time > interval.1 {
            count_interval(sessions, click.campaign, *interval, campaigns);
            interval.0 = click.time;
        }
        interval.1 = click.time + WEEK;
    }
    for (campaign, interval) in intervals.drain() {
        count_interval(sessions, campaign, interval, campaigns);
    }
}

fn count_interval(
    sessions: &[Session],
    campaign: u32,
    (begin, end): (i64, i64),
    campaigns: &mut CampaignTotals,
) {
    let lo = sessions.partition_point(|session| session.start < begin);
    let hi = sessions.partition_point(|session| session.start <= end);
    let mut remaining = &sessions[lo..hi];
    while let Some(first) = remaining.first() {
        let day = first.day;
        let count = remaining.partition_point(|session| session.day == day);
        campaigns.entry((campaign, day)).or_default().assists += count as i64;
        remaining = &remaining[count..];
    }
}
