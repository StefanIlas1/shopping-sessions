use crate::input::{DAY, Files, WEEK, Window, fields, number, timestamp, unique_rows};
use rayon::prelude::*;
use rustc_hash::FxHashMap;

const SESSION_GAP: i64 = 1_800_000;
const ORGANIC: u32 = 0;
const NO_PURCHASE: u32 = u32::MAX;

pub struct Dimensions<'a> {
    people: FxHashMap<&'a [u8], &'a [u8]>,
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
        self.people.get(id).copied().unwrap_or(id)
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

fn link_people(files: &Files) -> FxHashMap<&[u8], &[u8]> {
    let mut ids = FxHashMap::default();
    let mut sets = DisjointSet::default();
    for [a, b] in files.rows().filter_map(fields::<2>) {
        if !a.is_empty() && !b.is_empty() {
            let a = *ids.entry(a).or_insert_with(|| sets.add());
            let b = *ids.entry(b).or_insert_with(|| sets.add());
            sets.join(a, b);
        }
    }

    let mut names = vec![None; ids.len()];
    for (&id, &index) in &ids {
        let name = &mut names[sets.root(index)];
        *name = Some(name.map_or(id, |old: &[u8]| old.min(id)));
    }
    ids.into_iter()
        .map(|(id, index)| (id, names[sets.root(index)].unwrap()))
        .collect()
}

pub struct Event<'a> {
    person: &'a [u8],
    time: i64,
    price: u32,
    category: u32,
}

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
    unique_rows::<_, _, 7>(files, Some(3), |row, _| {
        let [_, person, time, ingest, kind, product, price] = fields::<7>(row)?;
        if person.is_empty() || product.is_empty() {
            return None;
        }
        let time = timestamp(time)?;
        if !(window.start..window.end).contains(&time) || timestamp(ingest)? - time > DAY {
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
    })
}

pub fn load_clicks<'a>(
    files: &'a Files,
    dimensions: &Dimensions<'a>,
    window: &Window,
) -> Vec<Click<'a>> {
    unique_rows::<_, _, 4>(files, None, |row, id| {
        let [_, person, campaign, time] = fields::<4>(row)?;
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

pub fn sort_inputs(events: &mut [Event<'_>], clicks: &mut [Click<'_>]) {
    rayon::join(
        || events.par_sort_unstable_by(|a, b| a.person.cmp(b.person).then(a.time.cmp(&b.time))),
        || {
            clicks.par_sort_unstable_by(|a, b| {
                a.person
                    .cmp(b.person)
                    .then(a.time.cmp(&b.time))
                    .then_with(|| b.id.cmp(a.id))
            })
        },
    );
}

pub fn aggregate<'a>(
    events: &[Event<'a>],
    clicks: &[Click<'a>],
) -> (Vec<UserDay<'a>>, CampaignTotals) {
    let rows_per_task = (events.len() / (rayon::current_num_threads() * 8)).max(1024);
    let mut chunks = Vec::new();
    let mut begin = 0;
    while begin < events.len() {
        let nominal_end = (begin + rows_per_task).min(events.len());
        let last_person = events[nominal_end - 1].person;
        let end = nominal_end
            + events[nominal_end..].partition_point(|event| event.person == last_person);
        let first_person = events[begin].person;
        let click_begin = clicks.partition_point(|click| click.person < first_person);
        let click_end = clicks.partition_point(|click| click.person <= last_person);
        chunks.push((&events[begin..end], &clicks[click_begin..click_end]));
        begin = end;
    }

    let mut all_users = Vec::new();
    let mut all_campaigns = CampaignTotals::default();
    let parts: Vec<_> = chunks
        .into_par_iter()
        .map(|(events, clicks)| aggregate_chunk(events, clicks))
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

fn aggregate_chunk<'a>(
    events: &[Event<'a>],
    mut clicks: &[Click<'a>],
) -> (Vec<UserDay<'a>>, CampaignTotals) {
    let mut users = Vec::new();
    let mut campaigns = CampaignTotals::default();
    let mut sessions = Vec::new();
    let mut categories = FxHashMap::default();
    let mut intervals = FxHashMap::default();
    for person_events in events.chunk_by(|a, b| a.person == b.person) {
        let person = person_events[0].person;
        let skip = clicks.partition_point(|click| click.person < person);
        clicks = &clicks[skip..];
        let n = clicks.partition_point(|click| click.person == person);
        let (person_clicks, rest) = clicks.split_at(n);
        clicks = rest;

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
