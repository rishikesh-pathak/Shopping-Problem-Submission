//! Parallel parser, session engine, and Parquet writer for the submission.
#[path = "../../csv_input.rs"]
mod csv_input;
mod id;
mod output;
mod rows;

use ahash::RandomState;
use csv_input::{digits, input_files, stamp};
use id::{HybridId, Identifier, WideIdentifier};
use memchr::memchr;
use memmap2::{Mmap, MmapOptions};
use rayon::prelude::*;
use rows::for_rows;
use std::collections::{hash_map::Entry, HashMap};
use std::error::Error;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};
use std::{env, fmt, io};

const PARTITIONS: usize = 128;
const CHUNK_BYTES: usize = 2 * 1024 * 1024;
const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const SESSION_GAP: i64 = 1_800_000;
const MAX_DIMENSION_BYTES: u64 = 64 * 1024 * 1024;
type Map<K, V> = HashMap<K, V, RandomState>;
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

fn profile(enabled: bool) -> impl FnMut(&str) {
    let mut last = Instant::now();
    move |label| {
        if enabled {
            eprintln!("[PROFILE] {label}: {:.3}s", last.elapsed().as_secs_f64());
            last = Instant::now();
        }
    }
}

#[derive(Debug)]
struct UseSql(&'static str);

impl fmt::Display for UseSql {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl Error for UseSql {}

fn env_path(var: &str, default: &str) -> PathBuf {
    env::var_os(var).map_or_else(|| default.into(), PathBuf::from)
}

fn env_usize(var: &str, default: usize) -> usize {
    env::var(var)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn data_files(dir: &Path) -> io::Result<Vec<Mmap>> {
    input_files(dir)?
        .iter()
        .filter_map(|path| {
            let map = || -> io::Result<Option<Mmap>> {
                let file = File::open(path)?;
                if file.metadata()?.len() == 0 {
                    return Ok(None);
                }
                // The input is mounted read-only by the runner and must stay immutable
                // while it is being processed.
                Ok(Some(unsafe { MmapOptions::new().map(&file)? }))
            };
            map().transpose()
        })
        .collect()
}

fn chunks(files: &[Mmap]) -> Vec<&[u8]> {
    let mut chunks = Vec::new();
    for file in files {
        let Some(header) = memchr(b'\n', file) else {
            continue;
        };
        let mut rest = &file[header + 1..];
        while rest.len() > CHUNK_BYTES {
            let Some(offset) = memchr(b'\n', &rest[CHUNK_BYTES..]) else {
                break;
            };
            let (chunk, tail) = rest.split_at(CHUNK_BYTES + offset + 1);
            chunks.push(chunk);
            rest = tail;
        }
        if !rest.is_empty() {
            chunks.push(rest);
        }
    }
    chunks
}

fn read_dimension(path: &Path) -> Result<String> {
    if fs::metadata(path)?.len() > MAX_DIMENSION_BYTES {
        return Err(UseSql("dimension exceeds native memory budget").into());
    }
    Ok(fs::read_to_string(path)?)
}

fn dimension<'a>(csv: &'a str, expected_header: &str) -> Result<Map<&'a str, &'a str>> {
    let mut lines = csv.split_terminator('\n');
    if lines.next() != Some(expected_header) {
        return Err(UseSql("dimension header requires the SQL CSV reader").into());
    }
    let mut out = Map::default();
    for line in lines {
        let Some((key, value)) = line.split_once(',') else {
            return Err(UseSql("dimension row requires the SQL CSV reader").into());
        };
        if key.is_empty()
            || value.is_empty()
            || value.contains(',')
            || line.contains(['"', '\r', '\0'])
            || out.insert(key, value).is_some()
        {
            return Err(UseSql("dimension row requires the SQL CSV reader").into());
        }
    }
    Ok(out)
}

struct Links<K> {
    index: Map<K, usize>,
    names: Vec<K>,
    roots: Vec<usize>,
}

impl<K: Copy + Eq + std::hash::Hash> Links<K> {
    fn add(&mut self, name: K) -> usize {
        *self.index.entry(name).or_insert_with(|| {
            let index = self.names.len();
            self.names.push(name);
            self.roots.push(index);
            index
        })
    }

    fn root(&mut self, mut index: usize) -> usize {
        while self.roots[index] != index {
            self.roots[index] = self.roots[self.roots[index]];
            index = self.roots[index];
        }
        index
    }

    fn read<'a>(files: &'a [Mmap]) -> Result<Map<K, K>>
    where
        K: Identifier<'a>,
    {
        let mut links = Self {
            index: Map::default(),
            names: Vec::new(),
            roots: Vec::new(),
        };
        for file in files {
            let Some(header) = memchr(b'\n', file) else {
                continue;
            };
            for_rows(&file[header + 1..], |[left, right]| {
                if left.is_empty() || right.is_empty() {
                    return Ok(());
                }
                let left = links.add(K::encode(left)?);
                let right = links.add(K::encode(right)?);
                let left = links.root(left);
                let right = links.root(right);
                if links.names[left] < links.names[right] {
                    links.roots[right] = left;
                } else {
                    links.roots[left] = right;
                }
                Ok(())
            })?;
        }
        for i in 0..links.names.len() {
            links.roots[i] = links.root(i);
        }
        Ok(links
            .index
            .into_iter()
            .map(|(name, i)| (name, links.names[links.roots[i]]))
            .collect())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Event<K> {
    user: K,
    ms: i64,
    price: u32,
    category: u32,
}

struct EventCandidate<K> {
    id: K,
    hash: u64,
    ingest: i64,
    event: Event<K>,
}

#[derive(Default)]
struct EventTimings {
    deduplication: Duration,
    identity_lookups: Duration,
}

struct Click<K> {
    id: K,
    user: K,
    ms: i64,
    campaign: u32,
}

struct Input<'a, K> {
    identity: Map<K, K>,
    products: Map<&'a str, u32>,
    campaigns: Map<&'a str, u32>,
    start: i64,
    end: i64,
    unknown: u32,
    hash: RandomState,
}

impl<'a, K: Identifier<'a>> Input<'a, K> {
    fn bucket(&self, id: K) -> usize {
        self.hash.hash_one(id) as usize & (PARTITIONS - 1)
    }

    fn person(&self, user: K) -> K {
        self.identity.get(&user).copied().unwrap_or(user)
    }

    fn events(&self, chunk: &'a [u8]) -> Result<Vec<Vec<EventCandidate<K>>>> {
        let mut lanes = buckets();
        for_rows(chunk, |[id, user, time, ingest, kind, product, price]| {
            if id.is_empty() {
                return Ok(());
            }
            let ingest = stamp(ingest).ok_or(UseSql("invalid ingest timestamp"))?;
            let mut event = Event {
                user: K::EMPTY,
                ms: 0,
                price: 0,
                category: self.unknown,
            };
            if let Some(ms) = stamp(time) {
                if ms >= self.start
                    && ms < self.end
                    && !user.is_empty()
                    && !product.is_empty()
                    && ingest <= ms.saturating_add(DAY)
                {
                    match kind {
                        "cart" | "view" => {
                            event = Event {
                                user: K::encode(user)?,
                                ms,
                                ..event
                            }
                        }
                        "purchase" => {
                            if let Some(price) =
                                digits(price.as_bytes()).filter(|p| (1..=10_000_000).contains(p))
                            {
                                let category =
                                    self.products.get(product).copied().unwrap_or(self.unknown);
                                event = Event {
                                    user: K::encode(user)?,
                                    ms,
                                    price: price as u32,
                                    category,
                                };
                            }
                        }
                        _ => {}
                    }
                }
            }
            // Invalid payloads are still candidates for earliest ingest.
            let id = K::encode(id)?;
            let hash = self.hash.hash_one(id);
            lanes[hash as usize & (PARTITIONS - 1)].push(EventCandidate { id, hash, ingest, event });
            Ok(())
        })?;
        Ok(lanes)
    }

    fn dedup_events(
        &self,
        chunks: Vec<Vec<EventCandidate<K>>>,
        mut timings: Option<&mut EventTimings>,
    ) -> Result<Vec<Vec<Event<K>>>> {
        let dedup_started = timings.as_ref().map(|_| Instant::now());
        let count = chunks.iter().map(Vec::len).sum();
        // Each lane shares the partition hash's low bits. Use a separate seed
        // so those bits don't also determine placement inside the dedup table.
        // Events arrive with the same hash that determined their partition.
        // Reuse it for dedup probes instead of hashing wide strings twice.
        let mut earliest: hashbrown::HashMap<K, (i64, Event<K>), RandomState> =
            hashbrown::HashMap::with_capacity_and_hasher(count, self.hash.clone());
        for EventCandidate { id, hash, ingest, event } in chunks.into_iter().flatten() {
            match earliest.raw_entry_mut().from_hash(hash, |key| *key == id) {
                hashbrown::hash_map::RawEntryMut::Vacant(slot) => {
                    slot.insert_hashed_nocheck(hash, id, (ingest, event));
                }
                hashbrown::hash_map::RawEntryMut::Occupied(mut slot) => {
                    let (previous, previous_event) = slot.get();
                    if ingest == *previous && event != *previous_event {
                        return Err(UseSql("event ID and ingest timestamp tie").into());
                    }
                    if ingest < *previous {
                        slot.insert((ingest, event));
                    }
                }
            }
        }
        if let (Some(timing), Some(started)) = (&mut timings, dedup_started) {
            timing.deduplication = started.elapsed();
        }
        let routing_started = timings.as_ref().map(|_| Instant::now());
        let mut lanes = buckets();
        for (_, event) in earliest.into_values() {
            if event.user != K::EMPTY {
                let user = self.person(event.user);
                lanes[self.bucket(user)].push(Event { user, ..event });
            }
        }
        if let (Some(timing), Some(started)) = (timings, routing_started) {
            timing.identity_lookups = started.elapsed();
        }
        Ok(lanes)
    }

    fn clicks(&self, chunk: &'a [u8]) -> Result<Vec<Vec<Click<K>>>> {
        let mut lanes = buckets();
        for_rows(chunk, |[id, user, campaign, time]| {
            if id.is_empty() {
                return Ok(());
            }
            let valid = stamp(time)
                .filter(|ms| *ms >= self.start.saturating_sub(WEEK) && *ms < self.end)
                .zip(self.campaigns.get(campaign).copied())
                .filter(|_| !user.is_empty());
            // All copies of a click have identical values. Invalid copies can
            // be dropped here, and valid copies land in the same person lane.
            if let Some((ms, campaign)) = valid {
                let user = self.person(K::encode(user)?);
                lanes[self.bucket(user)].push(Click {
                    id: K::encode(id)?,
                    user,
                    ms,
                    campaign,
                });
            }
            Ok(())
        })?;
        Ok(lanes)
    }
}

fn buckets<T>() -> Vec<Vec<T>> {
    (0..PARTITIONS).map(|_| Vec::new()).collect()
}

fn transpose<T>(chunks: Vec<Vec<Vec<T>>>) -> Vec<Vec<Vec<T>>> {
    let mut lanes = buckets();
    for chunk in chunks {
        for (lane, fragment) in lanes.iter_mut().zip(chunk) {
            if !fragment.is_empty() {
                lane.push(fragment);
            }
        }
    }
    lanes
}

fn flatten<T>(chunks: Vec<Vec<T>>) -> Vec<T> {
    let mut rows = Vec::with_capacity(chunks.iter().map(Vec::len).sum());
    for mut chunk in chunks {
        rows.append(&mut chunk);
    }
    rows
}

#[derive(Default)]
struct Totals {
    sessions: i64,
    purchases: i64,
    revenue: i64,
    assists: i64,
}

struct UserDay<K> {
    user: K,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    category: Option<u32>,
}

struct CampaignDay {
    campaign: u32,
    day: i32,
    totals: Totals,
}

type CampaignTotals = Map<(i32, u32), Totals>;

#[derive(Default)]
struct WorkerTimings {
    sorting: Duration,
    aggregation: Duration,
    sessionization: Duration,
    attribution: Duration,
    user_parquet: Duration,
}

fn sort_clicks<K: Ord>(clicks: &mut Vec<Click<K>>) {
    clicks.sort_unstable_by(|a, b| {
        a.user
            .cmp(&b.user)
            .then_with(|| a.ms.cmp(&b.ms))
            .then_with(|| b.id.cmp(&a.id))
    });
    // The ordering needed for attribution also makes identical copies adjacent.
    clicks.dedup_by(|a, b| a.id == b.id);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoted_copies_collapse_without_changing_click_precedence() {
        let input = Input::<&str> {
            identity: Map::default(),
            products: Map::default(),
            campaigns: [("c1", 1), ("c2", 2)].into_iter().collect(),
            start: 0,
            end: 2_000,
            unknown: 0,
            hash: RandomState::new(),
        };
        let a = input
            .clicks(b"k1,u,c1,1000\nk2,u,c2,1000\nk3,click-only,c1,1000\n")
            .unwrap();
        let b = input.clicks(b"\"k1\",\"u\",\"c1\",\"1000\"\n").unwrap();
        let mut clicks = flatten(a.into_iter().chain(b).collect());
        sort_clicks(&mut clicks);
        let ids: Vec<_> = clicks
            .iter()
            .filter(|row| row.user == "u")
            .map(|row| row.id)
            .collect();
        assert_eq!(ids, ["k2", "k1"]);
        assert_eq!(clicks.last().unwrap().campaign, 1);
    }
}

fn add_user_day<K>(
    day: &mut Option<UserDay<K>>,
    categories: &mut Map<u32, i64>,
    days: &mut Vec<UserDay<K>>,
) {
    if let Some(mut row) = day.take() {
        // Category indexes were assigned in byte order.
        row.category = categories
            .drain()
            .min_by_key(|&(category, revenue)| (-revenue, category))
            .map(|(category, _)| category);
        days.push(row);
    }
}

fn aggregate<K: Copy + Ord>(
    mut events: Vec<Event<K>>,
    mut clicks: Vec<Click<K>>,
    organic: u32,
    days: &mut Vec<UserDay<K>>,
    campaign_days: &mut CampaignTotals,
    detail: bool,
    mut timings: Option<&mut WorkerTimings>,
) {
    let sorting_started = timings.as_ref().map(|_| Instant::now());
    events.sort_unstable_by_key(|event| (event.user, event.ms));
    sort_clicks(&mut clicks);

    if let (Some(timing), Some(started)) = (&mut timings, sorting_started) {
        timing.sorting += started.elapsed();
    }
    let aggregation_started = timings.as_ref().map(|_| Instant::now());
    let mut clicks = clicks.as_slice();
    let mut active = Map::<u32, usize>::default();
    let mut categories = Map::<u32, i64>::default();
    for mut person in events.chunk_by(|a, b| a.user == b.user) {
        let user = person[0].user;
        let preceding = clicks.iter().take_while(|click| click.user < user).count();
        clicks = &clicks[preceding..];
        let n = clicks.iter().take_while(|click| click.user == user).count();
        let (touches, rest) = clicks.split_at(n);
        clicks = rest;
        active.clear();
        let (mut left, mut right) = (0, 0);
        let mut day = None;
        while let Some(first) = person.first() {
            let session_started = detail.then(Instant::now);
            let ms = first.ms;
            let date = ms.div_euclid(DAY) as i32;
            if day.as_ref().is_some_and(|row: &UserDay<K>| row.day != date) {
                add_user_day(&mut day, &mut categories, days);
            }
            let row = day.get_or_insert(UserDay {
                user,
                day: date,
                sessions: 0,
                events: 0,
                revenue: 0,
                category: None,
            });

            let mut last_ms = ms;
            let mut revenue = 0;
            let mut count = 0;
            for event in person {
                if event.ms - last_ms > SESSION_GAP {
                    break;
                }
                last_ms = event.ms;
                count += 1;
                if event.price != 0 {
                    let price = i64::from(event.price);
                    revenue += price;
                    *categories.entry(event.category).or_default() += price;
                }
            }
            person = &person[count..];
            row.sessions += 1;
            row.events += count as i64;
            row.revenue += revenue;

            if let (Some(timing), Some(started)) = (&mut timings, session_started) {
                timing.sessionization += started.elapsed();
            }
            let attribution_started = detail.then(Instant::now);
            let cutoff = ms.saturating_sub(WEEK);
            while left < right && touches[left].ms < cutoff {
                if let Entry::Occupied(mut count) = active.entry(touches[left].campaign) {
                    *count.get_mut() -= 1;
                    if *count.get() == 0 {
                        count.remove();
                    }
                }
                left += 1;
            }
            while right < touches.len() && touches[right].ms <= ms {
                if touches[right].ms < cutoff {
                    left += 1;
                } else {
                    *active.entry(touches[right].campaign).or_default() += 1;
                }
                right += 1;
            }
            let credited = if left < right {
                touches[right - 1].campaign
            } else {
                organic
            };
            let totals = campaign_days.entry((date, credited)).or_default();
            totals.sessions += 1;
            totals.purchases += i64::from(revenue != 0);
            totals.revenue += revenue;
            for &campaign in active.keys() {
                campaign_days.entry((date, campaign)).or_default().assists += 1;
            }
            if let (Some(timing), Some(started)) = (&mut timings, attribution_started) {
                timing.attribution += started.elapsed();
            }
        }
        add_user_day(&mut day, &mut categories, days);
    }
    if let (Some(timing), Some(started)) = (timings, aggregation_started) {
        timing.aggregation += started.elapsed();
    }
}

fn run() -> Result<()> {
    let profiling = env::var("SHOPPING_PROFILE").as_deref() == Ok("1");
    let mut lap = profile(profiling);
    let data = env_path("DATA_DIR", "/data");
    let out = env_path("OUTPUT_DIR", "/output");
    let threads = env_usize("NCPU", 8).max(1);
    rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build_global()?;
    let manifest: serde_json::Value =
        serde_json::from_reader(File::open(data.join("manifest.json"))?)?;
    let start = manifest["window_start"]
        .as_str()
        .and_then(stamp)
        .ok_or(UseSql("invalid start"))?;
    let end = manifest["window_end"]
        .as_str()
        .and_then(stamp)
        .ok_or(UseSql("invalid end"))?;
    if start > end
        || start.div_euclid(DAY) < i32::MIN.into()
        || end.div_euclid(DAY) > i32::MAX.into()
    {
        return Err(UseSql("manifest date range requires SQL").into());
    }

    let files = Dataset {
        links: data_files(&data.join("links"))?,
        events: data_files(&data.join("events"))?,
        clicks: data_files(&data.join("clicks"))?,
        products: read_dimension(&data.join("products.csv"))?,
        campaigns: read_dimension(&data.join("campaigns.csv"))?,
        start,
        end,
    };
    lap("open inputs and initialize workers");
    // HybridId handles packed short and arbitrary-width UTF-8 IDs without
    // discarding parsed work and restarting on the first long identifier.
    files.process::<HybridId<'_>>(&out, threads, profiling)
}

struct Dataset {
    links: Vec<Mmap>,
    events: Vec<Mmap>,
    clicks: Vec<Mmap>,
    products: String,
    campaigns: String,
    start: i64,
    end: i64,
}

impl Dataset {
    fn process<'a, K: Identifier<'a>>(
        &'a self,
        out: &Path,
        threads: usize,
        profiling: bool,
    ) -> Result<()> {
        let mut lap = profile(profiling);
        let detail = profiling && env::var("SHOPPING_PROFILE_DETAIL").as_deref() == Ok("1");
        let products = dimension(&self.products, "product_id,category")?;
        let campaign_dim = dimension(&self.campaigns, "campaign_id,channel")?;
        let mut categories: Vec<_> = products.values().copied().chain(["unknown"]).collect();
        categories.sort_unstable();
        categories.dedup();
        let category_ids: Map<_, _> = categories
            .iter()
            .enumerate()
            .map(|(i, &name)| (name, i as u32))
            .collect();
        let mut campaign_names: Vec<_> = campaign_dim
            .iter()
            .map(|(&name, &channel)| (name, channel))
            .collect();
        let campaigns: Map<_, _> = campaign_names
            .iter()
            .enumerate()
            .map(|(i, &(name, _))| (name, i as u32))
            .collect();
        let organic = campaign_names.len() as u32;
        campaign_names.push(("organic", "organic"));
        let organic = *campaigns.get("organic").unwrap_or(&organic);
        // Synthetic organic must not turn an otherwise unknown click campaign into a valid one.
        if let Some(campaign) = campaign_names.get_mut(organic as usize) {
            campaign.1 = "organic";
        }

        let memory_budget = env_usize("MEM_GB", 24) as u128 * 860_000_000;
        let bytes = [&self.links, &self.events, &self.clicks]
            .into_iter()
            .flatten()
            .map(|file| file.len() as u128)
            .sum::<u128>();
        if bytes >= memory_budget {
            return Err(UseSql("input exceeds native memory budget").into());
        }
        let event_chunks = chunks(&self.events);
        let click_chunks = chunks(&self.clicks);
        // Avoid a complete scan through all input streams to count newlines.
        // We obtain exact record counts from the parsed chunks.
        if bytes.saturating_mul(2).saturating_add(1 << 30) > memory_budget {
            return Err(UseSql("input exceeds native memory budget").into());
        }
        let identity = Links::<K>::read(&self.links)?;
        let input = Input {
            identity,
            products: products
                .into_iter()
                .map(|(id, category)| (id, category_ids[category]))
                .collect(),
            campaigns,
            start: self.start,
            end: self.end,
            unknown: category_ids["unknown"],
            hash: RandomState::new(),
        };
        lap("build identity map");
        let events: Vec<Vec<Vec<EventCandidate<K>>>> = event_chunks
            .par_iter()
            .map(|chunk| input.events(chunk))
            .collect::<Result<_>>()?;
        let event_rows: usize = events.iter().flat_map(|lanes| lanes.iter()).map(Vec::len).sum();
        lap("parse and partition events");
        let partitions = transpose(events)
            .into_par_iter()
            .map(|lane| {
                let mut timing = detail.then(EventTimings::default);
                let rows = input.dedup_events(lane, timing.as_mut())?;
                Ok((rows, timing))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut event_timings = EventTimings::default();
        let events = partitions
            .into_iter()
            .map(|(rows, timing)| {
                if let Some(timing) = timing {
                    event_timings.deduplication += timing.deduplication;
                    event_timings.identity_lookups += timing.identity_lookups;
                }
                rows
            })
            .collect();
        let events = transpose(events);
        lap("deduplicate and link events");
        if detail {
            eprintln!(
                "[PROFILE] Total partition time: deduplication {:.3}s, identity lookup and routing {:.3}s",
                event_timings.deduplication.as_secs_f64(),
                event_timings.identity_lookups.as_secs_f64()
            );
        }
        let clicks: Vec<Vec<Vec<Click<K>>>> = click_chunks
            .par_iter()
            .map(|chunk| input.clicks(chunk))
            .collect::<Result<_>>()?;
        let click_rows: usize = clicks.iter().flat_map(|lanes| lanes.iter()).map(Vec::len).sum();
        // Approximate the peak allocations of 56-byte candidates, their
        // hash-table entries, the 48-byte click records, identity maps, and
        // extra capacity. The old 175/130/192 coefficients unnecessarily sent
        // final-scale inputs into the slow SQL fallback.
        let estimated = bytes
            + event_rows as u128 * 155
            + click_rows as u128 * 95
            + input.identity.len() as u128 * 128
            + (1 << 30);
        if profiling {
            eprintln!("[PROFILE] memory: mapped={} event_rows={} click_rows={} identities={} estimate={} budget={}",
                bytes, event_rows, click_rows, input.identity.len(), estimated, memory_budget);
        }
        if estimated > memory_budget {
            return Err(UseSql("input exceeds native memory budget").into());
        }
        let clicks = transpose(clicks);
        lap("parse and link clicks");
        let users_dir = out.join("user_daily");
        let campaigns_dir = out.join("campaign_daily");
        fs::create_dir_all(&users_dir)?;
        fs::create_dir_all(&campaigns_dir)?;
        let workers = event_rows
            .div_ceil(250_000)
            .clamp(1, threads.min(PARTITIONS));
        let mut work: Vec<_> = (0..workers).map(|_| Vec::new()).collect();
        for (index, partitions) in events.into_iter().zip(clicks).enumerate() {
            work[index % workers].push(partitions);
        }
        let totals = work
            .into_par_iter()
            .enumerate()
            .map(|(worker, lanes)| {
                let mut days = Vec::new();
                let mut totals = Map::default();
                let mut timings = profiling.then(WorkerTimings::default);
                for (events, clicks) in lanes {
                    aggregate(
                        flatten(events),
                        flatten(clicks),
                        organic,
                        &mut days,
                        &mut totals,
                        detail,
                        timings.as_mut(),
                    );
                }
                let write_started = profiling.then(Instant::now);
                output::users(
                    &users_dir.join(format!("part-{worker}.parquet")),
                    &categories,
                    &days,
                )?;
                if let (Some(timing), Some(started)) = (&mut timings, write_started) {
                    timing.user_parquet = started.elapsed();
                }
                Ok((totals, timings))
            })
            .collect::<Result<Vec<_>>>()?;
        lap("sort, sessionize, attribute, and write user Parquet");
        let mut combined = CampaignTotals::default();
        let mut slowest = WorkerTimings::default();
        for (map, timings) in totals {
            if let Some(timing) = timings {
                slowest.sorting = slowest.sorting.max(timing.sorting);
                slowest.aggregation = slowest.aggregation.max(timing.aggregation);
                slowest.sessionization = slowest.sessionization.max(timing.sessionization);
                slowest.attribution = slowest.attribution.max(timing.attribution);
                slowest.user_parquet = slowest.user_parquet.max(timing.user_parquet);
            }
            for (key, totals) in map {
                let target = combined.entry(key).or_default();
                target.sessions += totals.sessions;
                target.purchases += totals.purchases;
                target.revenue += totals.revenue;
                target.assists += totals.assists;
            }
        }
        if profiling {
            // Worker phases overlap. These maxima are useful separately and do not sum.
            eprintln!(
                "[PROFILE] Max worker times: sort {:.3}s, aggregate {:.3}s, user Parquet {:.3}s",
                slowest.sorting.as_secs_f64(),
                slowest.aggregation.as_secs_f64(),
                slowest.user_parquet.as_secs_f64()
            );
        }
        if detail {
            eprintln!(
                "[PROFILE] Max worker times: sessionization {:.3}s, campaign attribution and assists {:.3}s",
                slowest.sessionization.as_secs_f64(),
                slowest.attribution.as_secs_f64()
            );
        }
        let rows: Vec<_> = combined
            .into_iter()
            .map(|((day, campaign), totals)| CampaignDay {
                campaign,
                day,
                totals,
            })
            .collect();
        output::campaigns(
            &campaigns_dir.join("part-0.parquet"),
            &campaign_names,
            &rows,
        )?;
        output::remove_unused_user_parts(&users_dir, workers)?;
        lap("merge campaign totals and finish outputs");
        Ok(())
    }
}

fn main() -> ExitCode {
    let began = std::time::Instant::now();
    match run() {
        Ok(()) => {
            eprintln!("Finished native in {:.3}s", began.elapsed().as_secs_f64());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Native pipeline: {error}");
            if error.is::<UseSql>() {
                ExitCode::from(75)
            } else {
                ExitCode::FAILURE
            }
        }
    }
}
