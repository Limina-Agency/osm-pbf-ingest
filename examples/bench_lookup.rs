//! Time pass 1, pass 2's decode, and resolving every way's node locations:
//! one by one, with a cursor, and against an area with `may_touch` (a box
//! around Zakopane, for the Małopolskie extract).
//!
//! Usage: `cargo run --release --example bench_lookup -- <file.osm.pbf> [ram|disk]`

use osm_pbf_ingest::node_store::NodeStoreStrategy;
use osm_pbf_ingest::{
    IngestBuildProfile, NodeBatch, Pass2Options, Pass2Sink, PrepareOptions, RelationBatch,
    WayBatch, prepare_ingest_index_with_options, run_pass2_with_options,
};
use rayon::prelude::*;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Default)]
struct Ways {
    nodes: Vec<Vec<i64>>,
}

impl Pass2Sink for Ways {
    fn on_relation_batch(&mut self, _: RelationBatch) -> Result<(), String> {
        Ok(())
    }
    fn on_way_batch(&mut self, batch: WayBatch) -> Result<(), String> {
        self.nodes
            .extend(batch.ways.into_iter().map(|way| way.node_ids));
        Ok(())
    }
    fn on_node_batch(&mut self, _: NodeBatch) -> Result<(), String> {
        Ok(())
    }
}

struct Nothing;
impl Pass2Sink for Nothing {
    fn on_relation_batch(&mut self, _: RelationBatch) -> Result<(), String> {
        Ok(())
    }
    fn on_way_batch(&mut self, _: WayBatch) -> Result<(), String> {
        Ok(())
    }
    fn on_node_batch(&mut self, _: NodeBatch) -> Result<(), String> {
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let path = PathBuf::from(
        args.next()
            .expect("usage: bench_lookup <file.osm.pbf> [ram|disk]"),
    );
    let mut options = PrepareOptions::new().worker_threads(4);
    match args.next().as_deref() {
        Some("ram") => options = options.node_store(NodeStoreStrategy::Ram),
        Some("disk") => options = options.node_store(NodeStoreStrategy::Disk),
        _ => {}
    }
    let temp_dir = std::env::temp_dir().join("osm-pbf-ingest-bench-lookup");
    std::fs::create_dir_all(&temp_dir)?;

    let started = Instant::now();
    let prepared = prepare_ingest_index_with_options(
        &path,
        IngestBuildProfile::DesktopFull,
        &temp_dir,
        options,
        |_| {},
        || false,
    )?;
    println!(
        "pass 1          {:>8.1?}  ({} nodes, finalize {} ms)",
        started.elapsed(),
        prepared.node_store.record_count(),
        prepared.metrics.node_store_finalize_ms
    );

    for (skip, none) in [(false, false), (true, false), (true, true)] {
        let started = Instant::now();
        let mut nothing = Nothing;
        let options = Pass2Options::new().worker_threads(4);
        let options = if skip { options.skip_nodes() } else { options };
        let options = if none {
            options.keep_ways(|_| false)
        } else {
            options
        };
        let summary = run_pass2_with_options(
            &path,
            IngestBuildProfile::DesktopFull,
            options,
            &mut nothing,
            |_| {},
            || false,
        )?;
        println!(
            "pass 2 null sink skip_nodes={skip} no ways={none} {:>6} ms  decode {} ms summed, read {} ms",
            started.elapsed().as_millis(),
            summary.decode_ms,
            summary.raw_block_read_ms
        );
    }
    let started = Instant::now();
    let mut ways = Ways::default();
    run_pass2_with_options(
        &path,
        IngestBuildProfile::DesktopFull,
        Pass2Options::new().worker_threads(4).skip_nodes(),
        &mut ways,
        |_| {},
        || false,
    )?;
    let refs: usize = ways.nodes.iter().map(Vec::len).sum();
    println!(
        "pass 2 ways     {:>8.1?}  ({} ways, {} node refs)",
        started.elapsed(),
        ways.nodes.len(),
        refs
    );

    let store = &prepared.node_store;
    if let osm_pbf_ingest::node_store::NodeLocationStore::Packed(packed) = store {
        println!(
            "packed store    {:.1} MiB, {:.2} bytes/node",
            packed.packed_bytes() as f64 / (1 << 20) as f64,
            packed.packed_bytes() as f64 / packed.record_count() as f64
        );
    }
    let started = Instant::now();
    let mut found = 0usize;
    for way in &ways.nodes {
        let mut cursor = store.cursor();
        found += way.iter().filter(|id| cursor.get(**id).is_some()).count();
    }
    let elapsed = started.elapsed();
    println!(
        "cursor serial   {:>8.1?} ({:.0} ns/ref) found {found}",
        elapsed,
        elapsed.as_nanos() as f64 / refs as f64
    );
    let area = [osm_pbf_ingest::node_store::DecimicroBox::around_degrees(
        49.2, 19.85, 49.37, 20.07,
    )];
    let started = Instant::now();
    let maybe = ways
        .nodes
        .iter()
        .filter(|way| store.may_touch(way, &area))
        .count();
    let elapsed = started.elapsed();
    let started = Instant::now();
    let exact = ways
        .nodes
        .iter()
        .filter(|way| {
            let mut cursor = store.cursor();
            let (mut s, mut w, mut n, mut e) = (f64::MAX, f64::MAX, f64::MIN, f64::MIN);
            for id in way.iter() {
                if let Some((lat, lng)) = cursor.get(*id) {
                    s = s.min(lat);
                    n = n.max(lat);
                    w = w.min(lng);
                    e = e.max(lng);
                }
            }
            s <= 49.37 && n >= 49.2 && w <= 20.07 && e >= 19.85
        })
        .count();
    println!(
        "may_touch       {:>8.1?} -> {maybe} ways; exact extents {:>8.1?} -> {exact} ways",
        elapsed,
        started.elapsed()
    );
    for round in 0..2 {
        let started = Instant::now();
        let mut found = 0usize;
        let mut sum = 0f64;
        for way in &ways.nodes {
            for id in way {
                if let Some((lat, lng)) = store.get(*id) {
                    found += 1;
                    sum += lat + lng;
                }
            }
        }
        let serial = started.elapsed();
        let started = Instant::now();
        let parallel_found: usize = ways
            .nodes
            .par_chunks(1024)
            .map(|chunk| {
                chunk
                    .iter()
                    .flatten()
                    .filter(|id| store.get(**id).is_some())
                    .count()
            })
            .sum();
        println!(
            "lookup round {round}  serial {:>8.1?} ({:.0} ns/ref)  parallel {:>8.1?}  found {found}/{parallel_found} checksum {sum:.3}",
            serial,
            serial.as_nanos() as f64 / refs as f64,
            started.elapsed(),
        );
    }
    Ok(())
}
