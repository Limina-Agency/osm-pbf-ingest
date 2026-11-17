//! Time `prepare_ingest_index` over a real PBF and report what it read.
//!
//! Usage: `cargo run --release -p osm-pbf-ingest --example bench_pass1 -- <file.osm.pbf>`

use osm_pbf_ingest::{IngestBuildProfile, prepare_ingest_index};
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::args().nth(1).map(PathBuf::from) else {
        eprintln!("usage: bench_pass1 <file.osm.pbf>");
        std::process::exit(2);
    };
    let temp_dir = std::env::temp_dir().join("osm-pbf-ingest-bench-pass1");
    std::fs::create_dir_all(&temp_dir)?;

    let started = Instant::now();
    let prepared = prepare_ingest_index(
        &path,
        IngestBuildProfile::DesktopFull,
        &temp_dir,
        |_progress| {},
        || false,
    )?;
    let wall = started.elapsed();

    let metrics = &prepared.metrics;
    let total = metrics.index_build_ms.max(1);
    println!("wall               {:?}", wall);
    println!("blocks_read        {}", metrics.blocks_read);
    println!("nodes indexed      {}", metrics.indexed_node_objects);
    println!("relations indexed  {}", metrics.indexed_relation_objects);
    println!();
    println!("index_build_ms     {:>7}", metrics.index_build_ms);
    println!(
        "  stream           {:>7} ms  {:>5.1}%",
        metrics.stream_wall_ms,
        pct(metrics.stream_wall_ms, total)
    );

    println!(
        "    relations      {:>7} ms  {:>5.1}%",
        metrics.collector_relation_ms,
        pct(metrics.collector_relation_ms, total)
    );
    println!(
        "    idle           {:>7} ms  {:>5.1}%   (starved => bottleneck is upstream)",
        metrics.collector_idle_ms,
        pct(metrics.collector_idle_ms, total)
    );
    println!(
        "  node finalize    {:>7} ms  {:>5.1}%   (sort + dedup + write + mmap)",
        metrics.node_store_finalize_ms,
        pct(metrics.node_store_finalize_ms, total)
    );
    println!(
        "  relation finalize{:>7} ms  {:>5.1}%",
        metrics.relation_finalize_ms,
        pct(metrics.relation_finalize_ms, total)
    );
    println!();
    println!(
        "reader thread      {:>7} ms   (elapsed, not CPU)",
        metrics.raw_block_read_ms
    );
    println!(
        "decode             {:>7} ms   (summed across workers)",
        metrics.decode_ms
    );
    println!(
        "node insert        {:>7} ms   (summed across workers)",
        metrics.node_insert_ms
    );

    // Reopening validates the manifest against the shards actually on disk, so
    // a successful open of a real store is an integrity check over every record.
    let store_dir = temp_dir.join("node_store");
    if store_dir.join("manifest.json").exists() {
        let reopen_started = Instant::now();
        let reopened = osm_pbf_ingest::node_store::NodeLocationStore::open_disk(&store_dir)?;
        drop(reopened);
        // `open` cross-checks the manifest's record count against the bytes
        // actually present in all 64 shards, so a clean open accounts for every
        // record. Lookup equivalence after a reopen is covered by the unit
        // tests, which can use ids they chose; sampling blind here would mostly
        // probe ids OSM never issued.
        println!(
            "\nreopen             {:>7?}  ({} records validated against the manifest)",
            reopen_started.elapsed(),
            metrics.indexed_node_objects
        );
    }

    Ok(())
}

fn pct(part: u64, whole: u64) -> f64 {
    part as f64 * 100.0 / whole as f64
}
