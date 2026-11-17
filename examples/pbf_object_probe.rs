//! Probe [`PbfObjectReader`] against a real extract.
//!
//! The unit tests run on synthetic blocks a few objects wide. This checks the
//! same code against a Geofabrik extract — thousands of blobs, real dense-node
//! packing, real string tables — and reports what a lookup actually costs.
//!
//! Usage:
//!   cargo run --release -p osm-pbf-ingest --example pbf_object_probe -- \
//!       <path/to/osm.pbf> [oracle.json]
//!
//! The optional oracle is `[{"kind":"node"|"way"|"relation","id":<i64>,
//! "tags":{...}}, ...]`; every entry is looked up and its tags compared.

use osm_pbf_ingest::blob_index::ObjectKind;
use osm_pbf_ingest::object_lookup::PbfObjectReader;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Instant;

fn parse_kind(value: &str) -> Option<ObjectKind> {
    match value {
        "node" => Some(ObjectKind::Node),
        "way" => Some(ObjectKind::Way),
        "relation" => Some(ObjectKind::Relation),
        _ => None,
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let pbf = PathBuf::from(
        args.next()
            .expect("usage: pbf_object_probe <pbf> [oracle.json]"),
    );
    let oracle_path = args.next().map(PathBuf::from);

    let opened = Instant::now();
    let mut reader = PbfObjectReader::open(&pbf).expect("open pbf");
    let open_ms = opened.elapsed().as_secs_f64() * 1000.0;
    println!(
        "opened {} blobs in {open_ms:.0} ms (scan + classify)",
        reader.index().len()
    );

    let Some(oracle_path) = oracle_path else {
        return;
    };
    let raw = std::fs::read_to_string(&oracle_path).expect("read oracle");
    let cases: Vec<serde_json::Value> = serde_json::from_str(&raw).expect("parse oracle");

    let mut checked = 0usize;
    let mut missing = 0usize;
    let mut mismatched = 0usize;
    let mut total_blobs = 0usize;
    let mut worst_blobs = 0usize;
    let mut total_ms = 0.0f64;
    let mut worst_ms = 0.0f64;

    for case in &cases {
        let kind = parse_kind(case["kind"].as_str().expect("kind")).expect("known kind");
        let id = case["id"].as_i64().expect("id");
        let expected: BTreeMap<String, String> =
            serde_json::from_value(case["tags"].clone()).expect("tags");

        let started = Instant::now();
        let found = reader.find(kind, id).expect("lookup");
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;

        let blobs = reader.last_stats().blobs_decompressed;
        total_blobs += blobs;
        worst_blobs = worst_blobs.max(blobs);
        total_ms += elapsed;
        worst_ms = worst_ms.max(elapsed);
        checked += 1;

        let Some(found) = found else {
            println!("MISSING  {kind:?} {id}");
            missing += 1;
            continue;
        };
        if found.tags != expected {
            mismatched += 1;
            let only_pbf: Vec<_> = found
                .tags
                .keys()
                .filter(|k| !expected.contains_key(*k))
                .collect();
            let only_oracle: Vec<_> = expected
                .keys()
                .filter(|k| !found.tags.contains_key(*k))
                .collect();
            println!("MISMATCH {kind:?} {id}: pbf-only {only_pbf:?} oracle-only {only_oracle:?}");
        }
    }

    println!("\nchecked        {checked}");
    println!("missing        {missing}");
    println!("tag mismatches {mismatched}");
    println!(
        "blobs/lookup   mean {:.1}  worst {worst_blobs}",
        total_blobs as f64 / checked.max(1) as f64
    );
    println!(
        "latency        mean {:.2} ms  worst {worst_ms:.2} ms",
        total_ms / checked.max(1) as f64
    );
}
