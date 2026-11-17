//! Print what the blob scanner sees in a PBF, without decompressing any data.
//!
//! Usage: `cargo run -p osm-pbf-ingest --example scan_pbf -- <file.osm.pbf> [--verify]`
//!
//! `--verify` additionally classifies every blob exhaustively and checks the
//! binary-search classification against it. That is slow — it decompresses the
//! whole file — but it is the only way to prove the fast path on real data.

use osm_pbf_ingest::blob_index::{ObjectKind, PbfBlobIndex};
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::args().nth(1).map(PathBuf::from) else {
        eprintln!("usage: scan_pbf <file.osm.pbf> [--verify]");
        std::process::exit(2);
    };
    let verify = std::env::args().any(|argument| argument == "--verify");

    let started = Instant::now();
    let mut index = PbfBlobIndex::scan(&path)?;
    let elapsed = started.elapsed();

    let header = index.header().clone();
    println!("file            {}", path.display());
    println!(
        "size            {:.1} MiB",
        index.file_len() as f64 / (1024.0 * 1024.0)
    );
    println!("scanned in      {:?}", elapsed);
    println!("data blobs      {}", index.len());
    println!(
        "mean blob       {:.0} KiB",
        index.mean_blob_payload_bytes() as f64 / 1024.0
    );
    println!("sorted type+id  {}", header.sorted_type_then_id());
    println!("locations/ways  {}", header.locations_on_ways());
    println!("required        {:?}", header.required_features);
    println!("optional        {:?}", header.optional_features);
    println!("writing program {:?}", header.writing_program);
    println!("source          {:?}", header.source);
    if let Some(bounds) = header.bounds {
        println!(
            "bounds          W {:.5} S {:.5} E {:.5} N {:.5}",
            bounds.west, bounds.south, bounds.east, bounds.north
        );
    }

    let classify_started = Instant::now();
    let summary = index.classify(&path)?;
    let classify_elapsed = classify_started.elapsed();
    println!("\nclassified in   {:?}", classify_elapsed);
    println!("method          {:?}", summary.method);
    println!(
        "blobs read      {} of {}",
        summary.blobs_decompressed,
        index.len()
    );
    println!(
        "node pass       {} blobs ({:.1}% of file)",
        summary.blobs_with_nodes,
        percent(summary.blobs_with_nodes, index.len())
    );
    println!(
        "way pass        {} blobs ({:.1}% of file)",
        summary.blobs_with_ways,
        percent(summary.blobs_with_ways, index.len())
    );
    println!(
        "relation pass   {} blobs ({:.1}% of file)",
        summary.blobs_with_relations,
        percent(summary.blobs_with_relations, index.len())
    );

    if verify {
        let mut truth = PbfBlobIndex::scan(&path)?;
        let verify_started = Instant::now();
        let exhaustive = truth.classify_exhaustive(&mut BufReader::new(File::open(&path)?))?;
        println!("\nexhaustive in   {:?}", verify_started.elapsed());
        println!(
            "exhaustive      nodes {} / ways {} / relations {}",
            exhaustive.blobs_with_nodes,
            exhaustive.blobs_with_ways,
            exhaustive.blobs_with_relations
        );

        let mut dropped = 0usize;
        let mut over_read = 0usize;
        for kind in [ObjectKind::Node, ObjectKind::Way, ObjectKind::Relation] {
            for blob in 0..index.len() {
                let actual = truth.contents(blob).contains(kind);
                let planned = index.contents(blob).contains(kind);
                if actual && !planned {
                    dropped += 1;
                }
                if planned && !actual {
                    over_read += 1;
                }
            }
        }
        if dropped > 0 {
            println!(
                "\nFAIL: binary search would skip {dropped} blob/pass pairs that hold objects"
            );
            std::process::exit(1);
        }
        println!("verified        no blob/pass pair dropped; {over_read} read conservatively");
    }

    let unsupported = header.unsupported_required_features();
    if !unsupported.is_empty() {
        println!("\nUNSUPPORTED required features: {:?}", unsupported);
    }
    if index.mean_blob_payload_bytes() > 1024 * 1024 {
        println!(
            "\nwarning: blocks average over 1 MiB, which limits read parallelism.\n         \
             to fix: osmium cat -f pbf {} -o optimized.osm.pbf",
            path.display()
        );
    }

    Ok(())
}

fn percent(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        return 0.0;
    }
    part as f64 * 100.0 / whole as f64
}
