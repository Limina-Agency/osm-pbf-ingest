//! Merge sorted PBFs into one, as `osmium merge` does, without osmium.
//!
//! Usage: `cargo run --release --example merge_pbf -- <out.osm.pbf> <in.osm.pbf>...`
//!
//! Where inputs hold the same object, the first input that holds it wins.

use osm_pbf_ingest::changes::{BaseIndex, MergeOptions, merge_files};
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), String> {
    let arguments: Vec<PathBuf> = std::env::args().skip(1).map(PathBuf::from).collect();
    if arguments.len() < 2 {
        eprintln!("usage: merge_pbf <out.osm.pbf> <in.osm.pbf>...");
        std::process::exit(2);
    }
    let started = Instant::now();
    let indexes = arguments[1..]
        .iter()
        .map(|path| BaseIndex::scan(path, false))
        .collect::<Result<Vec<_>, _>>()?;
    println!(
        "scanned {} files in {:.2?}",
        indexes.len(),
        started.elapsed()
    );
    let inputs: Vec<&BaseIndex> = indexes.iter().collect();
    let started = Instant::now();
    let report = merge_files(&inputs, &arguments[0], &MergeOptions::default())?;
    println!("merged in {:.2?}: {report:#?}", started.elapsed());
    Ok(())
}
