//! Apply OSM change files to a PBF, writing an updated PBF.
//!
//! Usage: `cargo run --release --example apply_osc -- <base.osm.pbf> <out.osm.pbf>
//! [--no-clip] <change.osc[.gz]>...`
//!
//! Without `--no-clip`, the changes are clipped to the base file's header box,
//! which is what planet replication diffs need.

use osm_pbf_ingest::changes::{ApplyOptions, BaseIndex, ChangeSet, Clip, apply_changes};
use std::path::PathBuf;
use std::time::Instant;

fn main() -> Result<(), String> {
    let mut arguments: Vec<String> = std::env::args().skip(1).collect();
    let clip = !arguments.iter().any(|argument| argument == "--no-clip");
    arguments.retain(|argument| argument != "--no-clip");
    if arguments.len() < 3 {
        eprintln!("usage: apply_osc <base.osm.pbf> <out.osm.pbf> [--no-clip] <change.osc[.gz]>...");
        std::process::exit(2);
    }
    let base_path = PathBuf::from(&arguments[0]);
    let output = PathBuf::from(&arguments[1]);

    let started = Instant::now();
    let base = BaseIndex::scan(&base_path, clip)?;
    println!(
        "scanned {} objects in {} blocks in {:.2?} (sorted: {})",
        base.object_count(),
        count_blocks(&base_path)?,
        started.elapsed(),
        base.is_sorted()
    );
    let clip = if clip {
        Some(Clip::to_header_bounds(&base).ok_or("the base file has no header box to clip to")?)
    } else {
        None
    };

    let started = Instant::now();
    let mut changes = ChangeSet::new();
    for path in &arguments[2..] {
        let report = changes.read_osc(path.as_ref(), clip.as_ref())?;
        println!("{path}: kept {} of {} changes", report.kept, report.objects);
    }
    let counts = changes.counts();
    println!(
        "read in {:.2?}: {} nodes, {} ways, {} relations, {} deletions",
        started.elapsed(),
        counts.nodes,
        counts.ways,
        counts.relations,
        counts.deletions
    );

    let started = Instant::now();
    let report = apply_changes(&base, &changes, &output, &ApplyOptions::default())?;
    println!("applied in {:.2?}: {report:#?}", started.elapsed());
    Ok(())
}

fn count_blocks(path: &std::path::Path) -> Result<usize, String> {
    osm_pbf_ingest::blob_index::PbfBlobIndex::scan(path)
        .map(|index| index.len())
        .map_err(|error| error.to_string())
}
