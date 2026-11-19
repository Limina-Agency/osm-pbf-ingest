//! Writing a PBF with a change set applied.

use super::base::{BaseIndex, payload, read_block};
use super::{Change, ChangeSet, Key, OsmNode, OsmObject, OsmRelation, OsmWay};
use crate::osm_block::{Block, for_each_node};
use crate::pbf_writer::{
    BLOB_TYPE_DATA, BLOB_TYPE_HEADER, Header, OBJECTS_PER_BLOCK, encode_block, encode_header,
    frame, frame_blob,
};
use crate::tags::{Repeat, TagsRef};
use memmap2::Mmap;
use rayon::prelude::*;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

const FEATURE_SORTED: &str = "Sort.Type_then_ID";
const FEATURE_LOCATIONS_ON_WAYS: &str = "LocationsOnWays";
const FEATURE_HAS_METADATA: &str = "Has_Metadata";

/// Segments processed in parallel before they are written, in order.
const WINDOW: usize = 64;

/// Header fields for the written file. Fields left `None` keep the base
/// file's.
#[derive(Debug, Clone, Default)]
pub struct ApplyOptions {
    pub replication_timestamp: Option<i64>,
    pub replication_sequence_number: Option<i64>,
    pub replication_base_url: Option<String>,
    /// [default: `osm-pbf-ingest <version>`]
    pub writing_program: Option<String>,
}

/// What [`apply_changes`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApplyReport {
    /// Blocks copied unchanged.
    pub blobs_copied: usize,
    /// Blocks decoded, changed and written again.
    pub blobs_rewritten: usize,
    /// Blocks holding only new objects.
    pub blobs_added: usize,
    /// Objects the base file did not hold.
    pub created: usize,
    /// Objects replaced.
    pub modified: usize,
    /// Objects removed.
    pub deleted: usize,
    /// Deletions of objects the base file did not hold, which change nothing.
    pub deletions_ignored: usize,
    /// Whether the written file is sorted by type then id (the base was).
    pub sorted: bool,
    pub output_bytes: u64,
}

/// Write `base` with `changes` applied to `output`, which must not exist.
/// Runs on rayon's current pool. On failure the partial output is removed.
pub fn apply_changes(
    base: &BaseIndex,
    changes: &ChangeSet,
    output: &Path,
    options: &ApplyOptions,
) -> Result<ApplyReport, String> {
    if base.header().locations_on_ways() {
        return Err(format!(
            "{} stores node locations on its ways, which changes cannot be applied to",
            base.path().display()
        ));
    }
    let file = File::options()
        .write(true)
        .create_new(true)
        .open(output)
        .map_err(|error| format!("{}: {error}", output.display()))?;
    let result = write(base, changes, file, options);
    if result.is_err() {
        let _ = std::fs::remove_file(output);
    }
    result
}

/// One piece of the written file.
enum Segment {
    /// A base block, unchanged.
    Copy(usize),
    /// A base block with the changes whose keys fall in its range.
    Rewrite(usize, Vec<Key>),
    /// New objects, which the base file doesn't hold, in key order.
    Insert(Vec<Key>),
}

/// A segment ready to write, and what it did.
#[derive(Default)]
struct Written {
    frames: Vec<Vec<u8>>,
    report: ApplyReport,
    /// The changed keys a rewritten block held.
    matched: Vec<Key>,
}

fn write(
    base: &BaseIndex,
    changes: &ChangeSet,
    file: File,
    options: &ApplyOptions,
) -> Result<ApplyReport, String> {
    let source =
        File::open(base.path()).map_err(|error| format!("{}: {error}", base.path().display()))?;
    // SAFETY: read only, for the duration of this call.
    let map = unsafe { Mmap::map(&source) }
        .map_err(|error| format!("{}: {error}", base.path().display()))?;
    let sorted = base.is_sorted();
    let mut out = BufWriter::with_capacity(4 << 20, file);
    let mut report = ApplyReport {
        sorted,
        ..ApplyReport::default()
    };

    let header = frame(
        BLOB_TYPE_HEADER,
        &encode_header(&output_header(base, options, sorted)),
    )?;
    out.write_all(&header).map_err(|error| error.to_string())?;
    report.output_bytes += header.len() as u64;

    let segments = plan(base, changes, sorted);
    let mut matched: HashSet<Key> = HashSet::new();
    for window in segments.chunks(WINDOW) {
        let written: Vec<Result<Written, String>> = window
            .par_iter()
            .map(|segment| run(segment, base, &map, changes, sorted))
            .collect();
        for written in written {
            let written = written?;
            for frame in &written.frames {
                out.write_all(frame).map_err(|error| error.to_string())?;
                report.output_bytes += frame.len() as u64;
            }
            add(&mut report, &written.report);
            matched.extend(written.matched);
        }
    }
    if !sorted {
        // New objects go after everything else.
        let remaining: Vec<Key> = changes
            .map()
            .iter()
            .filter(|(key, change)| !matched.contains(key) && matches!(change, Change::Upsert(_)))
            .map(|(key, _)| *key)
            .collect();
        let written = run(&Segment::Insert(remaining), base, &map, changes, sorted)?;
        for frame in &written.frames {
            out.write_all(frame).map_err(|error| error.to_string())?;
            report.output_bytes += frame.len() as u64;
        }
        add(&mut report, &written.report);
    }
    let deletions = changes
        .iter()
        .filter(|change| matches!(change, Change::Delete { .. }))
        .count();
    report.deletions_ignored = deletions - report.deleted;
    out.flush().map_err(|error| error.to_string())?;
    out.into_inner()
        .map_err(|error| error.to_string())?
        .sync_all()
        .map_err(|error| error.to_string())?;
    Ok(report)
}

fn add(total: &mut ApplyReport, part: &ApplyReport) {
    total.blobs_copied += part.blobs_copied;
    total.blobs_rewritten += part.blobs_rewritten;
    total.blobs_added += part.blobs_added;
    total.created += part.created;
    total.modified += part.modified;
    total.deleted += part.deleted;
}

fn output_header(base: &BaseIndex, options: &ApplyOptions, sorted: bool) -> Header {
    let source = base.header();
    let mut optional_features: Vec<String> = source
        .optional_features
        .iter()
        // Rewritten blocks carry no metadata, and only a sorted base gives a
        // sorted file.
        .filter(|feature| *feature != FEATURE_HAS_METADATA && *feature != FEATURE_SORTED)
        .filter(|feature| *feature != FEATURE_LOCATIONS_ON_WAYS)
        .cloned()
        .collect();
    if sorted {
        optional_features.push(FEATURE_SORTED.to_string());
    }
    const NANODEGREES: f64 = 1e9;
    Header {
        bbox: source.bounds.map(|bounds| {
            [
                (bounds.west * NANODEGREES).round() as i64,
                (bounds.south * NANODEGREES).round() as i64,
                (bounds.east * NANODEGREES).round() as i64,
                (bounds.north * NANODEGREES).round() as i64,
            ]
        }),
        required_features: source.required_features.clone(),
        optional_features,
        writing_program: Some(
            options.writing_program.clone().unwrap_or_else(|| {
                concat!("osm-pbf-ingest ", env!("CARGO_PKG_VERSION")).to_string()
            }),
        ),
        source: source.source.clone(),
        replication_timestamp: options
            .replication_timestamp
            .or(source.replication_timestamp),
        replication_sequence_number: options
            .replication_sequence_number
            .or(source.replication_sequence_number),
        replication_base_url: options
            .replication_base_url
            .clone()
            .or_else(|| source.replication_base_url.clone()),
    }
}

/// Which base blocks to copy and rewrite, and where new objects go. In a
/// sorted file a new object goes to the block whose range holds its key, or
/// in a new block between the blocks around it; in any other file it is
/// appended at the end (by the caller).
fn plan(base: &BaseIndex, changes: &ChangeSet, sorted: bool) -> Vec<Segment> {
    let keys: Vec<Key> = changes.map().keys().copied().collect();
    let is_upsert = |key: &Key| matches!(changes.map().get(key), Some(Change::Upsert(_)));
    let mut segments = Vec::with_capacity(base.blobs().len() + 1);
    let mut next = 0;
    for (index, blob) in base.blobs().iter().enumerate() {
        let Some((low, high)) = blob.range else {
            segments.push(Segment::Copy(index));
            continue;
        };
        if sorted {
            let before = keys[next..].partition_point(|key| *key < low) + next;
            let inserts: Vec<Key> = keys[next..before]
                .iter()
                .copied()
                .filter(is_upsert)
                .collect();
            if !inserts.is_empty() {
                segments.push(Segment::Insert(inserts));
            }
            let within = keys[before..].partition_point(|key| *key <= high) + before;
            segments.push(if within > before {
                Segment::Rewrite(index, keys[before..within].to_vec())
            } else {
                Segment::Copy(index)
            });
            next = within;
        } else {
            let start = keys.partition_point(|key| *key < low);
            let end = keys.partition_point(|key| *key <= high);
            segments.push(if end > start {
                Segment::Rewrite(index, keys[start..end].to_vec())
            } else {
                Segment::Copy(index)
            });
        }
    }
    if sorted {
        let inserts: Vec<Key> = keys[next..].iter().copied().filter(is_upsert).collect();
        if !inserts.is_empty() {
            segments.push(Segment::Insert(inserts));
        }
    }
    segments
}

fn run(
    segment: &Segment,
    base: &BaseIndex,
    map: &[u8],
    changes: &ChangeSet,
    sorted: bool,
) -> Result<Written, String> {
    let mut written = Written::default();
    match segment {
        Segment::Copy(index) => {
            let location = &base.blobs()[*index].location;
            written
                .frames
                .push(frame_blob(BLOB_TYPE_DATA, payload(map, location)?));
            written.report.blobs_copied = 1;
        }
        Segment::Insert(keys) => {
            let objects: Vec<&OsmObject> =
                keys.iter().filter_map(|key| upsert(changes, key)).collect();
            written.report.created = objects.len();
            for chunk in chunks_by_kind(&objects) {
                written
                    .frames
                    .push(frame(BLOB_TYPE_DATA, &encode_block(chunk))?);
                written.report.blobs_added += 1;
            }
        }
        Segment::Rewrite(index, keys) => {
            let location = &base.blobs()[*index].location;
            let existing = decode_objects(&read_block(map, location)?)?;
            let mut objects: Vec<&OsmObject> = Vec::with_capacity(existing.len() + keys.len());
            let mut pending = keys.iter().peekable();
            for object in &existing {
                let key = object.key();
                let change = if sorted {
                    // New objects that sort before this one.
                    while let Some(new) = pending.next_if(|new| **new < key) {
                        if let Some(object) = upsert(changes, new) {
                            objects.push(object);
                            written.report.created += 1;
                        }
                    }
                    pending
                        .next_if_eq(&&key)
                        .and_then(|key| changes.map().get(key))
                } else {
                    keys.binary_search(&key)
                        .ok()
                        .and_then(|_| changes.map().get(&key))
                };
                match change {
                    None => objects.push(object),
                    Some(change) => {
                        written.matched.push(key);
                        match change {
                            Change::Upsert(new) => {
                                objects.push(new);
                                written.report.modified += 1;
                            }
                            Change::Delete { .. } => written.report.deleted += 1,
                        }
                    }
                }
            }
            if sorted {
                for new in pending {
                    if let Some(object) = upsert(changes, new) {
                        objects.push(object);
                        written.report.created += 1;
                    }
                }
            }
            // A block that grew a lot is split; one emptied is left out.
            let chunks = if objects.len() > OBJECTS_PER_BLOCK * 2 {
                chunks_by_kind(&objects)
            } else if objects.is_empty() {
                Vec::new()
            } else {
                vec![objects.as_slice()]
            };
            for chunk in chunks {
                written
                    .frames
                    .push(frame(BLOB_TYPE_DATA, &encode_block(chunk))?);
            }
            written.report.blobs_rewritten = 1;
        }
    }
    Ok(written)
}

fn upsert<'c>(changes: &'c ChangeSet, key: &Key) -> Option<&'c OsmObject> {
    match changes.map().get(key) {
        Some(Change::Upsert(object)) => Some(object),
        _ => None,
    }
}

/// Runs of at most [`OBJECTS_PER_BLOCK`] objects, each of one type.
fn chunks_by_kind<'s, 'o>(objects: &'s [&'o OsmObject]) -> Vec<&'s [&'o OsmObject]> {
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < objects.len() {
        let kind = objects[start].kind();
        let limit = (start + OBJECTS_PER_BLOCK).min(objects.len());
        let end = objects[start..limit]
            .iter()
            .position(|object| object.kind() != kind)
            .map_or(limit, |offset| start + offset);
        chunks.push(&objects[start..end]);
        start = end;
    }
    chunks
}

/// Every object of a block, owned, in file order.
fn decode_objects(bytes: &[u8]) -> Result<Vec<OsmObject>, String> {
    let block = Block::decode(bytes)?;
    let mut objects = Vec::new();
    let mut pairs = Vec::new();
    let mut node_ids = Vec::new();
    for group in block.groups() {
        for_each_node(&block, group, true, &mut pairs, |node, tags, repeat| {
            objects.push(OsmObject::Node(OsmNode {
                id: node.id,
                version: 0,
                decimicro_lat: node.decimicro_lat,
                decimicro_lon: node.decimicro_lon,
                tags: TagsRef::new(tags, repeat).to_tags(),
            }));
            Ok(())
        })?;
        for way in group.ways.iter() {
            block.tag_pairs(&way.keys, &way.vals, &mut pairs)?;
            block.way_node_ids(way, &mut node_ids);
            objects.push(OsmObject::Way(OsmWay {
                id: way.id,
                version: 0,
                node_ids: node_ids.clone(),
                tags: TagsRef::new(&pairs, Repeat::FirstWins).to_tags(),
            }));
        }
        for relation in group.relations.iter() {
            let relation = block.normalized_relation(relation, &mut pairs)?;
            objects.push(OsmObject::Relation(OsmRelation {
                id: relation.relation_id,
                version: 0,
                members: relation.members,
                tags: relation.tags,
            }));
        }
    }
    Ok(objects)
}
