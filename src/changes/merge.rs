//! Merging several sorted PBFs into one, as `osmium merge` does.
//!
//! Search areas cross the edges of extracts, so a compile may need two or
//! more of them. [`merge_files`] reads every input in key order at once and
//! writes one file sorted by type then id. An object held by several inputs
//! (where extracts overlap) is written once. A block whose key range no other
//! input reaches is copied byte for byte; the rest are decoded, merged and
//! written again, in blocks of [`OBJECTS_PER_BLOCK`] objects, without object
//! metadata, as [`apply_changes`](super::apply_changes) writes them.
//!
//! Memory stays at one decoded block per input plus the blocks being encoded,
//! however large the inputs are.

use super::apply::decode_objects;
use super::base::{BaseIndex, payload, read_block};
use super::{Key, OsmObject};
use crate::pbf_writer::{
    BLOB_TYPE_DATA, BLOB_TYPE_HEADER, Header, OBJECTS_PER_BLOCK, encode_block, encode_header,
    frame, frame_blob,
};
use memmap2::Mmap;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

const FEATURE_SORTED: &str = "Sort.Type_then_ID";
const FEATURE_HAS_METADATA: &str = "Has_Metadata";
const FEATURE_LOCATIONS_ON_WAYS: &str = "LocationsOnWays";

/// Blocks encoded in parallel before they are written, in order.
const WINDOW: usize = 64;

/// Header fields for the merged file.
#[derive(Debug, Clone, Default)]
pub struct MergeOptions {
    /// [default: `osm-pbf-ingest <version>`]
    pub writing_program: Option<String>,
}

/// What [`merge_files`] did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MergeReport {
    /// Blocks copied unchanged from an input.
    pub blobs_copied: usize,
    /// Blocks written from decoded objects.
    pub blobs_written: usize,
    /// Objects written.
    pub objects: usize,
    /// Objects left out because an earlier input held them too.
    pub duplicates: usize,
    pub output_bytes: u64,
}

/// Writes the objects of every input to `output`, which must not exist,
/// sorted by type then id. Every input must be sorted by type then id
/// (Geofabrik's and osmium's extracts are) and keep node locations off its
/// ways.
///
/// Where inputs hold the same object, the earliest input in `inputs` wins:
/// blocks this crate decodes carry no versions, so put the newest extract
/// first. The header's box covers every input's box; its replication
/// timestamp is the oldest input's, since the data is current to at least
/// that moment, and its sequence number and server are kept only when every
/// input names the same ones.
///
/// Runs on rayon's current pool. On failure the partial output is removed.
pub fn merge_files(
    inputs: &[&BaseIndex],
    output: &Path,
    options: &MergeOptions,
) -> Result<MergeReport, String> {
    merge_files_with_progress(inputs, output, options, |_, _| Ok(()))
}

/// [`merge_files`], calling `progress` with the input blocks read so far and
/// in all every few dozen blocks written. An error it returns stops the
/// merge, removes the partial output and is returned: that is how a caller
/// cancels.
pub fn merge_files_with_progress(
    inputs: &[&BaseIndex],
    output: &Path,
    options: &MergeOptions,
    mut progress: impl FnMut(usize, usize) -> Result<(), String>,
) -> Result<MergeReport, String> {
    if inputs.is_empty() {
        return Err("no files to merge".to_string());
    }
    for input in inputs {
        if !input.is_sorted() {
            return Err(format!(
                "{} is not sorted by type then id, which merging needs",
                input.path().display()
            ));
        }
        if input.header().locations_on_ways() {
            return Err(format!(
                "{} stores node locations on its ways, which merging does not keep",
                input.path().display()
            ));
        }
    }
    let file = File::options()
        .write(true)
        .create_new(true)
        .open(output)
        .map_err(|error| format!("{}: {error}", output.display()))?;
    let result = write(inputs, file, options, &mut progress);
    if result.is_err() {
        let _ = std::fs::remove_file(output);
    }
    result
}

/// One input as the merge reads it: the next blocks and the objects decoded
/// from the current one.
struct Cursor<'a> {
    base: &'a BaseIndex,
    map: Mmap,
    next_blob: usize,
    pending: VecDeque<OsmObject>,
}

impl Cursor<'_> {
    /// The smallest key this input can still give: its next decoded object's,
    /// otherwise its next block's lowest. `None` when it is used up.
    fn lower_bound(&mut self) -> Option<Key> {
        if let Some(object) = self.pending.front() {
            return Some(object.key());
        }
        self.skip_empty_blobs();
        self.base
            .blobs()
            .get(self.next_blob)
            .and_then(|blob| blob.range.map(|(low, _)| low))
    }

    fn skip_empty_blobs(&mut self) {
        while self
            .base
            .blobs()
            .get(self.next_blob)
            .is_some_and(|blob| blob.range.is_none())
        {
            self.next_blob += 1;
        }
    }

    fn decode_next(&mut self) -> Result<(), String> {
        let location = &self.base.blobs()[self.next_blob].location;
        let objects = decode_objects(&read_block(&self.map, location)?)
            .map_err(|error| format!("{}: {error}", self.base.path().display()))?;
        self.pending.extend(objects);
        self.next_blob += 1;
        Ok(())
    }
}

/// The input blocks read so far.
fn read(cursors: &[Cursor<'_>]) -> usize {
    cursors.iter().map(|cursor| cursor.next_blob).sum()
}

/// A piece of the merged file, in order.
enum Piece {
    /// An input's block, unchanged, already framed.
    Copy(Vec<u8>),
    /// Objects of one type to encode as one block.
    Objects(Vec<OsmObject>),
}

fn write(
    inputs: &[&BaseIndex],
    file: File,
    options: &MergeOptions,
    progress: &mut dyn FnMut(usize, usize) -> Result<(), String>,
) -> Result<MergeReport, String> {
    let total: usize = inputs.iter().map(|input| input.blobs().len()).sum();
    let mut cursors = Vec::with_capacity(inputs.len());
    for base in inputs {
        let source = File::open(base.path())
            .map_err(|error| format!("{}: {error}", base.path().display()))?;
        // SAFETY: read only, for the duration of this call.
        let map = unsafe { Mmap::map(&source) }
            .map_err(|error| format!("{}: {error}", base.path().display()))?;
        cursors.push(Cursor {
            base,
            map,
            next_blob: 0,
            pending: VecDeque::new(),
        });
    }
    let mut out = BufWriter::with_capacity(4 << 20, file);
    let mut report = MergeReport::default();
    let header = frame(
        BLOB_TYPE_HEADER,
        &encode_header(&output_header(inputs, options)),
    )?;
    out.write_all(&header).map_err(|error| error.to_string())?;
    report.output_bytes += header.len() as u64;

    // Pieces wait here until a window is full, then are encoded in parallel.
    let mut queued: Vec<Piece> = Vec::new();
    let mut block: Vec<OsmObject> = Vec::with_capacity(OBJECTS_PER_BLOCK);
    let flush = |queued: &mut Vec<Piece>,
                 out: &mut BufWriter<File>,
                 report: &mut MergeReport|
     -> Result<(), String> {
        let framed: Vec<Result<Vec<u8>, String>> = queued
            .par_iter()
            .map(|piece| match piece {
                Piece::Copy(framed) => Ok(framed.clone()),
                Piece::Objects(objects) => {
                    let objects: Vec<&OsmObject> = objects.iter().collect();
                    frame(BLOB_TYPE_DATA, &encode_block(&objects))
                }
            })
            .collect();
        for (piece, framed) in queued.iter().zip(framed) {
            let framed = framed?;
            match piece {
                Piece::Copy(_) => report.blobs_copied += 1,
                Piece::Objects(_) => report.blobs_written += 1,
            }
            out.write_all(&framed).map_err(|error| error.to_string())?;
            report.output_bytes += framed.len() as u64;
        }
        queued.clear();
        Ok(())
    };

    loop {
        let bounds: Vec<Option<Key>> = cursors.iter_mut().map(Cursor::lower_bound).collect();
        let Some(lowest) = bounds.iter().flatten().min().copied() else {
            break;
        };
        // An input whose next block starts at the lowest key and sits below
        // what every other input can still give is copied whole; otherwise
        // the block is decoded and its objects merged one by one.
        let at_lowest: Vec<usize> = (0..cursors.len())
            .filter(|&index| bounds[index] == Some(lowest))
            .collect();
        if let Some(&index) = at_lowest
            .iter()
            .find(|&&index| cursors[index].pending.is_empty())
        {
            let cursor = &cursors[index];
            let blob = &cursor.base.blobs()[cursor.next_blob];
            let (_, high) = blob.range.expect("empty blocks are skipped");
            let others_above = bounds
                .iter()
                .enumerate()
                .filter(|(other, _)| *other != index)
                .all(|(_, bound)| bound.is_none_or(|bound| bound > high));
            if others_above && block.is_empty() {
                queued.push(Piece::Copy(frame_blob(
                    BLOB_TYPE_DATA,
                    payload(&cursor.map, &blob.location)?,
                )));
                report.objects += blob.objects;
                cursors[index].next_blob += 1;
                if queued.len() >= WINDOW {
                    flush(&mut queued, &mut out, &mut report)?;
                    progress(read(&cursors), total)?;
                }
                continue;
            }
            if others_above {
                // The objects merged so far go first, as a block of their own.
                queued.push(Piece::Objects(std::mem::take(&mut block)));
                if queued.len() >= WINDOW {
                    flush(&mut queued, &mut out, &mut report)?;
                    progress(read(&cursors), total)?;
                }
                continue;
            }
            cursors[index].decode_next()?;
            continue;
        }
        // Every input at the lowest key has it decoded: the first one's
        // object is written and the others' copies dropped.
        let mut winner = None;
        for &index in &at_lowest {
            let object = cursors[index]
                .pending
                .pop_front()
                .expect("a decoded object gave the bound");
            if winner.is_none() {
                winner = Some(object);
            } else {
                report.duplicates += 1;
            }
        }
        let object = winner.expect("at least one input holds the lowest key");
        if block
            .first()
            .is_some_and(|first| first.kind() != object.kind())
            || block.len() == OBJECTS_PER_BLOCK
        {
            queued.push(Piece::Objects(std::mem::take(&mut block)));
            if queued.len() >= WINDOW {
                flush(&mut queued, &mut out, &mut report)?;
                progress(read(&cursors), total)?;
            }
        }
        block.push(object);
        report.objects += 1;
    }
    if !block.is_empty() {
        queued.push(Piece::Objects(block));
    }
    flush(&mut queued, &mut out, &mut report)?;
    progress(total, total)?;
    out.flush().map_err(|error| error.to_string())?;
    out.into_inner()
        .map_err(|error| error.to_string())?
        .sync_all()
        .map_err(|error| error.to_string())?;
    Ok(report)
}

fn output_header(inputs: &[&BaseIndex], options: &MergeOptions) -> Header {
    let headers: Vec<_> = inputs.iter().map(|input| input.header()).collect();
    const NANODEGREES: f64 = 1e9;
    // The union of the boxes, when every input has one and none crosses the
    // antimeridian.
    let bbox = headers
        .iter()
        .map(|header| header.bounds.filter(|bounds| bounds.west <= bounds.east))
        .collect::<Option<Vec<_>>>()
        .map(|boxes| {
            let west = boxes.iter().map(|b| b.west).fold(f64::INFINITY, f64::min);
            let south = boxes.iter().map(|b| b.south).fold(f64::INFINITY, f64::min);
            let east = boxes
                .iter()
                .map(|b| b.east)
                .fold(f64::NEG_INFINITY, f64::max);
            let north = boxes
                .iter()
                .map(|b| b.north)
                .fold(f64::NEG_INFINITY, f64::max);
            [
                (west * NANODEGREES).round() as i64,
                (south * NANODEGREES).round() as i64,
                (east * NANODEGREES).round() as i64,
                (north * NANODEGREES).round() as i64,
            ]
        });
    let mut required_features: Vec<String> = Vec::new();
    for feature in headers.iter().flat_map(|header| &header.required_features) {
        if !required_features.contains(feature) {
            required_features.push(feature.clone());
        }
    }
    // The written blocks carry no metadata and no locations on ways; every
    // input is sorted, so the output is.
    let mut optional_features: Vec<String> = Vec::new();
    for feature in headers.iter().flat_map(|header| &header.optional_features) {
        if feature != FEATURE_HAS_METADATA
            && feature != FEATURE_LOCATIONS_ON_WAYS
            && feature != FEATURE_SORTED
            && !optional_features.contains(feature)
        {
            optional_features.push(feature.clone());
        }
    }
    optional_features.push(FEATURE_SORTED.to_string());
    let same = |field: fn(&crate::blob_index::PbfFileHeader) -> Option<String>| {
        let first = field(headers[0]);
        headers
            .iter()
            .all(|header| field(header) == first)
            .then_some(first)
            .flatten()
    };
    let replication_base_url = same(|header| header.replication_base_url.clone());
    let replication_sequence_number = same(|header| {
        header
            .replication_sequence_number
            .map(|sequence| sequence.to_string())
    })
    .and_then(|sequence| sequence.parse().ok())
    .filter(|_| replication_base_url.is_some());
    Header {
        bbox,
        required_features,
        optional_features,
        writing_program: Some(
            options.writing_program.clone().unwrap_or_else(|| {
                concat!("osm-pbf-ingest ", env!("CARGO_PKG_VERSION")).to_string()
            }),
        ),
        source: same(|header| header.source.clone()),
        replication_timestamp: headers
            .iter()
            .map(|header| header.replication_timestamp)
            .collect::<Option<Vec<_>>>()
            .and_then(|timestamps| timestamps.into_iter().min()),
        replication_sequence_number,
        replication_base_url: replication_base_url
            .filter(|_| replication_sequence_number.is_some()),
    }
}
