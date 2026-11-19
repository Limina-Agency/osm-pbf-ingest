//! Node locations for a set of node IDs, read in one pass over the node
//! blobs, without a node store.
//!
//! A caller that reads its ways first (pass 2 with
//! [`Pass2Options::skip_nodes`](crate::Pass2Options::skip_nodes)) knows which
//! nodes it needs before it reads any node. [`locate_nodes`] then reads the
//! node blobs once and keeps the locations of exactly those nodes, in RAM, in
//! the order of their IDs. Every node is looked at once and in file order, so
//! nothing is spilled to disk and nothing is looked up at random: where pass 1
//! stores every node of the file (about 6 bytes each, packed), this keeps 16
//! bytes for each needed node only. A road network needs about one node in
//! five.
//!
//! Each decode worker merges its block's nodes against the sorted IDs; the
//! collector writes the matches into place in file order. The file need not
//! be sorted: on a file that repeats a node, the first location in file order
//! is kept, as pass 1's store keeps it within a block.

use rayon::prelude::*;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

use crate::blob_index::ObjectKind;
use crate::model::NormalizedNode;
use crate::node_store::decode_decimicro;
use crate::osm_block::{Decoding, TagPair, for_each_node};
use crate::pass1::scan_blob_index;
use crate::pipeline::{self, BlobSource, PipelineConfig, RawBlock};
use crate::profile::{IngestBuildProfile, validate_worker_count};
use crate::progress::{
    INGEST_CANCELLED, IngestStage, SharedIngestProgress, read_fraction, reports_progress,
};
use crate::tags::{TagFilter, TagsRef};

/// Settings for [`locate_nodes`]. Left unset, it uses the profile's worker
/// count and keeps no tagged nodes.
///
/// ```
/// use osm_pbf_ingest::LocateOptions;
///
/// let options = LocateOptions::new()
///     .worker_threads(4)
///     .keep_nodes(|tags| tags.contains_key("barrier"));
/// # let _ = options;
/// ```
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct LocateOptions {
    /// Decode workers. Defaults to the profile's worker count.
    pub worker_threads: Option<usize>,
    /// Keep the tagged nodes this accepts in [`LocatedNodes::tagged_nodes`],
    /// whether or not their IDs were asked for, as
    /// [`PrepareOptions::keep_nodes`](crate::PrepareOptions::keep_nodes)
    /// keeps them in pass 1. Defaults to keeping none.
    pub keep_node: Option<fn(&TagsRef<'_>) -> bool>,
}

impl LocateOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn worker_threads(mut self, worker_threads: usize) -> Self {
        self.worker_threads = Some(worker_threads);
        self
    }

    pub fn keep_nodes(mut self, keep_node: fn(&TagsRef<'_>) -> bool) -> Self {
        self.keep_node = Some(keep_node);
        self
    }
}

/// What [`locate_nodes`] found.
#[derive(Debug)]
pub struct LocatedNodes {
    pub locations: NodeLocations,
    /// The tagged nodes [`LocateOptions::keep_nodes`] accepted, in file order;
    /// empty without that filter.
    pub tagged_nodes: Vec<NormalizedNode>,
    pub metrics: LocateMetrics,
}

#[derive(Debug, Clone, Default)]
pub struct LocateMetrics {
    pub raw_block_read_ms: u64,
    /// Summed across decode workers.
    pub decode_ms: u64,
    pub blocks_read: usize,
    /// Nodes in the blobs read, asked for or not.
    pub scanned_node_objects: usize,
    /// Distinct IDs asked for.
    pub requested_nodes: usize,
    /// Of those, the ones the file holds.
    pub located_nodes: usize,
    pub wall_ms: u64,
}

/// The locations of a sorted set of node IDs, those the file holds.
///
/// Lookups answer as [`NodeLocationStore::get`](crate::node_store::NodeLocationStore::get)
/// does for the same file: the same degrees, from the same 1e-7 degree units.
#[derive(Debug, Default)]
pub struct NodeLocations {
    /// Sorted, no repeats.
    ids: Vec<i64>,
    /// `(lat, lon)` in 1e-7 degrees, by position in `ids`; `(0, 0)` where
    /// `found` has no bit.
    decimicro: Vec<(i32, i32)>,
    /// One bit per position in `ids`.
    found: Vec<u64>,
    found_count: usize,
    index: IdIndex,
}

impl NodeLocations {
    fn at(&self, position: usize) -> Option<(i32, i32)> {
        (self.found[position / 64] & (1u64 << (position % 64)) != 0)
            .then(|| self.decimicro[position])
    }

    /// The location of `node_id` in degrees, `(lat, lng)`, when it was asked
    /// for and the file holds it.
    pub fn get(&self, node_id: i64) -> Option<(f64, f64)> {
        self.get_decimicro(node_id)
            .map(|(lat, lon)| decode_decimicro(lat, lon))
    }

    /// As [`get`](Self::get), in 1e-7 degree units.
    pub fn get_decimicro(&self, node_id: i64) -> Option<(i32, i32)> {
        let position = self.index.position(&self.ids, node_id)?;
        self.at(position)
    }

    /// A reader for runs of lookups, such as a way's nodes: it answers as
    /// [`get`](Self::get) does, and checks the IDs next to the last one found
    /// first, which a way's next node often is.
    pub fn cursor(&self) -> NodeLocationsCursor<'_> {
        NodeLocationsCursor {
            locations: self,
            last: None,
        }
    }

    /// The IDs asked for, sorted, without repeats.
    pub fn node_ids(&self) -> &[i64] {
        &self.ids
    }

    /// Distinct IDs asked for.
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// The IDs asked for that the file holds.
    pub fn located_count(&self) -> usize {
        self.found_count
    }
}

/// The locations found so far, by position in the sorted IDs, which the
/// decode workers share while the pass runs.
struct Slots {
    decimicro: Vec<(i32, i32)>,
    found: Vec<u64>,
    found_count: usize,
}

impl Slots {
    fn new(len: usize) -> Self {
        Self {
            decimicro: vec![(0, 0); len],
            found: vec![0; len.div_ceil(64)],
            found_count: 0,
        }
    }

    /// Records the location at `position` unless one is there already.
    fn set_first(&mut self, position: usize, lat: i32, lon: i32) {
        let (word, bit) = (position / 64, 1u64 << (position % 64));
        if self.found[word] & bit == 0 {
            self.found[word] |= bit;
            self.decimicro[position] = (lat, lon);
            self.found_count += 1;
        }
    }

    fn with_ids(self, ids: Vec<i64>) -> NodeLocations {
        NodeLocations {
            index: IdIndex::new(&ids),
            ids,
            decimicro: self.decimicro,
            found: self.found,
            found_count: self.found_count,
        }
    }
}

/// See [`NodeLocations::cursor`].
#[derive(Debug, Clone)]
pub struct NodeLocationsCursor<'a> {
    locations: &'a NodeLocations,
    /// The position of the last ID found.
    last: Option<usize>,
}

impl NodeLocationsCursor<'_> {
    /// The location of `node_id` in degrees, as [`NodeLocations::get`].
    pub fn get(&mut self, node_id: i64) -> Option<(f64, f64)> {
        self.get_decimicro(node_id)
            .map(|(lat, lon)| decode_decimicro(lat, lon))
    }

    /// As [`get`](Self::get), in 1e-7 degree units.
    pub fn get_decimicro(&mut self, node_id: i64) -> Option<(i32, i32)> {
        let locations = self.locations;
        let ids = &locations.ids;
        let position = match self.last {
            Some(last) if ids.get(last + 1) == Some(&node_id) => last + 1,
            Some(last) if ids.get(last) == Some(&node_id) => last,
            _ => locations.index.position(ids, node_id)?,
        };
        self.last = Some(position);
        locations.at(position)
    }
}

/// IDs per bucket of an [`IdIndex`], on average at most.
const IDS_PER_BUCKET: usize = 16;

/// Where each range of IDs starts in a sorted ID list: bucket `b` holds the
/// IDs whose offset from the smallest, shifted right by `shift`, is `b`. A
/// lookup bisects one bucket of about [`IDS_PER_BUCKET`] IDs instead of the
/// whole list, so it touches a cache line or two of IDs instead of one per
/// step of a search over hundreds of megabytes.
#[derive(Debug, Default)]
struct IdIndex {
    min: i64,
    shift: u32,
    /// `starts[b]..starts[b + 1]` are bucket `b`'s positions. Empty when the
    /// list is empty or too long for `u32` positions: lookups then bisect the
    /// whole list.
    starts: Vec<u32>,
}

impl IdIndex {
    fn new(ids: &[i64]) -> Self {
        let (Some(&min), Some(&max)) = (ids.first(), ids.last()) else {
            return Self::default();
        };
        if u32::try_from(ids.len()).is_err() {
            return Self::default();
        }
        let buckets = (ids.len() / IDS_PER_BUCKET).max(1).next_power_of_two() as u64;
        let span = max.abs_diff(min);
        let mut shift = 0u32;
        let shifted = |offset: u64, shift: u32| offset.checked_shr(shift).unwrap_or(0);
        while shifted(span, shift) >= buckets {
            shift += 1;
        }
        let bucket_count = shifted(span, shift) as usize + 1;
        let mut starts = Vec::with_capacity(bucket_count + 1);
        for (position, &id) in ids.iter().enumerate() {
            let bucket = shifted(id.abs_diff(min), shift) as usize;
            while starts.len() <= bucket {
                starts.push(position as u32);
            }
        }
        while starts.len() <= bucket_count {
            starts.push(ids.len() as u32);
        }
        Self { min, shift, starts }
    }

    /// The position of `node_id` in `ids`, the list this index was built
    /// from.
    fn position(&self, ids: &[i64], node_id: i64) -> Option<usize> {
        if self.starts.is_empty() {
            return ids.binary_search(&node_id).ok();
        }
        if node_id < self.min {
            return None;
        }
        let offset = node_id.abs_diff(self.min);
        let bucket = usize::try_from(offset.checked_shr(self.shift).unwrap_or(0)).ok()?;
        let (&start, &end) = (self.starts.get(bucket)?, self.starts.get(bucket + 1)?);
        let (start, end) = (start as usize, end as usize);
        ids[start..end]
            .binary_search(&node_id)
            .ok()
            .map(|offset| start + offset)
    }
}

/// Reads the node blobs of `pbf_path` once and returns the locations of the
/// nodes in `node_ids` (in any order, repeats allowed), and the tagged nodes
/// [`LocateOptions::keep_nodes`] accepts.
///
/// On a file sorted by type only the blobs holding nodes are read. IDs the
/// file does not hold are left without a location.
pub fn locate_nodes<F, C>(
    pbf_path: &Path,
    build_profile: IngestBuildProfile,
    mut node_ids: Vec<i64>,
    options: LocateOptions,
    mut on_progress: F,
    should_cancel: C,
) -> Result<LocatedNodes, String>
where
    F: FnMut(SharedIngestProgress),
    C: Fn() -> bool,
{
    let worker_threads = options
        .worker_threads
        .unwrap_or_else(|| build_profile.worker_threads());
    validate_worker_count(worker_threads)?;
    let started = Instant::now();
    if !node_ids.windows(2).all(|pair| pair[0] < pair[1]) {
        node_ids.par_sort_unstable();
        node_ids.dedup();
    }

    let blob_index = scan_blob_index(pbf_path)?;
    let wanted_blobs: Vec<_> = blob_index
        .blobs_containing(ObjectKind::Node)
        .copied()
        .collect();
    info!(
        pbf_path = %pbf_path.display(),
        node_blobs = wanted_blobs.len(),
        requested_nodes = node_ids.len(),
        "Locating nodes"
    );

    let ids = Arc::new(node_ids);
    let keep_node = options.keep_node.map(TagFilter::Borrowed);
    let mut collector = Collector {
        read_set_len: wanted_blobs.len(),
        requested: ids.len(),
        ..Collector::default()
    };
    let mut slots = Slots::new(ids.len());
    let stats = {
        let ids = Arc::clone(&ids);
        pipeline::run(
            pbf_path,
            BlobSource::Only(wanted_blobs),
            PipelineConfig {
                worker_threads,
                queue_capacity: build_profile.queue_capacity(),
            },
            move |block, inflated| decode_block(&block, inflated, &ids, keep_node),
            &should_cancel,
            |chunk| collector.add(chunk, &mut slots, &mut on_progress),
        )?
    };
    if should_cancel() {
        return Err(INGEST_CANCELLED.to_string());
    }
    collector.report_final_progress(&mut on_progress);

    // The workers held the other references, and they have all exited.
    let ids = Arc::try_unwrap(ids).unwrap_or_else(|shared| (*shared).clone());
    let located = slots.with_ids(ids);
    let metrics = LocateMetrics {
        raw_block_read_ms: stats.raw_block_read_ms,
        decode_ms: collector.decode_us / 1_000,
        blocks_read: stats.blocks_read,
        scanned_node_objects: collector.scanned,
        requested_nodes: located.len(),
        located_nodes: located.located_count(),
        wall_ms: started.elapsed().as_millis() as u64,
    };
    info!(
        stage = IngestStage::IngestNodeLocations.as_str(),
        raw_block_read_ms = metrics.raw_block_read_ms,
        decode_ms = metrics.decode_ms,
        blocks_read = metrics.blocks_read,
        scanned_node_objects = metrics.scanned_node_objects,
        requested_nodes = metrics.requested_nodes,
        located_nodes = metrics.located_nodes,
        wall_ms = metrics.wall_ms,
        "Located nodes"
    );
    Ok(LocatedNodes {
        locations: located,
        tagged_nodes: collector.tagged_nodes,
        metrics,
    })
}

/// What a decode worker makes of one block.
#[derive(Debug, Default)]
struct Chunk {
    /// `(position in the IDs, lat, lon)` for the block's nodes that were asked
    /// for, the first of a repeated ID only.
    matches: Vec<(usize, i32, i32)>,
    tagged_nodes: Vec<NormalizedNode>,
    node_count: usize,
    decode_us: u64,
}

fn decode_block(
    block: &RawBlock,
    inflated: &mut Vec<u8>,
    ids: &[i64],
    keep_node: Option<TagFilter>,
) -> Result<Chunk, String> {
    let decode_started = Instant::now();
    let decoded = block.decode_with(inflated, Decoding::WireWithoutWays)?;
    let mut chunk = Chunk::default();
    let mut records: Vec<(i64, i32, i32)> = Vec::new();
    let mut pairs: Vec<TagPair<'_>> = Vec::new();
    for group in decoded.each_group() {
        records.reserve(group.node_count());
        for_each_node(
            &decoded,
            group,
            keep_node.is_some(),
            &mut pairs,
            |node, pairs, repeat| {
                records.push((node.id, node.decimicro_lat, node.decimicro_lon));
                if let Some(keep_node) = keep_node
                    && !pairs.is_empty()
                    && let Some(tags) = keep_node.keep(pairs, repeat)
                {
                    chunk.tagged_nodes.push(NormalizedNode::from_decimicro(
                        node.id,
                        node.decimicro_lat,
                        node.decimicro_lon,
                        tags,
                    ));
                }
                Ok(())
            },
        )?;
    }
    chunk.node_count = records.len();
    // Sorted files list a block's nodes by increasing ID. Otherwise they are
    // sorted here, keeping the first of a repeated ID (the sort is stable).
    if !records.windows(2).all(|pair| pair[0].0 < pair[1].0) {
        records.sort_by_key(|record| record.0);
        records.dedup_by_key(|record| record.0);
    }
    merge_matches(ids, &records, &mut chunk.matches);
    chunk.decode_us = decode_started.elapsed().as_micros() as u64;
    Ok(chunk)
}

/// Appends `(position in ids, lat, lon)` for each record whose ID is in
/// `ids`. Both are sorted by ID without repeats.
fn merge_matches(ids: &[i64], records: &[(i64, i32, i32)], matches: &mut Vec<(usize, i32, i32)>) {
    let (Some(first), Some(last)) = (records.first(), records.last()) else {
        return;
    };
    let start = ids.partition_point(|&id| id < first.0);
    let end = start + ids[start..].partition_point(|&id| id <= last.0);
    let wanted = &ids[start..end];
    if wanted.is_empty() {
        return;
    }
    let (mut want, mut record) = (0usize, 0usize);
    while want < wanted.len() && record < records.len() {
        let (id, lat, lon) = records[record];
        match wanted[want].cmp(&id) {
            std::cmp::Ordering::Less => want += 1,
            std::cmp::Ordering::Greater => record += 1,
            std::cmp::Ordering::Equal => {
                matches.push((start + want, lat, lon));
                want += 1;
                record += 1;
            }
        }
    }
}

/// Gathers the chunks in file order.
#[derive(Default)]
struct Collector {
    tagged_nodes: Vec<NormalizedNode>,
    scanned: usize,
    decode_us: u64,
    requested: usize,
    /// Distinct IDs located so far.
    matched: usize,
    read_set_len: usize,
    chunks: usize,
    last_reported: usize,
}

impl Collector {
    fn add(
        &mut self,
        chunk: Chunk,
        slots: &mut Slots,
        on_progress: &mut impl FnMut(SharedIngestProgress),
    ) -> Result<(), String> {
        for (position, lat, lon) in chunk.matches {
            slots.set_first(position, lat, lon);
        }
        self.matched = slots.found_count;
        self.tagged_nodes.extend(chunk.tagged_nodes);
        self.scanned += chunk.node_count;
        self.decode_us += chunk.decode_us;
        self.chunks += 1;
        if reports_progress(self.chunks) {
            self.report_progress(on_progress);
        }
        Ok(())
    }

    fn report_final_progress(&mut self, on_progress: &mut impl FnMut(SharedIngestProgress)) {
        if self.last_reported != self.chunks {
            self.report_progress(on_progress);
        }
    }

    fn report_progress(&mut self, on_progress: &mut impl FnMut(SharedIngestProgress)) {
        on_progress(SharedIngestProgress {
            stage: IngestStage::IngestNodeLocations,
            batches_read: self.chunks,
            objects_read: self.scanned,
            message: format!(
                "Located {} of {} nodes in {} scanned",
                self.matched, self.requested, self.scanned
            ),
            fraction: Some(read_fraction(self.chunks, self.read_set_len)),
        });
        self.last_reported = self.chunks;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pass1::{PrepareOptions, prepare_ingest_index_with_options};
    use crate::test_pbf::{TestNode, node_block, pbf_file, sorted_fixture, write_fixture};

    fn locate(path: &Path, node_ids: Vec<i64>, options: LocateOptions) -> LocatedNodes {
        locate_nodes(
            path,
            IngestBuildProfile::DesktopFull,
            node_ids,
            options,
            |_progress| {},
            || false,
        )
        .expect("locate nodes")
    }

    #[test]
    fn locates_the_nodes_asked_for_as_the_store_does() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_fixture(dir.path(), &sorted_fixture());
        let prepared = prepare_ingest_index_with_options(
            &path,
            IngestBuildProfile::DesktopFull,
            dir.path(),
            PrepareOptions::new().keep_nodes(|tags| tags.contains_key("amenity")),
            |_progress| {},
            || false,
        )
        .expect("prepare ingest index");
        // Out of order, repeated, and one ID the file does not hold.
        let located = locate(
            &path,
            vec![3, 1, 3, 99],
            LocateOptions::new()
                .worker_threads(2)
                .keep_nodes(|tags| tags.contains_key("amenity")),
        );
        let locations = &located.locations;
        assert_eq!(locations.node_ids(), [1, 3, 99]);
        assert_eq!(locations.located_count(), 2);
        assert_eq!(locations.get(1), prepared.node_store.get(1));
        assert_eq!(locations.get(3), prepared.node_store.get(3));
        assert!(locations.get(1).is_some());
        // Not asked for, or not in the file.
        assert_eq!(locations.get(2), None);
        assert_eq!(locations.get(99), None);
        let mut cursor = locations.cursor();
        for id in [3, 1, 2, 99, 3] {
            assert_eq!(cursor.get(id), locations.get(id), "node {id}");
        }
        // The tagged nodes pass 1 keeps, whether asked for or not.
        let tagged = |nodes: &[NormalizedNode]| {
            nodes
                .iter()
                .map(|node| (node.node_id, node.lat, node.lng, node.tags.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            tagged(&located.tagged_nodes),
            tagged(&prepared.tagged_nodes)
        );
        assert_eq!(located.metrics.scanned_node_objects, 4);
        assert_eq!(located.metrics.requested_nodes, 3);
        assert_eq!(located.metrics.located_nodes, 2);
        assert!(
            locate(&path, vec![1], LocateOptions::new())
                .tagged_nodes
                .is_empty()
        );
    }

    #[test]
    fn an_unsorted_file_keeps_the_first_location_of_a_repeated_node() {
        let node = |id, lat, lng| TestNode {
            id,
            lat,
            lng,
            tags: &[],
        };
        // Blocks out of order, a block out of order within itself, and node 13
        // twice: once within a block, once in a later block.
        let blocks = [
            node_block(&[node(10, 50.0, 19.0), node(11, 50.1, 19.1)]),
            node_block(&[
                node(13, 50.3, 19.3),
                node(12, 50.2, 19.2),
                node(13, 1.0, 1.0),
            ]),
            node_block(&[node(1, 49.0, 18.0), node(2, 49.1, 18.1)]),
            node_block(&[node(20, 51.0, 20.0), node(13, 2.0, 2.0)]),
        ];
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_fixture(dir.path(), &pbf_file(false, &blocks));
        for workers in [1, 3] {
            let located = locate(
                &path,
                vec![20, 13, 12, 2, 3],
                LocateOptions::new().worker_threads(workers),
            );
            let locations = &located.locations;
            let near = |id: i64, lat: f64, lng: f64| {
                let (found_lat, found_lng) = locations.get(id).expect("located");
                (found_lat - lat).abs() < 1e-9 && (found_lng - lng).abs() < 1e-9
            };
            assert!(near(13, 50.3, 19.3));
            assert!(near(12, 50.2, 19.2));
            assert!(near(2, 49.1, 18.1));
            assert!(near(20, 51.0, 20.0));
            assert_eq!(locations.get(3), None);
            assert_eq!(locations.located_count(), 4);
        }
    }

    #[test]
    fn the_id_index_finds_what_a_binary_search_finds() {
        let lists: [Vec<i64>; 5] = [
            (0..1_000i64).map(|index| index * 7 - 300).collect(),
            (0..5_000i64).map(|index| index * index).collect(),
            vec![i64::MIN, -5, 0, 3, i64::MAX],
            vec![42],
            Vec::new(),
        ];
        for ids in &lists {
            let index = IdIndex::new(ids);
            let mut probes = vec![i64::MIN, i64::MAX, -1, 0, 1];
            for &id in ids {
                probes.extend([id.saturating_sub(1), id, id.saturating_add(1)]);
            }
            for target in probes {
                assert_eq!(
                    index.position(ids, target),
                    ids.binary_search(&target).ok(),
                    "target {target}"
                );
            }
        }
    }

    #[test]
    fn merge_matches_pairs_the_wanted_ids_in_range() {
        let ids = [2, 4, 6, 8, 100];
        let records = [(1, 10, 10), (4, 40, 40), (5, 50, 50), (8, 80, 80)];
        let mut matches = Vec::new();
        merge_matches(&ids, &records, &mut matches);
        assert_eq!(matches, [(1, 40, 40), (3, 80, 80)]);
        merge_matches(&ids, &[], &mut matches);
        merge_matches(&ids, &[(9, 0, 0), (99, 0, 0)], &mut matches);
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn rejects_zero_workers_and_reports_progress() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_fixture(dir.path(), &sorted_fixture());
        let zero = locate_nodes(
            &path,
            IngestBuildProfile::DesktopFull,
            vec![1],
            LocateOptions::new().worker_threads(0),
            |_progress| {},
            || false,
        );
        assert!(zero.unwrap_err().contains("worker count"));
        let mut reports = Vec::new();
        locate_nodes(
            &path,
            IngestBuildProfile::DesktopFull,
            vec![1, 4],
            LocateOptions::new(),
            |progress| reports.push(progress),
            || false,
        )
        .expect("locate nodes");
        let last = reports.last().expect("a final report");
        assert_eq!(last.stage, IngestStage::IngestNodeLocations);
        assert_eq!(last.fraction, Some(1.0));
        assert_eq!(last.message, "Located 2 of 2 nodes in 4 scanned");
        let cancelled = locate_nodes(
            &path,
            IngestBuildProfile::DesktopFull,
            vec![1],
            LocateOptions::new(),
            |_progress| {},
            || true,
        );
        assert_eq!(cancelled.unwrap_err(), INGEST_CANCELLED);
    }
}
