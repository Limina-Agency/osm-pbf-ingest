//! Pass 1: the node-location store and the relation indexes.
//!
//! Pass 1 reads the node and relation blobs (on a file sorted by type it
//! seeks past the way blobs). Decode workers insert each block's node
//! locations into the shared store builder as they go; the collector gathers
//! the relations and their way and parent memberships in file order.

use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

use crate::blob_index::{ObjectKind, PbfBlobIndex};
use crate::model::{NormalizedNode, NormalizedRelation, RelationMemberType};
use crate::node_store::{
    NodeLocationStore, NodeLocationStoreBuilder, NodeStoreStrategy, NodeWorkerChunk,
    choose_node_store_strategy, encode_decimicro,
};
use crate::osm_block::{TagPair, for_each_node};
use crate::pipeline::{self, BlobSource, PipelineConfig, RawBlock};
use crate::profile::{IngestBuildProfile, validate_worker_count};
use crate::progress::{
    INGEST_CANCELLED, IngestStage, SharedIngestProgress, read_fraction, reports_progress,
};
use crate::relation_index::{PackedRelationIndex, PackedRelationIndexBuilder};
use crate::tags::{TagFilter, Tags, TagsRef};

/// Settings for [`prepare_ingest_index_with_options`]. Every field left unset
/// falls back to the build profile's default.
///
/// ```
/// use osm_pbf_ingest::PrepareOptions;
/// use osm_pbf_ingest::node_store::NodeStoreStrategy;
///
/// let options = PrepareOptions::new()
///     .worker_threads(4)
///     .node_store(NodeStoreStrategy::Disk)
///     .keep_nodes(|tags| tags.contains_key("barrier"));
/// # let _ = options;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct PrepareOptions {
    /// Decode workers. Defaults to the profile's worker count.
    pub worker_threads: Option<usize>,
    /// Where the node-location store lives while it is built. Defaults to
    /// RAM for inputs up to a profile-dependent size, disk beyond it. Both
    /// strategies answer lookups identically and persist identical files.
    pub node_store: Option<NodeStoreStrategy>,
    /// Keep the tagged nodes this accepts in
    /// [`PreparedIngestIndex::tagged_nodes`]. Defaults to keeping none.
    ///
    /// It reads each node's tags borrowed from the block, so only the nodes
    /// it accepts get owned [`Tags`].
    pub keep_node: Option<fn(&TagsRef<'_>) -> bool>,
    /// The owned-tags filter of the deprecated
    /// [`prepare_ingest_index_keeping_nodes`], used when `keep_node` is unset.
    legacy_keep_node: Option<fn(&Tags) -> bool>,
}

impl PrepareOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn worker_threads(mut self, worker_threads: usize) -> Self {
        self.worker_threads = Some(worker_threads);
        self
    }

    pub fn node_store(mut self, strategy: NodeStoreStrategy) -> Self {
        self.node_store = Some(strategy);
        self
    }

    pub fn keep_nodes(mut self, keep_node: fn(&TagsRef<'_>) -> bool) -> Self {
        self.keep_node = Some(keep_node);
        self
    }

    fn node_filter(&self) -> Option<TagFilter> {
        match (self.keep_node, self.legacy_keep_node) {
            (Some(keep_node), _) => Some(TagFilter::Borrowed(keep_node)),
            (None, Some(keep_node)) => Some(TagFilter::Owned(keep_node)),
            (None, None) => None,
        }
    }
}

/// What pass 1 builds: everything pass 2's sinks need to resolve geometry
/// and relation membership.
#[derive(Debug)]
pub struct PreparedIngestIndex {
    pub node_store: NodeLocationStore,
    /// Every relation, sorted by ID with duplicates dropped.
    pub supported_relations: Vec<NormalizedRelation>,
    /// Relation IDs in original PBF traversal order, before the lookup copy is
    /// sorted by ID. Consumers that reproduce source-order joins need this.
    pub relation_input_order: Vec<i64>,
    /// Way ID → the relations that list it as a member.
    pub way_relation_memberships: PackedRelationIndex,
    /// Relation ID → the relations that list it as a member.
    pub parent_relation_memberships: PackedRelationIndex,
    /// Ways that are members of a `type=multipolygon` relation.
    pub multipolygon_way_ids: RoaringTreemap,
    /// The tagged nodes [`PrepareOptions::keep_nodes`] accepted, in file
    /// order; empty without that filter.
    pub tagged_nodes: Vec<NormalizedNode>,
    pub metrics: PreparedIngestMetrics,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedIngestMetrics {
    pub raw_block_read_ms: u64,
    /// Cumulative block-decode time across worker threads. This is summed
    /// wall time over parallel workers, so it exceeds the elapsed streaming
    /// phase whenever more than one worker is busy — compare it against
    /// `stream_wall_ms` times the worker count, not against `stream_wall_ms`.
    pub decode_ms: u64,
    pub index_build_ms: u64,
    pub blocks_read: usize,
    pub indexed_node_objects: usize,
    pub indexed_relation_objects: usize,
    /// Elapsed time from spawning the readers to draining the last chunk —
    /// everything except the finalize below.
    #[serde(default)]
    pub stream_wall_ms: u64,
    /// Always zero now: node locations are inserted by the decode workers
    /// (see `node_insert_ms`), not by the collector. Kept so serialized
    /// metrics from older builds still deserialize.
    #[serde(default)]
    pub collector_node_push_ms: u64,
    /// Time the decode workers spent inserting node locations into the store,
    /// summed across workers like `decode_ms`.
    #[serde(default)]
    pub node_insert_ms: u64,
    /// Of `stream_wall_ms`, collector time spent on relation indexes.
    #[serde(default)]
    pub collector_relation_ms: u64,
    /// Of `stream_wall_ms`, time the collector sat waiting for a chunk. High
    /// means the collector is starved and the bottleneck is upstream; near zero
    /// means the collector itself is the funnel.
    #[serde(default)]
    pub collector_idle_ms: u64,
    /// Sorting, deduplicating and materialising the node store after the last
    /// chunk lands.
    #[serde(default)]
    pub node_store_finalize_ms: u64,
    /// Sorting and deduplicating the retained relations after streaming.
    #[serde(default)]
    pub relation_finalize_ms: u64,
}

/// Pass 1 with the profile's defaults.
pub fn prepare_ingest_index<F, C>(
    pbf_path: &Path,
    build_profile: IngestBuildProfile,
    temp_dir: &Path,
    on_progress: F,
    should_cancel: C,
) -> Result<PreparedIngestIndex, String>
where
    F: FnMut(SharedIngestProgress),
    C: Fn() -> bool,
{
    prepare_ingest_index_with_options(
        pbf_path,
        build_profile,
        temp_dir,
        PrepareOptions::new(),
        on_progress,
        should_cancel,
    )
}

/// [`prepare_ingest_index`], also keeping the tagged nodes for which
/// `keep_node` returns true in [`PreparedIngestIndex::tagged_nodes`]. Pass 1
/// decodes every node anyway, so a caller that needs only these nodes can then
/// skip the node blobs in pass 2 ([`Pass2Options::skip_nodes`](crate::Pass2Options::skip_nodes)).
#[deprecated(
    since = "0.1.0",
    note = "use `prepare_ingest_index_with_options` with `PrepareOptions::keep_nodes`, \
            whose filter reads borrowed tags"
)]
pub fn prepare_ingest_index_keeping_nodes<F, C>(
    pbf_path: &Path,
    build_profile: IngestBuildProfile,
    temp_dir: &Path,
    keep_node: fn(&Tags) -> bool,
    on_progress: F,
    should_cancel: C,
) -> Result<PreparedIngestIndex, String>
where
    F: FnMut(SharedIngestProgress),
    C: Fn() -> bool,
{
    prepare_ingest_index_with_options(
        pbf_path,
        build_profile,
        temp_dir,
        PrepareOptions {
            legacy_keep_node: Some(keep_node),
            ..PrepareOptions::new()
        },
        on_progress,
        should_cancel,
    )
}

/// Pass 1 with an explicit decode-worker count.
pub fn prepare_ingest_index_with_threads<F, C>(
    pbf_path: &Path,
    build_profile: IngestBuildProfile,
    temp_dir: &Path,
    worker_threads: usize,
    on_progress: F,
    should_cancel: C,
) -> Result<PreparedIngestIndex, String>
where
    F: FnMut(SharedIngestProgress),
    C: Fn() -> bool,
{
    prepare_ingest_index_with_options(
        pbf_path,
        build_profile,
        temp_dir,
        PrepareOptions::new().worker_threads(worker_threads),
        on_progress,
        should_cancel,
    )
}

/// Pass 1 with explicit [`PrepareOptions`]; the other `prepare_ingest_index*`
/// functions are shorthands for this. `temp_dir` holds the disk node store
/// while it is built.
pub fn prepare_ingest_index_with_options<F, C>(
    pbf_path: &Path,
    build_profile: IngestBuildProfile,
    temp_dir: &Path,
    options: PrepareOptions,
    mut on_progress: F,
    should_cancel: C,
) -> Result<PreparedIngestIndex, String>
where
    F: FnMut(SharedIngestProgress),
    C: Fn() -> bool,
{
    let worker_threads = options
        .worker_threads
        .unwrap_or_else(|| build_profile.worker_threads());
    validate_worker_count(worker_threads)?;
    info!(
        pbf_path = %pbf_path.display(),
        build_profile = ?build_profile,
        "Starting prepared shared ingest index build"
    );
    let pbf_bytes = std::fs::metadata(pbf_path)
        .map_err(|error| format!("Failed to stat {pbf_path:?}: {error}"))?
        .len();
    let node_strategy = options
        .node_store
        .unwrap_or_else(|| choose_node_store_strategy(pbf_bytes, build_profile.is_mobile()));
    // Shared with the decode workers, which insert each block's nodes as they
    // decode it; the collector never touches node locations.
    let node_builder = Arc::new(NodeLocationStoreBuilder::new(node_strategy, temp_dir)?);
    let started = Instant::now();

    // On a file that does not declare `Sort.Type_then_ID` the classification
    // narrows nothing and every blob is read.
    let blob_index = scan_blob_index(pbf_path)?;
    let wanted_blobs: Vec<_> = blob_index
        .blobs_containing_any(&[ObjectKind::Node, ObjectKind::Relation])
        .copied()
        .collect();
    info!(
        pbf_path = %pbf_path.display(),
        total_blobs = blob_index.len(),
        pass1_blobs = wanted_blobs.len(),
        sorted_type_then_id = blob_index.header().sorted_type_then_id(),
        node_store = ?node_strategy,
        "Planned pass 1 blob read set"
    );

    let keep_node = options.node_filter();
    let worker_builder = Arc::clone(&node_builder);
    let mut collector = Collector {
        read_set_len: wanted_blobs.len(),
        ..Collector::default()
    };
    let stream_started = Instant::now();
    let stats = pipeline::run(
        pbf_path,
        BlobSource::Only(wanted_blobs),
        PipelineConfig {
            worker_threads,
            queue_capacity: build_profile.queue_capacity(),
        },
        move |block, inflated| decode_block(&block, inflated, keep_node, &worker_builder),
        &should_cancel,
        |chunk| collector.add(chunk, &mut on_progress),
    )?;
    let stream_wall_ms = stream_started.elapsed().as_millis() as u64;
    collector.report_final_progress(&mut on_progress);

    // Sorting the node store takes seconds on a region: skip it when the
    // caller cancelled while the last blocks drained.
    if should_cancel() {
        return Err(INGEST_CANCELLED.to_string());
    }
    // The pipeline has joined every worker, so this is the last reference.
    let node_builder = Arc::try_unwrap(node_builder)
        .map_err(|_| "Node-store builder is still shared after pass 1".to_string())?;
    let node_finalize_started = Instant::now();
    let node_store = node_builder.finish()?;
    let node_store_finalize_ms = node_finalize_started.elapsed().as_millis() as u64;

    let relation_finalize_started = Instant::now();
    let Collector {
        mut relations,
        relation_input_order,
        way_relations,
        parent_relations,
        multipolygon_way_ids,
        tagged_nodes,
        counts,
        ..
    } = collector;
    relations.sort_by_key(|relation| relation.relation_id);
    relations.dedup_by_key(|relation| relation.relation_id);
    let relation_finalize_ms = relation_finalize_started.elapsed().as_millis() as u64;

    let metrics = PreparedIngestMetrics {
        raw_block_read_ms: stats.raw_block_read_ms,
        decode_ms: counts.decode_us / 1_000,
        index_build_ms: started.elapsed().as_millis() as u64,
        blocks_read: stats.blocks_read,
        indexed_node_objects: counts.nodes,
        indexed_relation_objects: counts.relations,
        stream_wall_ms,
        collector_node_push_ms: 0,
        node_insert_ms: counts.node_insert_us / 1_000,
        collector_relation_ms: counts.collector_relation_us / 1_000,
        collector_idle_ms: stats.collector_idle_us / 1_000,
        node_store_finalize_ms,
        relation_finalize_ms,
    };
    info!(
        stage = IngestStage::IngestPass1Index.as_str(),
        raw_block_read_ms = metrics.raw_block_read_ms,
        decode_ms = metrics.decode_ms,
        index_build_ms = metrics.index_build_ms,
        stream_wall_ms = metrics.stream_wall_ms,
        node_insert_ms = metrics.node_insert_ms,
        collector_relation_ms = metrics.collector_relation_ms,
        collector_idle_ms = metrics.collector_idle_ms,
        node_store_finalize_ms = metrics.node_store_finalize_ms,
        relation_finalize_ms = metrics.relation_finalize_ms,
        blocks_read = metrics.blocks_read,
        indexed_node_objects = metrics.indexed_node_objects,
        indexed_relation_objects = metrics.indexed_relation_objects,
        "Prepared shared ingest index complete"
    );
    Ok(PreparedIngestIndex {
        node_store,
        supported_relations: relations,
        relation_input_order,
        way_relation_memberships: way_relations.finish(),
        parent_relation_memberships: parent_relations.finish(),
        multipolygon_way_ids,
        tagged_nodes,
        metrics,
    })
}

/// Indexes and classifies a PBF so a pass can read only the blobs it needs.
///
/// A classification failure is not fatal: every blob stays marked as holding
/// everything, so the pass reads them all rather than refusing to run. Only
/// the framing scan, which also proves the file is a readable PBF, and
/// unsupported required features are errors.
pub(crate) fn scan_blob_index(pbf_path: &Path) -> Result<PbfBlobIndex, String> {
    let mut index = PbfBlobIndex::scan(pbf_path).map_err(|error| error.to_string())?;
    let unsupported = index.header().unsupported_required_features();
    if !unsupported.is_empty() {
        return Err(format!(
            "PBF {pbf_path:?} requires unsupported features: {}",
            unsupported.join(", ")
        ));
    }
    match index.classify(pbf_path) {
        Ok(summary) => debug!(
            method = ?summary.method,
            blobs_decompressed = summary.blobs_decompressed,
            blobs_with_nodes = summary.blobs_with_nodes,
            blobs_with_ways = summary.blobs_with_ways,
            blobs_with_relations = summary.blobs_with_relations,
            "Classified PBF blob contents"
        ),
        Err(error) => warn!(
            error = %error,
            "Failed to classify PBF blob contents; reading every blob"
        ),
    }
    Ok(index)
}

/// What a decode worker makes of one block. Its node locations are already
/// in the store.
#[derive(Debug, Default)]
struct Chunk {
    relations: Vec<NormalizedRelation>,
    /// `(way_id, relation_id)` for every way member.
    way_relation_pairs: Vec<(i64, i64)>,
    /// `(member_relation_id, relation_id)` for every relation member.
    parent_relation_pairs: Vec<(i64, i64)>,
    multipolygon_way_ids: Vec<u64>,
    tagged_nodes: Vec<NormalizedNode>,
    node_count: usize,
    // Microseconds: a single block decodes or inserts in well under a
    // millisecond, so summing milliseconds would truncate almost every sample
    // to zero.
    decode_us: u64,
    node_insert_us: u64,
}

fn decode_block(
    block: &RawBlock,
    inflated: &mut Vec<u8>,
    keep_node: Option<TagFilter>,
    node_builder: &NodeLocationStoreBuilder,
) -> Result<Chunk, String> {
    let decode_started = Instant::now();
    let decoded = block.decode(inflated)?;
    let mut chunk = Chunk::default();
    // `(node_id, encoded_lat_lng)`, sharded once the block is done so every
    // shard bucket is allocated at its exact size.
    let mut node_records: Vec<(i64, i64)> = Vec::new();
    let mut pairs: Vec<TagPair<'_>> = Vec::new();

    for group in decoded.groups() {
        node_records.reserve(group.nodes.len() + group.dense.id.len());
        // Tags are only resolved when a filter will read them.
        for_each_node(
            &decoded,
            group,
            keep_node.is_some(),
            &mut pairs,
            |node, pairs, repeat| {
                node_records.push((
                    node.id,
                    encode_decimicro(node.decimicro_lat, node.decimicro_lon),
                ));
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
        for relation in group.relations.iter() {
            let relation = decoded.normalized_relation(relation, &mut pairs)?;
            chunk.index_members(&relation)?;
            chunk.relations.push(relation);
        }
    }
    chunk.node_count = node_records.len();
    chunk.decode_us = decode_started.elapsed().as_micros() as u64;

    let insert_started = Instant::now();
    node_builder.push_chunk(NodeWorkerChunk::from_encoded(&node_records))?;
    chunk.node_insert_us = insert_started.elapsed().as_micros() as u64;
    Ok(chunk)
}

impl Chunk {
    fn index_members(&mut self, relation: &NormalizedRelation) -> Result<(), String> {
        let is_multipolygon = relation.tags.get("type") == Some("multipolygon");
        for member in &relation.members {
            match member.member_type {
                RelationMemberType::Way => {
                    self.way_relation_pairs
                        .push((member.member_id, relation.relation_id));
                    if is_multipolygon {
                        let way_id = u64::try_from(member.member_id).map_err(|_| {
                            format!("OSM identifier {} must be non-negative", member.member_id)
                        })?;
                        self.multipolygon_way_ids.push(way_id);
                    }
                }
                RelationMemberType::Relation => self
                    .parent_relation_pairs
                    .push((member.member_id, relation.relation_id)),
                RelationMemberType::Node => {}
            }
        }
        Ok(())
    }
}

/// Gathers the chunks in file order.
#[derive(Default)]
struct Collector {
    relations: Vec<NormalizedRelation>,
    relation_input_order: Vec<i64>,
    way_relations: PackedRelationIndexBuilder,
    parent_relations: PackedRelationIndexBuilder,
    multipolygon_way_ids: RoaringTreemap,
    tagged_nodes: Vec<NormalizedNode>,
    counts: Counts,
    /// One chunk per block, so `chunks` over this is the share handled.
    read_set_len: usize,
    chunks: usize,
    last_reported: usize,
}

#[derive(Default)]
struct Counts {
    nodes: usize,
    relations: usize,
    decode_us: u64,
    node_insert_us: u64,
    collector_relation_us: u64,
}

impl Collector {
    fn add(
        &mut self,
        chunk: Chunk,
        on_progress: &mut impl FnMut(SharedIngestProgress),
    ) -> Result<(), String> {
        let relation_started = Instant::now();
        self.way_relations.extend(chunk.way_relation_pairs);
        self.parent_relations.extend(chunk.parent_relation_pairs);
        self.multipolygon_way_ids.extend(chunk.multipolygon_way_ids);
        self.relation_input_order
            .extend(chunk.relations.iter().map(|relation| relation.relation_id));
        self.counts.relations += chunk.relations.len();
        self.relations.extend(chunk.relations);
        self.tagged_nodes.extend(chunk.tagged_nodes);
        self.counts.collector_relation_us += relation_started.elapsed().as_micros() as u64;

        self.counts.nodes += chunk.node_count;
        self.counts.decode_us += chunk.decode_us;
        self.counts.node_insert_us += chunk.node_insert_us;
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
        let Counts {
            nodes, relations, ..
        } = self.counts;
        on_progress(SharedIngestProgress {
            stage: IngestStage::IngestPass1Index,
            batches_read: self.chunks,
            objects_read: nodes + relations,
            message: format!("Indexed {nodes} node locations and {relations} relations"),
            fraction: Some(read_fraction(self.chunks, self.read_set_len)),
        });
        self.last_reported = self.chunks;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_pbf::{
        TestRelation, TestRelationMember, header_frame_len, pbf_file, relation_block,
        sorted_fixture, write_fixture,
    };
    use osmpbfreader::osmformat::relation::MemberType;

    fn prepare(path: &Path, temp_dir: &Path) -> PreparedIngestIndex {
        prepare_ingest_index(
            path,
            IngestBuildProfile::DesktopFull,
            temp_dir,
            |_progress| {},
            || false,
        )
        .expect("prepare ingest index")
    }

    #[test]
    fn explicit_worker_budget_rejects_zero_before_opening_input() {
        let result = prepare_ingest_index_with_threads(
            Path::new("missing.pbf"),
            IngestBuildProfile::DesktopFull,
            Path::new("unused"),
            0,
            |_| {},
            || false,
        );
        assert!(result.unwrap_err().contains("worker count"));
    }

    #[test]
    fn pass1_skips_way_blobs_without_losing_nodes_or_relations() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_fixture(dir.path(), &sorted_fixture());
        let prepared = prepare(&path, dir.path());

        // Every node location survived, including those referenced only by the
        // ways in the blob the reader skipped.
        for (node_id, lat, lng) in [
            (1i64, 50.0, 19.0),
            (2, 50.1, 19.1),
            (3, 50.2, 19.2),
            (4, 50.3, 19.3),
        ] {
            let (found_lat, found_lng) = prepared
                .node_store
                .get(node_id)
                .unwrap_or_else(|| panic!("node {node_id} missing from store"));
            assert!((found_lat - lat).abs() < 1e-6, "node {node_id} latitude");
            assert!((found_lng - lng).abs() < 1e-6, "node {node_id} longitude");
        }

        assert_eq!(prepared.supported_relations.len(), 1);
        assert_eq!(prepared.supported_relations[0].relation_id, 500);
        assert_eq!(prepared.supported_relations[0].members.len(), 2);

        // The way→relation index is what pass 2 uses to decide a way is worth
        // keeping; it is built from relation members, so skipping way blobs
        // must not disturb it.
        assert!(prepared.way_relation_memberships.contains(100));
        assert!(prepared.way_relation_memberships.contains(101));
        assert!(prepared.multipolygon_way_ids.contains(100));
    }

    #[test]
    fn prepared_index_retains_original_relation_order_before_id_sort() {
        let dir = tempfile::tempdir().expect("temp dir");
        let member = [TestRelationMember {
            member_type: MemberType::WAY,
            member_id: 100,
            role: "",
        }];
        let tags = &[("route", "hiking"), ("network", "rwn")];
        let bytes = pbf_file(
            false,
            &[relation_block(&[
                TestRelation {
                    id: 42,
                    members: &member,
                    tags,
                },
                TestRelation {
                    id: 5,
                    members: &member,
                    tags,
                },
            ])],
        );
        let path = write_fixture(dir.path(), &bytes);
        let prepared = prepare(&path, dir.path());
        assert_eq!(prepared.relation_input_order, [42, 5]);
        assert_eq!(
            prepared
                .supported_relations
                .iter()
                .map(|relation| relation.relation_id)
                .collect::<Vec<_>>(),
            [5, 42]
        );
    }

    #[test]
    fn pass1_reads_fewer_blobs_than_the_file_contains() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_fixture(dir.path(), &sorted_fixture());
        let prepared = prepare(&path, dir.path());

        // Five data blobs: two node, two way, one relation. Pass 1 reads the
        // node blobs, the relation blob, and the first way blob because it sits
        // on the seam — but not the interior way blob.
        assert_eq!(prepared.metrics.blocks_read, 4);
    }

    #[test]
    fn unsorted_files_still_ingest_every_object() {
        // Without `Sort.Type_then_ID` no narrowing is sound, so the reader must
        // fall back to every blob rather than trusting a layout the file never
        // promised.
        let dir = tempfile::tempdir().expect("temp dir");
        let sorted = sorted_fixture();
        let unsorted = {
            let mut bytes = pbf_file(false, &[]);
            // Re-frame the same blocks without the sort declaration.
            bytes.extend_from_slice(&sorted[header_frame_len(&sorted)..]);
            bytes
        };
        let path = write_fixture(dir.path(), &unsorted);
        let prepared = prepare(&path, dir.path());

        assert_eq!(prepared.metrics.blocks_read, 5);
        assert_eq!(prepared.supported_relations.len(), 1);
        for node_id in [1i64, 2, 3, 4] {
            assert!(
                prepared.node_store.get(node_id).is_some(),
                "node {node_id} missing from store"
            );
        }
    }

    #[test]
    fn keeps_the_tagged_nodes_the_filter_accepts() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_fixture(dir.path(), &sorted_fixture());
        let keep = |options: PrepareOptions| {
            prepare_ingest_index_with_options(
                &path,
                IngestBuildProfile::DesktopFull,
                dir.path(),
                options,
                |_progress| {},
                || false,
            )
            .expect("prepare ingest index")
            .tagged_nodes
            .into_iter()
            .map(|node| (node.node_id, node.tags))
            .collect::<Vec<_>>()
        };
        let borrowed = keep(PrepareOptions::new().keep_nodes(|tags| tags.contains_key("amenity")));
        let owned = keep(PrepareOptions {
            legacy_keep_node: Some(|tags| tags.contains_key("amenity")),
            ..PrepareOptions::new()
        });
        assert_eq!(borrowed.iter().map(|node| node.0).collect::<Vec<_>>(), [2]);
        assert_eq!(borrowed, owned);
        assert!(keep(PrepareOptions::new()).is_empty());
    }

    #[test]
    fn ram_and_disk_node_stores_give_the_same_pass1_result() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_fixture(dir.path(), &sorted_fixture());
        let run = |strategy: NodeStoreStrategy, name: &str| {
            let temp = dir.path().join(name);
            std::fs::create_dir_all(&temp).expect("temp dir");
            let prepared = prepare_ingest_index_with_options(
                &path,
                IngestBuildProfile::DesktopFull,
                &temp,
                PrepareOptions::new().node_store(strategy).worker_threads(3),
                |_progress| {},
                || false,
            )
            .expect("prepare ingest index");
            assert!(matches!(
                (&prepared.node_store, strategy),
                (NodeLocationStore::Ram(_), NodeStoreStrategy::Ram)
                    | (NodeLocationStore::Disk(_), NodeStoreStrategy::Disk)
            ));
            let lookups = (0..=5)
                .map(|id| prepared.node_store.get(id))
                .collect::<Vec<_>>();
            let dest = dir.path().join(format!("{name}-persisted"));
            prepared.node_store.persist_to(&dest).expect("persist");
            (lookups, dest)
        };
        let (ram_lookups, ram_dir) = run(NodeStoreStrategy::Ram, "ram");
        let (disk_lookups, disk_dir) = run(NodeStoreStrategy::Disk, "disk");
        assert_eq!(ram_lookups, disk_lookups);
        assert_eq!(ram_lookups.iter().flatten().count(), 4);
        for entry in std::fs::read_dir(&ram_dir).expect("read persisted store") {
            let name = entry.expect("entry").file_name();
            assert_eq!(
                std::fs::read(ram_dir.join(&name)).expect("ram file"),
                std::fs::read(disk_dir.join(&name)).expect("disk file"),
                "{name:?} differs between strategies"
            );
        }
    }

    #[test]
    fn reports_progress_after_the_last_chunk() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_fixture(dir.path(), &sorted_fixture());
        let mut reports = Vec::new();
        prepare_ingest_index(
            &path,
            IngestBuildProfile::DesktopFull,
            dir.path(),
            |progress| {
                reports.push((
                    progress.batches_read,
                    progress.objects_read,
                    progress.fraction,
                ))
            },
            || false,
        )
        .expect("prepare ingest index");
        // Four chunks, fewer than ten: one report, after the last, with the
        // whole read set handled.
        assert_eq!(reports, [(4, 5, Some(1.0))]);
    }

    #[test]
    fn a_cancel_while_the_last_blocks_drain_skips_the_node_store_sort() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_fixture(dir.path(), &sorted_fixture());
        // The fixture's only progress report comes after the pipeline has
        // drained, so this cancel arrives after the last block.
        let drained = std::cell::Cell::new(false);
        let error = prepare_ingest_index(
            &path,
            IngestBuildProfile::DesktopFull,
            dir.path(),
            |_progress| drained.set(true),
            || drained.get(),
        )
        .expect_err("cancelled");
        assert_eq!(error, INGEST_CANCELLED);
    }
}
