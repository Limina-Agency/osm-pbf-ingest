//! Pass 2: every object handed to a sink, block by block in file order.
//!
//! Pass 2 takes no prepared index: it hands out the objects as they are, and
//! each sink resolves whatever geometry it needs from its own copy of the
//! node store.

use osmpbfreader::Tags;
use std::path::Path;
use std::time::Instant;
use tracing::{debug, info};

use crate::blob_index::ObjectKind;
use crate::model::{NodeBatch, NormalizedNode, RelationBatch, WayBatch};
use crate::osm_block::{TagPair, for_each_node};
use crate::pass1::scan_blob_index;
use crate::pipeline::{self, BlobSource, PipelineConfig, RawBlock};
use crate::profile::{IngestBuildProfile, validate_worker_count};
use crate::progress::{
    IngestStage, IngestStageMetric, SharedIngestProgress, SharedIngestSummary, read_fraction,
    reports_progress,
};
use crate::tags::{Repeat, TagFilter, TagsRef};

/// Receives pass 2 batches by value, so a sink can hand them to other
/// threads. Each call gets one block's objects of one kind, never an empty
/// batch; within a block, nodes come before ways and ways before relations.
pub trait Pass2Sink {
    fn on_relation_batch(&mut self, batch: RelationBatch) -> Result<(), String>;

    fn on_way_batch(&mut self, batch: WayBatch) -> Result<(), String>;

    fn on_node_batch(&mut self, batch: NodeBatch) -> Result<(), String>;
}

/// Settings for [`run_pass2_with_options`]. Left unset, pass 2 uses the
/// profile's worker count and streams every node, way and relation.
///
/// ```
/// use osm_pbf_ingest::Pass2Options;
///
/// // A caller that kept its nodes in pass 1 and wants only roads.
/// let options = Pass2Options::new()
///     .skip_nodes()
///     .keep_ways(|tags| tags.contains_key("highway"));
/// # let _ = options;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct Pass2Options {
    /// Decode workers. Defaults to the profile's worker count.
    pub worker_threads: Option<usize>,
    /// Read only the blobs that hold ways or relations, and hand the sink no
    /// nodes: for a caller that kept the nodes it needs in pass 1
    /// ([`PrepareOptions::keep_nodes`](crate::PrepareOptions::keep_nodes)).
    pub skip_nodes: bool,
    /// Hand the sink only the ways this accepts. Defaults to every way.
    ///
    /// It reads each way's tags borrowed from the block, so only the ways it
    /// accepts get owned [`Tags`].
    pub keep_way: Option<fn(&TagsRef<'_>) -> bool>,
}

impl Pass2Options {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn worker_threads(mut self, worker_threads: usize) -> Self {
        self.worker_threads = Some(worker_threads);
        self
    }

    pub fn skip_nodes(mut self) -> Self {
        self.skip_nodes = true;
        self
    }

    pub fn keep_ways(mut self, keep_way: fn(&TagsRef<'_>) -> bool) -> Self {
        self.keep_way = Some(keep_way);
        self
    }
}

/// Pass 2 over every object, with the profile's defaults.
pub fn run_pass2<S, F, C>(
    pbf_path: &Path,
    build_profile: IngestBuildProfile,
    sink: &mut S,
    on_progress: F,
    should_cancel: C,
) -> Result<SharedIngestSummary, String>
where
    S: Pass2Sink + ?Sized,
    F: FnMut(SharedIngestProgress),
    C: Fn() -> bool,
{
    run_pass2_with_options(
        pbf_path,
        build_profile,
        Pass2Options::new(),
        sink,
        on_progress,
        should_cancel,
    )
}

/// Pass 2 for a caller that took the tagged nodes it needs from pass 1: reads
/// only the blobs that hold ways or relations, hands `sink` no nodes, and only
/// the ways for which `keep_way` returns true.
#[deprecated(
    since = "0.1.0",
    note = "use `run_pass2_with_options` with `Pass2Options::skip_nodes` and \
            `Pass2Options::keep_ways`, whose filter reads borrowed tags"
)]
pub fn run_pass2_ways_and_relations<S, F, C>(
    pbf_path: &Path,
    build_profile: IngestBuildProfile,
    keep_way: fn(&Tags) -> bool,
    sink: &mut S,
    on_progress: F,
    should_cancel: C,
) -> Result<SharedIngestSummary, String>
where
    S: Pass2Sink + ?Sized,
    F: FnMut(SharedIngestProgress),
    C: Fn() -> bool,
{
    run(
        pbf_path,
        build_profile,
        build_profile.worker_threads(),
        Selection {
            skip_nodes: true,
            keep_way: TagFilter::Owned(keep_way),
        },
        sink,
        on_progress,
        should_cancel,
    )
}

/// Pass 2 over every object, with an explicit decode-worker count.
pub fn run_pass2_with_threads<S, F, C>(
    pbf_path: &Path,
    build_profile: IngestBuildProfile,
    worker_threads: usize,
    sink: &mut S,
    on_progress: F,
    should_cancel: C,
) -> Result<SharedIngestSummary, String>
where
    S: Pass2Sink + ?Sized,
    F: FnMut(SharedIngestProgress),
    C: Fn() -> bool,
{
    run_pass2_with_options(
        pbf_path,
        build_profile,
        Pass2Options::new().worker_threads(worker_threads),
        sink,
        on_progress,
        should_cancel,
    )
}

/// Pass 2 with explicit [`Pass2Options`]; the other `run_pass2*` functions
/// are shorthands for this. Objects arrive in file order.
pub fn run_pass2_with_options<S, F, C>(
    pbf_path: &Path,
    build_profile: IngestBuildProfile,
    options: Pass2Options,
    sink: &mut S,
    on_progress: F,
    should_cancel: C,
) -> Result<SharedIngestSummary, String>
where
    S: Pass2Sink + ?Sized,
    F: FnMut(SharedIngestProgress),
    C: Fn() -> bool,
{
    run(
        pbf_path,
        build_profile,
        options
            .worker_threads
            .unwrap_or_else(|| build_profile.worker_threads()),
        Selection {
            skip_nodes: options.skip_nodes,
            keep_way: options.keep_way.map_or(TagFilter::All, TagFilter::Borrowed),
        },
        sink,
        on_progress,
        should_cancel,
    )
}

/// Which objects pass 2 reads and hands to its sink.
#[derive(Debug, Clone, Copy)]
struct Selection {
    /// Read only the blobs that hold ways or relations, and no nodes.
    skip_nodes: bool,
    keep_way: TagFilter,
}

fn run<S, F, C>(
    pbf_path: &Path,
    build_profile: IngestBuildProfile,
    worker_threads: usize,
    selection: Selection,
    sink: &mut S,
    on_progress: F,
    should_cancel: C,
) -> Result<SharedIngestSummary, String>
where
    S: Pass2Sink + ?Sized,
    F: FnMut(SharedIngestProgress),
    C: Fn() -> bool,
{
    validate_worker_count(worker_threads)?;
    info!(
        pbf_path = %pbf_path.display(),
        build_profile = ?build_profile,
        "Starting prepared shared ingest pass 2"
    );
    let started = Instant::now();
    // As in pass 1: on a file not sorted by type the classification narrows
    // nothing and every blob is read.
    let source = if selection.skip_nodes {
        let blob_index = scan_blob_index(pbf_path)?;
        let wanted = blob_index
            .blobs_containing_any(&[ObjectKind::Way, ObjectKind::Relation])
            .copied()
            .collect();
        BlobSource::Only(wanted)
    } else {
        BlobSource::All
    };
    let read_set_len = match &source {
        BlobSource::Only(blobs) => Some(blobs.len()),
        BlobSource::All => None,
    };
    let mut emitter = Emitter {
        sink,
        on_progress,
        summary: SharedIngestSummary::default(),
        emit_ms: [0; 3],
        decode_us: 0,
        blocks: 0,
        read_set_len,
    };
    let stats = pipeline::run(
        pbf_path,
        source,
        PipelineConfig {
            worker_threads,
            queue_capacity: build_profile.queue_capacity(),
        },
        move |block, inflated| decode_block(&block, inflated, selection),
        &should_cancel,
        |block| emitter.emit(block),
    )?;

    let Emitter {
        mut summary,
        emit_ms: [node_emit_ms, way_emit_ms, relation_emit_ms],
        decode_us,
        ..
    } = emitter;
    summary.raw_block_read_ms = stats.raw_block_read_ms;
    summary.decode_ms = decode_us / 1_000;
    summary.stage_metrics = vec![
        IngestStageMetric {
            stage: IngestStage::IngestPass2Nodes,
            elapsed_ms: node_emit_ms,
            batches_read: summary.node_batches,
            objects_read: summary.node_objects,
        },
        IngestStageMetric {
            stage: IngestStage::IngestPass2Ways,
            elapsed_ms: way_emit_ms,
            batches_read: summary.way_batches,
            objects_read: summary.way_objects,
        },
        IngestStageMetric {
            stage: IngestStage::IngestPass2Relations,
            elapsed_ms: relation_emit_ms,
            batches_read: summary.relation_batches,
            objects_read: summary.relation_objects,
        },
    ];
    for metric in &summary.stage_metrics {
        debug!(
            stage = metric.stage.as_str(),
            batches = metric.batches_read,
            objects = metric.objects_read,
            "Completed prepared pass 2 stage"
        );
    }
    info!(
        total_wall_ms = started.elapsed().as_millis() as u64,
        raw_block_read_ms = summary.raw_block_read_ms,
        decode_ms = summary.decode_ms,
        blocks_read = stats.blocks_read,
        node_batches = summary.node_batches,
        way_batches = summary.way_batches,
        relation_batches = summary.relation_batches,
        "Prepared shared ingest pass 2 complete"
    );
    Ok(summary)
}

/// One block's objects, sorted by kind.
#[derive(Debug)]
struct DecodedBlock {
    nodes: NodeBatch,
    ways: WayBatch,
    relations: RelationBatch,
    decode_us: u64,
}

fn decode_block(
    block: &RawBlock,
    inflated: &mut Vec<u8>,
    selection: Selection,
) -> Result<DecodedBlock, String> {
    let decode_started = Instant::now();
    let decoded = block.decode(inflated)?;
    let mut nodes = NodeBatch::default();
    let mut ways = WayBatch::default();
    let mut relations = RelationBatch::default();
    let mut pairs: Vec<TagPair<'_>> = Vec::new();

    for group in decoded.groups() {
        if !selection.skip_nodes {
            // Only tagged nodes are handed out; their locations are pass 1's.
            for_each_node(&decoded, group, true, &mut pairs, |node, pairs, repeat| {
                if !pairs.is_empty() {
                    nodes.push(NormalizedNode::from_decimicro(
                        node.id,
                        node.decimicro_lat,
                        node.decimicro_lon,
                        TagsRef::new(pairs, repeat).to_tags(),
                    ));
                }
                Ok(())
            })?;
        }
        for way in group.ways.iter() {
            decoded.tag_pairs(&way.keys, &way.vals, &mut pairs)?;
            if let Some(tags) = selection.keep_way.keep(&pairs, Repeat::FirstWins) {
                ways.push(decoded.normalized_way(way, tags));
            }
        }
        for relation in group.relations.iter() {
            relations.push(decoded.normalized_relation(relation, &mut pairs)?);
        }
    }

    Ok(DecodedBlock {
        nodes,
        ways,
        relations,
        decode_us: decode_started.elapsed().as_micros() as u64,
    })
}

/// Hands decoded blocks to the sink in file order and keeps the tallies.
struct Emitter<'s, S: ?Sized, F> {
    sink: &'s mut S,
    on_progress: F,
    summary: SharedIngestSummary,
    /// Time spent in the sink for nodes, ways and relations.
    emit_ms: [u64; 3],
    decode_us: u64,
    /// Blocks handled so far, and the planned read set when there is one.
    blocks: usize,
    read_set_len: Option<usize>,
}

impl<S, F> Emitter<'_, S, F>
where
    S: Pass2Sink + ?Sized,
    F: FnMut(SharedIngestProgress),
{
    fn emit(&mut self, block: DecodedBlock) -> Result<(), String> {
        self.decode_us += block.decode_us;
        self.blocks += 1;
        if !block.nodes.nodes.is_empty() {
            let objects = block.nodes.nodes.len();
            let emit_started = Instant::now();
            self.sink.on_node_batch(block.nodes)?;
            self.emit_ms[0] += emit_started.elapsed().as_millis() as u64;
            let summary = &mut self.summary;
            summary.node_batches += 1;
            summary.node_objects += objects;
            let (batches, objects) = (summary.node_batches, summary.node_objects);
            self.report(
                IngestStage::IngestPass2Nodes,
                batches,
                objects,
                "tagged node",
            );
        }
        if !block.ways.ways.is_empty() {
            let objects = block.ways.ways.len();
            let emit_started = Instant::now();
            self.sink.on_way_batch(block.ways)?;
            self.emit_ms[1] += emit_started.elapsed().as_millis() as u64;
            let summary = &mut self.summary;
            summary.way_batches += 1;
            summary.way_objects += objects;
            let (batches, objects) = (summary.way_batches, summary.way_objects);
            self.report(IngestStage::IngestPass2Ways, batches, objects, "way");
        }
        if !block.relations.relations.is_empty() {
            let objects = block.relations.relations.len();
            let emit_started = Instant::now();
            self.sink.on_relation_batch(block.relations)?;
            self.emit_ms[2] += emit_started.elapsed().as_millis() as u64;
            let summary = &mut self.summary;
            summary.relation_batches += 1;
            summary.relation_objects += objects;
            let (batches, objects) = (summary.relation_batches, summary.relation_objects);
            self.report(
                IngestStage::IngestPass2Relations,
                batches,
                objects,
                "relation",
            );
        }
        Ok(())
    }

    fn report(&mut self, stage: IngestStage, batches: usize, objects: usize, label: &str) {
        if reports_progress(batches) {
            (self.on_progress)(SharedIngestProgress {
                stage,
                batches_read: batches,
                objects_read: objects,
                message: format!("Shared ingest scanned {objects} {label} objects"),
                fraction: self.read_set_len.map(|len| read_fraction(self.blocks, len)),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress::INGEST_CANCELLED;
    use crate::test_pbf::{
        TestNode, TestWay, node_block, pbf_file, sorted_fixture, way_block, write_fixture,
    };

    #[derive(Default)]
    struct Collected {
        node_ids: Vec<i64>,
        way_ids: Vec<i64>,
        relation_ids: Vec<i64>,
    }

    impl Pass2Sink for Collected {
        fn on_relation_batch(&mut self, batch: RelationBatch) -> Result<(), String> {
            self.relation_ids
                .extend(batch.relations.iter().map(|relation| relation.relation_id));
            Ok(())
        }

        fn on_way_batch(&mut self, batch: WayBatch) -> Result<(), String> {
            self.way_ids.extend(batch.ways.iter().map(|way| way.way_id));
            Ok(())
        }

        fn on_node_batch(&mut self, batch: NodeBatch) -> Result<(), String> {
            self.node_ids
                .extend(batch.nodes.iter().map(|node| node.node_id));
            Ok(())
        }
    }

    #[test]
    fn streams_every_tagged_node_way_and_relation_in_file_order() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_fixture(dir.path(), &sorted_fixture());
        let mut full = Collected::default();
        let summary = run_pass2(
            &path,
            IngestBuildProfile::DesktopFull,
            &mut full,
            |_progress| {},
            || false,
        )
        .expect("pass 2");
        assert_eq!(full.node_ids, [2]);
        assert_eq!(full.way_ids, [100, 101]);
        assert_eq!(full.relation_ids, [500]);
        assert_eq!(
            (
                summary.node_batches,
                summary.way_batches,
                summary.relation_batches
            ),
            (1, 2, 1)
        );
    }

    #[test]
    fn reports_its_share_of_the_read_set_only_when_it_planned_one() {
        let dir = tempfile::tempdir().expect("temp dir");
        // One node block, then twenty one-way blocks: progress is reported
        // at the tenth and twentieth way batch.
        let mut blocks = vec![node_block(&[TestNode {
            id: 1,
            lat: 50.0,
            lng: 19.0,
            tags: &[],
        }])];
        blocks.extend((0..20).map(|id| {
            way_block(&[TestWay {
                id: 100 + id,
                node_ids: &[1],
                tags: &[("highway", "path")],
            }])
        }));
        let path = write_fixture(dir.path(), &pbf_file(true, &blocks));

        let fractions = |options: Pass2Options| {
            let mut fractions = Vec::new();
            run_pass2_with_options(
                &path,
                IngestBuildProfile::DesktopFull,
                options,
                &mut Collected::default(),
                |progress| fractions.push(progress.fraction),
                || false,
            )
            .expect("pass 2");
            fractions
        };
        // Skipping nodes reads just the twenty way blocks.
        assert_eq!(
            fractions(Pass2Options::new().skip_nodes()),
            [Some(0.5), Some(1.0)]
        );
        // The whole-file pass has no planned read set.
        assert_eq!(fractions(Pass2Options::new()), [None, None]);
    }

    #[test]
    fn a_cancel_returns_ingest_cancelled() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_fixture(dir.path(), &sorted_fixture());
        let error = run_pass2(
            &path,
            IngestBuildProfile::DesktopFull,
            &mut Collected::default(),
            |_progress| {},
            || true,
        )
        .expect_err("cancelled");
        assert_eq!(error, INGEST_CANCELLED);
    }

    #[test]
    fn skips_nodes_and_filters_ways() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_fixture(dir.path(), &sorted_fixture());
        let borrowed = {
            let mut ways = Collected::default();
            let summary = run_pass2_with_options(
                &path,
                IngestBuildProfile::DesktopFull,
                Pass2Options::new()
                    .skip_nodes()
                    .keep_ways(|tags| tags.contains_key("highway")),
                &mut ways,
                |_progress| {},
                || false,
            )
            .expect("pass 2 over ways and relations");
            assert_eq!(summary.node_batches, 0);
            ways
        };
        assert!(borrowed.node_ids.is_empty());
        assert_eq!(borrowed.way_ids, [100]);
        assert_eq!(borrowed.relation_ids, [500]);

        let mut owned = Collected::default();
        #[allow(deprecated)]
        run_pass2_ways_and_relations(
            &path,
            IngestBuildProfile::DesktopFull,
            |tags| tags.contains_key("highway"),
            &mut owned,
            |_progress| {},
            || false,
        )
        .expect("pass 2 over ways and relations");
        assert_eq!(
            (owned.node_ids, owned.way_ids, owned.relation_ids),
            (borrowed.node_ids, borrowed.way_ids, borrowed.relation_ids)
        );
    }

    #[test]
    fn a_sink_error_ends_the_pass() {
        struct Failing;
        impl Pass2Sink for Failing {
            fn on_relation_batch(&mut self, _: RelationBatch) -> Result<(), String> {
                Ok(())
            }
            fn on_way_batch(&mut self, _: WayBatch) -> Result<(), String> {
                Err("sink is full".to_string())
            }
            fn on_node_batch(&mut self, _: NodeBatch) -> Result<(), String> {
                Ok(())
            }
        }
        let dir = tempfile::tempdir().expect("temp dir");
        let path = write_fixture(dir.path(), &sorted_fixture());
        let error = run_pass2(
            &path,
            IngestBuildProfile::DesktopFull,
            &mut Failing,
            |_progress| {},
            || false,
        )
        .unwrap_err();
        assert_eq!(error, "sink is full");
    }
}
