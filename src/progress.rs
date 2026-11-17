//! Progress reports and summaries the passes hand back.

use serde::{Deserialize, Serialize};

/// The error every pass returns when `should_cancel` stopped it, so a caller
/// can tell a cancelled ingest from a failed one.
pub const INGEST_CANCELLED: &str = "Shared ingest cancelled";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestStage {
    /// No longer reported: a stage of the removed three-pass shared ingest,
    /// kept so serialized metrics that name it still deserialize.
    Pass1Relations,
    /// No longer reported; see [`Pass1Relations`](Self::Pass1Relations).
    Pass2Ways,
    /// No longer reported; see [`Pass1Relations`](Self::Pass1Relations).
    Pass3Nodes,
    /// Pass 1: node locations and relation indexes.
    IngestPass1Index,
    /// Pass 2: tagged nodes handed to the sink.
    IngestPass2Nodes,
    /// Pass 2: ways handed to the sink.
    IngestPass2Ways,
    /// Pass 2: relations handed to the sink.
    IngestPass2Relations,
}

impl IngestStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pass1Relations => "pass1_relations",
            Self::Pass2Ways => "pass2_ways",
            Self::Pass3Nodes => "pass3_nodes",
            Self::IngestPass1Index => "ingest_pass1_index",
            Self::IngestPass2Nodes => "ingest_pass2_nodes",
            Self::IngestPass2Ways => "ingest_pass2_ways",
            Self::IngestPass2Relations => "ingest_pass2_relations",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestStageMetric {
    pub stage: IngestStage,
    /// Time spent in the sink for this stage's batches.
    pub elapsed_ms: u64,
    pub batches_read: usize,
    pub objects_read: usize,
}

/// What pass 2 handed to its sink, and how long it took.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SharedIngestSummary {
    pub relation_batches: usize,
    pub relation_objects: usize,
    pub way_batches: usize,
    pub way_objects: usize,
    pub node_batches: usize,
    pub node_objects: usize,
    #[serde(default)]
    pub raw_block_read_ms: u64,
    /// Summed across decode workers.
    #[serde(default)]
    pub decode_ms: u64,
    #[serde(default)]
    pub stage_metrics: Vec<IngestStageMetric>,
}

/// A progress report, sent every tenth batch of a stage. Pass 1 also sends
/// one after its last block.
#[derive(Debug, Clone)]
pub struct SharedIngestProgress {
    pub stage: IngestStage,
    pub batches_read: usize,
    pub objects_read: usize,
    pub message: String,
    /// Share of the pass's blobs handled, 0 to 1, when the pass plans its read
    /// set up front (pass 1, and pass 2 with
    /// [`Pass2Options::skip_nodes`](crate::Pass2Options::skip_nodes)); `None`
    /// for a pass that streams the whole file.
    pub fraction: Option<f64>,
}

/// Whether the batch with this 1-based index gets a progress report.
pub(crate) fn reports_progress(batch_index: usize) -> bool {
    batch_index.is_multiple_of(10)
}

/// Blobs handled over the pass's read set, 0 to 1.
pub(crate) fn read_fraction(handled: usize, read_set_len: usize) -> f64 {
    if read_set_len == 0 {
        1.0
    } else {
        (handled as f64 / read_set_len as f64).min(1.0)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn reports_every_tenth_batch() {
        assert!(!super::reports_progress(1));
        assert!(!super::reports_progress(9));
        assert!(super::reports_progress(10));
        assert!(super::reports_progress(20));
    }

    #[test]
    fn read_fraction_is_a_share_capped_at_one() {
        assert_eq!(super::read_fraction(0, 4), 0.0);
        assert_eq!(super::read_fraction(1, 4), 0.25);
        assert_eq!(super::read_fraction(5, 4), 1.0);
        // An empty read set is done before it starts.
        assert_eq!(super::read_fraction(0, 0), 1.0);
    }
}
