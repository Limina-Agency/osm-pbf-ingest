//! Build profiles: the defaults a pass falls back to when the caller does not
//! set them.

use serde::{Deserialize, Serialize};

/// The kind of machine an ingest runs on. It sets the defaults for decode
/// workers, queue depth and the node store; [`PrepareOptions`](crate::PrepareOptions)
/// and [`Pass2Options`](crate::Pass2Options) override them one by one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestBuildProfile {
    /// All but two cores (at least two), deep queues, and the RAM node store
    /// for inputs up to 32 MiB.
    DesktopFull,
    /// At most two cores, shallow queues, and the RAM node store for inputs up
    /// to 24 MiB.
    MobileOperational,
}

impl IngestBuildProfile {
    /// The decode workers a pass uses when the caller sets no count
    /// ([`PrepareOptions::worker_threads`](crate::PrepareOptions::worker_threads),
    /// [`Pass2Options::worker_threads`](crate::Pass2Options::worker_threads)):
    /// all but two of the available cores (at least two) for `DesktopFull`,
    /// at most two for `MobileOperational`.
    pub fn worker_threads(self) -> usize {
        let available = std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(2);
        match self {
            Self::DesktopFull => available.saturating_sub(2).max(2),
            Self::MobileOperational => available.min(2),
        }
    }

    /// Blocks in flight between each pipeline stage: read but not decoded,
    /// and decoded but not collected.
    pub(crate) fn queue_capacity(self) -> usize {
        match self {
            Self::DesktopFull => 16,
            Self::MobileOperational => 4,
        }
    }

    pub(crate) fn is_mobile(self) -> bool {
        matches!(self, Self::MobileOperational)
    }
}

/// Rejects a zero worker count before any file is opened.
pub(crate) fn validate_worker_count(workers: usize) -> Result<(), String> {
    if workers == 0 {
        Err("OSM decode worker count must be at least one".to_string())
    } else {
        Ok(())
    }
}
