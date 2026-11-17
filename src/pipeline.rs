//! The threads every pass runs: one reader, a pool of decode workers, and the
//! calling thread collecting the decoded blocks in file order.
//!
//! ```text
//! reader ──blobs──▶ workers (decode) ──results──▶ collector (in order)
//! ```
//!
//! The reader numbers each blob it sends. Workers finish out of order, so the
//! collector holds early results until every earlier one has arrived. Both
//! queues are bounded, so a slow collector stalls the workers and then the
//! reader rather than buffering the file.
//!
//! The first error, a cancellation, or the end of the read set stops every
//! thread, and the pipeline returns only once all of them have exited. A
//! panic in `decode` is reported as that block's error.

use crossbeam_channel::{Receiver, RecvTimeoutError, SendTimeoutError, Sender, bounded};
use osmpbfreader::OsmPbfReader;
use osmpbfreader::fileformat::Blob;
use std::any::Any;
use std::collections::BTreeMap;
use std::fs::File;
use std::panic::{self, AssertUnwindSafe};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::blob_index::{BlobLocation, blob_bytes, read_blob_at};
use crate::osm_block::Block;
use crate::progress::INGEST_CANCELLED;

/// How long a blocked thread waits before checking the stop flag again.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Which blobs the reader sends.
pub(crate) enum BlobSource {
    /// Every data blob, in file order.
    All,
    /// These blobs, in the order given.
    Only(Vec<BlobLocation>),
}

/// A compressed blob and its position in the read set.
pub(crate) struct RawBlock {
    /// 1-based position in the read set. It names the block in errors.
    pub sequence: usize,
    pub blob: Blob,
}

impl RawBlock {
    /// Inflates the blob into `inflated`, a buffer the worker reuses from
    /// block to block, and decodes the block from it.
    pub fn decode<'b>(&'b self, inflated: &'b mut Vec<u8>) -> Result<Block<'b>, String> {
        let failed =
            |error: String| format!("Failed to decode PBF block {}: {error}", self.sequence);
        // Blobs read in order carry no file offset; the sequence names them.
        let bytes =
            blob_bytes(&self.blob, 0, inflated).map_err(|error| failed(error.to_string()))?;
        Block::decode(bytes).map_err(failed)
    }
}

/// Settings shared by both passes.
pub(crate) struct PipelineConfig {
    pub worker_threads: usize,
    /// Capacity of each of the two queues.
    pub queue_capacity: usize,
}

pub(crate) struct PipelineStats {
    pub raw_block_read_ms: u64,
    pub blocks_read: usize,
    /// Time the collector spent waiting for the next result.
    pub collector_idle_us: u64,
}

/// Runs `decode` on every blob of `source` across the worker pool and passes
/// the results to `collect` in read order.
///
/// `decode` gets each worker's own inflate buffer. `should_cancel` is polled
/// each time the collector waits for a result; when it returns true the
/// pipeline stops and returns [`INGEST_CANCELLED`].
pub(crate) fn run<T, D, C, K>(
    pbf_path: &Path,
    source: BlobSource,
    config: PipelineConfig,
    decode: D,
    should_cancel: &K,
    mut collect: C,
) -> Result<PipelineStats, String>
where
    T: Send + 'static,
    D: Fn(RawBlock, &mut Vec<u8>) -> Result<T, String> + Send + Sync + 'static,
    C: FnMut(T) -> Result<(), String>,
    K: Fn() -> bool + ?Sized,
{
    // Deliberately an unbuffered `File` for selected blobs: each read is a
    // whole payload at a known offset, so a `BufReader` would only add a copy,
    // and its buffer is discarded on every seek anyway.
    let file = File::open(pbf_path)
        .map_err(|error| format!("Failed to open PBF {pbf_path:?}: {error}"))?;
    let stop = Arc::new(AtomicBool::new(false));
    let (block_tx, block_rx) = bounded::<RawBlock>(config.queue_capacity);
    let (result_tx, result_rx) = bounded::<(usize, Result<T, String>)>(config.queue_capacity);

    let reader = spawn_reader(pbf_path, file, source, block_tx, Arc::clone(&stop));
    let decode = Arc::new(decode);
    let workers = (0..config.worker_threads)
        .map(|_| {
            spawn_worker(
                block_rx.clone(),
                result_tx.clone(),
                Arc::clone(&decode),
                Arc::clone(&stop),
            )
        })
        .collect::<Vec<_>>();
    // The threads hold the only other ends, so each channel disconnects when
    // its senders are done.
    drop(block_rx);
    drop(result_tx);

    let collected = collect_in_order(&result_rx, should_cancel, &mut collect);
    stop.store(true, Ordering::Relaxed);
    drop(result_rx);
    let read = join(reader, workers);
    let collector_idle_us = collected?;
    let (raw_block_read_ms, blocks_read) = read?;
    Ok(PipelineStats {
        raw_block_read_ms,
        blocks_read,
        collector_idle_us,
    })
}

/// Hands results to `collect` in sequence order until the workers are done.
/// Returns the time spent waiting for results.
fn collect_in_order<T, K>(
    results: &Receiver<(usize, Result<T, String>)>,
    should_cancel: &K,
    collect: &mut impl FnMut(T) -> Result<(), String>,
) -> Result<u64, String>
where
    K: Fn() -> bool + ?Sized,
{
    let mut pending = BTreeMap::new();
    let mut next_sequence = 1usize;
    let mut idle_us = 0u64;
    loop {
        while let Some(result) = pending.remove(&next_sequence) {
            collect(result?)?;
            next_sequence += 1;
        }
        if should_cancel() {
            return Err(INGEST_CANCELLED.to_string());
        }
        let wait_started = Instant::now();
        let received = results.recv_timeout(POLL_INTERVAL);
        idle_us += wait_started.elapsed().as_micros() as u64;
        match received {
            Ok((sequence, result)) => {
                pending.insert(sequence, result);
            }
            Err(RecvTimeoutError::Timeout) => {}
            // Every worker has exited, so every block read has a result, and
            // a reader error surfaces when the reader is joined.
            Err(RecvTimeoutError::Disconnected) => return Ok(idle_us),
        }
    }
}

fn spawn_reader(
    pbf_path: &Path,
    file: File,
    source: BlobSource,
    blocks: Sender<RawBlock>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<Result<(u64, usize), String>> {
    let path = pbf_path.to_path_buf();
    thread::spawn(move || {
        let read_started = Instant::now();
        let failed = |error: &dyn std::fmt::Display| format!("Failed to read {path:?}: {error}");
        let mut blocks_read = 0usize;
        let mut send = |blob: Blob| {
            blocks_read += 1;
            // Sequence numbers count the read set, not the file: the collector
            // waits for each number in turn, so a skipped blob must not leave
            // a hole.
            let block = RawBlock {
                sequence: blocks_read,
                blob,
            };
            send_until_stopped(&blocks, block, &stop)
        };
        match source {
            BlobSource::Only(locations) => {
                let mut file = file;
                for location in &locations {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let blob = read_blob_at(&mut file, location).map_err(|e| failed(&e))?;
                    if !send(blob) {
                        break;
                    }
                }
            }
            BlobSource::All => {
                for blob in OsmPbfReader::new(file).blobs() {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    if !send(blob.map_err(|e| failed(&e))?) {
                        break;
                    }
                }
            }
        }
        Ok((read_started.elapsed().as_millis() as u64, blocks_read))
    })
}

fn spawn_worker<T, D>(
    blocks: Receiver<RawBlock>,
    results: Sender<(usize, Result<T, String>)>,
    decode: Arc<D>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<()>
where
    T: Send + 'static,
    D: Fn(RawBlock, &mut Vec<u8>) -> Result<T, String> + Send + Sync + 'static,
{
    thread::spawn(move || {
        let mut inflated = Vec::new();
        loop {
            match blocks.recv_timeout(POLL_INTERVAL) {
                Ok(block) => {
                    let sequence = block.sequence;
                    // A panic becomes this block's error. Without a result the
                    // collector would hold every later block until the end.
                    let result =
                        panic::catch_unwind(AssertUnwindSafe(|| decode(block, &mut inflated)))
                            .unwrap_or_else(|payload| {
                                Err(format!(
                                    "PBF decode worker panicked on block {sequence}: {}",
                                    panic_message(&*payload)
                                ))
                            });
                    let failed = result.is_err();
                    if !send_until_stopped(&results, (sequence, result), &stop) || failed {
                        return;
                    }
                }
                Err(RecvTimeoutError::Timeout) if stop.load(Ordering::Relaxed) => return,
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    })
}

/// Sends `message`, retrying while the queue is full. Returns false when the
/// receiver is gone or the pipeline was stopped.
fn send_until_stopped<T>(sender: &Sender<T>, mut message: T, stop: &AtomicBool) -> bool {
    loop {
        match sender.send_timeout(message, POLL_INTERVAL) {
            Ok(()) => return true,
            Err(SendTimeoutError::Timeout(_)) if stop.load(Ordering::Relaxed) => return false,
            Err(SendTimeoutError::Timeout(returned)) => message = returned,
            Err(SendTimeoutError::Disconnected(_)) => return false,
        }
    }
}

fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic")
}

/// Joins every thread, then reports the reader's result.
fn join(
    reader: JoinHandle<Result<(u64, usize), String>>,
    workers: Vec<JoinHandle<()>>,
) -> Result<(u64, usize), String> {
    let read = reader.join();
    let mut workers_panicked = false;
    for worker in workers {
        workers_panicked |= worker.join().is_err();
    }
    let read = read.map_err(|_| "PBF reader thread panicked".to_string())?;
    if workers_panicked {
        return Err("PBF decode worker thread panicked".to_string());
    }
    read
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_pbf::{TestNode, node_block, pbf_file};

    fn fixture(dir: &Path, blocks: usize) -> std::path::PathBuf {
        let blocks = (0..blocks)
            .map(|index| {
                node_block(&[TestNode {
                    id: index as i64 + 1,
                    lat: 50.0,
                    lng: 19.0,
                    tags: &[],
                }])
            })
            .collect::<Vec<_>>();
        let path = dir.join("fixture.osm.pbf");
        std::fs::write(&path, pbf_file(false, &blocks)).expect("write fixture");
        path
    }

    fn config(worker_threads: usize) -> PipelineConfig {
        PipelineConfig {
            worker_threads,
            queue_capacity: 2,
        }
    }

    #[test]
    fn collects_in_read_order_whatever_order_workers_finish_in() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = fixture(dir.path(), 12);
        let mut seen = Vec::new();
        let stats = run(
            &path,
            BlobSource::All,
            config(4),
            |block, _inflated| {
                // Later blocks finish first.
                thread::sleep(Duration::from_millis(13 - block.sequence as u64));
                Ok(block.sequence)
            },
            &|| false,
            |sequence| {
                seen.push(sequence);
                Ok(())
            },
        )
        .expect("pipeline");
        assert_eq!(seen, (1..=12).collect::<Vec<_>>());
        assert_eq!(stats.blocks_read, 12);
    }

    #[test]
    fn a_decode_error_stops_the_pipeline_after_the_blocks_before_it() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = fixture(dir.path(), 12);
        let mut seen = Vec::new();
        let error = run(
            &path,
            BlobSource::All,
            config(3),
            |block, _inflated| match block.sequence {
                5 => Err("bad block".to_string()),
                sequence => Ok(sequence),
            },
            &|| false,
            |sequence| {
                seen.push(sequence);
                Ok(())
            },
        )
        .err();
        assert_eq!(error.as_deref(), Some("bad block"));
        assert_eq!(seen, [1, 2, 3, 4]);
    }

    #[test]
    fn a_collect_error_or_a_cancel_stops_the_pipeline() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = fixture(dir.path(), 12);
        let error = run(
            &path,
            BlobSource::All,
            config(2),
            |block, _inflated| Ok(block.sequence),
            &|| false,
            |sequence| match sequence {
                3 => Err("sink failed".to_string()),
                _ => Ok(()),
            },
        )
        .err();
        assert_eq!(error.as_deref(), Some("sink failed"));

        let error = run(
            &path,
            BlobSource::All,
            config(2),
            |block, _inflated| Ok(block.sequence),
            &|| true,
            |_| Ok(()),
        )
        .err();
        assert_eq!(error.as_deref(), Some(INGEST_CANCELLED));
    }

    #[test]
    fn a_decode_panic_is_that_blocks_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = fixture(dir.path(), 12);
        let mut seen = Vec::new();
        let error = run(
            &path,
            BlobSource::All,
            config(3),
            |block, _inflated| {
                assert_ne!(block.sequence, 4, "corrupt block");
                Ok(block.sequence)
            },
            &|| false,
            |sequence| {
                seen.push(sequence);
                Ok(())
            },
        )
        .err()
        .expect("the panic fails the pipeline");
        assert!(
            error.contains("block 4") && error.contains("corrupt block"),
            "{error}"
        );
        assert_eq!(seen, [1, 2, 3]);
    }

    #[test]
    fn reads_only_the_selected_blobs() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = fixture(dir.path(), 6);
        let index = crate::blob_index::PbfBlobIndex::scan(&path).expect("scan");
        let wanted = index
            .blobs_containing(crate::blob_index::ObjectKind::Node)
            .skip(1)
            .step_by(2)
            .copied()
            .collect();
        let mut ids = Vec::new();
        let stats = run(
            &path,
            BlobSource::Only(wanted),
            config(2),
            |block, inflated| {
                let block = block.decode(inflated)?;
                Ok(block.groups()[0].dense.id[0])
            },
            &|| false,
            |id| {
                ids.push(id);
                Ok(())
            },
        )
        .expect("pipeline");
        assert_eq!(ids, [2, 4, 6]);
        assert_eq!(stats.blocks_read, 3);
    }
}
