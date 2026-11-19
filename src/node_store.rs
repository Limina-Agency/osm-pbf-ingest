use crate::binary_layout::NodeShardRecord;
pub use crate::packed_node_store::{DecimicroBox, PackedCursor, PackedNodeStore};
use memmap2::Mmap;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use zerocopy::{FromBytes, IntoBytes};

const DEFAULT_SHARDS: usize = 64;
/// Desktop builds keep the store in RAM up to this input size, and in a
/// mapped file beyond it. On a file sorted by ID (every Geofabrik extract and
/// the planet) pass 1 builds the packed store, about 6 bytes per node or 0.75
/// bytes per PBF byte, in either place without sorting. Other files get the
/// sharded stores, 16 bytes per node: the disk one spills, reads back, sorts
/// and rewrites every shard. The default stays small so mid-sized extracts
/// don't silently grow peak RSS; callers with memory to spare pick RAM with
/// `PrepareOptions::node_store`. All of them persist byte-identical artifacts.
const RAM_STORE_PBF_THRESHOLD_BYTES: u64 = 32 * 1024 * 1024;
const MOBILE_RAM_STORE_PBF_THRESHOLD_BYTES: u64 = 24 * 1024 * 1024;
/// Per-shard write buffer for the disk store. Decode workers append to the
/// shards concurrently, each holding a shard's lock only for one buffered
/// write, so a larger buffer means fewer write syscalls made under the lock.
const DISK_SHARD_BUFFER_BYTES: usize = 128 * 1024;

/// Bumped whenever the on-disk shard layout or coordinate encoding changes, so
/// a store written by an older build is rejected rather than misread. The
/// records carry no self-description, so a silent format drift would return
/// plausible-looking wrong coordinates.
const NODE_STORE_FORMAT_VERSION: u32 = 1;
const NODE_STORE_MANIFEST_FILE: &str = "manifest.json";

/// Fixed-point scale for stored coordinates: 1e7 gives ~1.1 cm resolution,
/// which is finer than OSM's own coordinate precision.
const COORDINATE_SCALE: f64 = 10_000_000.0;

/// Describes a finalized on-disk node store so it can be reopened later —
/// after the build that produced it has exited, or after the directory has been
/// moved from staging into a published package.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStoreManifest {
    pub format_version: u32,
    pub shard_count: usize,
    pub record_count: u64,
    /// Fixed-point degrees scaling applied to both latitude and longitude.
    pub coordinate_scale: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStoreStrategy {
    Ram,
    Disk,
}

pub fn choose_node_store_strategy(pbf_bytes: u64, mobile_operational: bool) -> NodeStoreStrategy {
    let threshold = if mobile_operational {
        MOBILE_RAM_STORE_PBF_THRESHOLD_BYTES
    } else {
        RAM_STORE_PBF_THRESHOLD_BYTES
    };
    if pbf_bytes <= threshold {
        NodeStoreStrategy::Ram
    } else {
        NodeStoreStrategy::Disk
    }
}

/// Pass 1 builds a [`NodeLocationStore::Packed`] store from a file sorted by
/// ID, and falls back to the sharded `Ram` or `Disk` store otherwise.
/// [`NodeLocationStore::open_disk`] opens a persisted store as `Disk`.
#[derive(Debug)]
pub enum NodeLocationStore {
    /// Sharded by ID, 16 bytes a node, in RAM.
    Ram(PackedRamNodeStore),
    /// Sharded by ID, 16 bytes a node, in mapped files.
    Disk(DiskSortedNodeStore),
    /// Sorted by ID and packed, about 6 bytes a node, in RAM or a mapped
    /// file as [`NodeStoreStrategy`] chose.
    Packed(PackedNodeStore),
}

impl NodeLocationStore {
    /// Reopen a node store persisted by an earlier build.
    pub fn open_disk(base_dir: &Path) -> Result<Self, String> {
        DiskSortedNodeStore::open(base_dir).map(Self::Disk)
    }

    /// Write this store to `dest_dir` as a durable artifact.
    ///
    /// Both strategies land the same layout, so a consumer never has to care
    /// which one the build happened to pick: small extracts keep the in-memory
    /// fast path during the build and pay for the shard files only at the end,
    /// while a disk-backed store is simply moved into place.
    ///
    /// Takes `self` by value because the disk path renames the directory it is
    /// mapping, and a live mapping blocks that on Windows.
    pub fn persist_to(self, dest_dir: &Path) -> Result<NodeStoreManifest, String> {
        match self {
            Self::Ram(store) => store.persist_to(dest_dir),
            Self::Disk(store) => store.persist_to(dest_dir),
            Self::Packed(store) => persist_packed(&store, dest_dir),
        }
    }

    /// The location of `node_id` in degrees, `(lat, lng)`.
    pub fn get(&self, node_id: i64) -> Option<(f64, f64)> {
        match self {
            Self::Ram(store) => store.get(node_id).map(decode_lat_lng),
            Self::Disk(store) => store.get(node_id).map(decode_lat_lng),
            Self::Packed(store) => store
                .get_decimicro(node_id)
                .map(|(lat, lng)| decode_decimicro(lat, lng)),
        }
    }

    /// A reader for runs of lookups, such as a way's nodes: it answers as
    /// [`get`](Self::get) does, faster when consecutive IDs lie close
    /// together, as they usually do within a way.
    pub fn cursor(&self) -> NodeCursor<'_> {
        match self {
            Self::Packed(store) => NodeCursor::Packed(store.cursor()),
            other => NodeCursor::Store(other),
        }
    }

    /// Whether the nodes in `node_ids` that the store holds may lie in one of
    /// `areas`. False only when none of them can; a true answer can be wrong,
    /// so check the locations when it matters. The packed store answers from
    /// the boxes of its blocks, several times faster than looking the nodes
    /// up; the sharded stores always answer true.
    pub fn may_touch(&self, node_ids: &[i64], areas: &[DecimicroBox]) -> bool {
        match self {
            Self::Packed(store) => store.may_touch(node_ids, areas),
            _ => true,
        }
    }

    /// Resolve a batch of ids, skipping any the store does not hold.
    ///
    /// Callers that previously issued `WHERE node_id IN (…)` against SQLite in
    /// chunks of 900 want this shape. Ids are visited in sorted order so shard
    /// pages are touched once and in sequence rather than jumped between.
    pub fn locate_all(&self, node_ids: &[i64]) -> HashMap<i64, (f64, f64)> {
        let mut sorted: Vec<i64> = node_ids.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        let mut located = HashMap::with_capacity(sorted.len());
        for node_id in sorted {
            if let Some(point) = self.get(node_id) {
                located.insert(node_id, point);
            }
        }
        located
    }

    /// Number of node locations held.
    pub fn record_count(&self) -> u64 {
        match self {
            Self::Ram(store) => store.shards.iter().map(|shard| shard.len() as u64).sum(),
            Self::Disk(store) => store.record_count(),
            Self::Packed(store) => store.record_count(),
        }
    }
}

/// See [`NodeLocationStore::cursor`].
pub enum NodeCursor<'a> {
    Packed(PackedCursor<'a>),
    Store(&'a NodeLocationStore),
}

impl NodeCursor<'_> {
    /// The location of `node_id` in degrees, `(lat, lng)`, as
    /// [`NodeLocationStore::get`] gives it.
    #[inline]
    pub fn get(&mut self, node_id: i64) -> Option<(f64, f64)> {
        match self {
            Self::Packed(cursor) => cursor
                .get_decimicro(node_id)
                .map(|(lat, lng)| decode_decimicro(lat, lng)),
            Self::Store(store) => store.get(node_id),
        }
    }
}

/// Writes a packed store in the sharded layout. The packed store holds every
/// node in ID order, so each shard's records arrive sorted.
fn persist_packed(store: &PackedNodeStore, dest_dir: &Path) -> Result<NodeStoreManifest, String> {
    if store.dir() == Some(dest_dir) {
        return Err(format!(
            "Cannot persist a node store into its own directory {dest_dir:?}"
        ));
    }
    fs::create_dir_all(dest_dir)
        .map_err(|error| format!("Failed to create {:?}: {}", dest_dir, error))?;
    let mut writers = (0..DEFAULT_SHARDS)
        .map(|shard| {
            let path = shard_path(dest_dir, shard);
            File::create(&path)
                .map(|file| BufWriter::with_capacity(DISK_SHARD_BUFFER_BYTES, file))
                .map_err(|error| format!("Failed to create {:?}: {}", path, error))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut failure = None;
    store.for_each_block(|records| {
        for &(node_id, lat, lon) in records {
            let record = NodeShardRecord::new(node_id, encode_decimicro(lat, lon));
            if let Err(error) = writers[shard_for_node(node_id)].write_all(record.as_bytes()) {
                failure.get_or_insert(error);
            }
        }
    });
    if let Some(error) = failure {
        return Err(format!(
            "Failed to write node store to {dest_dir:?}: {error}"
        ));
    }
    for mut writer in writers {
        writer
            .flush()
            .map_err(|error| format!("Failed to write node store to {dest_dir:?}: {error}"))?;
    }
    write_node_store_manifest(dest_dir, store.record_count())?;
    Ok(node_store_manifest(store.record_count()))
}

#[derive(Debug)]
pub struct NodeWorkerChunk {
    shards: Vec<Vec<NodeShardRecord>>,
    pub node_count: usize,
}

impl Default for NodeWorkerChunk {
    fn default() -> Self {
        Self {
            shards: (0..DEFAULT_SHARDS).map(|_| Vec::new()).collect(),
            node_count: 0,
        }
    }
}

impl NodeWorkerChunk {
    pub fn push(&mut self, node_id: i64, lat: f64, lng: f64) {
        self.push_encoded(node_id, encode_lat_lng(lat, lng));
    }

    /// Add a node whose coordinates are already in 1e-7 degree units (the
    /// PBF's own "decimicro" precision), skipping the round trip through
    /// `f64` degrees.
    pub fn push_decimicro(&mut self, node_id: i64, decimicro_lat: i32, decimicro_lng: i32) {
        self.push_encoded(node_id, encode_decimicro(decimicro_lat, decimicro_lng));
    }

    fn push_encoded(&mut self, node_id: i64, encoded: i64) {
        self.shards[shard_for_node(node_id)].push(NodeShardRecord::new(node_id, encoded));
        self.node_count += 1;
    }

    /// Group `(node_id, encoded_lat_lng)` pairs by shard, allocating each
    /// shard bucket once at its exact size rather than growing 64 buckets
    /// push by push.
    pub(crate) fn from_encoded(records: &[(i64, i64)]) -> Self {
        let mut counts = [0usize; DEFAULT_SHARDS];
        for (node_id, _) in records {
            counts[shard_for_node(*node_id)] += 1;
        }
        let mut shards: Vec<Vec<NodeShardRecord>> = counts
            .iter()
            .map(|count| Vec::with_capacity(*count))
            .collect();
        for (node_id, encoded) in records {
            shards[shard_for_node(*node_id)].push(NodeShardRecord::new(*node_id, *encoded));
        }
        Self {
            shards,
            node_count: records.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.node_count == 0
    }
}

/// Collects node locations while pass 1 runs.
///
/// Shared by reference across the decode workers: each worker inserts the
/// nodes of the block it just decoded, so node insertion runs in parallel
/// rather than on the single thread that reassembles blocks in file order.
/// Order does not matter here, because `finish` sorts every shard by id.
pub struct NodeLocationStoreBuilder {
    inner: BuilderInner,
    /// Rotates the shard each `push_chunk` starts at, so concurrent workers
    /// do not all queue on shard 0 and then convoy through the rest.
    next_start_shard: AtomicUsize,
}

enum BuilderInner {
    /// One growing run per shard, appended to under that shard's lock and
    /// sorted by `finish`. Appending as chunks arrive, rather than holding
    /// each chunk's buckets until the end and joining them, keeps peak memory
    /// near the store's own size: a shard run soon outgrows the allocator's
    /// mmap threshold, after which growing it remaps pages instead of copying.
    Ram {
        shards: Vec<Mutex<Vec<NodeShardRecord>>>,
    },
    Disk {
        store: DiskSortedNodeStore,
    },
}

impl NodeLocationStoreBuilder {
    pub fn new(strategy: NodeStoreStrategy, temp_dir: &Path) -> Result<Self, String> {
        Self::new_in(strategy, temp_dir.join("node_store"))
    }

    /// A builder whose disk store lives in `base_dir`.
    pub(crate) fn new_in(strategy: NodeStoreStrategy, base_dir: PathBuf) -> Result<Self, String> {
        let inner = match strategy {
            NodeStoreStrategy::Ram => BuilderInner::Ram {
                shards: (0..DEFAULT_SHARDS)
                    .map(|_| Mutex::new(Vec::new()))
                    .collect(),
            },
            NodeStoreStrategy::Disk => BuilderInner::Disk {
                store: DiskSortedNodeStore::new_in(base_dir)?,
            },
        };
        Ok(Self {
            inner,
            next_start_shard: AtomicUsize::new(0),
        })
    }

    pub fn strategy(&self) -> NodeStoreStrategy {
        match self.inner {
            BuilderInner::Ram { .. } => NodeStoreStrategy::Ram,
            BuilderInner::Disk { .. } => NodeStoreStrategy::Disk,
        }
    }

    /// Insert one chunk. Safe to call from several threads at once.
    pub fn push_chunk(&self, chunk: NodeWorkerChunk) -> Result<(), String> {
        if chunk.is_empty() {
            return Ok(());
        }
        let start = self.next_start_shard.fetch_add(1, Ordering::Relaxed) % DEFAULT_SHARDS;
        let mut shards = chunk.shards;
        for offset in 0..DEFAULT_SHARDS {
            let shard = (start + offset) % DEFAULT_SHARDS;
            let records = std::mem::take(&mut shards[shard]);
            if records.is_empty() {
                continue;
            }
            match &self.inner {
                BuilderInner::Ram { shards } => shards[shard]
                    .lock()
                    .map_err(|_| "Node-store shard lock poisoned".to_string())?
                    .extend_from_slice(&records),
                // The chunk already groups records by shard, so each shard's
                // records are contiguous and go out in a single write.
                BuilderInner::Disk { store } => store.append_shard_records(shard, &records)?,
            }
        }
        Ok(())
    }

    pub fn finish(self) -> Result<NodeLocationStore, String> {
        match self.inner {
            BuilderInner::Ram { shards } => {
                let shards = shards
                    .into_iter()
                    .map(|shard| {
                        shard
                            .into_inner()
                            .map_err(|_| "Node-store shard lock poisoned".to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut store = PackedRamNodeStore::from_shards(shards);
                store.finalize();
                Ok(NodeLocationStore::Ram(store))
            }
            BuilderInner::Disk { mut store } => {
                store.finalize()?;
                Ok(NodeLocationStore::Disk(store))
            }
        }
    }
}

fn node_store_manifest(record_count: u64) -> NodeStoreManifest {
    NodeStoreManifest {
        format_version: NODE_STORE_FORMAT_VERSION,
        shard_count: DEFAULT_SHARDS,
        record_count,
        coordinate_scale: COORDINATE_SCALE as i64,
    }
}

fn write_node_store_manifest(base_dir: &Path, record_count: u64) -> Result<(), String> {
    let path = base_dir.join(NODE_STORE_MANIFEST_FILE);
    let encoded = serde_json::to_vec_pretty(&node_store_manifest(record_count))
        .map_err(|error| format!("Failed to encode node-store manifest: {}", error))?;
    fs::write(&path, encoded).map_err(|error| format!("Failed to write {:?}: {}", path, error))
}

fn shard_path(base_dir: &Path, shard: usize) -> PathBuf {
    base_dir.join(format!("shard-{shard:02}.sorted.bin"))
}

#[derive(Debug)]
pub struct PackedRamNodeStore {
    shards: Vec<Vec<NodeShardRecord>>,
}

impl Default for PackedRamNodeStore {
    fn default() -> Self {
        Self {
            shards: (0..DEFAULT_SHARDS).map(|_| Vec::new()).collect(),
        }
    }
}

impl PackedRamNodeStore {
    fn from_shards(shards: Vec<Vec<NodeShardRecord>>) -> Self {
        Self { shards }
    }

    #[cfg(test)]
    pub fn insert(&mut self, node_id: i64, encoded: i64) {
        self.shards[shard_for_node(node_id)].push(NodeShardRecord::new(node_id, encoded));
    }

    pub fn finalize(&mut self) {
        self.shards.par_iter_mut().for_each(|records| {
            records.par_sort_unstable_by_key(|record| record.node_id());
            records.dedup_by_key(|record| record.node_id());
            // Growth slack from appending is address space, not memory, until
            // touched; release it so the finished store holds exactly its records.
            records.shrink_to_fit();
        });
    }

    pub fn get(&self, node_id: i64) -> Option<i64> {
        let records = self.shards.get(shard_for_node(node_id))?;
        let index = records
            .binary_search_by_key(&node_id, |record| record.node_id())
            .ok()?;
        Some(records[index].encoded_lat_lng())
    }

    /// Write the in-memory shards out in the on-disk layout.
    ///
    /// `finalize` has already sorted and deduplicated each shard, which is
    /// exactly the invariant the disk format relies on, so this is a plain
    /// serialization rather than a rebuild.
    fn persist_to(self, dest_dir: &Path) -> Result<NodeStoreManifest, String> {
        fs::create_dir_all(dest_dir)
            .map_err(|error| format!("Failed to create {:?}: {}", dest_dir, error))?;
        let mut record_count = 0u64;
        for (shard, records) in self.shards.iter().enumerate() {
            let path = shard_path(dest_dir, shard);
            let mut file = File::create(&path)
                .map_err(|error| format!("Failed to create {:?}: {}", path, error))?;
            file.write_all(records.as_bytes())
                .map_err(|error| format!("Failed to write {:?}: {}", path, error))?;
            file.flush()
                .map_err(|error| format!("Failed to flush {:?}: {}", path, error))?;
            record_count += records.len() as u64;
        }
        write_node_store_manifest(dest_dir, record_count)?;
        Ok(node_store_manifest(record_count))
    }
}

#[derive(Debug)]
pub struct DiskSortedNodeStore {
    base_dir: PathBuf,
    writers: Vec<Mutex<Option<BufWriter<File>>>>,
    sorted_paths: Vec<PathBuf>,
    maps: Vec<Option<Mmap>>,
}

impl DiskSortedNodeStore {
    pub fn new(temp_dir: &Path) -> Result<Self, String> {
        Self::new_in(temp_dir.join("node_store"))
    }

    /// A store in `base_dir`, emptied first.
    pub(crate) fn new_in(base_dir: PathBuf) -> Result<Self, String> {
        if base_dir.exists() {
            fs::remove_dir_all(&base_dir).map_err(|error| {
                format!("Failed to reset node-store dir {:?}: {}", base_dir, error)
            })?;
        }
        fs::create_dir_all(&base_dir).map_err(|error| {
            format!("Failed to create node-store dir {:?}: {}", base_dir, error)
        })?;
        let mut writers = Vec::with_capacity(DEFAULT_SHARDS);
        let mut sorted_paths = Vec::with_capacity(DEFAULT_SHARDS);
        let mut maps = Vec::with_capacity(DEFAULT_SHARDS);
        for shard in 0..DEFAULT_SHARDS {
            let path = base_dir.join(format!("shard-{shard:02}.bin"));
            let file = File::create(&path)
                .map_err(|error| format!("Failed to create {:?}: {}", path, error))?;
            writers.push(Mutex::new(Some(BufWriter::with_capacity(
                DISK_SHARD_BUFFER_BYTES,
                file,
            ))));
            sorted_paths.push(base_dir.join(format!("shard-{shard:02}.sorted.bin")));
            maps.push(None);
        }
        Ok(Self {
            base_dir,
            writers,
            sorted_paths,
            maps,
        })
    }

    #[cfg(test)]
    pub fn insert(&mut self, node_id: i64, lat: f64, lng: f64) -> Result<(), String> {
        let shard = shard_for_node(node_id);
        let record = NodeShardRecord::new(node_id, encode_lat_lng(lat, lng));
        self.append_shard_records(shard, &[record])
    }

    /// Append a run of records that all belong to `shard`.
    ///
    /// The caller owns the routing: passing records that hash to a different
    /// shard would file them where no lookup will ever look, so the invariant is
    /// asserted in debug builds rather than re-derived per record.
    fn append_shard_records(
        &self,
        shard: usize,
        records: &[NodeShardRecord],
    ) -> Result<(), String> {
        debug_assert!(
            records
                .iter()
                .all(|record| shard_for_node(record.node_id()) == shard),
            "records routed to shard {shard} do not all hash to it"
        );
        let mut writer = self.writers[shard]
            .lock()
            .map_err(|_| "Node-store shard lock poisoned".to_string())?;
        let writer = writer
            .as_mut()
            .ok_or_else(|| "Node store is already finalized".to_string())?;
        writer.write_all(records.as_bytes()).map_err(|error| {
            format!(
                "Failed to append {} nodes to shard {}: {}",
                records.len(),
                shard,
                error
            )
        })
    }

    pub fn finalize(&mut self) -> Result<(), String> {
        for writer in &mut self.writers {
            let writer = writer
                .get_mut()
                .map_err(|_| "Node-store shard lock poisoned".to_string())?;
            if let Some(mut writer) = writer.take() {
                writer
                    .flush()
                    .map_err(|error| format!("Failed to flush node-store shard: {}", error))?;
            }
        }

        let base_dir = self.base_dir.clone();
        let sorted_paths = self.sorted_paths.clone();
        let mut maps = (0..DEFAULT_SHARDS)
            .into_par_iter()
            .map(|shard| -> Result<(usize, Option<Mmap>, usize), String> {
                let input_path = base_dir.join(format!("shard-{shard:02}.bin"));
                let output_path = &sorted_paths[shard];
                let mut input = File::open(&input_path)
                    .map_err(|error| format!("Failed to open {:?}: {}", input_path, error))?;
                let bytes = input
                    .metadata()
                    .map_err(|error| format!("Failed to stat {:?}: {}", input_path, error))?
                    .len() as usize;
                if bytes == 0 {
                    File::create(output_path).map_err(|error| {
                        format!("Failed to create {:?}: {}", output_path, error)
                    })?;
                    return Ok((shard, None, 0));
                }

                let mut buffer = vec![0u8; bytes];
                input
                    .read_exact(&mut buffer)
                    .map_err(|error| format!("Failed to read {:?}: {}", input_path, error))?;
                let (chunks, remainder) = buffer.as_chunks::<{ NodeShardRecord::SIZE }>();
                if !remainder.is_empty() {
                    return Err(format!(
                        "Shard {:?} ended with a partial node record",
                        input_path
                    ));
                }
                let mut records = chunks
                    .iter()
                    .map(|chunk| parse_node_record(chunk.as_slice()))
                    .collect::<Result<Vec<_>, _>>()?;
                records.par_sort_unstable_by_key(|record| record.node_id());
                records.dedup_by_key(|record| record.node_id());

                let mut output = File::create(output_path)
                    .map_err(|error| format!("Failed to create {:?}: {}", output_path, error))?;
                // One write for the whole shard, not one per record.
                // `NodeShardRecord` is `repr(C)` with `IntoBytes + Immutable +
                // Unaligned` fields, so the sorted slice is already exactly the
                // on-disk layout and needs no per-record round trip. Writing
                // records individually to an unbuffered `File` cost a syscall
                // each — tens of millions of them on a region-sized extract,
                // which dominated the entire pass.
                output
                    .write_all(records.as_bytes())
                    .map_err(|error| format!("Failed to write {:?}: {}", output_path, error))?;
                output
                    .flush()
                    .map_err(|error| format!("Failed to flush {:?}: {}", output_path, error))?;
                drop(output);
                let output = File::open(output_path)
                    .map_err(|error| format!("Failed to reopen {:?}: {}", output_path, error))?;
                let map = unsafe {
                    Mmap::map(&output)
                        .map_err(|error| format!("Failed to mmap {:?}: {}", output_path, error))?
                };
                // The unsorted spill is dead once its sorted twin exists, and it
                // is exactly as large. Dropping it here halves the store's peak
                // disk footprint, which is the difference between fitting and
                // not fitting on a constrained device.
                if let Err(error) = fs::remove_file(&input_path) {
                    tracing::warn!(
                        path = ?input_path,
                        %error,
                        "Failed to remove unsorted node-store spill"
                    );
                }
                let records = map.len() / NodeShardRecord::SIZE;
                Ok((shard, Some(map), records))
            })
            .collect::<Result<Vec<_>, _>>()?;
        maps.sort_unstable_by_key(|(shard, _, _)| *shard);
        let record_count: u64 = maps.iter().map(|(_, _, records)| *records as u64).sum();
        self.maps = maps.into_iter().map(|(_, map, _)| map).collect();
        write_node_store_manifest(&self.base_dir, record_count)?;

        Ok(())
    }

    /// Move the finalized shards to `dest_dir`.
    ///
    /// A rename inside the same filesystem, so a multi-hundred-megabyte store
    /// costs nothing to promote from build scratch into the published package.
    /// The mappings are dropped first: renaming a directory with live mappings
    /// fails on Windows.
    fn persist_to(mut self, dest_dir: &Path) -> Result<NodeStoreManifest, String> {
        let record_count = self.record_count();
        if self.base_dir == dest_dir {
            return Ok(node_store_manifest(record_count));
        }
        self.maps.clear();

        if let Some(parent) = dest_dir.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("Failed to create {:?}: {}", parent, error))?;
        }
        if dest_dir.exists() {
            fs::remove_dir_all(dest_dir)
                .map_err(|error| format!("Failed to clear {:?}: {}", dest_dir, error))?;
        }
        fs::rename(&self.base_dir, dest_dir).map_err(|error| {
            format!(
                "Failed to move node store {:?} to {:?}: {}",
                self.base_dir, dest_dir, error
            )
        })?;
        write_node_store_manifest(dest_dir, record_count)?;
        Ok(node_store_manifest(record_count))
    }

    fn record_count(&self) -> u64 {
        self.maps
            .iter()
            .filter_map(|map| map.as_ref())
            .map(|map| (map.len() / NodeShardRecord::SIZE) as u64)
            .sum()
    }

    /// Reopen a store finalized by an earlier run.
    ///
    /// `base_dir` is the directory `new` created — the same one, or a copy of
    /// it moved elsewhere, since every path the store needs is derived from it
    /// rather than recorded.
    pub fn open(base_dir: &Path) -> Result<Self, String> {
        let manifest_path = base_dir.join(NODE_STORE_MANIFEST_FILE);
        let encoded = fs::read(&manifest_path).map_err(|error| {
            format!(
                "Failed to read node-store manifest {:?}: {}",
                manifest_path, error
            )
        })?;
        let manifest: NodeStoreManifest = serde_json::from_slice(&encoded).map_err(|error| {
            format!(
                "Failed to decode node-store manifest {:?}: {}",
                manifest_path, error
            )
        })?;
        if manifest.format_version != NODE_STORE_FORMAT_VERSION {
            return Err(format!(
                "Node store {:?} has format version {}, expected {}",
                base_dir, manifest.format_version, NODE_STORE_FORMAT_VERSION
            ));
        }
        if manifest.shard_count != DEFAULT_SHARDS {
            return Err(format!(
                "Node store {:?} has {} shards, expected {}",
                base_dir, manifest.shard_count, DEFAULT_SHARDS
            ));
        }
        if manifest.coordinate_scale != COORDINATE_SCALE as i64 {
            return Err(format!(
                "Node store {:?} uses coordinate scale {}, expected {}",
                base_dir, manifest.coordinate_scale, COORDINATE_SCALE
            ));
        }

        let mut sorted_paths = Vec::with_capacity(DEFAULT_SHARDS);
        let mut maps = Vec::with_capacity(DEFAULT_SHARDS);
        let mut record_count = 0u64;
        for shard in 0..DEFAULT_SHARDS {
            let path = base_dir.join(format!("shard-{shard:02}.sorted.bin"));
            let file = File::open(&path).map_err(|error| {
                format!("Failed to open node-store shard {:?}: {}", path, error)
            })?;
            let bytes = file
                .metadata()
                .map_err(|error| format!("Failed to stat {:?}: {}", path, error))?
                .len();
            if bytes % NodeShardRecord::SIZE as u64 != 0 {
                return Err(format!(
                    "Node-store shard {:?} is {} bytes, not a whole number of {}-byte records",
                    path,
                    bytes,
                    NodeShardRecord::SIZE
                ));
            }
            record_count += bytes / NodeShardRecord::SIZE as u64;
            let map = if bytes == 0 {
                None
            } else {
                Some(unsafe {
                    Mmap::map(&file)
                        .map_err(|error| format!("Failed to mmap {:?}: {}", path, error))?
                })
            };
            sorted_paths.push(path);
            maps.push(map);
        }

        if record_count != manifest.record_count {
            return Err(format!(
                "Node store {:?} holds {} records, manifest claims {}",
                base_dir, record_count, manifest.record_count
            ));
        }

        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            writers: (0..DEFAULT_SHARDS).map(|_| Mutex::new(None)).collect(),
            sorted_paths,
            maps,
        })
    }

    pub fn get(&self, node_id: i64) -> Option<i64> {
        let shard = shard_for_node(node_id);
        let map = self.maps.get(shard)?.as_ref()?;
        if map.len() < NodeShardRecord::SIZE {
            return None;
        }

        let mut left = 0usize;
        let mut right = map.len() / NodeShardRecord::SIZE;
        while left < right {
            let mid = (left + right) / 2;
            let record = record_at_index(map, mid)?;
            let mid_id = record.node_id();
            if mid_id == node_id {
                return Some(record.encoded_lat_lng());
            }
            if mid_id < node_id {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        None
    }
}

fn shard_for_node(node_id: i64) -> usize {
    (node_id.unsigned_abs() as usize) % DEFAULT_SHARDS
}

fn encode_fixed(value: f64) -> i32 {
    (value * COORDINATE_SCALE).round() as i32
}

pub fn encode_lat_lng(lat: f64, lng: f64) -> i64 {
    encode_decimicro(encode_fixed(lat), encode_fixed(lng))
}

/// [`encode_lat_lng`] for coordinates already scaled by `COORDINATE_SCALE`.
pub fn encode_decimicro(lat: i32, lng: i32) -> i64 {
    let lat_fixed = lat.to_be_bytes();
    let lng_fixed = lng.to_be_bytes();
    let mut bytes = [0u8; 8];
    bytes[..4].copy_from_slice(&lat_fixed);
    bytes[4..].copy_from_slice(&lng_fixed);
    i64::from_be_bytes(bytes)
}

/// Degrees from 1e-7 degree units, exactly as [`decode_lat_lng`] converts.
#[inline]
pub fn decode_decimicro(lat: i32, lng: i32) -> (f64, f64) {
    (lat as f64 / COORDINATE_SCALE, lng as f64 / COORDINATE_SCALE)
}

pub fn decode_lat_lng(encoded: i64) -> (f64, f64) {
    let bytes = encoded.to_be_bytes();
    let lat = i32::from_be_bytes(bytes[..4].try_into().unwrap()) as f64 / COORDINATE_SCALE;
    let lng = i32::from_be_bytes(bytes[4..].try_into().unwrap()) as f64 / COORDINATE_SCALE;
    (lat, lng)
}

fn parse_node_record(chunk: &[u8]) -> Result<NodeShardRecord, String> {
    NodeShardRecord::ref_from_bytes(chunk)
        .copied()
        .map_err(|_| "Invalid node shard record layout".to_string())
}

fn record_at_index(bytes: &[u8], index: usize) -> Option<NodeShardRecord> {
    let start = index.checked_mul(NodeShardRecord::SIZE)?;
    let end = start.checked_add(NodeShardRecord::SIZE)?;
    let slice = bytes.get(start..end)?;
    NodeShardRecord::ref_from_bytes(slice).copied().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn chooses_disk_strategy_for_larger_mobile_inputs() {
        assert_eq!(
            choose_node_store_strategy(64 * 1024 * 1024, true),
            NodeStoreStrategy::Disk
        );
        assert_eq!(
            choose_node_store_strategy(8 * 1024 * 1024, true),
            NodeStoreStrategy::Ram
        );
    }

    #[test]
    fn desktop_profile_keeps_small_inputs_in_ram_and_spills_larger_ones() {
        assert_eq!(
            choose_node_store_strategy(32 * 1024 * 1024, false),
            NodeStoreStrategy::Ram
        );
        assert_eq!(
            choose_node_store_strategy(33 * 1024 * 1024, false),
            NodeStoreStrategy::Disk
        );
    }

    #[test]
    fn a_record_slice_is_byte_identical_to_the_records_written_one_at_a_time() {
        // `finalize` writes a whole shard with a single `records.as_bytes()`.
        // That is only equivalent to the per-record writes it replaced because
        // `NodeShardRecord` is `repr(C)` over `Unaligned` fields and therefore
        // carries no padding. If a field is ever added that reintroduces
        // padding, the shard file silently gains gaps and every lookup past the
        // first record returns garbage — so pin the property here.
        let records = vec![
            NodeShardRecord::new(1, encode_lat_lng(50.0, 19.0)),
            NodeShardRecord::new(2, encode_lat_lng(50.1, 19.1)),
            NodeShardRecord::new(3, encode_lat_lng(50.2, 19.2)),
        ];

        let bulk = records.as_bytes().to_vec();
        let mut one_at_a_time = Vec::new();
        for record in &records {
            one_at_a_time.extend_from_slice(record.as_bytes());
        }

        assert_eq!(bulk, one_at_a_time);
        assert_eq!(bulk.len(), records.len() * NodeShardRecord::SIZE);
    }

    fn build_store(dir: &Path, fixtures: &[(i64, f64, f64)]) -> PathBuf {
        let mut store = DiskSortedNodeStore::new(dir).unwrap();
        for (node_id, lat, lng) in fixtures {
            store.insert(*node_id, *lat, *lng).unwrap();
        }
        store.finalize().unwrap();
        dir.join("node_store")
    }

    fn sample_fixtures(count: i64) -> Vec<(i64, f64, f64)> {
        (0..count)
            .map(|index| {
                (
                    index * 11 + 1,
                    49.0 + index as f64 * 1e-5,
                    19.0 + index as f64 * 1e-5,
                )
            })
            .collect()
    }

    fn persist_via(
        strategy: NodeStoreStrategy,
        fixtures: &[(i64, f64, f64)],
        dir: &Path,
    ) -> PathBuf {
        let build_dir = dir.join(match strategy {
            NodeStoreStrategy::Ram => "build-ram",
            NodeStoreStrategy::Disk => "build-disk",
        });
        fs::create_dir_all(&build_dir).unwrap();
        let builder = NodeLocationStoreBuilder::new(strategy, &build_dir).unwrap();
        let mut chunk = NodeWorkerChunk::default();
        for (node_id, lat, lng) in fixtures {
            chunk.push(*node_id, *lat, *lng);
        }
        builder.push_chunk(chunk).unwrap();
        let store = builder.finish().unwrap();

        let dest = dir.join(match strategy {
            NodeStoreStrategy::Ram => "persisted-ram",
            NodeStoreStrategy::Disk => "persisted-disk",
        });
        store.persist_to(&dest).unwrap();
        dest
    }

    #[test]
    fn ram_and_disk_strategies_persist_byte_identical_artifacts() {
        // Which strategy a build picks depends only on input size, but the
        // artifact is consumed by readers that have no idea which ran. If the
        // two layouts ever diverged, packages would work or not depending on
        // how big their extract happened to be.
        let dir = tempdir().unwrap();
        let fixtures = sample_fixtures(20_000);

        let ram = persist_via(NodeStoreStrategy::Ram, &fixtures, dir.path());
        let disk = persist_via(NodeStoreStrategy::Disk, &fixtures, dir.path());

        for shard in 0..DEFAULT_SHARDS {
            let ram_bytes = fs::read(shard_path(&ram, shard)).unwrap();
            let disk_bytes = fs::read(shard_path(&disk, shard)).unwrap();
            assert_eq!(
                ram_bytes, disk_bytes,
                "shard {shard} differs between strategies"
            );
        }
        assert_eq!(
            fs::read(ram.join(NODE_STORE_MANIFEST_FILE)).unwrap(),
            fs::read(disk.join(NODE_STORE_MANIFEST_FILE)).unwrap(),
            "manifests differ between strategies"
        );
    }

    #[test]
    fn a_persisted_store_reopens_whichever_strategy_wrote_it() {
        let dir = tempdir().unwrap();
        let fixtures = sample_fixtures(10_000);

        for strategy in [NodeStoreStrategy::Ram, NodeStoreStrategy::Disk] {
            let dest = persist_via(strategy, &fixtures, dir.path());
            let reopened = NodeLocationStore::open_disk(&dest).unwrap();
            for (node_id, lat, lng) in &fixtures {
                let (found_lat, found_lng) = reopened
                    .get(*node_id)
                    .unwrap_or_else(|| panic!("node {node_id} missing for {strategy:?}"));
                assert!((found_lat - lat).abs() < 1e-6, "{strategy:?} latitude");
                assert!((found_lng - lng).abs() < 1e-6, "{strategy:?} longitude");
            }
        }
    }

    #[test]
    fn persisting_a_disk_store_leaves_nothing_behind_in_the_build_directory() {
        let dir = tempdir().unwrap();
        let build_dir = dir.path().join("build");
        fs::create_dir_all(&build_dir).unwrap();
        let mut store = DiskSortedNodeStore::new(&build_dir).unwrap();
        for (node_id, lat, lng) in sample_fixtures(1_000) {
            store.insert(node_id, lat, lng).unwrap();
        }
        store.finalize().unwrap();

        let dest = dir.path().join("published");
        NodeLocationStore::Disk(store).persist_to(&dest).unwrap();

        assert!(
            !build_dir.join("node_store").exists(),
            "build copy should be moved, not copied"
        );
        assert!(dest.join(NODE_STORE_MANIFEST_FILE).exists());
    }

    #[test]
    fn a_finalized_store_reopens_with_the_same_coordinates() {
        let dir = tempdir().unwrap();
        let fixtures = sample_fixtures(20_000);
        let base_dir = build_store(dir.path(), &fixtures);

        // Reopen from scratch — no in-process state carried over, which is the
        // whole point: the build that wrote this store has exited.
        let reopened = NodeLocationStore::open_disk(&base_dir).unwrap();
        for (node_id, lat, lng) in &fixtures {
            let (found_lat, found_lng) = reopened
                .get(*node_id)
                .unwrap_or_else(|| panic!("node {node_id} missing after reopen"));
            assert!((found_lat - lat).abs() < 1e-6, "node {node_id} latitude");
            assert!((found_lng - lng).abs() < 1e-6, "node {node_id} longitude");
        }
        assert!(reopened.get(4).is_none(), "unwritten id should not resolve");
    }

    #[test]
    fn a_store_survives_being_moved_after_finalize() {
        // The build writes into staging and the package is then renamed into
        // place, so every path the store needs must derive from the directory
        // it is opened from rather than from anything recorded at write time.
        let dir = tempdir().unwrap();
        let fixtures = sample_fixtures(5_000);
        let base_dir = build_store(dir.path(), &fixtures);

        let moved = dir.path().join("published").join("nodes");
        fs::create_dir_all(moved.parent().unwrap()).unwrap();
        fs::rename(&base_dir, &moved).unwrap();

        let reopened = NodeLocationStore::open_disk(&moved).unwrap();
        for (node_id, lat, lng) in &fixtures {
            let (found_lat, found_lng) = reopened
                .get(*node_id)
                .unwrap_or_else(|| panic!("node {node_id} missing after move"));
            assert!((found_lat - lat).abs() < 1e-6);
            assert!((found_lng - lng).abs() < 1e-6);
        }
    }

    #[test]
    fn finalize_removes_the_unsorted_spill_files() {
        // The unsorted spill is the same size as the sorted output, so leaving
        // it behind doubles the store's peak disk use for no benefit.
        let dir = tempdir().unwrap();
        let base_dir = build_store(dir.path(), &sample_fixtures(1_000));

        for shard in 0..DEFAULT_SHARDS {
            let unsorted = base_dir.join(format!("shard-{shard:02}.bin"));
            assert!(
                !unsorted.exists(),
                "unsorted spill {unsorted:?} should be gone after finalize"
            );
            assert!(
                base_dir
                    .join(format!("shard-{shard:02}.sorted.bin"))
                    .exists()
            );
        }
    }

    #[test]
    fn a_store_written_by_an_incompatible_build_is_rejected() {
        let dir = tempdir().unwrap();
        let base_dir = build_store(dir.path(), &sample_fixtures(100));

        let manifest_path = base_dir.join(NODE_STORE_MANIFEST_FILE);
        let mut manifest: NodeStoreManifest =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        manifest.format_version += 1;
        fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        let error = NodeLocationStore::open_disk(&base_dir).expect_err("expected rejection");
        assert!(
            error.contains("format version"),
            "expected a format-version error, got: {error}"
        );
    }

    #[test]
    fn a_truncated_shard_is_rejected_rather_than_misread() {
        let dir = tempdir().unwrap();
        let fixtures = sample_fixtures(2_000);
        let base_dir = build_store(dir.path(), &fixtures);

        // Lop bytes off one shard: the record count will no longer match the
        // manifest, and a store that silently answered from it would return
        // wrong coordinates for whatever landed past the cut.
        let victim = (0..DEFAULT_SHARDS)
            .map(|shard| base_dir.join(format!("shard-{shard:02}.sorted.bin")))
            .find(|path| {
                fs::metadata(path)
                    .map(|meta| meta.len() > 0)
                    .unwrap_or(false)
            })
            .expect("at least one non-empty shard");
        let bytes = fs::read(&victim).unwrap();
        fs::write(&victim, &bytes[..bytes.len() - NodeShardRecord::SIZE]).unwrap();

        let error = NodeLocationStore::open_disk(&base_dir).expect_err("expected rejection");
        assert!(
            error.contains("manifest claims"),
            "expected a record-count error, got: {error}"
        );
    }

    #[test]
    fn bulk_chunk_insertion_matches_record_by_record_insertion() {
        // `push_chunk` trusts that a chunk's shard buckets are already routed
        // correctly and appends each bucket wholesale. Prove that lands the same
        // data as computing the shard per record, which is what it replaced.
        let dir = tempdir().unwrap();
        let fixtures: Vec<(i64, f64, f64)> = (0..5_000)
            .map(|index: i64| {
                (
                    index * 13 + 1,
                    49.0 + index as f64 * 1e-5,
                    19.0 + index as f64 * 1e-5,
                )
            })
            .collect();

        let mut chunk = NodeWorkerChunk::default();
        for (node_id, lat, lng) in &fixtures {
            chunk.push(*node_id, *lat, *lng);
        }
        let bulk = NodeLocationStoreBuilder::new(NodeStoreStrategy::Disk, dir.path()).unwrap();
        bulk.push_chunk(chunk).unwrap();
        let bulk = bulk.finish().unwrap();

        let per_record_dir = tempdir().unwrap();
        let mut per_record = DiskSortedNodeStore::new(per_record_dir.path()).unwrap();
        for (node_id, lat, lng) in &fixtures {
            per_record.insert(*node_id, *lat, *lng).unwrap();
        }
        per_record.finalize().unwrap();

        for (node_id, _, _) in &fixtures {
            assert_eq!(
                bulk.get(*node_id),
                per_record.get(*node_id).map(decode_lat_lng),
                "node {node_id} differs between bulk and per-record insertion"
            );
        }
    }

    #[test]
    fn disk_store_round_trips_every_record_across_all_shards() {
        // Enough records to spread over all 64 shards and to make any offset
        // error in the bulk write show up as a wrong or missing lookup.
        let dir = tempdir().unwrap();
        let mut store = DiskSortedNodeStore::new(dir.path()).unwrap();
        let count = 50_000i64;

        for index in 0..count {
            // Scatter ids so shards receive uneven, unsorted input.
            let node_id = index * 7 + 1;
            let lat = 49.0 + (index as f64) * 1e-5;
            let lng = 19.0 + (index as f64) * 1e-5;
            store.insert(node_id, lat, lng).unwrap();
        }
        store.finalize().unwrap();

        for index in 0..count {
            let node_id = index * 7 + 1;
            let expected_lat = 49.0 + (index as f64) * 1e-5;
            let expected_lng = 19.0 + (index as f64) * 1e-5;
            let (lat, lng) = store
                .get(node_id)
                .map(decode_lat_lng)
                .unwrap_or_else(|| panic!("node {node_id} missing after finalize"));
            assert!((lat - expected_lat).abs() < 1e-6, "node {node_id} latitude");
            assert!(
                (lng - expected_lng).abs() < 1e-6,
                "node {node_id} longitude"
            );
        }

        assert!(store.get(2).is_none(), "unwritten id should not resolve");
    }

    #[test]
    fn decimicro_encoding_matches_the_f64_degree_path() {
        // Pass 1 now encodes the PBF's integer coordinates directly instead of
        // converting to degrees and back. That is only output-preserving if
        // `round(x * 1e-7 * 1e7) == x` for every coordinate OSM can hold.
        let check = |lat: i32, lng: i32| {
            assert_eq!(
                encode_decimicro(lat, lng),
                encode_lat_lng(lat as f64 * 1e-7, lng as f64 * 1e-7),
                "({lat}, {lng})"
            );
        };
        for value in -20_000..=20_000 {
            check(value, -value);
        }
        for value in (-1_800_000_000i32..=1_800_000_000).step_by(7_919) {
            check(value / 2, value);
        }
        for value in [
            i32::MIN,
            i32::MAX,
            -900_000_000,
            900_000_000,
            -1_800_000_000,
        ] {
            check(value, value);
        }
    }

    #[test]
    fn concurrent_chunk_pushes_build_the_same_store_as_sequential_ones() {
        // Pass 1 decode workers push chunks in whatever order they finish, so
        // the store must not depend on push order.
        let chunks = (0..32i64)
            .map(|chunk| {
                (0..500i64)
                    .map(|offset| {
                        let node_id = chunk * 1_000 + offset * 3 + 1;
                        (node_id, encode_decimicro(node_id as i32, -(node_id as i32)))
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        for strategy in [NodeStoreStrategy::Ram, NodeStoreStrategy::Disk] {
            let dir = tempdir().unwrap();
            let concurrent_dir = dir.path().join("concurrent");
            let sequential_dir = dir.path().join("sequential");
            fs::create_dir_all(&concurrent_dir).unwrap();
            fs::create_dir_all(&sequential_dir).unwrap();

            let concurrent = NodeLocationStoreBuilder::new(strategy, &concurrent_dir).unwrap();
            std::thread::scope(|scope| {
                for worker in 0..4 {
                    let concurrent = &concurrent;
                    let chunks = &chunks;
                    scope.spawn(move || {
                        for records in chunks.iter().skip(worker).step_by(4).rev() {
                            concurrent
                                .push_chunk(NodeWorkerChunk::from_encoded(records))
                                .unwrap();
                        }
                    });
                }
            });
            let sequential = NodeLocationStoreBuilder::new(strategy, &sequential_dir).unwrap();
            for records in &chunks {
                sequential
                    .push_chunk(NodeWorkerChunk::from_encoded(records))
                    .unwrap();
            }

            let concurrent = concurrent.finish().unwrap();
            let sequential = sequential.finish().unwrap();
            assert_eq!(concurrent.record_count(), 32 * 500);
            for records in &chunks {
                for (node_id, encoded) in records {
                    assert_eq!(concurrent.get(*node_id), Some(decode_lat_lng(*encoded)));
                }
            }
            let concurrent_out = dir.path().join("concurrent-out");
            let sequential_out = dir.path().join("sequential-out");
            concurrent.persist_to(&concurrent_out).unwrap();
            sequential.persist_to(&sequential_out).unwrap();
            for shard in 0..DEFAULT_SHARDS {
                assert_eq!(
                    fs::read(shard_path(&concurrent_out, shard)).unwrap(),
                    fs::read(shard_path(&sequential_out, shard)).unwrap(),
                    "{strategy:?} shard {shard} depends on push order"
                );
            }
        }
    }

    #[test]
    fn disk_store_round_trips_node_locations() {
        let dir = tempdir().unwrap();
        let mut store = DiskSortedNodeStore::new(dir.path()).unwrap();
        store.insert(3, 50.06143, 19.93658).unwrap();
        store.insert(1, 50.00001, 19.00002).unwrap();
        store.finalize().unwrap();
        let (lat, lng) = store.get(3).map(decode_lat_lng).unwrap();
        assert!((lat - 50.06143).abs() < 1e-6);
        assert!((lng - 19.93658).abs() < 1e-6);
    }

    #[test]
    fn ram_and_disk_stores_return_identical_coordinates() {
        let dir = tempdir().unwrap();
        let mut ram = PackedRamNodeStore::default();
        let mut disk = DiskSortedNodeStore::new(dir.path()).unwrap();
        let fixtures = [
            (3, 50.06143, 19.93658),
            (1, 50.00001, 19.00002),
            (7, 49.81234, 20.12345),
        ];

        for (node_id, lat, lng) in fixtures {
            let encoded = encode_lat_lng(lat, lng);
            ram.insert(node_id, encoded);
            disk.insert(node_id, lat, lng).unwrap();
        }

        ram.finalize();
        disk.finalize().unwrap();

        for (node_id, _, _) in fixtures {
            assert_eq!(ram.get(node_id), disk.get(node_id));
        }
    }
}
