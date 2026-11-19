//! The packed node-location store: every node location sorted by ID, in
//! blocks of up to [`BLOCK_NODES`] nodes packed with a frame of reference.
//!
//! A block stores its first ID outside the packed bytes (in
//! [`PackedNodeStore::firsts`], which lookups search) and, packed, the other
//! IDs as offsets from it, and the latitudes and longitudes as offsets from
//! the block's smallest. Each of the three runs uses the fewest bits that
//! hold its largest offset. OSM gives consecutive IDs to nodes mapped
//! together, so a block's nodes usually lie close together and its offsets
//! are short: a Geofabrik extract packs into about 6 bytes per node, where
//! the sharded stores take 16.
//!
//! A lookup finds the block from a table indexed by the ID's high bits and a
//! short binary search over the block starts, then binary-searches the packed
//! ID offsets. Both searches stay within a few cache lines.
//!
//! Pass 1 packs each PBF block's nodes on its decode worker and appends the
//! packed runs in file order, so a file sorted by ID needs no sort and no
//! spill. A run that does not continue the IDs before it sends pass 1 back to
//! the sharded stores (see `pass1::NodeCollector`).
use memmap2::Mmap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Nodes per block at most.
pub(crate) const BLOCK_NODES: usize = 128;
/// Bytes before a block's packed bits: the node count less one, the three bit
/// widths, and the smallest latitude and longitude.
const HEADER_BYTES: usize = 12;
/// Zero bytes after the last block, so that every packed value can be read
/// with one unaligned 16-byte load.
const PADDING_BYTES: usize = 16;
/// The packed file inside the store's directory.
pub(crate) const PACKED_FILE: &str = "nodes.packed";

/// A box of node locations, in 1e-7 degree units, edges included.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecimicroBox {
    pub min_lat: i32,
    pub min_lon: i32,
    pub max_lat: i32,
    pub max_lon: i32,
}

impl DecimicroBox {
    /// The smallest box holding every location of a box in degrees whose
    /// edges are `south`, `west`, `north` and `east`, so that a node the
    /// degree box holds is always inside it. Edges are clamped to the range
    /// a coordinate can hold.
    pub fn around_degrees(south: f64, west: f64, north: f64, east: f64) -> Self {
        let floor = |value: f64| {
            (value * 1e7)
                .floor()
                .clamp(i32::MIN as f64, i32::MAX as f64)
        };
        let ceil = |value: f64| (value * 1e7).ceil().clamp(i32::MIN as f64, i32::MAX as f64);
        // One unit more on each side absorbs the rounding of `value * 1e7`.
        Self {
            min_lat: (floor(south) as i32).saturating_sub(1),
            min_lon: (floor(west) as i32).saturating_sub(1),
            max_lat: (ceil(north) as i32).saturating_add(1),
            max_lon: (ceil(east) as i32).saturating_add(1),
        }
    }

    /// Whether the box holds the location, edges included.
    #[inline]
    pub fn contains(&self, lat: i32, lon: i32) -> bool {
        (self.min_lat..=self.max_lat).contains(&lat) && (self.min_lon..=self.max_lon).contains(&lon)
    }

    fn intersects(&self, other: &Self) -> bool {
        self.min_lat <= other.max_lat
            && other.min_lat <= self.max_lat
            && self.min_lon <= other.max_lon
            && other.min_lon <= self.max_lon
    }

    fn extend(&mut self, other: &Self) {
        self.min_lat = self.min_lat.min(other.min_lat);
        self.min_lon = self.min_lon.min(other.min_lon);
        self.max_lat = self.max_lat.max(other.max_lat);
        self.max_lon = self.max_lon.max(other.max_lon);
    }

    const EMPTY: Self = Self {
        min_lat: i32::MAX,
        min_lon: i32::MAX,
        max_lat: i32::MIN,
        max_lon: i32::MIN,
    };
}

/// One PBF block's nodes, packed by a decode worker: blocks whose offsets
/// count from the start of `data`.
#[derive(Debug, Default)]
pub(crate) struct PackedRun {
    firsts: Vec<i64>,
    offsets: Vec<u64>,
    boxes: Vec<DecimicroBox>,
    data: Vec<u8>,
    nodes: usize,
}

impl PackedRun {
    /// Packs `records`, `(node_id, decimicro_lat, decimicro_lon)` sorted by
    /// strictly increasing ID.
    pub(crate) fn pack(records: &[(i64, i32, i32)]) -> Self {
        debug_assert!(records.windows(2).all(|pair| pair[0].0 < pair[1].0));
        let blocks = records.len().div_ceil(BLOCK_NODES);
        let mut run = Self {
            firsts: Vec::with_capacity(blocks),
            offsets: Vec::with_capacity(blocks),
            boxes: Vec::with_capacity(blocks),
            // About 6 bytes a node, from Geofabrik extracts.
            data: Vec::with_capacity(records.len() * 6 + blocks * HEADER_BYTES),
            nodes: records.len(),
        };
        for block in records.chunks(BLOCK_NODES) {
            run.firsts.push(block[0].0);
            run.offsets.push(run.data.len() as u64);
            run.boxes.push(pack_block(block, &mut run.data));
        }
        run
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.nodes == 0
    }

    fn first_id(&self) -> Option<i64> {
        self.firsts.first().copied()
    }

    /// Every record of the run, in ID order.
    pub(crate) fn records(&self) -> Vec<(i64, i32, i32)> {
        let mut data = self.data.clone();
        data.resize(data.len() + PADDING_BYTES, 0);
        let mut records = Vec::with_capacity(self.nodes);
        for (block, &first) in self.firsts.iter().enumerate() {
            unpack_block(&data[self.offsets[block] as usize..], first, &mut records);
        }
        records
    }
}

/// Packs one block onto `data` and returns its box.
fn pack_block(block: &[(i64, i32, i32)], data: &mut Vec<u8>) -> DecimicroBox {
    let first = block[0].0;
    let mut bounds = DecimicroBox::EMPTY;
    for &(_, lat, lon) in block {
        bounds.extend(&DecimicroBox {
            min_lat: lat,
            min_lon: lon,
            max_lat: lat,
            max_lon: lon,
        });
    }
    let id_offset = |id: i64| id.wrapping_sub(first) as u64;
    let lat_offset = |lat: i32| (i64::from(lat) - i64::from(bounds.min_lat)) as u64;
    let lon_offset = |lon: i32| (i64::from(lon) - i64::from(bounds.min_lon)) as u64;
    let id_bits = width(id_offset(block[block.len() - 1].0));
    let lat_bits = width(lat_offset(bounds.max_lat));
    let lon_bits = width(lon_offset(bounds.max_lon));
    data.push((block.len() - 1) as u8);
    data.extend_from_slice(&[id_bits, lat_bits, lon_bits]);
    data.extend_from_slice(&bounds.min_lat.to_le_bytes());
    data.extend_from_slice(&bounds.min_lon.to_le_bytes());
    let mut bits = BitWriter::new(data);
    for &(id, _, _) in &block[1..] {
        bits.push(id_offset(id), id_bits);
    }
    for &(_, lat, _) in block {
        bits.push(lat_offset(lat), lat_bits);
    }
    for &(_, _, lon) in block {
        bits.push(lon_offset(lon), lon_bits);
    }
    bits.finish();
    bounds
}

fn width(value: u64) -> u8 {
    (u64::BITS - value.leading_zeros()) as u8
}

struct BitWriter<'a> {
    out: &'a mut Vec<u8>,
    pending: u128,
    bits: u32,
}

impl<'a> BitWriter<'a> {
    fn new(out: &'a mut Vec<u8>) -> Self {
        Self {
            out,
            pending: 0,
            bits: 0,
        }
    }

    fn push(&mut self, value: u64, width: u8) {
        if width == 0 {
            return;
        }
        self.pending |= u128::from(value) << self.bits;
        self.bits += u32::from(width);
        while self.bits >= 8 {
            self.out.push(self.pending as u8);
            self.pending >>= 8;
            self.bits -= 8;
        }
    }

    fn finish(self) {
        if self.bits > 0 {
            self.out.push(self.pending as u8);
        }
    }
}

/// The `width`-bit value at bit `bit` of `bytes`, which must hold 16 bytes
/// from the value's first byte on.
#[inline]
fn read_bits(bytes: &[u8], bit: usize, width: u8) -> u64 {
    if width == 0 {
        return 0;
    }
    let start = bit >> 3;
    let chunk = u128::from_le_bytes(bytes[start..start + 16].try_into().unwrap());
    let value = (chunk >> (bit & 7)) as u64;
    if width >= 64 {
        value
    } else {
        value & ((1u64 << width) - 1)
    }
}

/// A packed block's layout, read from its header.
struct Block<'a> {
    bytes: &'a [u8],
    nodes: usize,
    id_bits: u8,
    lat_bits: u8,
    lon_bits: u8,
    min_lat: i32,
    min_lon: i32,
}

impl<'a> Block<'a> {
    #[inline]
    fn at(bytes: &'a [u8]) -> Self {
        let header: &[u8; HEADER_BYTES] = bytes[..HEADER_BYTES].try_into().unwrap();
        Self {
            bytes: &bytes[HEADER_BYTES..],
            nodes: usize::from(header[0]) + 1,
            id_bits: header[1],
            lat_bits: header[2],
            lon_bits: header[3],
            min_lat: i32::from_le_bytes(header[4..8].try_into().unwrap()),
            min_lon: i32::from_le_bytes(header[8..12].try_into().unwrap()),
        }
    }

    /// The offset from the first ID of node `index`, for `index` from 1.
    #[inline]
    fn id_offset(&self, index: usize) -> u64 {
        read_bits(
            self.bytes,
            (index - 1) * usize::from(self.id_bits),
            self.id_bits,
        )
    }

    #[inline]
    fn location(&self, index: usize) -> (i32, i32) {
        let lat_start = (self.nodes - 1) * usize::from(self.id_bits);
        let lon_start = lat_start + self.nodes * usize::from(self.lat_bits);
        let lat = read_bits(
            self.bytes,
            lat_start + index * usize::from(self.lat_bits),
            self.lat_bits,
        );
        let lon = read_bits(
            self.bytes,
            lon_start + index * usize::from(self.lon_bits),
            self.lon_bits,
        );
        (
            (i64::from(self.min_lat) + lat as i64) as i32,
            (i64::from(self.min_lon) + lon as i64) as i32,
        )
    }

    /// The location of the node `offset` IDs after the block's first.
    #[inline]
    fn find(&self, offset: u64) -> Option<(i32, i32)> {
        if offset == 0 {
            return Some(self.location(0));
        }
        // Binary search over nodes 1..nodes, whose offsets increase.
        let (mut low, mut high) = (1, self.nodes);
        while low < high {
            let middle = (low + high) / 2;
            let found = self.id_offset(middle);
            if found == offset {
                return Some(self.location(middle));
            }
            if found < offset {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        None
    }
}

fn unpack_block(bytes: &[u8], first: i64, records: &mut Vec<(i64, i32, i32)>) {
    let block = Block::at(bytes);
    for index in 0..block.nodes {
        let id = match index {
            0 => first,
            _ => first.wrapping_add(block.id_offset(index) as i64),
        };
        let (lat, lon) = block.location(index);
        records.push((id, lat, lon));
    }
}

#[derive(Debug)]
enum PackedBytes {
    Ram(Vec<u8>),
    Disk(Mmap),
}

impl PackedBytes {
    #[inline]
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Ram(bytes) => bytes,
            Self::Disk(map) => map,
        }
    }
}

/// See the module documentation.
#[derive(Debug)]
pub struct PackedNodeStore {
    /// The first ID of each block, increasing.
    firsts: Vec<i64>,
    /// Where each block starts in `bytes`.
    offsets: Vec<u64>,
    /// The box of each block's nodes.
    boxes: Vec<DecimicroBox>,
    bytes: PackedBytes,
    /// For IDs from 0 to `buckets.len() - 2 << bucket_shift`: bucket `b`
    /// holds the IDs from `b << bucket_shift`, and `buckets[b]` is the number
    /// of blocks that start before it.
    buckets: Vec<u32>,
    bucket_shift: u32,
    record_count: u64,
    /// The directory of a disk store, removed on drop.
    dir: Option<PathBuf>,
}

impl PackedNodeStore {
    /// The encoded location (as [`crate::node_store::encode_decimicro`]
    /// makes) of `node_id`.
    #[inline]
    pub fn get(&self, node_id: i64) -> Option<i64> {
        self.get_decimicro(node_id)
            .map(|(lat, lon)| crate::node_store::encode_decimicro(lat, lon))
    }

    /// The location of `node_id` in 1e-7 degree units.
    #[inline]
    pub fn get_decimicro(&self, node_id: i64) -> Option<(i32, i32)> {
        let block = self.block_of(node_id)?;
        self.find_in(block, node_id)
    }

    #[inline]
    fn find_in(&self, block: usize, node_id: i64) -> Option<(i32, i32)> {
        let bytes = &self.bytes.bytes()[self.offsets[block] as usize..];
        Block::at(bytes).find(node_id.wrapping_sub(self.firsts[block]) as u64)
    }

    /// The block whose ID range holds `node_id`: the last one starting at or
    /// before it.
    #[inline]
    fn block_of(&self, node_id: i64) -> Option<usize> {
        let (low, high) = match usize::try_from(node_id >> self.bucket_shift) {
            Ok(bucket) if node_id >= 0 && bucket + 1 < self.buckets.len() => (
                self.buckets[bucket] as usize,
                self.buckets[bucket + 1] as usize,
            ),
            _ => (0, self.firsts.len()),
        };
        let after = low + self.firsts[low..high].partition_point(|&first| first <= node_id);
        after.checked_sub(1)
    }

    /// Number of node locations held.
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// A reader that remembers the block of the last lookup, for runs of
    /// lookups whose IDs lie close together, such as a way's nodes.
    pub fn cursor(&self) -> PackedCursor<'_> {
        PackedCursor {
            store: self,
            block: usize::MAX,
            start: 1,
            end: 0,
        }
    }

    /// Whether the nodes in `node_ids` that the store holds may lie in one of
    /// `areas`: false only when no such node can, judged by the boxes of the
    /// blocks their IDs fall in, without unpacking a block.
    pub fn may_touch(&self, node_ids: &[i64], areas: &[DecimicroBox]) -> bool {
        let mut bounds = DecimicroBox::EMPTY;
        let mut last = usize::MAX;
        for &id in node_ids {
            let Some(block) = self.block_of(id) else {
                continue;
            };
            if block != last {
                bounds.extend(&self.boxes[block]);
                last = block;
            }
        }
        areas.iter().any(|area| area.intersects(&bounds))
    }

    /// Every block's records, `(node_id, decimicro_lat, decimicro_lon)`, in
    /// ID order.
    pub(crate) fn for_each_block(&self, mut each: impl FnMut(&[(i64, i32, i32)])) {
        let mut records = Vec::with_capacity(BLOCK_NODES);
        for (block, &first) in self.firsts.iter().enumerate() {
            records.clear();
            unpack_block(
                &self.bytes.bytes()[self.offsets[block] as usize..],
                first,
                &mut records,
            );
            each(&records);
        }
    }

    pub(crate) fn dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    /// Bytes the packed blocks take, without the block index.
    pub fn packed_bytes(&self) -> u64 {
        self.bytes.bytes().len() as u64
    }
}

impl Drop for PackedNodeStore {
    fn drop(&mut self) {
        if let Some(dir) = self.dir.take() {
            // Unmap before removing, which Windows needs.
            self.bytes = PackedBytes::Ram(Vec::new());
            let _ = fs::remove_file(dir.join(PACKED_FILE));
            // Left alone when anything else is in it.
            let _ = fs::remove_dir(&dir);
        }
    }
}

/// See [`PackedNodeStore::cursor`].
pub struct PackedCursor<'a> {
    store: &'a PackedNodeStore,
    block: usize,
    /// The IDs the cached block's range covers, `start..=end`.
    start: i64,
    end: i64,
}

impl PackedCursor<'_> {
    /// The location of `node_id` in 1e-7 degree units.
    #[inline]
    pub fn get_decimicro(&mut self, node_id: i64) -> Option<(i32, i32)> {
        if !(self.start..=self.end).contains(&node_id) {
            let block = self.store.block_of(node_id)?;
            self.block = block;
            self.start = self.store.firsts[block];
            self.end = self
                .store
                .firsts
                .get(block + 1)
                .map_or(i64::MAX, |next| next - 1);
        }
        self.store.find_in(self.block, node_id)
    }
}

enum Sink {
    Ram(Vec<u8>),
    Disk {
        writer: BufWriter<File>,
        dir: PathBuf,
        written: u64,
    },
}

/// Appends the packed runs pass 1's workers make, in file order.
pub(crate) struct PackedStoreBuilder {
    firsts: Vec<i64>,
    offsets: Vec<u64>,
    boxes: Vec<DecimicroBox>,
    sink: Sink,
    last_id: Option<i64>,
    record_count: u64,
}

impl PackedStoreBuilder {
    pub(crate) fn ram() -> Self {
        Self::with_sink(Sink::Ram(Vec::new()))
    }

    /// A store kept in `temp_dir/node_store`, the directory the sharded disk
    /// store uses.
    pub(crate) fn disk(temp_dir: &Path) -> Result<Self, String> {
        let dir = temp_dir.join("node_store");
        if dir.exists() {
            fs::remove_dir_all(&dir)
                .map_err(|error| format!("Failed to reset node-store dir {dir:?}: {error}"))?;
        }
        fs::create_dir_all(&dir)
            .map_err(|error| format!("Failed to create node-store dir {dir:?}: {error}"))?;
        let path = dir.join(PACKED_FILE);
        let file =
            File::create(&path).map_err(|error| format!("Failed to create {path:?}: {error}"))?;
        Ok(Self::with_sink(Sink::Disk {
            writer: BufWriter::with_capacity(1 << 20, file),
            dir,
            written: 0,
        }))
    }

    fn with_sink(sink: Sink) -> Self {
        Self {
            firsts: Vec::new(),
            offsets: Vec::new(),
            boxes: Vec::new(),
            sink,
            last_id: None,
            record_count: 0,
        }
    }

    /// Appends `run`, or hands it back when its IDs do not all follow the
    /// ones appended before.
    pub(crate) fn append(&mut self, run: PackedRun) -> Result<Result<(), PackedRun>, String> {
        let Some(first) = run.first_id() else {
            return Ok(Ok(()));
        };
        if self.last_id.is_some_and(|last| first <= last) {
            return Ok(Err(run));
        }
        // The last block's IDs end at the run's last ID, which only its
        // records know; unpacking the one block finds it.
        let last_block = run.firsts.len() - 1;
        let mut tail = Vec::with_capacity(BLOCK_NODES);
        let mut padded = run.data[run.offsets[last_block] as usize..].to_vec();
        padded.resize(padded.len() + PADDING_BYTES, 0);
        unpack_block(&padded, run.firsts[last_block], &mut tail);
        self.last_id = tail.last().map(|record| record.0);

        let base = match &self.sink {
            Sink::Ram(bytes) => bytes.len() as u64,
            Sink::Disk { written, .. } => *written,
        };
        self.firsts.extend_from_slice(&run.firsts);
        self.offsets
            .extend(run.offsets.iter().map(|offset| base + offset));
        self.boxes.extend_from_slice(&run.boxes);
        self.record_count += run.nodes as u64;
        match &mut self.sink {
            Sink::Ram(bytes) => bytes.extend_from_slice(&run.data),
            Sink::Disk {
                writer, written, ..
            } => {
                writer
                    .write_all(&run.data)
                    .map_err(|error| format!("Failed to write the node store: {error}"))?;
                *written += run.data.len() as u64;
            }
        }
        Ok(Ok(()))
    }

    /// Hands every block appended so far to `each`, in ID order, and
    /// removes a disk store's file: for falling back to the sharded stores.
    pub(crate) fn drain(
        self,
        mut each: impl FnMut(&[(i64, i32, i32)]) -> Result<(), String>,
    ) -> Result<(), String> {
        let mut records = Vec::with_capacity(BLOCK_NODES);
        let mut unpack = |bytes: &[u8], first: i64| {
            records.clear();
            unpack_block(bytes, first, &mut records);
            each(&records)
        };
        match self.sink {
            Sink::Ram(mut bytes) => {
                bytes.resize(bytes.len() + PADDING_BYTES, 0);
                for (block, &first) in self.firsts.iter().enumerate() {
                    unpack(&bytes[self.offsets[block] as usize..], first)?;
                }
                Ok(())
            }
            Sink::Disk {
                mut writer,
                dir,
                written,
            } => {
                writer
                    .flush()
                    .map_err(|error| format!("Failed to write the node store: {error}"))?;
                drop(writer);
                let path = dir.join(PACKED_FILE);
                let file = File::open(&path)
                    .map_err(|error| format!("Failed to open {path:?}: {error}"))?;
                let mut reader = std::io::BufReader::with_capacity(1 << 20, file);
                let mut bytes = Vec::new();
                for (block, &first) in self.firsts.iter().enumerate() {
                    let end = self.offsets.get(block + 1).copied().unwrap_or(written);
                    let len = (end - self.offsets[block]) as usize;
                    bytes.clear();
                    bytes.resize(len + PADDING_BYTES, 0);
                    std::io::Read::read_exact(&mut reader, &mut bytes[..len])
                        .map_err(|error| format!("Failed to read {path:?}: {error}"))?;
                    unpack(&bytes, first)?;
                }
                drop(reader);
                fs::remove_file(&path)
                    .map_err(|error| format!("Failed to remove {path:?}: {error}"))
            }
        }
    }

    pub(crate) fn finish(self) -> Result<PackedNodeStore, String> {
        let (bytes, dir) = match self.sink {
            Sink::Ram(mut bytes) => {
                bytes.resize(bytes.len() + PADDING_BYTES, 0);
                bytes.shrink_to_fit();
                (PackedBytes::Ram(bytes), None)
            }
            Sink::Disk {
                mut writer, dir, ..
            } => {
                writer
                    .write_all(&[0; PADDING_BYTES])
                    .and_then(|()| writer.flush())
                    .map_err(|error| format!("Failed to write the node store: {error}"))?;
                drop(writer);
                let path = dir.join(PACKED_FILE);
                let file = File::open(&path)
                    .map_err(|error| format!("Failed to open {path:?}: {error}"))?;
                // SAFETY: the file is this store's own, in its own directory,
                // and nothing writes it after this.
                let map = unsafe { Mmap::map(&file) }
                    .map_err(|error| format!("Failed to mmap {path:?}: {error}"))?;
                (PackedBytes::Disk(map), Some(dir))
            }
        };
        let (buckets, bucket_shift) = bucket_table(&self.firsts);
        Ok(PackedNodeStore {
            firsts: self.firsts,
            offsets: self.offsets,
            boxes: self.boxes,
            bytes,
            buckets,
            bucket_shift,
            record_count: self.record_count,
            dir,
        })
    }
}

/// The bucket table over the non-negative block starts: about one bucket per
/// block, so a lookup's binary search covers a block or two.
fn bucket_table(firsts: &[i64]) -> (Vec<u32>, u32) {
    let Some(&max) = firsts.last().filter(|&&max| max >= 0) else {
        return (Vec::new(), 0);
    };
    let blocks = firsts.len().max(1) as u64;
    let span = max as u64 + 1;
    let mut shift = 0;
    while (span >> shift) > blocks {
        shift += 1;
    }
    let count = (span >> shift) as usize + 1;
    let mut buckets = Vec::with_capacity(count + 1);
    let mut block = 0;
    for bucket in 0..=count {
        let start = (bucket as i128) << shift;
        while block < firsts.len() && i128::from(firsts[block]) < start {
            block += 1;
        }
        buckets.push(block as u32);
    }
    (buckets, shift)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_from(runs: &[Vec<(i64, i32, i32)>]) -> PackedNodeStore {
        let mut builder = PackedStoreBuilder::ram();
        for run in runs {
            builder
                .append(PackedRun::pack(run))
                .unwrap()
                .expect("runs in order");
        }
        builder.finish().unwrap()
    }

    /// IDs increase as long as `step` is over 6.
    fn sample(count: i64, step: i64, seed: i64) -> Vec<(i64, i32, i32)> {
        (0..count)
            .map(|index| {
                let id = seed + index * step + (index * index) % 7;
                let lat = 500_000_000 + ((index * 7_919) % 100_000) as i32 - 50_000;
                let lon = 199_000_000 - ((index * 104_729) % 3_000_000) as i32;
                (id, lat, lon)
            })
            .collect()
    }

    #[test]
    fn every_record_comes_back_and_nothing_else() {
        let records = sample(10_000, 9, 1);
        let store = store_from(std::slice::from_ref(&records));
        assert_eq!(store.record_count(), 10_000);
        let held = records
            .iter()
            .map(|r| r.0)
            .collect::<std::collections::HashSet<_>>();
        for &(id, lat, lon) in &records {
            assert_eq!(store.get_decimicro(id), Some((lat, lon)), "node {id}");
            assert_eq!(
                store.get(id),
                Some(crate::node_store::encode_decimicro(lat, lon))
            );
        }
        for id in -5..records.last().unwrap().0 + 20 {
            if !held.contains(&id) {
                assert_eq!(store.get_decimicro(id), None, "node {id}");
            }
        }
        assert_eq!(store.get_decimicro(i64::MAX), None);
        assert_eq!(store.get_decimicro(i64::MIN), None);
    }

    #[test]
    fn extreme_ids_and_coordinates_round_trip() {
        let records = vec![
            (i64::MIN, i32::MIN, i32::MIN),
            (-3, i32::MAX, i32::MIN),
            (0, 0, 0),
            (1, -900_000_000, 1_800_000_000),
            (1 << 40, 900_000_000, -1_800_000_000),
            (i64::MAX - 1, i32::MAX, i32::MAX),
            (i64::MAX, 5, -5),
        ];
        let store = store_from(std::slice::from_ref(&records));
        for &(id, lat, lon) in &records {
            assert_eq!(store.get_decimicro(id), Some((lat, lon)), "node {id}");
        }
        assert_eq!(store.get_decimicro(2), None);
        assert_eq!(store.get_decimicro(-2), None);
        let mut cursor = store.cursor();
        for &(id, lat, lon) in records.iter().rev() {
            assert_eq!(cursor.get_decimicro(id), Some((lat, lon)), "node {id}");
        }
    }

    #[test]
    fn runs_append_in_order_and_refuse_to_go_back() {
        let first = sample(1_000, 8, 10);
        let second = sample(700, 10, first.last().unwrap().0 + 1);
        let store = store_from(&[first.clone(), Vec::new(), second.clone()]);
        let mut cursor = store.cursor();
        for &(id, lat, lon) in first.iter().chain(&second) {
            assert_eq!(store.get_decimicro(id), Some((lat, lon)));
            assert_eq!(cursor.get_decimicro(id), Some((lat, lon)));
        }
        let mut blocks = Vec::new();
        store.for_each_block(|records| blocks.extend_from_slice(records));
        assert_eq!(blocks, [first.clone(), second.clone()].concat());

        let mut builder = PackedStoreBuilder::ram();
        builder.append(PackedRun::pack(&second)).unwrap().unwrap();
        let back = builder.append(PackedRun::pack(&first)).unwrap();
        assert_eq!(back.unwrap_err().records(), first);
        // An ID repeated across runs goes back too.
        let repeat = vec![(second.last().unwrap().0, 1, 1)];
        assert!(builder.append(PackedRun::pack(&repeat)).unwrap().is_err());
    }

    #[test]
    fn a_disk_store_answers_like_a_ram_store_and_cleans_up() {
        let records = sample(5_000, 11, 100);
        let temp = tempfile::tempdir().unwrap();
        let mut builder = PackedStoreBuilder::disk(temp.path()).unwrap();
        builder.append(PackedRun::pack(&records)).unwrap().unwrap();
        let disk = builder.finish().unwrap();
        let ram = store_from(std::slice::from_ref(&records));
        assert_eq!(disk.packed_bytes(), ram.packed_bytes());
        for &(id, _, _) in &records {
            assert_eq!(disk.get(id), ram.get(id));
        }
        let path = temp.path().join("node_store").join(PACKED_FILE);
        assert!(path.exists());
        drop(disk);
        assert!(!path.exists());
    }

    #[test]
    fn may_touch_never_misses_a_node_inside() {
        let records = sample(20_000, 7, 1);
        let store = store_from(std::slice::from_ref(&records));
        let area = DecimicroBox::around_degrees(49.99, 19.0, 50.0, 19.5);
        for way in records.chunks(37) {
            let ids = way.iter().map(|r| r.0).collect::<Vec<_>>();
            let inside = way.iter().any(|&(_, lat, lon)| {
                let (lat, lon) = (lat as f64 / 1e7, lon as f64 / 1e7);
                (49.99..=50.0).contains(&lat) && (19.0..=19.5).contains(&lon)
            });
            if inside {
                assert!(store.may_touch(&ids, &[area]));
            }
        }
        let far = DecimicroBox::around_degrees(-10.0, -10.0, -9.0, -9.0);
        assert!(!store.may_touch(&[1, 3, 5], &[far]));
        assert!(!store.may_touch(&[], &[area]));
    }

    #[test]
    fn a_degree_box_rounds_outwards() {
        let area = DecimicroBox::around_degrees(49.25, 19.9, 49.32, 20.02);
        assert!(area.min_lat <= 492_500_000 && area.max_lat >= 493_200_000);
        assert!(area.min_lon <= 199_000_000 && area.max_lon >= 200_200_000);
        let world = DecimicroBox::around_degrees(-1000.0, -1000.0, 1000.0, 1000.0);
        assert_eq!((world.min_lat, world.max_lon), (i32::MIN, i32::MAX));
    }
}
