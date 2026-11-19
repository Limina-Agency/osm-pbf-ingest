//! The PBF a change set is applied to: where each block's objects start and
//! end, and optionally which objects it holds.

use super::Key;
use crate::blob_index::{
    Blob, BlobLocation, ObjectKind, PbfBlobIndex, PbfFileHeader, decode_blob_bytes,
};
use crate::osm_block::{Block, for_each_node};
use buffa::Message as _;
use memmap2::Mmap;
use rayon::prelude::*;
use roaring::RoaringTreemap;
use std::fs::File;
use std::path::{Path, PathBuf};

/// Blocks decoded together while scanning, to bound the ids held at once.
const SCAN_CHUNK: usize = 64;

/// One data block of the base file.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BaseBlob {
    pub(crate) location: BlobLocation,
    /// The lowest and highest key in the block; `None` when it holds no
    /// objects.
    pub(crate) range: Option<(Key, Key)>,
    pub(crate) objects: usize,
}

/// A PBF scanned for applying changes to it: each block's key range, whether
/// the whole file is sorted by type then id, and, when asked for, the ids of
/// every object (which [`Clip`](super::Clip) needs).
#[derive(Debug)]
pub struct BaseIndex {
    path: PathBuf,
    header: PbfFileHeader,
    blobs: Vec<BaseBlob>,
    sorted: bool,
    ids: Option<[RoaringTreemap; 3]>,
}

impl BaseIndex {
    /// Decodes every block once, on rayon's current pool. `with_ids` also
    /// collects every object's id: about 2 bytes per object for an extract
    /// sorted by id.
    pub fn scan(path: &Path, with_ids: bool) -> Result<Self, String> {
        let index = PbfBlobIndex::scan(path).map_err(|error| error.to_string())?;
        let header = index.header().clone();
        let unsupported = header.unsupported_required_features();
        if !unsupported.is_empty() {
            return Err(format!(
                "{} needs features this crate does not read: {}",
                path.display(),
                unsupported.join(", ")
            ));
        }
        let file = File::open(path).map_err(|error| format!("{}: {error}", path.display()))?;
        // SAFETY: the file is only read, and the map lives no longer than this
        // call; a file truncated underneath us is an error the OS reports.
        let map =
            unsafe { Mmap::map(&file) }.map_err(|error| format!("{}: {error}", path.display()))?;

        let mut blobs = Vec::with_capacity(index.len());
        let mut ids = with_ids.then(|| std::array::from_fn(|_| RoaringTreemap::new()));
        let mut sorted = true;
        let mut previous: Option<Key> = None;
        for chunk in index.data_blobs().chunks(SCAN_CHUNK) {
            let scanned: Vec<Result<ScannedBlob, String>> = chunk
                .par_iter()
                .map(|location| scan_blob(&map, location, with_ids))
                .collect();
            for (location, scanned) in chunk.iter().zip(scanned) {
                let scanned = scanned?;
                sorted &= scanned.sorted;
                if let Some((first, _)) = scanned.first_last {
                    if previous.is_some_and(|previous| first <= previous) {
                        sorted = false;
                    }
                    previous = scanned.first_last.map(|(_, last)| last);
                }
                if let (Some(ids), Some(keys)) = (&mut ids, scanned.keys) {
                    for key in keys {
                        let set: &mut RoaringTreemap = &mut ids[usize::from(key.kind)];
                        set.insert(key.id() as u64);
                    }
                }
                blobs.push(BaseBlob {
                    location: *location,
                    range: scanned.range,
                    objects: scanned.objects,
                });
            }
        }
        Ok(Self {
            path: path.to_path_buf(),
            header,
            blobs,
            sorted,
            ids,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn header(&self) -> &PbfFileHeader {
        &self.header
    }

    /// True when every object follows the one before it in type-then-id
    /// order, whatever the header declares.
    pub fn is_sorted(&self) -> bool {
        self.sorted
    }

    /// Whether the file holds the object. Always false for an index scanned
    /// without ids.
    pub fn contains(&self, kind: ObjectKind, id: i64) -> bool {
        self.ids
            .as_ref()
            .is_some_and(|ids| ids[usize::from(Key::new(kind, id).kind)].contains(id as u64))
    }

    pub fn has_ids(&self) -> bool {
        self.ids.is_some()
    }

    /// Objects in the file.
    pub fn object_count(&self) -> usize {
        self.blobs.iter().map(|blob| blob.objects).sum()
    }

    pub(crate) fn blobs(&self) -> &[BaseBlob] {
        &self.blobs
    }
}

struct ScannedBlob {
    range: Option<(Key, Key)>,
    first_last: Option<(Key, Key)>,
    objects: usize,
    sorted: bool,
    keys: Option<Vec<Key>>,
}

fn scan_blob(map: &[u8], location: &BlobLocation, with_ids: bool) -> Result<ScannedBlob, String> {
    let mut keys = Vec::new();
    let mut objects = 0;
    let mut sorted = true;
    let mut first: Option<Key> = None;
    let mut last: Option<Key> = None;
    let mut low: Option<Key> = None;
    let mut high: Option<Key> = None;
    for_each_key(map, location, |key| {
        objects += 1;
        if last.is_some_and(|last| key <= last) {
            sorted = false;
        }
        first.get_or_insert(key);
        last = Some(key);
        low = Some(low.map_or(key, |low| low.min(key)));
        high = Some(high.map_or(key, |high| high.max(key)));
        if with_ids {
            keys.push(key);
        }
    })?;
    Ok(ScannedBlob {
        range: low.zip(high),
        first_last: first.zip(last),
        objects,
        sorted,
        keys: with_ids.then_some(keys),
    })
}

/// Calls `visit` with the key of every object in a block, in file order.
fn for_each_key(
    map: &[u8],
    location: &BlobLocation,
    mut visit: impl FnMut(Key),
) -> Result<(), String> {
    let bytes = read_block(map, location)?;
    let block = Block::decode(&bytes)?;
    let mut pairs = Vec::new();
    for group in block.groups() {
        for_each_node(&block, group, false, &mut pairs, |node, _, _| {
            visit(Key::new(ObjectKind::Node, node.id));
            Ok(())
        })?;
        for way in group.ways.iter() {
            visit(Key::new(ObjectKind::Way, way.id));
        }
        for relation in group.relations.iter() {
            visit(Key::new(ObjectKind::Relation, relation.id));
        }
    }
    Ok(())
}

/// The payload bytes of one blob in a mapped file.
pub(crate) fn payload<'m>(map: &'m [u8], location: &BlobLocation) -> Result<&'m [u8], String> {
    let start = usize::try_from(location.payload_offset).map_err(|error| error.to_string())?;
    map.get(start..start + location.payload_len as usize)
        .ok_or_else(|| format!("blob {} runs past the end of the file", location.index))
}

/// One blob's `PrimitiveBlock` bytes, inflated when compressed.
pub(crate) fn read_block(map: &[u8], location: &BlobLocation) -> Result<Vec<u8>, String> {
    let blob = Blob::decode_from_slice(payload(map, location)?)
        .map_err(|error| format!("blob {}: {error}", location.index))?;
    decode_blob_bytes(&blob, location.payload_offset).map_err(|error| error.to_string())
}
