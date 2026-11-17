//! Point lookup of a single OSM object in a PBF, by `(kind, id)`.
//!
//! A PBF is sorted by type then id (the `Sort.Type_then_ID` optional feature),
//! so the blob holding a given object can be found by binary search over blob
//! *first ids* — roughly `log2(blob count)` decompressions, about eleven on a
//! region-sized extract, rather than a pass over the file.
//!
//! This exists so a package does not have to carry a second copy of every
//! object's tags just to answer "what did the user click". The PBF is already
//! shipped with every operational package and is a required core asset, so the
//! tags are on disk either way; what was missing was a way to reach one object
//! without streaming all of them.
//!
//! # Soundness
//!
//! Binary search is only valid on a sorted file. [`PbfObjectReader::open`]
//! refuses a file that does not declare `Sort.Type_then_ID` rather than falling
//! back to a linear scan: a silent fallback would turn a millisecond lookup
//! into a multi-second one under exactly the conditions nobody tests, and a
//! silent *wrong answer* if the caller assumed sortedness elsewhere.

use crate::blob_index::{BlobLocation, ObjectKind, PbfBlobIndex, PbfScanError, read_blob_at};
use crate::osm_block::Block;
use buffa::RepeatedView;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Read, Seek};
use std::path::Path;

/// Nanodegrees per unit of the block's coordinate granularity.
const NANODEGREE: f64 = 1e-9;

/// One resolved OSM object.
#[derive(Debug, Clone, PartialEq)]
pub struct PbfObject {
    pub kind: ObjectKind,
    pub id: i64,
    pub tags: BTreeMap<String, String>,
    /// Present for nodes only. Ways and relations carry references, not
    /// coordinates; a caller that needs a position for them resolves
    /// `node_refs` through the package's node store.
    pub position: Option<(f64, f64)>,
    /// Node ids referenced by a way, in order. Empty for nodes and relations.
    pub node_refs: Vec<i64>,
}

#[derive(Debug)]
pub enum ObjectLookupError {
    Scan(PbfScanError),
    /// The file does not declare `Sort.Type_then_ID`, so blob-level binary
    /// search would be unsound.
    NotSorted,
}

impl std::fmt::Display for ObjectLookupError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scan(error) => write!(formatter, "{error}"),
            Self::NotSorted => write!(
                formatter,
                "PBF does not declare Sort.Type_then_ID; object lookup by id needs a sorted file"
            ),
        }
    }
}

impl std::error::Error for ObjectLookupError {}

impl From<PbfScanError> for ObjectLookupError {
    fn from(error: PbfScanError) -> Self {
        Self::Scan(error)
    }
}

/// The blob range that can hold objects of one kind, and how many blobs were
/// decompressed to answer the last lookup.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LookupStats {
    pub blobs_decompressed: usize,
}

/// Resolves single objects out of a PBF by seeking to the blob that holds them.
///
/// Holds the blob index (about 16 bytes per blob — tens of kilobytes for a
/// region) and an open reader. Nothing else is cached, so the memory cost does
/// not grow with lookups.
#[derive(Debug)]
pub struct PbfObjectReader<R> {
    index: PbfBlobIndex,
    reader: R,
    last_stats: LookupStats,
    /// Memoized first id per (kind, blob). The outer `Option` is "not probed
    /// yet", the inner one is "this blob holds none of that kind".
    ///
    /// Every binary search re-probes the same handful of blobs near the middle
    /// of the range, and reading a first id costs a full block parse — ~8000
    /// entities materialized to read one number. Remembering them turns the
    /// second and later lookups into roughly one block parse each. It fills
    /// lazily, so nothing is paid up front, and it is bounded by the blob count
    /// (a few tens of kilobytes for a region).
    first_ids: [Vec<Option<Option<i64>>>; 3],
}

fn kind_slot(kind: ObjectKind) -> usize {
    match kind {
        ObjectKind::Node => 0,
        ObjectKind::Way => 1,
        ObjectKind::Relation => 2,
    }
}

impl PbfObjectReader<BufReader<File>> {
    /// Scan and classify `path`, leaving it open for lookups.
    ///
    /// The scan reads only blob framing (no decompression); classification
    /// costs a logarithmic number of decompressions to find the type
    /// boundaries. Both are paid once per reader.
    pub fn open(path: &Path) -> Result<Self, ObjectLookupError> {
        let mut index = PbfBlobIndex::scan(path)?;
        if !index.header().sorted_type_then_id() {
            return Err(ObjectLookupError::NotSorted);
        }
        index.classify(path)?;
        let file = File::open(path).map_err(|source| PbfScanError::Io {
            path: path.display().to_string(),
            source,
        })?;
        let blob_count = index.len();
        Ok(Self {
            index,
            reader: BufReader::new(file),
            last_stats: LookupStats::default(),
            first_ids: std::array::from_fn(|_| vec![None; blob_count]),
        })
    }
}

impl<R: Read + Seek> PbfObjectReader<R> {
    /// Build over an already-scanned index and an open stream. The index must
    /// have been classified, or every lookup degrades to searching the whole
    /// file's blob list (correct, just slower).
    pub fn from_parts(index: PbfBlobIndex, reader: R) -> Result<Self, ObjectLookupError> {
        if !index.header().sorted_type_then_id() {
            return Err(ObjectLookupError::NotSorted);
        }
        let blob_count = index.len();
        Ok(Self {
            index,
            reader,
            last_stats: LookupStats::default(),
            first_ids: std::array::from_fn(|_| vec![None; blob_count]),
        })
    }

    pub fn index(&self) -> &PbfBlobIndex {
        &self.index
    }

    /// Blobs decompressed by the most recent [`find`](Self::find).
    pub fn last_stats(&self) -> LookupStats {
        self.last_stats
    }

    /// Locate one object. `Ok(None)` means the file does not contain it.
    pub fn find(
        &mut self,
        kind: ObjectKind,
        id: i64,
    ) -> Result<Option<PbfObject>, ObjectLookupError> {
        self.last_stats = LookupStats::default();

        let candidates: Vec<BlobLocation> = self
            .index
            .blobs_containing(kind)
            .copied()
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Ok(None);
        }

        // Find the last blob whose first id of `kind` is <= the target. Because
        // the file is sorted, the object can only be in that blob: the next
        // blob starts after it, and the previous blob ends before this one
        // starts.
        //
        // `classify` over-approximates the range at both ends — a transition
        // blob is claimed by both neighbouring types — so a blob in this list
        // may hold none of `kind`. Treating that as "starts after the target"
        // (+infinity) is right, because such a blob can only be at the end of
        // the run: every blob before the last one genuinely holds the kind.
        let mut low = 0usize;
        let mut high = candidates.len();
        let mut found: Option<BlobLocation> = None;
        while low < high {
            let mid = low + (high - low) / 2;
            match self.first_id(&candidates[mid], kind)? {
                Some(first) if first <= id => {
                    found = Some(candidates[mid]);
                    low = mid + 1;
                }
                _ => high = mid,
            }
        }

        let Some(location) = found else {
            return Ok(None);
        };
        let bytes = self.read_block(&location)?;
        let block = decode_block(&bytes, &location)?;
        Ok(extract_object(&block, kind, id))
    }

    /// First id of `kind` in this blob, reading and remembering it on a miss.
    fn first_id(
        &mut self,
        location: &BlobLocation,
        kind: ObjectKind,
    ) -> Result<Option<i64>, ObjectLookupError> {
        let slot = kind_slot(kind);
        if let Some(Some(cached)) = self.first_ids[slot].get(location.index).copied() {
            return Ok(cached);
        }
        let bytes = self.read_block(location)?;
        let first = first_id_of_kind(&decode_block(&bytes, location)?, kind);
        if let Some(entry) = self.first_ids[slot].get_mut(location.index) {
            *entry = Some(first);
        }
        Ok(first)
    }

    /// One blob's encoded `PrimitiveBlock` bytes.
    fn read_block(&mut self, location: &BlobLocation) -> Result<Vec<u8>, ObjectLookupError> {
        let blob = read_blob_at(&mut self.reader, location)?;
        let bytes = crate::blob_index::decode_blob_bytes(&blob, location.payload_offset)?;
        self.last_stats.blobs_decompressed += 1;
        Ok(bytes)
    }
}

fn decode_block<'a>(
    bytes: &'a [u8],
    location: &BlobLocation,
) -> Result<Block<'a>, ObjectLookupError> {
    Block::decode(bytes).map_err(|detail| {
        ObjectLookupError::from(PbfScanError::MalformedPrimitiveBlock {
            offset: location.payload_offset,
            detail,
        })
    })
}

/// The smallest id of `kind` in this block, or `None` when it holds none.
fn first_id_of_kind(block: &Block<'_>, kind: ObjectKind) -> Option<i64> {
    let mut smallest: Option<i64> = None;
    let mut consider = |id: i64| {
        smallest = Some(match smallest {
            Some(current) if current <= id => current,
            _ => id,
        });
    };
    for group in block.groups() {
        match kind {
            ObjectKind::Node => {
                // Dense ids are delta-encoded and ascending, so the first
                // delta is the smallest id.
                if let Some(first) = group.dense.id.first() {
                    consider(*first);
                }
                for node in group.nodes.iter() {
                    consider(node.id);
                }
            }
            ObjectKind::Way => {
                for way in group.ways.iter() {
                    consider(way.id);
                }
            }
            ObjectKind::Relation => {
                for relation in group.relations.iter() {
                    consider(relation.id);
                }
            }
        }
    }
    smallest
}

fn extract_object(block: &Block<'_>, kind: ObjectKind, id: i64) -> Option<PbfObject> {
    // A string index past the table reads as an empty string.
    let lookup = |index: i64| -> String { block.string(index).unwrap_or_default().to_string() };

    for group in block.groups() {
        match kind {
            ObjectKind::Node => {
                let dense = &group.dense;
                let mut current_id = 0i64;
                let mut current_lat = 0i64;
                let mut current_lon = 0i64;
                // `keys_vals` is one flat array over all nodes in the group:
                // alternating key/value string indexes, each node terminated by
                // a single 0. It is only advanced in step with the node loop,
                // so it cannot be indexed directly.
                let mut keys_vals_cursor = 0usize;
                for position in 0..dense.id.len() {
                    current_id += dense.id[position];
                    current_lat += *dense.lat.get(position).unwrap_or(&0);
                    current_lon += *dense.lon.get(position).unwrap_or(&0);

                    let mut tags = BTreeMap::new();
                    while keys_vals_cursor < dense.keys_vals.len() {
                        let key = dense.keys_vals[keys_vals_cursor];
                        keys_vals_cursor += 1;
                        if key == 0 {
                            break;
                        }
                        let value = dense
                            .keys_vals
                            .get(keys_vals_cursor)
                            .copied()
                            .unwrap_or_default();
                        keys_vals_cursor += 1;
                        if current_id == id {
                            tags.insert(lookup(i64::from(key)), lookup(i64::from(value)));
                        }
                    }

                    if current_id == id {
                        return Some(PbfObject {
                            kind,
                            id,
                            tags,
                            position: Some(decode_position(block, current_lat, current_lon)),
                            node_refs: Vec::new(),
                        });
                    }
                    if current_id > id {
                        // Ids ascend within a group; we have passed it.
                        break;
                    }
                }
                for node in group.nodes.iter() {
                    if node.id != id {
                        continue;
                    }
                    return Some(PbfObject {
                        kind,
                        id,
                        tags: paired_tags(&node.keys, &node.vals, &lookup),
                        position: Some(decode_position(block, node.lat, node.lon)),
                        node_refs: Vec::new(),
                    });
                }
            }
            ObjectKind::Way => {
                for way in group.ways.iter() {
                    if way.id != id {
                        continue;
                    }
                    let mut node_refs = Vec::with_capacity(way.refs.len());
                    let mut current = 0i64;
                    for delta in &way.refs {
                        current += delta;
                        node_refs.push(current);
                    }
                    return Some(PbfObject {
                        kind,
                        id,
                        tags: paired_tags(&way.keys, &way.vals, &lookup),
                        position: None,
                        node_refs,
                    });
                }
            }
            ObjectKind::Relation => {
                for relation in group.relations.iter() {
                    if relation.id != id {
                        continue;
                    }
                    return Some(PbfObject {
                        kind,
                        id,
                        tags: paired_tags(&relation.keys, &relation.vals, &lookup),
                        position: None,
                        node_refs: Vec::new(),
                    });
                }
            }
        }
    }
    None
}

fn paired_tags(
    keys: &RepeatedView<'_, u32>,
    vals: &RepeatedView<'_, u32>,
    lookup: &impl Fn(i64) -> String,
) -> BTreeMap<String, String> {
    keys.iter()
        .copied()
        .zip(vals.iter().copied())
        .map(|(key, value)| (lookup(i64::from(key)), lookup(i64::from(value))))
        .collect()
}

/// Block coordinates are stored as offsets in units of `granularity`
/// nanodegrees. Both the offset and the granularity are per-block, so this
/// cannot be hoisted out of the block that produced the values.
fn decode_position(block: &Block<'_>, lat: i64, lon: i64) -> (f64, f64) {
    let latitude = NANODEGREE * block.lat_nanodegrees(lat) as f64;
    let longitude = NANODEGREE * block.lon_nanodegrees(lon) as f64;
    (latitude, longitude)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_pbf::{
        TestNode, TestRelation, TestRelationMember, TestWay, node_block, pbf_file, relation_block,
        way_block,
    };
    use osmpbfreader::osmformat::relation::MemberType;
    use std::io::Cursor;

    /// Several blocks per type, so the search has something to bisect and the
    /// node->way and way->relation transitions are both crossed.
    fn fixture() -> Vec<u8> {
        let mut blocks = Vec::new();
        for block in 0..6i64 {
            let base = block * 100;
            let nodes: Vec<TestNode<'_>> = (0..5)
                .map(|offset| TestNode {
                    id: base + offset,
                    lat: 50.0 + (base + offset) as f64 * 0.001,
                    lng: 20.0 + (base + offset) as f64 * 0.001,
                    tags: if (base + offset) == 302 {
                        &[("amenity", "cafe"), ("name", "Kawiarnia")]
                    } else {
                        &[]
                    },
                })
                .collect();
            blocks.push(node_block(&nodes));
        }
        for block in 0..4i64 {
            let base = 1000 + block * 100;
            let ways: Vec<TestWay<'_>> = (0..5)
                .map(|offset| TestWay {
                    id: base + offset,
                    node_ids: &[1, 2, 3],
                    tags: if (base + offset) == 1203 {
                        &[("highway", "residential"), ("name", "Floriańska")]
                    } else {
                        &[("highway", "service")]
                    },
                })
                .collect();
            blocks.push(way_block(&ways));
        }
        for block in 0..3i64 {
            let base = 5000 + block * 100;
            let relations: Vec<TestRelation<'_>> = (0..3)
                .map(|offset| TestRelation {
                    id: base + offset,
                    members: &[TestRelationMember {
                        member_type: MemberType::WAY,
                        member_id: 1000,
                        role: "outer",
                    }],
                    tags: if (base + offset) == 5102 {
                        &[("boundary", "postal_code"), ("postal_code", "31-019")]
                    } else {
                        &[("type", "multipolygon")]
                    },
                })
                .collect();
            blocks.push(relation_block(&relations));
        }
        pbf_file(true, &blocks)
    }

    fn reader() -> PbfObjectReader<Cursor<Vec<u8>>> {
        let bytes = fixture();
        let mut cursor = Cursor::new(bytes);
        let mut index = PbfBlobIndex::scan_reader(&mut cursor).unwrap();
        index.classify_reader(&mut cursor).unwrap();
        PbfObjectReader::from_parts(index, cursor).unwrap()
    }

    #[test]
    fn finds_a_dense_node_with_its_tags_and_position() {
        let mut reader = reader();
        let found = reader.find(ObjectKind::Node, 302).unwrap().expect("node");
        assert_eq!(found.id, 302);
        assert_eq!(found.tags.get("amenity").map(String::as_str), Some("cafe"));
        assert_eq!(
            found.tags.get("name").map(String::as_str),
            Some("Kawiarnia")
        );
        let (lat, lng) = found.position.expect("nodes carry a position");
        assert!((lat - 50.302).abs() < 1e-6, "lat was {lat}");
        assert!((lng - 20.302).abs() < 1e-6, "lng was {lng}");
    }

    /// A node with no tags still resolves. The dense `keys_vals` cursor has to
    /// advance past its terminator even when nothing is collected, or every
    /// later node in the block reads another node's tags.
    #[test]
    fn untagged_nodes_do_not_desynchronise_the_tag_cursor() {
        let mut reader = reader();
        let untagged = reader.find(ObjectKind::Node, 303).unwrap().expect("node");
        assert!(untagged.tags.is_empty(), "got {:?}", untagged.tags);
        // The tagged node in the same block must still read correctly after it.
        let tagged = reader.find(ObjectKind::Node, 302).unwrap().expect("node");
        assert_eq!(tagged.tags.len(), 2);
    }

    #[test]
    fn finds_a_way_with_its_node_refs() {
        let mut reader = reader();
        let found = reader.find(ObjectKind::Way, 1203).unwrap().expect("way");
        assert_eq!(
            found.tags.get("name").map(String::as_str),
            Some("Floriańska")
        );
        assert_eq!(found.node_refs, vec![1, 2, 3]);
        assert!(found.position.is_none(), "ways carry refs, not coordinates");
    }

    #[test]
    fn finds_a_relation() {
        let mut reader = reader();
        let found = reader
            .find(ObjectKind::Relation, 5102)
            .unwrap()
            .expect("relation");
        assert_eq!(
            found.tags.get("postal_code").map(String::as_str),
            Some("31-019")
        );
    }

    /// An id that is absent, one before the first and one after the last, must
    /// all report absence rather than returning a neighbour.
    #[test]
    fn absent_ids_return_none() {
        let mut reader = reader();
        assert!(reader.find(ObjectKind::Node, 47).unwrap().is_none());
        assert!(reader.find(ObjectKind::Node, -1).unwrap().is_none());
        assert!(reader.find(ObjectKind::Node, 999_999).unwrap().is_none());
        assert!(reader.find(ObjectKind::Way, 1_500).unwrap().is_none());
        assert!(reader.find(ObjectKind::Relation, 4_000).unwrap().is_none());
    }

    /// Ids are namespaced by type: way 1203 exists, node 1203 does not.
    #[test]
    fn kinds_do_not_alias_each_other() {
        let mut reader = reader();
        assert!(reader.find(ObjectKind::Way, 1203).unwrap().is_some());
        assert!(reader.find(ObjectKind::Node, 1203).unwrap().is_none());
        assert!(reader.find(ObjectKind::Relation, 1203).unwrap().is_none());
    }

    /// Every object in the file must be reachable — this is the property the
    /// binary search exists for, and an off-by-one at a block boundary would
    /// only show up on the specific ids that sit at one.
    #[test]
    fn every_object_in_the_file_is_reachable() {
        let mut reader = reader();
        for block in 0..6i64 {
            for offset in 0..5i64 {
                let id = block * 100 + offset;
                assert!(
                    reader.find(ObjectKind::Node, id).unwrap().is_some(),
                    "node {id} unreachable"
                );
            }
        }
        for block in 0..4i64 {
            for offset in 0..5i64 {
                let id = 1000 + block * 100 + offset;
                assert!(
                    reader.find(ObjectKind::Way, id).unwrap().is_some(),
                    "way {id} unreachable"
                );
            }
        }
        for block in 0..3i64 {
            for offset in 0..3i64 {
                let id = 5000 + block * 100 + offset;
                assert!(
                    reader.find(ObjectKind::Relation, id).unwrap().is_some(),
                    "relation {id} unreachable"
                );
            }
        }
    }

    /// The point of the exercise: a lookup must cost a logarithmic number of
    /// decompressions, not a pass over the file. 13 data blocks means at most
    /// ~4 probes plus the final read.
    #[test]
    fn lookup_cost_is_logarithmic_not_linear() {
        let mut reader = reader();
        reader.find(ObjectKind::Way, 1203).unwrap().expect("way");
        let stats = reader.last_stats();
        assert!(
            stats.blobs_decompressed <= 6,
            "expected a bisection, decompressed {} blobs",
            stats.blobs_decompressed
        );
    }

    /// An unsorted file cannot be bisected. Refusing is the whole contract —
    /// a silent linear fallback would be slow exactly where nobody measures.
    #[test]
    fn unsorted_files_are_refused_rather_than_scanned() {
        let bytes = pbf_file(
            false,
            &[node_block(&[TestNode {
                id: 1,
                lat: 50.0,
                lng: 20.0,
                tags: &[],
            }])],
        );
        let mut cursor = Cursor::new(bytes);
        let index = PbfBlobIndex::scan_reader(&mut cursor).unwrap();
        let error = PbfObjectReader::from_parts(index, cursor).unwrap_err();
        assert!(matches!(error, ObjectLookupError::NotSorted));
    }
}
