//! Encoding PBF blocks and blob frames, for files written by
//! [`changes::apply_changes`](crate::changes::apply_changes).
//!
//! Blocks hold one primitive group per run of objects of one type, nodes as
//! dense nodes, at the default granularity of 100 nanodegrees, without object
//! metadata.

use crate::blob_index::BlobData;
use crate::changes::OsmObject;
use crate::model::RelationMemberType;
use crate::osm_block::proto::OSMPBF::{
    Blob, BlobHeader, DenseNodes, HeaderBBox, HeaderBlock, PrimitiveBlock, PrimitiveGroup,
    Relation, StringTable, Way,
};
use buffa::Message as _;
use flate2::Compression;
use flate2::write::ZlibEncoder;
use std::collections::HashMap;
use std::io::Write;

pub(crate) const BLOB_TYPE_HEADER: &str = "OSMHeader";
pub(crate) const BLOB_TYPE_DATA: &str = "OSMData";

/// Objects per written block, as osmium and Osmosis write them.
pub(crate) const OBJECTS_PER_BLOCK: usize = 8000;

/// The header block fields a written file carries.
#[derive(Debug, Clone, Default)]
pub(crate) struct Header {
    /// West, south, east, north, in nanodegrees.
    pub bbox: Option<[i64; 4]>,
    pub required_features: Vec<String>,
    pub optional_features: Vec<String>,
    pub writing_program: Option<String>,
    pub source: Option<String>,
    pub replication_timestamp: Option<i64>,
    pub replication_sequence_number: Option<i64>,
    pub replication_base_url: Option<String>,
}

pub(crate) fn encode_header(header: &Header) -> Vec<u8> {
    let mut block = HeaderBlock {
        required_features: header.required_features.clone(),
        optional_features: header.optional_features.clone(),
        writingprogram: header.writing_program.clone(),
        source: header.source.clone(),
        osmosis_replication_timestamp: header.replication_timestamp,
        osmosis_replication_sequence_number: header.replication_sequence_number,
        osmosis_replication_base_url: header.replication_base_url.clone(),
        ..Default::default()
    };
    if let Some([left, bottom, right, top]) = header.bbox {
        block.bbox = buffa::MessageField::some(HeaderBBox {
            left,
            right,
            top,
            bottom,
        });
    }
    block.encode_to_vec()
}

/// Interns a block's strings; index 0 is the empty string, as the format
/// reserves it.
struct Strings<'a> {
    table: Vec<&'a str>,
    lookup: HashMap<&'a str, u32>,
}

impl<'a> Strings<'a> {
    fn new() -> Self {
        Self {
            table: vec![""],
            lookup: HashMap::new(),
        }
    }

    fn intern(&mut self, value: &'a str) -> u32 {
        if let Some(&index) = self.lookup.get(value) {
            return index;
        }
        let index = self.table.len() as u32;
        self.table.push(value);
        self.lookup.insert(value, index);
        index
    }
}

/// One `PrimitiveBlock`, encoded, holding `objects` in the given order.
pub(crate) fn encode_block(objects: &[&OsmObject]) -> Vec<u8> {
    let mut strings = Strings::new();
    let mut groups: Vec<PrimitiveGroup> = Vec::new();
    let mut start = 0;
    while start < objects.len() {
        let kind = objects[start].kind();
        let end = objects[start..]
            .iter()
            .position(|object| object.kind() != kind)
            .map_or(objects.len(), |offset| start + offset);
        groups.push(encode_group(&objects[start..end], &mut strings));
        start = end;
    }
    let block = PrimitiveBlock {
        stringtable: buffa::MessageField::some(StringTable {
            s: strings
                .table
                .iter()
                .map(|value| value.as_bytes().to_vec())
                .collect(),
        }),
        primitivegroup: groups,
        ..Default::default()
    };
    block.encode_to_vec()
}

/// One group of objects all of one type.
fn encode_group<'a>(objects: &[&'a OsmObject], strings: &mut Strings<'a>) -> PrimitiveGroup {
    let mut group = PrimitiveGroup::default();
    let mut dense = DenseNodes::default();
    let (mut id, mut lat, mut lon) = (0i64, 0i64, 0i64);
    for object in objects {
        match object {
            OsmObject::Node(node) => {
                dense.id.push(node.id - id);
                id = node.id;
                dense.lat.push(i64::from(node.decimicro_lat) - lat);
                lat = i64::from(node.decimicro_lat);
                dense.lon.push(i64::from(node.decimicro_lon) - lon);
                lon = i64::from(node.decimicro_lon);
                for (key, value) in node.tags.iter() {
                    dense.keys_vals.push(strings.intern(key) as i32);
                    dense.keys_vals.push(strings.intern(value) as i32);
                }
                dense.keys_vals.push(0);
            }
            OsmObject::Way(way) => {
                let mut encoded = Way {
                    id: way.id,
                    ..Default::default()
                };
                for (key, value) in way.tags.iter() {
                    encoded.keys.push(strings.intern(key));
                    encoded.vals.push(strings.intern(value));
                }
                let mut previous = 0i64;
                for &node in &way.node_ids {
                    encoded.refs.push(node - previous);
                    previous = node;
                }
                group.ways.push(encoded);
            }
            OsmObject::Relation(relation) => {
                let mut encoded = Relation {
                    id: relation.id,
                    ..Default::default()
                };
                for (key, value) in relation.tags.iter() {
                    encoded.keys.push(strings.intern(key));
                    encoded.vals.push(strings.intern(value));
                }
                let mut previous = 0i64;
                for member in &relation.members {
                    encoded.roles_sid.push(strings.intern(&member.role) as i32);
                    encoded.memids.push(member.member_id - previous);
                    previous = member.member_id;
                    encoded.types.push(match member.member_type {
                        RelationMemberType::Node => 0,
                        RelationMemberType::Way => 1,
                        RelationMemberType::Relation => 2,
                    });
                }
                group.relations.push(encoded);
            }
        }
    }
    if !dense.id.is_empty() {
        group.dense = buffa::MessageField::some(dense);
    }
    group
}

/// A blob frame: big-endian header length, `BlobHeader`, then the blob, its
/// payload zlib-compressed.
pub(crate) fn frame(blob_type: &str, payload: &[u8]) -> Result<Vec<u8>, String> {
    let mut encoder = ZlibEncoder::new(
        Vec::with_capacity(payload.len() / 2),
        Compression::default(),
    );
    encoder
        .write_all(payload)
        .and_then(|()| encoder.flush())
        .map_err(|error| error.to_string())?;
    let compressed = encoder.finish().map_err(|error| error.to_string())?;
    let blob = Blob {
        raw_size: Some(i32::try_from(payload.len()).map_err(|_| "block too large".to_string())?),
        data: Some(BlobData::ZlibData(compressed)),
    };
    Ok(frame_blob(blob_type, &blob.encode_to_vec()))
}

/// A frame around an already encoded `Blob`.
pub(crate) fn frame_blob(blob_type: &str, blob: &[u8]) -> Vec<u8> {
    let header = BlobHeader {
        r#type: blob_type.to_string(),
        datasize: blob.len() as i32,
    }
    .encode_to_vec();
    let mut out = Vec::with_capacity(4 + header.len() + blob.len());
    out.extend_from_slice(&(header.len() as u32).to_be_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(blob);
    out
}
