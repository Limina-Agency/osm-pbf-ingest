//! Builders for synthetic PBF files used in tests.
//!
//! These emit real, spec-shaped PBFs — string tables, delta-encoded dense
//! nodes, delta-encoded way refs — so tests exercise the same decode path as a
//! Geofabrik extract rather than a convenient approximation.

use osmpbfreader::fileformat::{Blob, BlobHeader};
use osmpbfreader::osmformat::relation::MemberType;
use osmpbfreader::osmformat::{
    DenseNodes, HeaderBBox, HeaderBlock, PrimitiveBlock, PrimitiveGroup, Relation, StringTable, Way,
};
use protobuf::Message;

pub const BLOB_TYPE_HEADER: &str = "OSMHeader";
pub const BLOB_TYPE_DATA: &str = "OSMData";

/// PBF coordinates are nanodegrees scaled by `granularity`; the default 100
/// gives 100-nanodegree units, i.e. 1e-7 degrees.
const GRANULARITY: i32 = 100;
const COORD_SCALE: f64 = 1e7;

/// Interns strings into a `StringTable`. Index 0 is reserved as the empty
/// string by convention, and `keys_vals` uses 0 as its per-node terminator.
#[derive(Default)]
struct Strings {
    table: Vec<Vec<u8>>,
    lookup: std::collections::HashMap<String, u32>,
}

impl Strings {
    fn new() -> Self {
        Self {
            table: vec![Vec::new()],
            lookup: std::collections::HashMap::new(),
        }
    }

    fn intern(&mut self, value: &str) -> u32 {
        if let Some(index) = self.lookup.get(value) {
            return *index;
        }
        let index = self.table.len() as u32;
        self.table.push(value.as_bytes().to_vec());
        self.lookup.insert(value.to_string(), index);
        index
    }

    fn finish(self) -> StringTable {
        let mut table = StringTable::new();
        table.s = self.table;
        table
    }
}

/// Wrap a payload in the PBF frame: big-endian length, `BlobHeader`, blob.
pub fn frame(blob_type: &str, payload: &[u8]) -> Vec<u8> {
    let mut blob = Blob::new();
    blob.set_raw(payload.to_vec());
    blob.set_raw_size(payload.len() as i32);
    let blob_bytes = blob.write_to_bytes().expect("encode blob");

    let mut blob_header = BlobHeader::new();
    blob_header.set_type(blob_type.to_string());
    blob_header.set_datasize(blob_bytes.len() as i32);
    let header_bytes = blob_header.write_to_bytes().expect("encode blob header");

    let mut out = Vec::new();
    out.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(&header_bytes);
    out.extend_from_slice(&blob_bytes);
    out
}

pub fn header_payload(optional_features: &[&str], required_features: &[&str]) -> Vec<u8> {
    let mut block = HeaderBlock::new();
    for feature in optional_features {
        block.optional_features.push((*feature).to_string());
    }
    for feature in required_features {
        block.required_features.push((*feature).to_string());
    }
    let mut bbox = HeaderBBox::new();
    bbox.set_left(18_850_000_000);
    bbox.set_right(21_500_000_000);
    bbox.set_top(50_650_000_000);
    bbox.set_bottom(48_950_000_000);
    block.bbox = Some(bbox).into();
    block.set_writingprogram("osm-pbf-ingest-test".to_string());
    block.write_to_bytes().expect("encode header block")
}

fn finish_block(group: PrimitiveGroup, strings: Strings) -> Vec<u8> {
    let mut block = PrimitiveBlock::new();
    block.stringtable = Some(strings.finish()).into();
    block.set_granularity(GRANULARITY);
    block.primitivegroup.push(group);
    block.write_to_bytes().expect("encode primitive block")
}

pub struct TestNode<'a> {
    pub id: i64,
    pub lat: f64,
    pub lng: f64,
    pub tags: &'a [(&'a str, &'a str)],
}

/// A block of dense-encoded nodes, which is how every real PBF stores them.
pub fn node_block(nodes: &[TestNode<'_>]) -> Vec<u8> {
    let mut strings = Strings::new();
    let mut dense = DenseNodes::new();
    let (mut last_id, mut last_lat, mut last_lon) = (0i64, 0i64, 0i64);

    for node in nodes {
        let lat = (node.lat * COORD_SCALE).round() as i64;
        let lon = (node.lng * COORD_SCALE).round() as i64;
        dense.id.push(node.id - last_id);
        dense.lat.push(lat - last_lat);
        dense.lon.push(lon - last_lon);
        last_id = node.id;
        last_lat = lat;
        last_lon = lon;

        // keys_vals is a flat run per node, terminated by a zero.
        for (key, value) in node.tags {
            let key_index = strings.intern(key);
            let value_index = strings.intern(value);
            dense.keys_vals.push(key_index as i32);
            dense.keys_vals.push(value_index as i32);
        }
        dense.keys_vals.push(0);
    }

    let mut group = PrimitiveGroup::new();
    group.dense = Some(dense).into();
    finish_block(group, strings)
}

pub struct TestWay<'a> {
    pub id: i64,
    pub node_ids: &'a [i64],
    pub tags: &'a [(&'a str, &'a str)],
}

pub fn way_block(ways: &[TestWay<'_>]) -> Vec<u8> {
    let mut strings = Strings::new();
    let mut group = PrimitiveGroup::new();

    for way in ways {
        let mut encoded = Way::new();
        encoded.set_id(way.id);
        for (key, value) in way.tags {
            encoded.keys.push(strings.intern(key));
            encoded.vals.push(strings.intern(value));
        }
        let mut last = 0i64;
        for node_id in way.node_ids {
            encoded.refs.push(node_id - last);
            last = *node_id;
        }
        group.ways.push(encoded);
    }

    finish_block(group, strings)
}

pub struct TestRelationMember<'a> {
    pub member_type: MemberType,
    pub member_id: i64,
    pub role: &'a str,
}

pub struct TestRelation<'a> {
    pub id: i64,
    pub members: &'a [TestRelationMember<'a>],
    pub tags: &'a [(&'a str, &'a str)],
}

pub fn relation_block(relations: &[TestRelation<'_>]) -> Vec<u8> {
    let mut strings = Strings::new();
    let mut group = PrimitiveGroup::new();

    for relation in relations {
        let mut encoded = Relation::new();
        encoded.set_id(relation.id);
        for (key, value) in relation.tags {
            encoded.keys.push(strings.intern(key));
            encoded.vals.push(strings.intern(value));
        }
        let mut last = 0i64;
        for member in relation.members {
            encoded.roles_sid.push(strings.intern(member.role) as i32);
            encoded.memids.push(member.member_id - last);
            encoded.types.push(member.member_type.into());
            last = member.member_id;
        }
        group.relations.push(encoded);
    }

    finish_block(group, strings)
}

/// Assemble a whole file from pre-built block payloads.
pub fn pbf_file(sorted: bool, blocks: &[Vec<u8>]) -> Vec<u8> {
    let optional: &[&str] = if sorted { &["Sort.Type_then_ID"] } else { &[] };
    let mut bytes = frame(
        BLOB_TYPE_HEADER,
        &header_payload(optional, &["OsmSchema-V0.6", "DenseNodes"]),
    );
    for block in blocks {
        bytes.extend_from_slice(&frame(BLOB_TYPE_DATA, block));
    }
    bytes
}

/// A file shaped like a real sorted extract: two node blocks, two way blocks
/// and a relation block. Node 2 is the only tagged node, way 100 the only
/// tagged way, and relation 500 a multipolygon over both ways.
///
/// Two way blocks, not one, because the *first* blob holding ways sits on the
/// node→way seam and is read conservatively. Only an interior way blob can
/// actually be skipped, and a fixture that cannot show that would not be
/// testing anything.
pub fn sorted_fixture() -> Vec<u8> {
    let node = |id, lat, lng, tags| TestNode { id, lat, lng, tags };
    pbf_file(
        true,
        &[
            node_block(&[
                node(1, 50.0, 19.0, &[]),
                node(2, 50.1, 19.1, &[("amenity", "cafe")]),
            ]),
            node_block(&[node(3, 50.2, 19.2, &[]), node(4, 50.3, 19.3, &[])]),
            way_block(&[TestWay {
                id: 100,
                node_ids: &[1, 2, 3],
                tags: &[("highway", "residential")],
            }]),
            way_block(&[TestWay {
                id: 101,
                node_ids: &[3, 4],
                tags: &[],
            }]),
            relation_block(&[TestRelation {
                id: 500,
                members: &[
                    TestRelationMember {
                        member_type: MemberType::WAY,
                        member_id: 100,
                        role: "outer",
                    },
                    TestRelationMember {
                        member_type: MemberType::WAY,
                        member_id: 101,
                        role: "inner",
                    },
                ],
                tags: &[("type", "multipolygon"), ("landuse", "forest")],
            }]),
        ],
    )
}

pub fn write_fixture(dir: &std::path::Path, bytes: &[u8]) -> std::path::PathBuf {
    let path = dir.join("fixture.osm.pbf");
    std::fs::write(&path, bytes).expect("write fixture");
    path
}

/// Byte length of the leading OSMHeader frame, so a fixture's data frames can
/// be spliced onto a different header.
pub fn header_frame_len(bytes: &[u8]) -> usize {
    let header_len = u32::from_be_bytes(bytes[..4].try_into().expect("length prefix")) as usize;
    let blob_header = BlobHeader::parse_from_bytes(&bytes[4..4 + header_len]).expect("blob header");
    4 + header_len + blob_header.datasize() as usize
}
