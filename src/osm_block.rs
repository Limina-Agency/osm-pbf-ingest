//! `PrimitiveBlock` decoding: buffa views over the trimmed schema in
//! `proto/osmformat.proto`, generated into `src/proto/`.
//!
//! A view borrows the inflated block: string-table entries and packed arrays
//! are read in place, and the fields the schema leaves out (object metadata,
//! changesets) are skipped without being parsed. What each pass builds from a
//! block — node records, normalized objects, owned tags — matches what
//! `osmpbfreader`'s `groups` iterators produced, value for value; the tests at
//! the bottom check that against `osmpbfreader` itself.

use crate::tags::{Repeat, Tags, TagsRef};
use crate::{NormalizedRelation, NormalizedRelationMember, NormalizedWay, RelationMemberType};
use buffa::MessageView as _;
use buffa::RepeatedView;
use std::borrow::Cow;
use std::cell::OnceCell;

#[allow(
    clippy::all,
    clippy::pedantic,
    dead_code,
    missing_docs,
    non_camel_case_types,
    non_snake_case,
    unreachable_pub,
    unused_imports,
    unused_qualifications
)]
pub(crate) mod proto {
    include!("proto/mod.rs");
}

use proto::OSMPBF::__buffa::view::{
    DenseNodesView, NodeView, PrimitiveBlockView, PrimitiveGroupView, RelationView, WayView,
};

/// A decoded block and the per-block values its objects are read against.
pub(crate) struct Block<'a> {
    view: PrimitiveBlockView<'a>,
    granularity: i64,
    lat_offset: i64,
    lon_offset: i64,
    /// The string table as text, built on first use: a node-only pass with no
    /// node filter never reads a string.
    strings: OnceCell<Vec<Cow<'a, str>>>,
}

/// One tag key/value pair, borrowed from a block's string table.
pub(crate) type TagPair<'s> = (&'s str, &'s str);

impl<'a> Block<'a> {
    pub(crate) fn decode(bytes: &'a [u8]) -> Result<Self, String> {
        let view = PrimitiveBlockView::decode_view(bytes)
            .map_err(|error| format!("malformed PrimitiveBlock: {error}"))?;
        Ok(Self {
            granularity: i64::from(view.granularity.unwrap_or(100)),
            lat_offset: view.lat_offset.unwrap_or(0),
            lon_offset: view.lon_offset.unwrap_or(0),
            view,
            strings: OnceCell::new(),
        })
    }

    pub(crate) fn groups(&self) -> &[PrimitiveGroupView<'a>] {
        &self.view.primitivegroup
    }

    /// A latitude in units of 1e-7 degrees, as `osmpbfreader::Node` stores it.
    pub(crate) fn decimicro_lat(&self, lat: i64) -> i32 {
        (self.lat_nanodegrees(lat) / 100) as i32
    }

    pub(crate) fn decimicro_lon(&self, lon: i64) -> i32 {
        (self.lon_nanodegrees(lon) / 100) as i32
    }

    pub(crate) fn lat_nanodegrees(&self, lat: i64) -> i64 {
        self.lat_offset + self.granularity * lat
    }

    pub(crate) fn lon_nanodegrees(&self, lon: i64) -> i64 {
        self.lon_offset + self.granularity * lon
    }

    fn strings(&self) -> &[Cow<'a, str>] {
        self.strings.get_or_init(|| {
            self.view
                .stringtable
                .s
                .iter()
                .map(|bytes| String::from_utf8_lossy(bytes))
                .collect()
        })
    }

    /// String-table entry `index`, decoded as `osmpbfreader` does: lossy UTF-8.
    /// `osmpbfreader` panics on an index past the table; this is an error.
    pub(crate) fn string(&self, index: i64) -> Result<&str, String> {
        let strings = self.strings();
        usize::try_from(index)
            .ok()
            .and_then(|index| strings.get(index))
            .map(|value| value.as_ref())
            .ok_or_else(|| {
                format!(
                    "string-table index {index} is out of range ({} strings)",
                    strings.len()
                )
            })
    }

    /// Fill `pairs` with the tags of a node, way or relation: its `keys` and
    /// `vals` zipped, the shorter list deciding the count.
    pub(crate) fn tag_pairs<'s>(
        &'s self,
        keys: &RepeatedView<'a, u32>,
        vals: &RepeatedView<'a, u32>,
        pairs: &mut Vec<TagPair<'s>>,
    ) -> Result<(), String> {
        pairs.clear();
        for (key, value) in keys.iter().copied().zip(vals.iter().copied()) {
            pairs.push((self.string(i64::from(key))?, self.string(i64::from(value))?));
        }
        Ok(())
    }

    /// A way, normalized, with the owned `tags` built from its
    /// [`tag_pairs`](Self::tag_pairs).
    pub(crate) fn normalized_way(&self, way: &WayView<'a>, tags: Tags) -> NormalizedWay {
        let mut node_ids = Vec::with_capacity(way.refs.len());
        self.way_node_ids(way, &mut node_ids);
        let is_closed = node_ids.len() >= 4 && node_ids.first() == node_ids.last();
        NormalizedWay {
            way_id: way.id,
            node_ids,
            tags,
            is_closed,
        }
    }

    /// The way's node IDs, decoded into `node_ids`.
    pub(crate) fn way_node_ids(&self, way: &WayView<'a>, node_ids: &mut Vec<i64>) {
        node_ids.clear();
        let mut id = 0i64;
        for delta in &way.refs {
            id += delta;
            node_ids.push(id);
        }
    }

    /// A relation, normalized.
    ///
    /// Members are `memids`, `types` and `roles_sid` zipped, the shortest list
    /// deciding the count; a member of a type other than node, way or relation
    /// is dropped, but its id delta still counts — as in `osmpbfreader`.
    pub(crate) fn normalized_relation<'s>(
        &'s self,
        relation: &RelationView<'a>,
        pairs: &mut Vec<TagPair<'s>>,
    ) -> Result<NormalizedRelation, String> {
        let mut members = Vec::new();
        let mut id = 0i64;
        for ((delta, member_type), role) in relation
            .memids
            .iter()
            .zip(relation.types.iter().copied())
            .zip(relation.roles_sid.iter().copied())
        {
            id += delta;
            let member_type = match member_type {
                0 => RelationMemberType::Node,
                1 => RelationMemberType::Way,
                2 => RelationMemberType::Relation,
                _ => continue,
            };
            members.push(NormalizedRelationMember {
                member_type,
                member_id: id,
                role: self.string(i64::from(role))?.to_string(),
            });
        }
        self.tag_pairs(&relation.keys, &relation.vals, pairs)?;
        Ok(NormalizedRelation {
            relation_id: relation.id,
            tags: TagsRef::new(pairs, Repeat::FirstWins).to_tags(),
            members,
        })
    }
}

/// One node of a group, simple or dense, with its tag pairs when the caller
/// asked for them.
pub(crate) struct NodeEntry {
    pub id: i64,
    pub decimicro_lat: i32,
    pub decimicro_lon: i32,
}

/// Walk a group's simple nodes, then its dense nodes, in file order.
///
/// `with_tags` decides whether `visit` gets each node's tag pairs (resolved
/// against the string table) or an empty list; tags are only resolved when it
/// returns true for the node, which a caller uses to skip string lookups when
/// it keeps no tags. The [`Repeat`] passed along says how a repeated key
/// resolves, which differs between the two encodings in `osmpbfreader`.
pub(crate) fn for_each_node<'s, 'a>(
    block: &'s Block<'a>,
    group: &'s PrimitiveGroupView<'a>,
    with_tags: bool,
    pairs: &mut Vec<TagPair<'s>>,
    mut visit: impl FnMut(NodeEntry, &[TagPair<'s>], Repeat) -> Result<(), String>,
) -> Result<(), String> {
    for node in group.nodes.iter() {
        pairs.clear();
        if with_tags {
            block.tag_pairs(&node.keys, &node.vals, pairs)?;
        }
        visit(simple_node_entry(block, node), pairs, Repeat::FirstWins)?;
    }
    let dense: &DenseNodesView<'a> = &group.dense;
    let count = dense.id.len().min(dense.lat.len()).min(dense.lon.len());
    let keys_vals: &[i32] = &dense.keys_vals;
    let mut cursor = 0usize;
    let (mut id, mut lat, mut lon) = (0i64, 0i64, 0i64);
    for index in 0..count {
        id += dense.id[index];
        lat += dense.lat[index];
        lon += dense.lon[index];
        // Each node's tags are `key, value` string indexes ending in a 0; a
        // truncated tail ends the walk where `osmpbfreader` ends it.
        pairs.clear();
        while let Some(&key) = keys_vals.get(cursor) {
            cursor += 1;
            if key == 0 {
                break;
            }
            let Some(&value) = keys_vals.get(cursor) else {
                break;
            };
            cursor += 1;
            if with_tags {
                pairs.push((
                    block.string(i64::from(key))?,
                    block.string(i64::from(value))?,
                ));
            }
        }
        let entry = NodeEntry {
            id,
            decimicro_lat: block.decimicro_lat(lat),
            decimicro_lon: block.decimicro_lon(lon),
        };
        visit(entry, pairs, Repeat::LastWins)?;
    }
    Ok(())
}

fn simple_node_entry(block: &Block<'_>, node: &NodeView<'_>) -> NodeEntry {
    NodeEntry {
        id: node.id,
        decimicro_lat: block.decimicro_lat(node.lat),
        decimicro_lon: block.decimicro_lon(node.lon),
    }
}

#[cfg(test)]
mod tests {
    //! The decoder against `osmpbfreader` on blocks shaped to hit edge cases.

    use super::*;
    use crate::tags::{TagsRef, same_as_osmpbfreader};
    use osmpbfreader::osmformat::{
        DenseNodes, Node as PbfNode, PrimitiveBlock, PrimitiveGroup, Relation as PbfRelation,
        StringTable, Way as PbfWay, relation::MemberType,
    };
    use osmpbfreader::{OsmId, groups};
    use protobuf::{Enum, EnumOrUnknown, Message};

    const STRINGS: &[&[u8]] = &[
        b"",
        b"amenity",
        b"cafe",
        b"name",
        b"Kawiarnia \xff",
        b"x",
        b"highway",
        b"primary",
        b"outer",
        b"type",
        b"multipolygon",
    ];

    /// One block, every group shape at once, with non-default granularity
    /// and offsets, negative coordinates, a lossy UTF-8 string, repeated keys,
    /// lists of mismatched length, and a relation member of an unknown type.
    fn awkward_block() -> PrimitiveBlock {
        let mut strings = StringTable::new();
        strings.s = STRINGS.iter().map(|value| value.to_vec()).collect();

        let mut dense = DenseNodes::new();
        let nodes: [(i64, i64, i64); 6] = [
            (10, 500_123, 190_456),
            (11, -899_999, -1_799_999),
            (15, 0, 0),
            (16, 899_999, 1_799_999),
            (40, -1, 1),
            (41, 123_457, -654_321),
        ];
        let (mut last_id, mut last_lat, mut last_lon) = (0, 0, 0);
        for (id, lat, lon) in nodes {
            dense.id.push(id - last_id);
            dense.lat.push(lat - last_lat);
            dense.lon.push(lon - last_lon);
            (last_id, last_lat, last_lon) = (id, lat, lon);
        }
        dense.keys_vals = vec![
            0, // 10: untagged
            1, 2, 3, 4, 0, // 11: amenity=cafe, name=<lossy>
            0, // 15: untagged
            3, 5, 3, 2, 0, // 16: name=x, then name=cafe (repeated key)
            0, // 40: untagged
            1, // 41: a key with no value and no terminator
        ];
        let mut dense_group = PrimitiveGroup::new();
        dense_group.dense = Some(dense).into();

        let mut simple_group = PrimitiveGroup::new();
        for (id, lat, lon, keys, vals) in [
            (
                7i64,
                1_000i64,
                -2_000i64,
                vec![1u32, 3, 3],
                vec![2u32, 5, 2],
            ),
            (8, -3, 4, vec![], vec![]),
            (9, 5, 6, vec![6], vec![]),
        ] {
            let mut node = PbfNode::new();
            node.set_id(id);
            node.set_lat(lat);
            node.set_lon(lon);
            node.keys = keys;
            node.vals = vals;
            simple_group.nodes.push(node);
        }

        let mut way_group = PrimitiveGroup::new();
        for (id, refs, keys, vals) in [
            (
                100i64,
                vec![5i64, 1, 1, -2],
                vec![6u32, 3, 3],
                vec![7u32, 5, 2],
            ),
            (101, vec![9, 1], vec![], vec![]),
            (102, vec![], vec![6, 3], vec![7]),
        ] {
            let mut way = PbfWay::new();
            way.set_id(id);
            way.refs = refs;
            way.keys = keys;
            way.vals = vals;
            way_group.ways.push(way);
        }

        let mut relation_group = PrimitiveGroup::new();
        let mut relation = PbfRelation::new();
        relation.set_id(500);
        relation.keys = vec![9, 3, 9];
        relation.vals = vec![10, 5, 2];
        relation.memids = vec![100, 1, -50, 7, 3];
        relation.types = vec![
            EnumOrUnknown::new(MemberType::WAY),
            EnumOrUnknown::new(MemberType::WAY),
            EnumOrUnknown::from_i32(7),
            EnumOrUnknown::new(MemberType::RELATION),
            EnumOrUnknown::new(MemberType::NODE),
        ];
        relation.roles_sid = vec![8, 0, 8, 0];
        relation_group.relations.push(relation);

        let mut block = PrimitiveBlock::new();
        block.stringtable = Some(strings).into();
        block.set_granularity(1_000);
        block.set_lat_offset(123_456);
        block.set_lon_offset(-987_654);
        block.primitivegroup = vec![dense_group, simple_group, way_group, relation_group];
        block
    }

    #[test]
    fn nodes_ways_and_relations_match_osmpbfreader() {
        let expected = awkward_block();
        let bytes = expected.write_to_bytes().expect("encode block");
        let block = Block::decode(&bytes).expect("decode block");
        assert_eq!(block.groups().len(), expected.primitivegroup.len());

        for (group, expected_group) in block.groups().iter().zip(&expected.primitivegroup) {
            let mut pairs = Vec::new();
            let mut found = Vec::new();
            for_each_node(&block, group, true, &mut pairs, |node, pairs, repeat| {
                found.push((
                    node.id,
                    node.decimicro_lat,
                    node.decimicro_lon,
                    TagsRef::new(pairs, repeat).to_tags(),
                ));
                Ok(())
            })
            .expect("walk nodes");
            let wanted = groups::simple_nodes(expected_group, &expected)
                .chain(groups::dense_nodes(expected_group, &expected))
                .map(|node| (node.id.0, node.decimicro_lat, node.decimicro_lon, node.tags))
                .collect::<Vec<_>>();
            assert_eq!(found.len(), wanted.len());
            for (found, wanted) in found.iter().zip(&wanted) {
                assert_eq!((found.0, found.1, found.2), (wanted.0, wanted.1, wanted.2));
                assert!(
                    same_as_osmpbfreader(&found.3, &wanted.3),
                    "node {}: {:?} != {:?}",
                    found.0,
                    found.3,
                    wanted.3
                );
            }

            // Without tags the walk visits the same nodes and resolves nothing.
            let mut untagged = Vec::new();
            for_each_node(&block, group, false, &mut pairs, |node, pairs, _| {
                assert!(pairs.is_empty());
                untagged.push(node.id);
                Ok(())
            })
            .expect("walk nodes without tags");
            assert_eq!(
                untagged,
                wanted.iter().map(|node| node.0).collect::<Vec<_>>()
            );

            for (way, expected_way) in group
                .ways
                .iter()
                .zip(groups::ways(expected_group, &expected))
            {
                block
                    .tag_pairs(&way.keys, &way.vals, &mut pairs)
                    .expect("way tags");
                let tags = TagsRef::new(&pairs, Repeat::FirstWins).to_tags();
                let normalized = block.normalized_way(way, tags);
                assert_eq!(normalized.way_id, expected_way.id.0);
                let node_ids = expected_way.nodes.iter().map(|id| id.0).collect::<Vec<_>>();
                assert_eq!(normalized.node_ids, node_ids);
                assert_eq!(
                    normalized.is_closed,
                    node_ids.len() >= 4 && node_ids.first() == node_ids.last()
                );
                assert!(same_as_osmpbfreader(&normalized.tags, &expected_way.tags));
            }
            assert_eq!(group.ways.len(), expected_group.ways.len());

            let relations = groups::relations(expected_group, &expected).collect::<Vec<_>>();
            assert_eq!(group.relations.len(), relations.len());
            for (relation, expected_relation) in group.relations.iter().zip(relations) {
                let normalized = block
                    .normalized_relation(relation, &mut pairs)
                    .expect("relation");
                assert_eq!(normalized.relation_id, expected_relation.id.0);
                assert!(same_as_osmpbfreader(
                    &normalized.tags,
                    &expected_relation.tags
                ));
                let members = expected_relation
                    .refs
                    .iter()
                    .map(|member| {
                        let (member_type, id) = match member.member {
                            OsmId::Node(id) => (RelationMemberType::Node, id.0),
                            OsmId::Way(id) => (RelationMemberType::Way, id.0),
                            OsmId::Relation(id) => (RelationMemberType::Relation, id.0),
                        };
                        NormalizedRelationMember {
                            member_type,
                            member_id: id,
                            role: member.role.to_string(),
                        }
                    })
                    .collect::<Vec<_>>();
                assert_eq!(normalized.members, members);
            }
        }
    }

    #[test]
    fn the_awkward_block_reaches_every_edge_case() {
        let block = awkward_block();
        let relation = &block.primitivegroup[3].relations[0];
        assert!(relation.types.iter().any(|t| t.enum_value().is_err()));
        assert_eq!(MemberType::WAY.value(), 1);
        // The unknown member is dropped and the lists' lengths differ.
        assert_eq!(
            groups::relations(&block.primitivegroup[3], &block)
                .next()
                .unwrap()
                .refs
                .len(),
            3
        );
    }

    #[test]
    fn a_string_index_past_the_table_is_an_error() {
        let mut expected = awkward_block();
        expected.primitivegroup[2].ways[0].keys[0] = 99;
        let bytes = expected.write_to_bytes().expect("encode block");
        let block = Block::decode(&bytes).expect("decode block");
        let way = &block.groups()[2].ways[0];
        let mut pairs = Vec::new();
        let error = block
            .tag_pairs(&way.keys, &way.vals, &mut pairs)
            .expect_err("index 99 is past the table");
        assert!(error.contains("99"), "{error}");
    }

    #[test]
    fn block_metadata_the_schema_leaves_out_is_skipped() {
        use osmpbfreader::osmformat::{DenseInfo, Info};
        let mut expected = awkward_block();
        let mut info = Info::new();
        info.set_version(3);
        info.set_timestamp(1_700_000_000);
        expected.primitivegroup[2].ways[0].info = Some(info).into();
        let mut dense_info = DenseInfo::new();
        dense_info.version = vec![1; 6];
        dense_info.timestamp = vec![5; 6];
        expected.primitivegroup[0].dense.as_mut().unwrap().denseinfo = Some(dense_info).into();
        let bytes = expected.write_to_bytes().expect("encode block");
        let block = Block::decode(&bytes).expect("decode block with metadata");
        assert_eq!(block.groups()[2].ways[0].id, 100);
        assert_eq!(block.groups()[0].dense.id.len(), 6);
    }
}
