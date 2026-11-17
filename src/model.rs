//! The objects the passes hand out: nodes, ways and relations with owned tags,
//! grouped into batches.

use crate::tags::Tags;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationMemberType {
    Node,
    Way,
    Relation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedRelationMember {
    pub member_type: RelationMemberType,
    pub member_id: i64,
    #[serde(default)]
    pub role: String,
}

#[derive(Debug, Clone)]
pub struct NormalizedRelation {
    pub relation_id: i64,
    pub tags: Tags,
    /// In file order.
    pub members: Vec<NormalizedRelationMember>,
}

#[derive(Debug, Clone)]
pub struct NormalizedWay {
    pub way_id: i64,
    /// In file order, repeats included.
    pub node_ids: Vec<i64>,
    pub tags: Tags,
    /// At least four node refs, the first equal to the last.
    pub is_closed: bool,
}

#[derive(Debug, Clone)]
pub struct NormalizedNode {
    pub node_id: i64,
    /// Degrees.
    pub lat: f64,
    /// Degrees.
    pub lng: f64,
    pub tags: Tags,
}

impl NormalizedNode {
    /// A node at a position in units of 100 nanodegrees, converted to degrees
    /// exactly as `osmpbfreader::Node::lat` and `lon` convert it.
    pub(crate) fn from_decimicro(
        node_id: i64,
        decimicro_lat: i32,
        decimicro_lon: i32,
        tags: Tags,
    ) -> Self {
        Self {
            node_id,
            lat: decimicro_lat as f64 * 1e-7,
            lng: decimicro_lon as f64 * 1e-7,
            tags,
        }
    }
}

/// The relations of one block, in file order.
#[derive(Debug, Clone, Default)]
pub struct RelationBatch {
    pub relations: Vec<NormalizedRelation>,
    /// A rough size of the batch in memory, for sinks that buffer batches.
    pub estimated_bytes: usize,
}

/// The ways of one block, in file order.
#[derive(Debug, Clone, Default)]
pub struct WayBatch {
    pub ways: Vec<NormalizedWay>,
    /// A rough size of the batch in memory, for sinks that buffer batches.
    pub estimated_bytes: usize,
}

/// The tagged nodes of one block, in file order.
#[derive(Debug, Clone, Default)]
pub struct NodeBatch {
    pub nodes: Vec<NormalizedNode>,
    /// A rough size of the batch in memory, for sinks that buffer batches.
    pub estimated_bytes: usize,
}

// `push` adds a rough heap footprint to `estimated_bytes`: a fixed overhead,
// 64 bytes per tag, and the size of each member or node ref.

impl RelationBatch {
    pub(crate) fn push(&mut self, relation: NormalizedRelation) {
        self.estimated_bytes += 48usize
            .saturating_add(relation.tags.len().saturating_mul(64))
            .saturating_add(relation.members.len().saturating_mul(32));
        self.relations.push(relation);
    }
}

impl WayBatch {
    pub(crate) fn push(&mut self, way: NormalizedWay) {
        self.estimated_bytes += 32usize
            .saturating_add(way.tags.len().saturating_mul(64))
            .saturating_add(way.node_ids.len().saturating_mul(size_of::<i64>()));
        self.ways.push(way);
    }
}

impl NodeBatch {
    pub(crate) fn push(&mut self, node: NormalizedNode) {
        self.estimated_bytes += 24usize.saturating_add(node.tags.len().saturating_mul(64));
        self.nodes.push(node);
    }
}
