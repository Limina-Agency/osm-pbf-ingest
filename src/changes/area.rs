//! Where a change set changes the map: the boxes of every changed object,
//! before and after, and of the objects whose geometry changed because a node
//! or member of theirs did.

use super::base::{BaseBlob, BaseIndex, read_block};
use super::{Change, ChangeSet, Key, OsmObject};
use crate::RelationMemberType;
use crate::model::NormalizedRelationMember;
use crate::osm_block::{Block, Group, for_each_node};
use crate::tags::Tags;
use memmap2::Mmap;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::fs::File;

/// A box in degrees, west to east and south to north. A point is a box with
/// no extent.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChangedBox {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
}

impl ChangedBox {
    fn point(position: Position) -> Self {
        let (lon, lat) = position.degrees();
        Self {
            west: lon,
            south: lat,
            east: lon,
            north: lat,
        }
    }

    fn extend(&mut self, position: Position) {
        let (lon, lat) = position.degrees();
        self.west = self.west.min(lon);
        self.east = self.east.max(lon);
        self.south = self.south.min(lat);
        self.north = self.north.max(lat);
    }
}

/// What [`changed_area`] found.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ChangedArea {
    /// The boxes, in no particular order; boxes may overlap.
    pub boxes: Vec<ChangedBox>,
    /// Ways the change set doesn't hold whose geometry changed because one of
    /// their nodes did.
    pub ways_moved: usize,
    /// Area relations (multipolygons and boundaries) the change set doesn't
    /// hold whose geometry changed because a member did.
    pub relations_moved: usize,
    /// Nodes a box needed that neither the base file nor the change set
    /// places; their objects' boxes leave them out.
    pub nodes_missing: usize,
}

/// A position in units of 100 nanodegrees, latitude then longitude.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Position(i32, i32);

impl Position {
    fn degrees(self) -> (f64, f64) {
        (f64::from(self.1) / 1e7, f64::from(self.0) / 1e7)
    }
}

/// A relation as the base file holds it.
struct BaseRelation {
    members: Vec<NormalizedRelationMember>,
}

/// Every place the map changes when `changes` is applied to `base`: for each
/// changed object its box before and after, and the same for each way whose
/// node moved and each multipolygon or boundary whose member changed. A
/// relation's box covers its node and way members (not member relations).
///
/// Reads the base file's way and relation blocks, a second time when a
/// relation's other members are needed, and the node blocks holding a node
/// the boxes need. Runs on rayon's current pool.
pub fn changed_area(base: &BaseIndex, changes: &ChangeSet) -> Result<ChangedArea, String> {
    let file =
        File::open(base.path()).map_err(|error| format!("{}: {error}", base.path().display()))?;
    // SAFETY: read only, for the duration of this call.
    let map = unsafe { Mmap::map(&file) }
        .map_err(|error| format!("{}: {error}", base.path().display()))?;
    let reader = Reader {
        map: &map,
        blobs: base.blobs(),
    };

    let mut changed: [FxHashSet<i64>; 3] = Default::default();
    for key in changes.map().keys() {
        changed[usize::from(key.kind)].insert(key.id);
    }
    let [changed_nodes, changed_ways, changed_relations] = &changed;

    // Ways: the old node lists of the changed ones and of those reaching a
    // changed node.
    let mut old_ways: FxHashMap<i64, Vec<i64>> = FxHashMap::default();
    let mut moved_ways: Vec<i64> = Vec::new();
    if !changed_nodes.is_empty() || !changed_ways.is_empty() {
        for (id, nodes) in reader.ways(|id, nodes| {
            changed_ways.contains(&id) || nodes.iter().any(|node| changed_nodes.contains(node))
        })? {
            if !changed_ways.contains(&id) {
                moved_ways.push(id);
            }
            old_ways.insert(id, nodes);
        }
    }

    // Relations: the changed ones, and the areas a changed or moved member
    // reaches.
    let moved_way_set: FxHashSet<i64> = moved_ways.iter().copied().collect();
    let mut old_relations: FxHashMap<i64, BaseRelation> = FxHashMap::default();
    let mut moved_relations: Vec<i64> = Vec::new();
    for (id, relation) in reader.relations(|id, members, area| {
        changed_relations.contains(&id)
            || (area
                && members.iter().any(|member| match member.member_type {
                    RelationMemberType::Node => changed_nodes.contains(&member.member_id),
                    RelationMemberType::Way => {
                        changed_ways.contains(&member.member_id)
                            || moved_way_set.contains(&member.member_id)
                    }
                    RelationMemberType::Relation => false,
                }))
    })? {
        if !changed_relations.contains(&id) {
            moved_relations.push(id);
        }
        old_relations.insert(id, relation);
    }

    // The relations' other member ways, as the base holds them.
    let new_relations = changes.map().values().filter_map(|change| match change {
        Change::Upsert(OsmObject::Relation(relation)) => Some(&relation.members),
        _ => None,
    });
    let wanted: FxHashSet<i64> = old_relations
        .values()
        .map(|relation| &relation.members)
        .chain(new_relations)
        .flatten()
        .filter(|member| member.member_type == RelationMemberType::Way)
        .map(|member| member.member_id)
        .filter(|id| !old_ways.contains_key(id))
        .collect();
    if !wanted.is_empty() {
        old_ways.extend(reader.ways(|id, _| wanted.contains(&id))?);
    }

    // Where every node those objects reach was.
    let mut wanted_nodes: FxHashSet<i64> = changed_nodes.clone();
    wanted_nodes.extend(old_ways.values().flatten().copied());
    for change in changes.map().values() {
        if let Change::Upsert(OsmObject::Way(way)) = change {
            wanted_nodes.extend(way.node_ids.iter().copied());
        }
    }
    for relation in old_relations.values() {
        wanted_nodes.extend(
            relation
                .members
                .iter()
                .filter(|member| member.member_type == RelationMemberType::Node)
                .map(|member| member.member_id),
        );
    }
    for change in changes.map().values() {
        if let Change::Upsert(OsmObject::Relation(relation)) = change {
            wanted_nodes.extend(
                relation
                    .members
                    .iter()
                    .filter(|member| member.member_type == RelationMemberType::Node)
                    .map(|member| member.member_id),
            );
        }
    }
    let old_positions = reader.nodes(&wanted_nodes)?;

    let geometry = Geometry {
        changes,
        old_positions: &old_positions,
        old_ways: &old_ways,
    };
    let mut area = ChangedArea {
        ways_moved: moved_ways.len(),
        relations_moved: moved_relations.len(),
        ..ChangedArea::default()
    };
    let mut missing: FxHashSet<i64> = FxHashSet::default();
    let mut add = |found: Option<ChangedBox>| area.boxes.extend(found);
    for (key, change) in changes.map() {
        let id = key.id;
        match key.kind {
            0 => {
                add(old_positions.get(&id).copied().map(ChangedBox::point));
                if let Change::Upsert(OsmObject::Node(node)) = change {
                    add(Some(ChangedBox::point(Position(
                        node.decimicro_lat,
                        node.decimicro_lon,
                    ))));
                }
            }
            1 => {
                if let Some(nodes) = old_ways.get(&id) {
                    add(geometry.way_box(nodes, Era::Old, &mut missing));
                }
                if let Change::Upsert(OsmObject::Way(way)) = change {
                    add(geometry.way_box(&way.node_ids, Era::New, &mut missing));
                }
            }
            _ => {
                if let Some(relation) = old_relations.get(&id) {
                    add(geometry.relation_box(&relation.members, Era::Old, &mut missing));
                }
                if let Change::Upsert(OsmObject::Relation(relation)) = change {
                    add(geometry.relation_box(&relation.members, Era::New, &mut missing));
                }
            }
        }
    }
    for id in &moved_ways {
        let nodes = &old_ways[id];
        add(geometry.way_box(nodes, Era::Old, &mut missing));
        add(geometry.way_box(nodes, Era::New, &mut missing));
    }
    for id in &moved_relations {
        let members = &old_relations[id].members;
        add(geometry.relation_box(members, Era::Old, &mut missing));
        add(geometry.relation_box(members, Era::New, &mut missing));
    }
    area.nodes_missing = missing.len();
    Ok(area)
}

/// Whether a relation's tags make it an area: a multipolygon or a boundary,
/// whose fill or label can move when any member moves.
fn is_area(tags: &Tags) -> bool {
    matches!(tags.get("type"), Some("multipolygon" | "boundary"))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Era {
    Old,
    New,
}

/// Objects' geometry before and after the change set.
struct Geometry<'a> {
    changes: &'a ChangeSet,
    old_positions: &'a FxHashMap<i64, Position>,
    old_ways: &'a FxHashMap<i64, Vec<i64>>,
}

impl Geometry<'_> {
    fn position(&self, id: i64, era: Era, missing: &mut FxHashSet<i64>) -> Option<Position> {
        let found = match era {
            Era::Old => self.old_positions.get(&id).copied(),
            Era::New => match self.changes.map().get(&Key { kind: 0, id }) {
                Some(Change::Upsert(OsmObject::Node(node))) => {
                    Some(Position(node.decimicro_lat, node.decimicro_lon))
                }
                Some(_) => None,
                None => self.old_positions.get(&id).copied(),
            },
        };
        if found.is_none()
            && !(era == Era::New
                && matches!(
                    self.changes.map().get(&Key { kind: 0, id }),
                    Some(Change::Delete { .. })
                ))
        {
            missing.insert(id);
        }
        found
    }

    fn way_nodes(&self, id: i64, era: Era) -> Option<&[i64]> {
        match era {
            Era::Old => self.old_ways.get(&id).map(Vec::as_slice),
            Era::New => match self.changes.map().get(&Key { kind: 1, id }) {
                Some(Change::Upsert(OsmObject::Way(way))) => Some(&way.node_ids),
                Some(_) => None,
                None => self.old_ways.get(&id).map(Vec::as_slice),
            },
        }
    }

    fn extend(
        &self,
        found: &mut Option<ChangedBox>,
        nodes: &[i64],
        era: Era,
        missing: &mut FxHashSet<i64>,
    ) {
        for &node in nodes {
            if let Some(position) = self.position(node, era, missing) {
                match found {
                    Some(found) => found.extend(position),
                    None => *found = Some(ChangedBox::point(position)),
                }
            }
        }
    }

    fn way_box(&self, nodes: &[i64], era: Era, missing: &mut FxHashSet<i64>) -> Option<ChangedBox> {
        let mut found = None;
        self.extend(&mut found, nodes, era, missing);
        found
    }

    fn relation_box(
        &self,
        members: &[NormalizedRelationMember],
        era: Era,
        missing: &mut FxHashSet<i64>,
    ) -> Option<ChangedBox> {
        let mut found = None;
        for member in members {
            match member.member_type {
                RelationMemberType::Node => {
                    self.extend(&mut found, &[member.member_id], era, missing);
                }
                RelationMemberType::Way => {
                    if let Some(nodes) = self.way_nodes(member.member_id, era) {
                        self.extend(&mut found, nodes, era, missing);
                    }
                }
                RelationMemberType::Relation => {}
            }
        }
        found
    }
}

/// A way's id and node ids.
type WayNodes = (i64, Vec<i64>);

/// Reads the base file's blocks of one kind in parallel.
struct Reader<'m> {
    map: &'m [u8],
    blobs: &'m [BaseBlob],
}

impl Reader<'_> {
    /// The blocks whose key range reaches objects of `kind`.
    fn blobs_of(&self, kind: u8) -> impl Iterator<Item = &BaseBlob> + '_ {
        self.blobs.iter().filter(move |blob| {
            blob.range
                .is_some_and(|(low, high)| low.kind <= kind && kind <= high.kind)
        })
    }

    /// The ways `keep` accepts, with their node lists.
    fn ways(&self, keep: impl Fn(i64, &[i64]) -> bool + Sync) -> Result<Vec<WayNodes>, String> {
        let blobs: Vec<&BaseBlob> = self.blobs_of(1).collect();
        let found: Vec<Result<Vec<WayNodes>, String>> = blobs
            .par_iter()
            .map(|blob| {
                let bytes = read_block(self.map, &blob.location)?;
                let block = Block::decode(&bytes)?;
                let mut kept = Vec::new();
                let mut nodes = Vec::new();
                for group in block.groups() {
                    for way in group.ways.iter() {
                        block.way_node_ids(way, &mut nodes);
                        if keep(way.id, &nodes) {
                            kept.push((way.id, nodes.clone()));
                        }
                    }
                }
                Ok(kept)
            })
            .collect();
        let mut ways = Vec::new();
        for found in found {
            ways.extend(found?);
        }
        Ok(ways)
    }

    /// The relations `keep` accepts, given their members and whether they
    /// are areas.
    fn relations(
        &self,
        keep: impl Fn(i64, &[NormalizedRelationMember], bool) -> bool + Sync,
    ) -> Result<Vec<(i64, BaseRelation)>, String> {
        let blobs: Vec<&BaseBlob> = self.blobs_of(2).collect();
        let found: Vec<Result<Vec<(i64, BaseRelation)>, String>> = blobs
            .par_iter()
            .map(|blob| {
                let bytes = read_block(self.map, &blob.location)?;
                let block = Block::decode(&bytes)?;
                let mut kept = Vec::new();
                let mut pairs = Vec::new();
                for group in block.groups() {
                    for relation in group.relations.iter() {
                        let relation = block.normalized_relation(relation, &mut pairs)?;
                        let area = is_area(&relation.tags);
                        if keep(relation.relation_id, &relation.members, area) {
                            kept.push((
                                relation.relation_id,
                                BaseRelation {
                                    members: relation.members,
                                },
                            ));
                        }
                    }
                }
                Ok(kept)
            })
            .collect();
        let mut relations = Vec::new();
        for found in found {
            relations.extend(found?);
        }
        Ok(relations)
    }

    /// Where the base file places each of `wanted`. Only the node blocks
    /// whose id range holds a wanted node are decoded.
    fn nodes(&self, wanted: &FxHashSet<i64>) -> Result<FxHashMap<i64, Position>, String> {
        if wanted.is_empty() {
            return Ok(FxHashMap::default());
        }
        let mut sorted: Vec<i64> = wanted.iter().copied().collect();
        sorted.sort_unstable();
        let blobs: Vec<&BaseBlob> = self
            .blobs_of(0)
            .filter(|blob| {
                let Some((low, high)) = blob.range else {
                    return false;
                };
                let low = if low.kind == 0 { low.id } else { i64::MIN };
                let high = if high.kind == 0 { high.id } else { i64::MAX };
                let at = sorted.partition_point(|id| *id < low);
                sorted.get(at).is_some_and(|id| *id <= high)
            })
            .collect();
        let found: Vec<Result<Vec<(i64, Position)>, String>> = blobs
            .par_iter()
            .map(|blob| {
                let bytes = read_block(self.map, &blob.location)?;
                let block = Block::decode(&bytes)?;
                let mut kept = Vec::new();
                let mut pairs = Vec::new();
                for group in block.groups() {
                    for_each_node(
                        &block,
                        Group::View(group),
                        false,
                        &mut pairs,
                        |node, _, _| {
                            if wanted.contains(&node.id) {
                                kept.push((
                                    node.id,
                                    Position(node.decimicro_lat, node.decimicro_lon),
                                ));
                            }
                            Ok(())
                        },
                    )?;
                }
                Ok(kept)
            })
            .collect();
        let mut positions = FxHashMap::default();
        for found in found {
            positions.extend(found?);
        }
        Ok(positions)
    }
}
