//! OSM change files (`.osc`, `.osc.gz`): reading them into a [`ChangeSet`],
//! and applying the set to a PBF, which writes an updated PBF.
//!
//! This is how a compiler keeps an extract fresh without a database: read the
//! extract once into a [`BaseIndex`], read each replication diff since the
//! extract's timestamp into one [`ChangeSet`], and [`apply_changes`] writes the
//! updated file. Only the blocks a change touches are decoded and written
//! again; every other block is copied byte for byte.
//!
//! ```no_run
//! use osm_pbf_ingest::changes::{ApplyOptions, BaseIndex, ChangeSet, Clip, apply_changes};
//! use std::path::Path;
//!
//! # fn main() -> Result<(), String> {
//! let base = BaseIndex::scan(Path::new("malopolskie.osm.pbf"), true)?;
//! let clip = Clip::to_header_bounds(&base).expect("the extract has a header box");
//! let mut changes = ChangeSet::new();
//! for diff in ["124335.osc.gz", "124336.osc.gz"] {
//!     changes.read_osc(Path::new(diff), Some(&clip))?;
//! }
//! let report = apply_changes(
//!     &base,
//!     &changes,
//!     Path::new("malopolskie-updated.osm.pbf"),
//!     &ApplyOptions::default(),
//! )?;
//! println!("{} blocks rewritten", report.blobs_rewritten);
//! # Ok(())
//! # }
//! ```
//!
//! # Planet diffs and a region
//!
//! Replication diffs from planet.openstreetmap.org hold the whole world's
//! edits. A [`Clip`] keeps the ones that matter to an extract: every change to
//! an object the extract holds, new nodes inside its box, new ways with a node
//! in the extract, the nodes of every kept way, and new relations with a kept
//! member. This mirrors how extracts are cut, but is not identical to cutting
//! a fresh extract: a new way that reaches into the region only through nodes
//! created in an earlier, discarded diff is missed. Diffs from a regional
//! replication server (Geofabrik's `-updates`) need no clip.
//!
//! # What is written
//!
//! Rewritten and new blocks carry no object metadata (version, timestamp,
//! user), which this crate never reads; copied blocks keep theirs. A file
//! sorted by type then id stays sorted: new objects are placed in id order.
//! For any other file they are appended after the existing objects.

mod apply;
mod base;
mod osc;

pub use apply::{ApplyOptions, ApplyReport, apply_changes};
pub use base::BaseIndex;

use crate::RelationMemberType;
use crate::blob_index::{ObjectKind, PbfHeaderBounds};
use crate::model::NormalizedRelationMember;
use crate::tags::Tags;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

/// A node with its position in units of 100 nanodegrees (1e-7 degrees), the
/// precision OSM stores.
#[derive(Debug, Clone, PartialEq)]
pub struct OsmNode {
    pub id: i64,
    /// 0 when the source gave none.
    pub version: u32,
    pub decimicro_lat: i32,
    pub decimicro_lon: i32,
    pub tags: Tags,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OsmWay {
    pub id: i64,
    /// 0 when the source gave none.
    pub version: u32,
    pub node_ids: Vec<i64>,
    pub tags: Tags,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OsmRelation {
    pub id: i64,
    /// 0 when the source gave none.
    pub version: u32,
    pub members: Vec<NormalizedRelationMember>,
    pub tags: Tags,
}

/// A node, way or relation, complete.
#[derive(Debug, Clone, PartialEq)]
pub enum OsmObject {
    Node(OsmNode),
    Way(OsmWay),
    Relation(OsmRelation),
}

impl OsmObject {
    pub fn kind(&self) -> ObjectKind {
        match self {
            Self::Node(_) => ObjectKind::Node,
            Self::Way(_) => ObjectKind::Way,
            Self::Relation(_) => ObjectKind::Relation,
        }
    }

    pub fn id(&self) -> i64 {
        match self {
            Self::Node(node) => node.id,
            Self::Way(way) => way.id,
            Self::Relation(relation) => relation.id,
        }
    }

    pub fn version(&self) -> u32 {
        match self {
            Self::Node(node) => node.version,
            Self::Way(way) => way.version,
            Self::Relation(relation) => relation.version,
        }
    }

    pub fn tags(&self) -> &Tags {
        match self {
            Self::Node(node) => &node.tags,
            Self::Way(way) => &way.tags,
            Self::Relation(relation) => &relation.tags,
        }
    }

    pub(crate) fn key(&self) -> Key {
        Key::new(self.kind(), self.id())
    }
}

/// What a change file does to an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeAction {
    Create,
    Modify,
    Delete,
}

/// The final state of one changed object: its new contents, or deleted.
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    Upsert(OsmObject),
    Delete {
        kind: ObjectKind,
        id: i64,
        version: u32,
    },
}

impl Change {
    pub fn kind(&self) -> ObjectKind {
        match self {
            Self::Upsert(object) => object.kind(),
            Self::Delete { kind, .. } => *kind,
        }
    }

    pub fn id(&self) -> i64 {
        match self {
            Self::Upsert(object) => object.id(),
            Self::Delete { id, .. } => *id,
        }
    }

    pub fn version(&self) -> u32 {
        match self {
            Self::Upsert(object) => object.version(),
            Self::Delete { version, .. } => *version,
        }
    }

    pub(crate) fn key(&self) -> Key {
        Key::new(self.kind(), self.id())
    }
}

/// Orders objects as a file sorted by type then id does: nodes, ways, then
/// relations, each by id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Key {
    kind: u8,
    id: i64,
}

impl Key {
    pub(crate) fn new(kind: ObjectKind, id: i64) -> Self {
        let kind = match kind {
            ObjectKind::Node => 0,
            ObjectKind::Way => 1,
            ObjectKind::Relation => 2,
        };
        Self { kind, id }
    }

    pub(crate) fn id(self) -> i64 {
        self.id
    }
}

/// Keeps the changes that matter to one extract: see the module docs.
#[derive(Debug, Clone, Copy)]
pub struct Clip<'a> {
    base: &'a BaseIndex,
    bounds: PbfHeaderBounds,
}

impl<'a> Clip<'a> {
    /// Clip to `bounds`, in degrees; `west > east` crosses the antimeridian.
    /// `base` must have been scanned with its object ids.
    pub fn new(base: &'a BaseIndex, bounds: PbfHeaderBounds) -> Result<Self, String> {
        if !base.has_ids() {
            return Err("clipping changes needs a base index scanned with its ids".to_string());
        }
        Ok(Self { base, bounds })
    }

    /// Clip to the box in the base file's header, when it has one.
    pub fn to_header_bounds(base: &'a BaseIndex) -> Option<Self> {
        base.header()
            .bounds
            .and_then(|bounds| Self::new(base, bounds).ok())
    }

    fn contains(&self, node: &OsmNode) -> bool {
        let lat = f64::from(node.decimicro_lat) / 1e7;
        let lon = f64::from(node.decimicro_lon) / 1e7;
        let bounds = &self.bounds;
        let within_lon = if bounds.west <= bounds.east {
            (bounds.west..=bounds.east).contains(&lon)
        } else {
            lon >= bounds.west || lon <= bounds.east
        };
        within_lon && (bounds.south..=bounds.north).contains(&lat)
    }
}

/// What reading one change file did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReadReport {
    /// Objects the file holds.
    pub objects: usize,
    /// Of those, the ones the clip kept (all of them without a clip).
    pub kept: usize,
}

/// Counts of the objects in a [`ChangeSet`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChangeCounts {
    pub nodes: usize,
    pub ways: usize,
    pub relations: usize,
    pub deletions: usize,
}

/// The latest state of every object a series of change files touched.
///
/// Read the files oldest first. A later change replaces an earlier one unless
/// the earlier one carries a higher version, so reading a diff twice, or
/// diffs that overlap, gives the same set.
#[derive(Debug, Clone, Default)]
pub struct ChangeSet {
    changes: BTreeMap<Key, Change>,
}

impl ChangeSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read a change file, gzip-compressed or not, and add its changes; with a
    /// `clip`, only the ones it keeps.
    pub fn read_osc(&mut self, path: &Path, clip: Option<&Clip<'_>>) -> Result<ReadReport, String> {
        let entries =
            osc::read_path(path).map_err(|error| format!("{}: {error}", path.display()))?;
        Ok(self.add_entries(entries, clip))
    }

    /// [`read_osc`](Self::read_osc) from bytes in memory.
    pub fn read_osc_bytes(
        &mut self,
        bytes: &[u8],
        clip: Option<&Clip<'_>>,
    ) -> Result<ReadReport, String> {
        let entries = osc::read_bytes(bytes)?;
        Ok(self.add_entries(entries, clip))
    }

    /// Record one change, unless the set holds a higher version of the object.
    pub fn insert(&mut self, action: ChangeAction, object: OsmObject) {
        let change = match action {
            ChangeAction::Delete => Change::Delete {
                kind: object.kind(),
                id: object.id(),
                version: object.version(),
            },
            ChangeAction::Create | ChangeAction::Modify => Change::Upsert(object),
        };
        self.insert_change(change);
    }

    fn insert_change(&mut self, change: Change) {
        let key = change.key();
        match self.changes.get(&key) {
            Some(existing) if existing.version() > change.version() => {}
            _ => {
                self.changes.insert(key, change);
            }
        }
    }

    pub fn get(&self, kind: ObjectKind, id: i64) -> Option<&Change> {
        self.changes.get(&Key::new(kind, id))
    }

    /// The changes, nodes first, then ways, then relations, each by id.
    pub fn iter(&self) -> impl Iterator<Item = &Change> + '_ {
        self.changes.values()
    }

    pub fn len(&self) -> usize {
        self.changes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn counts(&self) -> ChangeCounts {
        let mut counts = ChangeCounts::default();
        for change in self.changes.values() {
            match change {
                Change::Delete { .. } => counts.deletions += 1,
                Change::Upsert(OsmObject::Node(_)) => counts.nodes += 1,
                Change::Upsert(OsmObject::Way(_)) => counts.ways += 1,
                Change::Upsert(OsmObject::Relation(_)) => counts.relations += 1,
            }
        }
        counts
    }

    pub(crate) fn map(&self) -> &BTreeMap<Key, Change> {
        &self.changes
    }

    fn add_entries(
        &mut self,
        entries: Vec<(ChangeAction, OsmObject)>,
        clip: Option<&Clip<'_>>,
    ) -> ReadReport {
        let objects = entries.len();
        let keep = match clip {
            Some(clip) => self.clip(&entries, clip),
            None => vec![true; objects],
        };
        let mut kept = 0;
        for ((action, object), keep) in entries.into_iter().zip(keep) {
            if keep {
                kept += 1;
                self.insert(action, object);
            }
        }
        ReadReport { objects, kept }
    }

    /// Which of one file's entries matter to the clip's extract.
    fn clip(&self, entries: &[(ChangeAction, OsmObject)], clip: &Clip<'_>) -> Vec<bool> {
        let base = clip.base;
        // Known: the extract holds the object, or an earlier file's change to
        // it was kept. Every change to a known object is kept.
        let known = |kind: ObjectKind, id: i64| {
            base.contains(kind, id) || self.changes.contains_key(&Key::new(kind, id))
        };
        let mut keep = vec![false; entries.len()];
        // Objects that exist after this file, as far as the region goes.
        let mut present: HashSet<Key> = HashSet::new();
        let exists = |present: &HashSet<Key>, kind: ObjectKind, id: i64| {
            let key = Key::new(kind, id);
            present.contains(&key)
                || match self.changes.get(&key) {
                    Some(Change::Upsert(_)) => true,
                    Some(Change::Delete { .. }) => false,
                    None => base.contains(kind, id),
                }
        };
        let mut node_entries: HashMap<i64, Vec<usize>> = HashMap::new();

        for (index, (action, object)) in entries.iter().enumerate() {
            let OsmObject::Node(node) = object else {
                continue;
            };
            node_entries.entry(node.id).or_default().push(index);
            let upsert = *action != ChangeAction::Delete;
            if known(ObjectKind::Node, node.id) || (upsert && clip.contains(node)) {
                keep[index] = true;
                if upsert {
                    present.insert(object.key());
                }
            }
        }
        let mut kept_ways = Vec::new();
        for (index, (action, object)) in entries.iter().enumerate() {
            let OsmObject::Way(way) = object else {
                continue;
            };
            let upsert = *action != ChangeAction::Delete;
            let touches = || {
                way.node_ids
                    .iter()
                    .any(|&node| exists(&present, ObjectKind::Node, node))
            };
            if known(ObjectKind::Way, way.id) || (upsert && touches()) {
                keep[index] = true;
                if upsert {
                    kept_ways.push(index);
                }
            }
        }
        // The nodes of every kept way, wherever they are.
        for &index in &kept_ways {
            let OsmObject::Way(way) = &entries[index].1 else {
                continue;
            };
            for node in &way.node_ids {
                for &node_index in node_entries.get(node).into_iter().flatten() {
                    if entries[node_index].0 != ChangeAction::Delete && !keep[node_index] {
                        keep[node_index] = true;
                        present.insert(Key::new(ObjectKind::Node, *node));
                    }
                }
            }
        }
        for &index in &kept_ways {
            present.insert(entries[index].1.key());
        }
        for (index, (action, object)) in entries.iter().enumerate() {
            let OsmObject::Relation(relation) = object else {
                continue;
            };
            let upsert = *action != ChangeAction::Delete;
            let touches = || {
                relation.members.iter().any(|member| {
                    let kind = match member.member_type {
                        RelationMemberType::Node => ObjectKind::Node,
                        RelationMemberType::Way => ObjectKind::Way,
                        RelationMemberType::Relation => ObjectKind::Relation,
                    };
                    exists(&present, kind, member.member_id)
                })
            };
            if known(ObjectKind::Relation, relation.id) || (upsert && touches()) {
                keep[index] = true;
                if upsert {
                    present.insert(object.key());
                }
            }
        }
        keep
    }
}

#[cfg(test)]
mod tests;
