//! Applying change sets to synthetic files, read back with `osmpbfreader`, an
//! encoder-independent decoder, and with this crate's own passes.

use super::*;
use crate::test_pbf::{
    TestNode, TestRelation, TestRelationMember, TestWay, node_block, pbf_file, relation_block,
    sorted_fixture, way_block, write_fixture,
};
use osmpbfreader::osmformat::relation::MemberType;
use osmpbfreader::{OsmId, OsmObj, OsmPbfReader};
use std::path::PathBuf;

/// Every object of a file in file order, as text: `n2 lat lon k=v,...`,
/// `w100 1,2,3 k=v`, `r500 w100:outer k=v`.
fn objects(path: &Path) -> Vec<String> {
    let mut reader = OsmPbfReader::new(std::fs::File::open(path).unwrap());
    reader
        .iter()
        .map(|object| {
            let object = object.unwrap();
            let mut tags: Vec<String> = object
                .tags()
                .iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect();
            tags.sort();
            let tags = tags.join(",");
            match object {
                OsmObj::Node(node) => format!(
                    "n{} {} {} {tags}",
                    node.id.0, node.decimicro_lat, node.decimicro_lon
                ),
                OsmObj::Way(way) => {
                    let nodes: Vec<String> =
                        way.nodes.iter().map(|node| node.0.to_string()).collect();
                    format!("w{} {} {tags}", way.id.0, nodes.join(","))
                }
                OsmObj::Relation(relation) => {
                    let members: Vec<String> = relation
                        .refs
                        .iter()
                        .map(|member| {
                            let id = match member.member {
                                OsmId::Node(id) => format!("n{}", id.0),
                                OsmId::Way(id) => format!("w{}", id.0),
                                OsmId::Relation(id) => format!("r{}", id.0),
                            };
                            format!("{id}:{}", member.role)
                        })
                        .collect();
                    format!("r{} {} {tags}", relation.id.0, members.join(","))
                }
            }
        })
        .collect()
}

fn node(id: i64, version: u32, lat: i32, lon: i32, tags: &[(&str, &str)]) -> OsmObject {
    OsmObject::Node(OsmNode {
        id,
        version,
        decimicro_lat: lat,
        decimicro_lon: lon,
        tags: tags.iter().copied().collect(),
    })
}

fn way(id: i64, version: u32, node_ids: &[i64], tags: &[(&str, &str)]) -> OsmObject {
    OsmObject::Way(OsmWay {
        id,
        version,
        node_ids: node_ids.to_vec(),
        tags: tags.iter().copied().collect(),
    })
}

fn relation(
    id: i64,
    members: &[(RelationMemberType, i64, &str)],
    tags: &[(&str, &str)],
) -> OsmObject {
    OsmObject::Relation(OsmRelation {
        id,
        version: 1,
        members: members
            .iter()
            .map(|&(member_type, member_id, role)| NormalizedRelationMember {
                member_type,
                member_id,
                role: role.to_string(),
            })
            .collect(),
        tags: tags.iter().copied().collect(),
    })
}

fn apply(dir: &Path, base: &[u8], changes: &ChangeSet) -> (ApplyReport, PathBuf) {
    let input = write_fixture(dir, base);
    let index = BaseIndex::scan(&input, false).unwrap();
    let output = dir.join("updated.osm.pbf");
    let report = apply_changes(&index, changes, &output, &ApplyOptions::default()).unwrap();
    (report, output)
}

#[test]
fn a_sorted_file_stays_sorted_and_untouched_blocks_are_copied() {
    let dir = tempfile::tempdir().unwrap();
    let mut changes = ChangeSet::new();
    use ChangeAction::*;
    changes.insert(
        Modify,
        node(2, 2, 501_000_000, 191_000_000, &[("amenity", "pub")]),
    );
    changes.insert(Delete, node(4, 3, 0, 0, &[]));
    changes.insert(
        Create,
        node(10, 1, 502_500_000, 192_500_000, &[("shop", "bakery")]),
    );
    changes.insert(Modify, way(101, 2, &[3, 10], &[("highway", "path")]));
    changes.insert(Create, way(102, 1, &[1, 10], &[]));
    changes.insert(
        Create,
        relation(
            501,
            &[(RelationMemberType::Node, 10, "")],
            &[("type", "site")],
        ),
    );
    let (report, output) = apply(dir.path(), &sorted_fixture(), &changes);

    assert_eq!(
        objects(&output),
        [
            "n1 500000000 190000000 ",
            "n2 501000000 191000000 amenity=pub",
            "n3 502000000 192000000 ",
            "n10 502500000 192500000 shop=bakery",
            "w100 1,2,3 highway=residential",
            "w101 3,10 highway=path",
            "w102 1,10 ",
            "r500 w100:outer,w101:inner landuse=forest,type=multipolygon",
            "r501 n10: type=site",
        ]
    );
    assert!(report.sorted);
    // Nodes 2 and 4 and way 101 rewrite their blocks; node 10, way 102 and
    // relation 501 fall within no block's range and get blocks of their own.
    assert_eq!(report.blobs_rewritten, 3);
    assert_eq!(report.blobs_copied, 2);
    assert_eq!(report.blobs_added, 3);
    assert_eq!((report.created, report.modified, report.deleted), (3, 2, 1));
    assert_eq!(
        report.output_bytes,
        std::fs::metadata(&output).unwrap().len()
    );
    let header = crate::blob_index::PbfBlobIndex::scan(&output)
        .unwrap()
        .header()
        .clone();
    assert!(header.sorted_type_then_id());
    assert!(BaseIndex::scan(&output, false).unwrap().is_sorted());
}

#[test]
fn the_crate_reads_the_written_file() {
    use crate::{IngestBuildProfile, NodeBatch, Pass2Sink, RelationBatch, WayBatch};
    let dir = tempfile::tempdir().unwrap();
    let mut changes = ChangeSet::new();
    changes.insert(
        ChangeAction::Create,
        node(5, 1, 503_000_000, 193_000_000, &[]),
    );
    changes.insert(
        ChangeAction::Create,
        way(103, 1, &[4, 5], &[("highway", "service")]),
    );
    let (_, output) = apply(dir.path(), &sorted_fixture(), &changes);

    #[derive(Default)]
    struct Ways(Vec<(i64, Vec<i64>)>);
    impl Pass2Sink for Ways {
        fn on_relation_batch(&mut self, _: RelationBatch) -> Result<(), String> {
            Ok(())
        }
        fn on_way_batch(&mut self, batch: WayBatch) -> Result<(), String> {
            self.0
                .extend(batch.ways.into_iter().map(|way| (way.way_id, way.node_ids)));
            Ok(())
        }
        fn on_node_batch(&mut self, _: NodeBatch) -> Result<(), String> {
            Ok(())
        }
    }
    let index = crate::prepare_ingest_index_with_options(
        &output,
        IngestBuildProfile::DesktopFull,
        dir.path(),
        crate::PrepareOptions::new(),
        |_| {},
        || false,
    )
    .unwrap();
    assert!(index.node_store.get(5).is_some());
    let mut ways = Ways::default();
    crate::run_pass2_with_options(
        &output,
        IngestBuildProfile::DesktopFull,
        crate::Pass2Options::new().skip_nodes(),
        &mut ways,
        |_| {},
        || false,
    )
    .unwrap();
    assert_eq!(ways.0.last(), Some(&(103, vec![4, 5])));
}

#[test]
fn new_objects_go_last_in_an_unsorted_file() {
    let dir = tempfile::tempdir().unwrap();
    let base = pbf_file(
        false,
        &[
            node_block(&[
                TestNode {
                    id: 7,
                    lat: 50.0,
                    lng: 19.0,
                    tags: &[],
                },
                TestNode {
                    id: 3,
                    lat: 50.1,
                    lng: 19.1,
                    tags: &[],
                },
            ]),
            way_block(&[TestWay {
                id: 20,
                node_ids: &[7, 3],
                tags: &[],
            }]),
            node_block(&[TestNode {
                id: 1,
                lat: 50.2,
                lng: 19.2,
                tags: &[],
            }]),
        ],
    );
    let mut changes = ChangeSet::new();
    changes.insert(ChangeAction::Modify, node(3, 2, 0, 0, &[]));
    changes.insert(ChangeAction::Create, node(5, 1, 1, 1, &[]));
    changes.insert(ChangeAction::Delete, node(1, 2, 0, 0, &[]));
    changes.insert(ChangeAction::Delete, node(99, 2, 0, 0, &[]));
    let (report, output) = apply(dir.path(), &base, &changes);
    assert_eq!(
        objects(&output),
        ["n7 500000000 190000000 ", "n3 0 0 ", "w20 7,3 ", "n5 1 1 "]
    );
    assert!(!report.sorted);
    assert_eq!(report.deletions_ignored, 1);
    let header = crate::blob_index::PbfBlobIndex::scan(&output)
        .unwrap()
        .header()
        .clone();
    assert!(!header.sorted_type_then_id());
}

#[test]
fn the_highest_version_wins_whatever_the_order() {
    let mut changes = ChangeSet::new();
    changes.insert(ChangeAction::Modify, node(1, 3, 3, 3, &[]));
    changes.insert(ChangeAction::Modify, node(1, 2, 2, 2, &[]));
    changes.insert(ChangeAction::Delete, node(2, 5, 0, 0, &[]));
    changes.insert(ChangeAction::Modify, node(2, 4, 4, 4, &[]));
    changes.insert(ChangeAction::Modify, node(3, 1, 1, 1, &[]));
    changes.insert(ChangeAction::Delete, node(3, 2, 0, 0, &[]));
    let Some(Change::Upsert(OsmObject::Node(first))) = changes.get(ObjectKind::Node, 1) else {
        panic!()
    };
    assert_eq!(first.version, 3);
    assert!(matches!(
        changes.get(ObjectKind::Node, 2),
        Some(Change::Delete { version: 5, .. })
    ));
    assert!(matches!(
        changes.get(ObjectKind::Node, 3),
        Some(Change::Delete { .. })
    ));
    assert_eq!(changes.counts().deletions, 2);
}

#[test]
fn a_clip_keeps_what_touches_the_extract() {
    let dir = tempfile::tempdir().unwrap();
    let input = write_fixture(dir.path(), &sorted_fixture());
    let base = BaseIndex::scan(&input, true).unwrap();
    assert!(base.contains(ObjectKind::Node, 4));
    assert!(base.contains(ObjectKind::Relation, 500));
    assert!(!base.contains(ObjectKind::Way, 4));
    let clip = Clip::to_header_bounds(&base).unwrap();
    // The fixture's header box: 18.85 to 21.5 east, 48.95 to 50.65 north.
    let xml = r#"<osmChange version="0.6">
  <create>
    <node id="900" version="1" lat="-33.9" lon="151.2"/>
    <node id="901" version="1" lat="50.06" lon="19.94"/>
    <node id="902" version="1" lat="52.2" lon="21.0"/>
    <node id="903" version="1" lat="52.3" lon="21.1"/>
    <node id="904" version="1" lat="52.4" lon="21.2"/>
    <way id="300" version="1"><nd ref="903"/><nd ref="904"/></way>
    <way id="301" version="1"><nd ref="1"/><nd ref="902"/></way>
    <node id="910" version="1" lat="50.07" lon="19.95"/>
    <way id="303" version="1"><nd ref="901"/><nd ref="910"/></way>
    <way id="304" version="1"><nd ref="901"/><nd ref="909"/></way>
    <relation id="600" version="1"><member type="way" ref="300" role=""/></relation>
    <relation id="601" version="1"><member type="way" ref="301" role=""/></relation>
  </create>
  <modify>
    <node id="3" version="2" lat="40.0" lon="-3.7"/>
    <node id="905" version="7" lat="10.0" lon="10.0"/>
    <node id="907" version="3" lat="49.1" lon="19.6"/>
    <way id="302" version="4"><nd ref="901"/><nd ref="908"/></way>
  </modify>
  <delete>
    <node id="906" version="2"/>
    <way id="101" version="2"/>
  </delete>
</osmChange>"#;
    let mut changes = ChangeSet::new();
    let report = changes.read_osc_bytes(xml.as_bytes(), Some(&clip)).unwrap();
    assert_eq!(report.objects, 18);
    let mut kept: Vec<String> = changes
        .iter()
        .map(|change| format!("{:?}{}", change.kind(), change.id()))
        .collect();
    kept.sort();
    assert_eq!(
        kept,
        [
            "Node3",
            "Node901",
            "Node902",
            "Node910",
            "Relation601",
            "Way101",
            "Way301",
            "Way303"
        ]
    );
    assert_eq!(report.kept, 8);

    // A later file changing a node an earlier file added keeps that change,
    // though the node is outside the box.
    let later = r#"<osmChange version="0.6"><modify>
    <node id="902" version="2" lat="52.25" lon="21.0"/>
    <node id="903" version="2" lat="52.35" lon="21.1"/>
    </modify></osmChange>"#;
    let report = changes
        .read_osc_bytes(later.as_bytes(), Some(&clip))
        .unwrap();
    assert_eq!(report.kept, 1);
    let Some(Change::Upsert(OsmObject::Node(moved))) = changes.get(ObjectKind::Node, 902) else {
        panic!()
    };
    assert_eq!(moved.version, 2);
}

#[test]
fn a_clip_needs_ids() {
    let dir = tempfile::tempdir().unwrap();
    let input = write_fixture(dir.path(), &sorted_fixture());
    let base = BaseIndex::scan(&input, false).unwrap();
    assert!(Clip::to_header_bounds(&base).is_none());
}

#[test]
fn an_existing_output_is_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let input = write_fixture(dir.path(), &sorted_fixture());
    let base = BaseIndex::scan(&input, false).unwrap();
    let output = dir.path().join("taken.osm.pbf");
    std::fs::write(&output, b"keep").unwrap();
    assert!(apply_changes(&base, &ChangeSet::new(), &output, &ApplyOptions::default()).is_err());
    assert_eq!(std::fs::read(&output).unwrap(), b"keep");
}

#[test]
fn no_changes_copy_every_block_and_keep_the_header() {
    let dir = tempfile::tempdir().unwrap();
    let input = write_fixture(dir.path(), &sorted_fixture());
    let base = BaseIndex::scan(&input, false).unwrap();
    let output = dir.path().join("copy.osm.pbf");
    let options = ApplyOptions {
        replication_timestamp: Some(1_795_036_862),
        replication_sequence_number: Some(124_347),
        replication_base_url: Some("https://planet.openstreetmap.org/replication/hour".to_string()),
        writing_program: None,
    };
    let report = apply_changes(&base, &ChangeSet::new(), &output, &options).unwrap();
    assert_eq!(report.blobs_copied, 5);
    assert_eq!(objects(&output), objects(&input));
    let header = crate::blob_index::PbfBlobIndex::scan(&output)
        .unwrap()
        .header()
        .clone();
    assert_eq!(header.replication_sequence_number, Some(124_347));
    assert_eq!(header.replication_timestamp, Some(1_795_036_862));
    assert_eq!(
        header.replication_base_url.as_deref(),
        Some("https://planet.openstreetmap.org/replication/hour")
    );
    assert_eq!(header.bounds, base.header().bounds);
    assert_eq!(header.required_features, base.header().required_features);
}

#[test]
fn relations_keep_their_members_through_a_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let base = pbf_file(
        true,
        &[relation_block(&[
            TestRelation {
                id: 1,
                members: &[TestRelationMember {
                    member_type: MemberType::NODE,
                    member_id: 9,
                    role: "label",
                }],
                tags: &[("type", "boundary")],
            },
            TestRelation {
                id: 3,
                members: &[],
                tags: &[],
            },
        ])],
    );
    let mut changes = ChangeSet::new();
    changes.insert(
        ChangeAction::Create,
        relation(
            2,
            &[
                (RelationMemberType::Relation, 1, "subarea"),
                (RelationMemberType::Way, 7, ""),
            ],
            &[],
        ),
    );
    let (_, output) = apply(dir.path(), &base, &changes);
    assert_eq!(
        objects(&output),
        ["r1 n9:label type=boundary", "r2 r1:subarea,w7: ", "r3  "]
    );
}

fn merge(dir: &Path, files: &[Vec<u8>]) -> (MergeReport, PathBuf) {
    let paths: Vec<PathBuf> = files
        .iter()
        .enumerate()
        .map(|(index, bytes)| {
            let path = dir.join(format!("input-{index}.osm.pbf"));
            std::fs::write(&path, bytes).unwrap();
            path
        })
        .collect();
    let indexes: Vec<BaseIndex> = paths
        .iter()
        .map(|path| BaseIndex::scan(path, false).unwrap())
        .collect();
    let inputs: Vec<&BaseIndex> = indexes.iter().collect();
    let output = dir.join("merged.osm.pbf");
    let report = merge_files(&inputs, &output, &MergeOptions::default()).unwrap();
    (report, output)
}

#[test]
fn merging_overlapping_extracts_writes_each_object_once_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let node = |id, lat, lng, tags| TestNode { id, lat, lng, tags };
    let west = pbf_file(
        true,
        &[
            node_block(&[node(1, 50.0, 19.0, &[]), node(3, 50.2, 19.2, &[])]),
            // No other file reaches nodes 20 to 21: copied as it is.
            node_block(&[node(20, 50.5, 19.5, &[]), node(21, 50.6, 19.6, &[])]),
            way_block(&[TestWay {
                id: 100,
                node_ids: &[1, 3],
                tags: &[("highway", "track")],
            }]),
        ],
    );
    let east = pbf_file(
        true,
        &[
            node_block(&[
                node(2, 50.1, 19.1, &[("amenity", "cafe")]),
                // The same node as in the west file, where it wins.
                node(3, 50.2, 19.2, &[("east", "copy")]),
                node(4, 50.3, 19.3, &[]),
            ]),
            way_block(&[
                TestWay {
                    id: 100,
                    node_ids: &[1, 3],
                    tags: &[("highway", "east-copy")],
                },
                TestWay {
                    id: 101,
                    node_ids: &[3, 4],
                    tags: &[],
                },
            ]),
            relation_block(&[TestRelation {
                id: 500,
                members: &[TestRelationMember {
                    member_type: MemberType::WAY,
                    member_id: 101,
                    role: "",
                }],
                tags: &[("type", "route")],
            }]),
        ],
    );
    let (report, output) = merge(dir.path(), &[west, east]);

    assert_eq!(
        objects(&output),
        [
            "n1 500000000 190000000 ",
            "n2 501000000 191000000 amenity=cafe",
            "n3 502000000 192000000 ",
            "n4 503000000 193000000 ",
            "n20 505000000 195000000 ",
            "n21 506000000 196000000 ",
            "w100 1,3 highway=track",
            "w101 3,4 ",
            "r500 w101: type=route",
        ]
    );
    assert_eq!(report.objects, 9);
    assert_eq!(report.duplicates, 2);
    // The block of nodes 20 and 21 and the relation block are copied.
    assert_eq!(report.blobs_copied, 2);
    let merged = BaseIndex::scan(&output, false).unwrap();
    assert!(merged.is_sorted());
    assert!(merged.header().sorted_type_then_id());
}

#[test]
fn merging_a_file_with_itself_gives_its_objects() {
    let dir = tempfile::tempdir().unwrap();
    let (report, output) = merge(dir.path(), &[sorted_fixture(), sorted_fixture()]);
    let original = write_fixture(dir.path(), &sorted_fixture());
    assert_eq!(objects(&output), objects(&original));
    assert_eq!(report.duplicates, report.objects);
}

#[test]
fn merging_needs_sorted_files() {
    let dir = tempfile::tempdir().unwrap();
    let unsorted = pbf_file(
        false,
        &[node_block(&[
            TestNode {
                id: 2,
                lat: 50.0,
                lng: 19.0,
                tags: &[],
            },
            TestNode {
                id: 1,
                lat: 50.0,
                lng: 19.0,
                tags: &[],
            },
        ])],
    );
    let path = dir.path().join("unsorted.osm.pbf");
    std::fs::write(&path, unsorted).unwrap();
    let index = BaseIndex::scan(&path, false).unwrap();
    let error = merge_files(
        &[&index],
        &dir.path().join("merged.osm.pbf"),
        &MergeOptions::default(),
    )
    .unwrap_err();
    assert!(error.contains("not sorted"), "{error}");
    assert!(!dir.path().join("merged.osm.pbf").exists());
}

#[test]
fn a_merge_stopped_by_its_progress_callback_leaves_no_output() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_fixture(dir.path(), &sorted_fixture());
    let index = BaseIndex::scan(&path, false).unwrap();
    let output = dir.path().join("merged.osm.pbf");
    let mut calls = Vec::new();
    let error = merge_files_with_progress(
        &[&index, &index],
        &output,
        &MergeOptions::default(),
        |done, total| {
            calls.push((done, total));
            Err("cancelled".to_string())
        },
    )
    .unwrap_err();
    assert_eq!(error, "cancelled");
    assert_eq!(calls, [(10, 10)]);
    assert!(!output.exists());
}

fn area_of(dir: &Path, base: &[u8], changes: &ChangeSet) -> ChangedArea {
    let input = write_fixture(dir, base);
    let index = BaseIndex::scan(&input, false).unwrap();
    changed_area(&index, changes).unwrap()
}

/// The boxes as `w s e n` in whole hundredths of a degree, sorted.
fn boxes(area: &ChangedArea) -> Vec<String> {
    let mut boxes: Vec<String> = area
        .boxes
        .iter()
        .map(|b| {
            let r = |v: f64| (v * 100.0).round() as i64;
            format!("{} {} {} {}", r(b.west), r(b.south), r(b.east), r(b.north))
        })
        .collect();
    boxes.sort();
    boxes
}

#[test]
fn a_moved_node_moves_its_ways_and_areas() {
    let dir = tempfile::tempdir().unwrap();
    let mut changes = ChangeSet::new();
    changes.insert(
        ChangeAction::Modify,
        node(4, 2, 510_000_000, 200_000_000, &[]),
    );
    let area = area_of(dir.path(), &sorted_fixture(), &changes);
    assert_eq!((area.ways_moved, area.relations_moved), (1, 1));
    assert_eq!(area.nodes_missing, 0);
    assert_eq!(
        boxes(&area),
        [
            // Relation 500 before and after: ways 100 and 101.
            "1900 5000 1930 5030",
            "1900 5000 2000 5100",
            // Way 101 before and after.
            "1920 5020 1930 5030",
            "1920 5020 2000 5100",
            // Node 4 before and after.
            "1930 5030 1930 5030",
            "2000 5100 2000 5100",
        ]
    );
}

#[test]
fn new_and_deleted_objects_have_one_box() {
    let dir = tempfile::tempdir().unwrap();
    let mut changes = ChangeSet::new();
    use ChangeAction::*;
    changes.insert(Create, node(10, 1, 520_000_000, 210_000_000, &[]));
    changes.insert(Create, way(102, 1, &[1, 10], &[("highway", "path")]));
    changes.insert(Delete, way(101, 3, &[], &[]));
    let area = area_of(dir.path(), &sorted_fixture(), &changes);
    assert_eq!((area.ways_moved, area.relations_moved), (0, 1));
    assert_eq!(
        boxes(&area),
        [
            // Relation 500 before (ways 100 and 101) and after (101 is gone).
            "1900 5000 1920 5020",
            "1900 5000 1930 5030",
            // Way 102, from node 1 in the base to the new node 10.
            "1900 5000 2100 5200",
            // Way 101, before.
            "1920 5020 1930 5030",
            // Node 10.
            "2100 5200 2100 5200",
        ]
    );
}

#[test]
fn a_changed_relation_covers_its_members_before_and_after() {
    let dir = tempfile::tempdir().unwrap();
    let mut changes = ChangeSet::new();
    changes.insert(
        ChangeAction::Modify,
        relation(
            500,
            &[(RelationMemberType::Way, 101, "outer")],
            &[("type", "multipolygon"), ("landuse", "meadow")],
        ),
    );
    let area = area_of(dir.path(), &sorted_fixture(), &changes);
    assert_eq!((area.ways_moved, area.relations_moved), (0, 0));
    assert_eq!(boxes(&area), ["1900 5000 1930 5030", "1920 5020 1930 5030"]);
}

#[test]
fn a_deleted_node_leaves_its_way_without_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut changes = ChangeSet::new();
    changes.insert(ChangeAction::Delete, node(3, 2, 0, 0, &[]));
    let area = area_of(dir.path(), &sorted_fixture(), &changes);
    assert_eq!((area.ways_moved, area.relations_moved), (2, 1));
    assert_eq!(area.nodes_missing, 0);
    assert!(boxes(&area).contains(&"1930 5030 1930 5030".to_string()));
    assert!(boxes(&area).contains(&"1900 5000 1910 5010".to_string()));
}
