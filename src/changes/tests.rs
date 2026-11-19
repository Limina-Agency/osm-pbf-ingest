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
    <relation id="600" version="1"><member type="way" ref="300" role=""/></relation>
    <relation id="601" version="1"><member type="way" ref="301" role=""/></relation>
  </create>
  <modify>
    <node id="3" version="2" lat="40.0" lon="-3.7"/>
    <node id="905" version="7" lat="10.0" lon="10.0"/>
  </modify>
  <delete>
    <node id="906" version="2"/>
    <way id="101" version="2"/>
  </delete>
</osmChange>"#;
    let mut changes = ChangeSet::new();
    let report = changes.read_osc_bytes(xml.as_bytes(), Some(&clip)).unwrap();
    assert_eq!(report.objects, 13);
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
            "Relation601",
            "Way101",
            "Way301"
        ]
    );
    assert_eq!(report.kept, 6);

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
