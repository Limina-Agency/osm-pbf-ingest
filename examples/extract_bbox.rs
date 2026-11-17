//! Cut a bounding box out of a PBF into a small routing fixture, without
//! osmium or osmconvert.
//!
//! Usage:
//!   cargo run --release -p osm-pbf-ingest --example extract_bbox -- \
//!       <in.osm.pbf> <out.osm.pbf> <west> <south> <east> <north>
//!
//! Keeps every routable way (`highway`, or `route=ferry|shuttle_train`) with a
//! node inside the box, whole, with all its nodes and their tags; the
//! `restriction` relations with a kept way as member, whole; and the `route`
//! relations with a kept way, reduced to their kept ways. Everything else
//! (buildings, landuse, boundaries) is dropped. The output is sorted by type
//! then id, in zlib blobs of at most 8000 objects, as Geofabrik writes them.

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::Write;

use flate2::Compression;
use flate2::write::ZlibEncoder;
use osmpbfreader::fileformat::{Blob, BlobHeader};
use osmpbfreader::osmformat::relation::MemberType;
use osmpbfreader::osmformat::{
    DenseNodes, HeaderBBox, HeaderBlock, PrimitiveBlock, PrimitiveGroup, Relation as PbfRelation,
    StringTable, Way as PbfWay,
};
use osmpbfreader::{Node, OsmId, OsmPbfReader, Relation, Tags, Way};
use protobuf::Message;

const BLOCK_OBJECTS: usize = 8000;

fn is_routable(tags: &Tags) -> bool {
    tags.contains_key("highway")
        || matches!(
            tags.get("route").map(|value| value.as_str()),
            Some("ferry" | "shuttle_train")
        )
}

#[derive(Default)]
struct Strings {
    table: Vec<Vec<u8>>,
    lookup: std::collections::HashMap<String, u32>,
}

impl Strings {
    fn new() -> Self {
        Self {
            table: vec![Vec::new()],
            ..Self::default()
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
}

fn block(group: PrimitiveGroup, strings: Strings) -> Vec<u8> {
    let mut table = StringTable::new();
    table.s = strings.table;
    let mut block = PrimitiveBlock::new();
    block.stringtable = Some(table).into();
    block.primitivegroup.push(group);
    block.write_to_bytes().expect("encode primitive block")
}

fn node_block(nodes: &[Node]) -> Vec<u8> {
    let mut strings = Strings::new();
    let mut dense = DenseNodes::new();
    let (mut id, mut lat, mut lon) = (0i64, 0i64, 0i64);
    for node in nodes {
        // Default granularity 100 nanodegrees: decimicro degrees as stored.
        let (node_lat, node_lon) = (i64::from(node.decimicro_lat), i64::from(node.decimicro_lon));
        dense.id.push(node.id.0 - id);
        dense.lat.push(node_lat - lat);
        dense.lon.push(node_lon - lon);
        (id, lat, lon) = (node.id.0, node_lat, node_lon);
        for (key, value) in node.tags.iter() {
            dense.keys_vals.push(strings.intern(key) as i32);
            dense.keys_vals.push(strings.intern(value) as i32);
        }
        dense.keys_vals.push(0);
    }
    let mut group = PrimitiveGroup::new();
    group.dense = Some(dense).into();
    block(group, strings)
}

fn way_block(ways: &[Way]) -> Vec<u8> {
    let mut strings = Strings::new();
    let mut group = PrimitiveGroup::new();
    for way in ways {
        let mut encoded = PbfWay::new();
        encoded.set_id(way.id.0);
        for (key, value) in way.tags.iter() {
            encoded.keys.push(strings.intern(key));
            encoded.vals.push(strings.intern(value));
        }
        let mut last = 0i64;
        for node in &way.nodes {
            encoded.refs.push(node.0 - last);
            last = node.0;
        }
        group.ways.push(encoded);
    }
    block(group, strings)
}

fn relation_block(relations: &[Relation]) -> Vec<u8> {
    let mut strings = Strings::new();
    let mut group = PrimitiveGroup::new();
    for relation in relations {
        let mut encoded = PbfRelation::new();
        encoded.set_id(relation.id.0);
        for (key, value) in relation.tags.iter() {
            encoded.keys.push(strings.intern(key));
            encoded.vals.push(strings.intern(value));
        }
        let mut last = 0i64;
        for member in &relation.refs {
            let (member_type, id) = match member.member {
                OsmId::Node(id) => (MemberType::NODE, id.0),
                OsmId::Way(id) => (MemberType::WAY, id.0),
                OsmId::Relation(id) => (MemberType::RELATION, id.0),
            };
            encoded.roles_sid.push(strings.intern(&member.role) as i32);
            encoded.memids.push(id - last);
            encoded.types.push(member_type.into());
            last = id;
        }
        group.relations.push(encoded);
    }
    block(group, strings)
}

/// Frame a payload: big-endian header length, `BlobHeader`, zlib `Blob`.
fn write_blob(out: &mut impl Write, blob_type: &str, payload: &[u8]) -> std::io::Result<()> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::best());
    encoder.write_all(payload)?;
    let mut blob = Blob::new();
    blob.set_zlib_data(encoder.finish()?);
    blob.set_raw_size(payload.len() as i32);
    let blob_bytes = blob.write_to_bytes().expect("encode blob");
    let mut header = BlobHeader::new();
    header.set_type(blob_type.to_string());
    header.set_datasize(blob_bytes.len() as i32);
    let header_bytes = header.write_to_bytes().expect("encode blob header");
    out.write_all(&(header_bytes.len() as u32).to_be_bytes())?;
    out.write_all(&header_bytes)?;
    out.write_all(&blob_bytes)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let [input, output, west, south, east, north] = args.as_slice() else {
        eprintln!("usage: extract_bbox <in.osm.pbf> <out.osm.pbf> <west> <south> <east> <north>");
        std::process::exit(2);
    };
    let [west, south, east, north]: [f64; 4] = [west, south, east, north].map(|v| {
        v.parse()
            .unwrap_or_else(|_| panic!("{v} is not a coordinate"))
    });
    let mut reader = OsmPbfReader::new(File::open(input)?);

    let inside = reader
        .par_iter_nodes()
        .map(Result::unwrap)
        .filter(|node| (south..=north).contains(&node.lat()) && (west..=east).contains(&node.lon()))
        .map(|node| node.id.0)
        .collect::<HashSet<_>>();

    reader.rewind()?;
    let ways = reader
        .par_iter_ways()
        .map(Result::unwrap)
        .filter(|way| is_routable(&way.tags) && way.nodes.iter().any(|n| inside.contains(&n.0)))
        .map(|way| (way.id.0, way))
        .collect::<BTreeMap<_, _>>();
    let needed = ways
        .values()
        .flat_map(|way| way.nodes.iter().map(|node| node.0))
        .collect::<HashSet<_>>();

    reader.rewind()?;
    let mut relations = BTreeMap::new();
    for mut relation in reader.par_iter_relations().map(Result::unwrap) {
        let kept_way = |member: &osmpbfreader::Ref| {
            member
                .member
                .way()
                .is_some_and(|way| ways.contains_key(&way.0))
        };
        if !relation.refs.iter().any(kept_way) {
            continue;
        }
        match relation.tags.get("type").map(|value| value.as_str()) {
            Some("restriction") => {}
            Some("route") => relation.refs.retain(kept_way),
            _ => continue,
        }
        relations.insert(relation.id.0, relation);
    }

    reader.rewind()?;
    let nodes = reader
        .par_iter_nodes()
        .map(Result::unwrap)
        .filter(|node| needed.contains(&node.id.0))
        .map(|node| (node.id.0, node))
        .collect::<BTreeMap<_, _>>();

    let mut out = std::io::BufWriter::new(File::create(output)?);
    let mut header = HeaderBlock::new();
    header.required_features = vec!["OsmSchema-V0.6".into(), "DenseNodes".into()];
    header.optional_features = vec!["Sort.Type_then_ID".into()];
    let mut bbox = HeaderBBox::new();
    let nano = |degrees: f64| (degrees * 1e9).round() as i64;
    bbox.set_left(nano(west));
    bbox.set_right(nano(east));
    bbox.set_bottom(nano(south));
    bbox.set_top(nano(north));
    header.bbox = Some(bbox).into();
    header.set_writingprogram("osm-pbf-ingest extract_bbox".into());
    write_blob(&mut out, "OSMHeader", &header.write_to_bytes()?)?;
    let nodes = nodes.into_values().collect::<Vec<_>>();
    for chunk in nodes.chunks(BLOCK_OBJECTS) {
        write_blob(&mut out, "OSMData", &node_block(chunk))?;
    }
    let ways = ways.into_values().collect::<Vec<_>>();
    for chunk in ways.chunks(BLOCK_OBJECTS) {
        write_blob(&mut out, "OSMData", &way_block(chunk))?;
    }
    let relations = relations.into_values().collect::<Vec<_>>();
    for chunk in relations.chunks(BLOCK_OBJECTS) {
        write_blob(&mut out, "OSMData", &relation_block(chunk))?;
    }
    out.flush()?;
    println!(
        "{} nodes ({} inside the box), {} ways, {} relations",
        nodes.len(),
        inside.len(),
        ways.len(),
        relations.len()
    );
    Ok(())
}
