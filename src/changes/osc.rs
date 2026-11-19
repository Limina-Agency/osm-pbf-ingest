//! Reading the OsmChange XML format (`.osc`), as replication servers and
//! editors write it, gzip-compressed or not.

use super::{ChangeAction, OsmNode, OsmObject, OsmRelation, OsmWay};
use crate::model::{NormalizedRelationMember, RelationMemberType};
use crate::tags::Tags;
use flate2::read::MultiGzDecoder;
use quick_xml::XmlVersion;
use quick_xml::events::{BytesStart, Event};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

/// Every change in a file, in file order.
pub(super) fn read_path(path: &Path) -> Result<Vec<(ChangeAction, OsmObject)>, String> {
    let file = File::open(path).map_err(|error| error.to_string())?;
    let mut reader = BufReader::with_capacity(1 << 20, file);
    let gzip = reader
        .fill_buf()
        .map_err(|error| error.to_string())?
        .starts_with(&GZIP_MAGIC);
    if gzip {
        read(BufReader::with_capacity(
            1 << 20,
            MultiGzDecoder::new(reader),
        ))
    } else {
        read(reader)
    }
}

pub(super) fn read_bytes(bytes: &[u8]) -> Result<Vec<(ChangeAction, OsmObject)>, String> {
    if bytes.starts_with(&GZIP_MAGIC) {
        read(BufReader::new(MultiGzDecoder::new(bytes)))
    } else {
        read(bytes)
    }
}

/// The object being read, before its closing tag.
enum Open {
    Node(OsmNode),
    Way(OsmWay),
    Relation(OsmRelation),
}

fn read<R: BufRead>(input: R) -> Result<Vec<(ChangeAction, OsmObject)>, String> {
    let mut reader = quick_xml::Reader::from_reader(input);
    let mut buffer = Vec::new();
    let mut entries = Vec::new();
    let mut action: Option<ChangeAction> = None;
    let mut open: Option<(Open, Vec<(String, String)>)> = None;
    let mut seen_root = false;
    loop {
        let position = reader.buffer_position();
        let event = reader
            .read_event_into(&mut buffer)
            .map_err(|error| format!("malformed XML at byte {position}: {error}"))?;
        let at = |message: String| format!("{message} at byte {position}");
        match event {
            Event::Start(ref element) | Event::Empty(ref element) => {
                let empty = matches!(event, Event::Empty(_));
                match element.local_name().as_ref() {
                    "osmChange" => seen_root = true,
                    "create" => action = Some(ChangeAction::Create),
                    "modify" => action = Some(ChangeAction::Modify),
                    "delete" => action = Some(ChangeAction::Delete),
                    name @ ("node" | "way" | "relation") => {
                        if !seen_root {
                            return Err(at("not an osmChange file".to_string()));
                        }
                        let Some(current) = action else {
                            return Err(at(
                                "an object outside create, modify or delete".to_string()
                            ));
                        };
                        if open.is_some() {
                            return Err(at("an object inside another".to_string()));
                        }
                        let object = start_object(name, element, current).map_err(at)?;
                        if empty {
                            entries.push((current, finish(object, Vec::new())));
                        } else {
                            open = Some((object, Vec::new()));
                        }
                    }
                    "tag" => {
                        if let Some((_, tags)) = &mut open {
                            let key = attribute(element, "k").map_err(at)?;
                            let value = attribute(element, "v").map_err(at)?;
                            if let (Some(key), Some(value)) = (key, value) {
                                tags.push((key, value));
                            }
                        }
                    }
                    "nd" => {
                        if let Some((Open::Way(way), _)) = &mut open {
                            let id = required_i64(element, "ref").map_err(at)?;
                            way.node_ids.push(id);
                        }
                    }
                    "member" => {
                        if let Some((Open::Relation(relation), _)) = &mut open {
                            let member_type =
                                match attribute(element, "type").map_err(at)?.as_deref() {
                                    Some("node") => RelationMemberType::Node,
                                    Some("way") => RelationMemberType::Way,
                                    Some("relation") => RelationMemberType::Relation,
                                    other => {
                                        return Err(at(format!("unknown member type {other:?}")));
                                    }
                                };
                            relation.members.push(NormalizedRelationMember {
                                member_type,
                                member_id: required_i64(element, "ref").map_err(at)?,
                                role: attribute(element, "role").map_err(at)?.unwrap_or_default(),
                            });
                        }
                    }
                    _ => {}
                }
            }
            Event::End(ref element) => match element.local_name().as_ref() {
                "node" | "way" | "relation" => {
                    if let (Some((object, tags)), Some(current)) = (open.take(), action) {
                        entries.push((current, finish(object, tags)));
                    }
                }
                "create" | "modify" | "delete" => action = None,
                _ => {}
            },
            Event::Eof => break,
            _ => {}
        }
        buffer.clear();
    }
    if !seen_root {
        return Err("not an osmChange file".to_string());
    }
    if open.is_some() {
        return Err("the file ends inside an object".to_string());
    }
    Ok(entries)
}

fn start_object(
    name: &str,
    element: &BytesStart<'_>,
    action: ChangeAction,
) -> Result<Open, String> {
    let id = required_i64(element, "id")?;
    let version = match attribute(element, "version")? {
        Some(value) => value
            .parse()
            .map_err(|_| format!("version {value:?} of object {id} is not a number"))?,
        None => 0,
    };
    Ok(match name {
        "node" => {
            // A deletion may leave the position out.
            let (lat, lon) = match (attribute(element, "lat")?, attribute(element, "lon")?) {
                (Some(lat), Some(lon)) => (parse_decimicro(&lat, 90)?, parse_decimicro(&lon, 180)?),
                _ if action == ChangeAction::Delete => (0, 0),
                _ => return Err(format!("node {id} has no position")),
            };
            Open::Node(OsmNode {
                id,
                version,
                decimicro_lat: lat,
                decimicro_lon: lon,
                tags: Tags::new(),
            })
        }
        "way" => Open::Way(OsmWay {
            id,
            version,
            node_ids: Vec::new(),
            tags: Tags::new(),
        }),
        _ => Open::Relation(OsmRelation {
            id,
            version,
            members: Vec::new(),
            tags: Tags::new(),
        }),
    })
}

fn finish(object: Open, tags: Vec<(String, String)>) -> OsmObject {
    // A repeated key keeps its first value, as `Tags` does everywhere.
    let tags: Tags = tags.into_iter().collect();
    match object {
        Open::Node(node) => OsmObject::Node(OsmNode { tags, ..node }),
        Open::Way(way) => OsmObject::Way(OsmWay { tags, ..way }),
        Open::Relation(relation) => OsmObject::Relation(OsmRelation { tags, ..relation }),
    }
}

fn attribute(element: &BytesStart<'_>, name: &str) -> Result<Option<String>, String> {
    for attribute in element.attributes() {
        let attribute = attribute.map_err(|error| format!("malformed attribute: {error}"))?;
        if attribute.key.as_ref() == name {
            let value = attribute
                .normalized_value(XmlVersion::Implicit1_0)
                .map_err(|error| format!("malformed attribute value: {error}"))?;
            return Ok(Some(value.into_owned()));
        }
    }
    Ok(None)
}

fn required_i64(element: &BytesStart<'_>, name: &str) -> Result<i64, String> {
    let name_text = name;
    let value =
        attribute(element, name)?.ok_or_else(|| format!("missing attribute {name_text}"))?;
    value
        .parse()
        .map_err(|_| format!("attribute {name_text}={value:?} is not an integer"))
}

/// A coordinate in degrees, as written, in units of 1e-7 degrees, rounded
/// half away from zero; parsed from the digits, so no float rounding creeps
/// in.
pub(super) fn parse_decimicro(text: &str, limit: i64) -> Result<i32, String> {
    let invalid = || format!("coordinate {text:?} is not a number of degrees");
    let (negative, digits) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let (whole, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    if whole.is_empty() && fraction.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || whole.len() > 4
    {
        return Err(invalid());
    }
    let mut value: i64 = if whole.is_empty() {
        0
    } else {
        whole.parse().map_err(|_| invalid())?
    };
    let fraction = fraction.as_bytes();
    for index in 0..7 {
        let digit = fraction.get(index).map_or(0, |byte| i64::from(byte - b'0'));
        value = value * 10 + digit;
    }
    if fraction.get(7).is_some_and(|&byte| byte >= b'5') {
        value += 1;
    }
    if value > limit * 10_000_000 {
        return Err(format!("coordinate {text:?} is out of range"));
    }
    let value = if negative { -value } else { value };
    i32::try_from(value).map_err(|_| invalid())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinates_parse_exactly() {
        assert_eq!(parse_decimicro("50.0614300", 90), Ok(500_614_300));
        assert_eq!(parse_decimicro("-1.427499", 180), Ok(-14_274_990));
        assert_eq!(parse_decimicro("19.93658", 180), Ok(199_365_800));
        assert_eq!(parse_decimicro("0.00000005", 90), Ok(1));
        assert_eq!(parse_decimicro("-0.00000004", 90), Ok(0));
        assert_eq!(parse_decimicro("180", 180), Ok(1_800_000_000));
        assert_eq!(parse_decimicro(".5", 90), Ok(5_000_000));
        assert!(parse_decimicro("90.0000001", 90).is_err());
        assert!(parse_decimicro("1e5", 90).is_err());
        assert!(parse_decimicro("", 90).is_err());
        assert!(parse_decimicro("-", 90).is_err());
    }

    #[test]
    fn reads_every_action_and_object() {
        let xml = r#"<?xml version='1.0' encoding='UTF-8'?>
<osmChange version="0.6" generator="test">
  <create>
    <node id="1" version="1" lat="50.1" lon="19.9">
      <tag k="name" v="A &amp; B"/>
    </node>
    <node id="2" version="1" lat="50.2" lon="19.8"/>
  </create>
  <modify>
    <way id="10" version="3">
      <nd ref="1"/><nd ref="2"/>
      <tag k="highway" v="residential"/>
    </way>
    <relation id="100" version="2">
      <member type="way" ref="10" role="outer"/>
      <member type="node" ref="1" role=""/>
      <tag k="type" v="multipolygon"/>
    </relation>
  </modify>
  <delete>
    <node id="3" version="4"/>
    <way id="11" version="2"/>
  </delete>
</osmChange>"#;
        let entries = read_bytes(xml.as_bytes()).unwrap();
        assert_eq!(entries.len(), 6);
        let (action, OsmObject::Node(node)) = &entries[0] else {
            panic!()
        };
        assert_eq!(*action, ChangeAction::Create);
        assert_eq!((node.id, node.version), (1, 1));
        assert_eq!(
            (node.decimicro_lat, node.decimicro_lon),
            (501_000_000, 199_000_000)
        );
        assert_eq!(node.tags.get("name"), Some("A & B"));
        let (action, OsmObject::Way(way)) = &entries[2] else {
            panic!()
        };
        assert_eq!(*action, ChangeAction::Modify);
        assert_eq!(way.node_ids, vec![1, 2]);
        assert_eq!(way.tags.get("highway"), Some("residential"));
        let (_, OsmObject::Relation(relation)) = &entries[3] else {
            panic!()
        };
        assert_eq!(relation.members.len(), 2);
        assert_eq!(relation.members[0].member_type, RelationMemberType::Way);
        assert_eq!(relation.members[0].role, "outer");
        let (action, object) = &entries[4];
        assert_eq!(
            (*action, object.id(), object.version()),
            (ChangeAction::Delete, 3, 4)
        );
        assert_eq!(entries[5].1.id(), 11);
    }

    #[test]
    fn reads_gzip() {
        use flate2::{Compression, write::GzEncoder};
        use std::io::Write;
        let xml =
            br#"<osmChange version="0.6"><delete><node id="5" version="2"/></delete></osmChange>"#;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(xml).unwrap();
        let entries = read_bytes(&encoder.finish().unwrap()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1.id(), 5);
    }

    #[test]
    fn rejects_other_files() {
        assert!(
            read_bytes(b"<osm version=\"0.6\"><node id=\"1\" lat=\"0\" lon=\"0\"/></osm>").is_err()
        );
        assert!(read_bytes(b"not xml at all").is_err());
        assert!(read_bytes(b"<osmChange><create><node id=\"1\"/></create></osmChange>").is_err());
    }
}
