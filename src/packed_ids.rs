//! Compact encoding for a way's node-id list.
//!
//! Stored as JSON, a way's node ids cost about 11 bytes each — an OSM node id
//! is ten digits plus a separator — and every read pays a JSON parse to get
//! back a `Vec<i64>`. Consecutive ids within a way are usually close together,
//! so delta-encoding them and writing the deltas as zigzag varints turns the
//! common case into one or two bytes per node with no parsing beyond a shift
//! and an add.
//!
//! Format: a sequence of LEB128 unsigned varints, each holding a zigzag-encoded
//! delta from the previous id. The first delta is measured from zero, so a
//! single-node way is just its id. An empty list encodes to an empty blob.

/// Map a signed value onto an unsigned one that keeps small magnitudes small.
/// Deltas run in both directions — OSM node ids within a way are not monotonic
/// — and a plain two's-complement cast would make every negative delta ten
/// bytes.
fn zigzag_encode(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| format!("node-id blob ends mid-varint at byte {}", *cursor))?;
        *cursor += 1;
        // 10 groups of 7 bits is the most a u64 can hold; refuse rather than
        // silently wrapping on a corrupt or foreign blob.
        if shift >= 64 {
            return Err(format!(
                "node-id blob has an over-long varint at byte {}",
                *cursor
            ));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

/// Encode a way's node ids in file order. Order is meaningful — it is the
/// geometry — so this never sorts or deduplicates.
pub fn encode_node_ids(node_ids: &[i64]) -> Vec<u8> {
    // Most ways are short; one byte per node covers the common delta range.
    let mut out = Vec::with_capacity(node_ids.len() + 4);
    let mut previous = 0i64;
    for node_id in node_ids {
        write_varint(&mut out, zigzag_encode(node_id.wrapping_sub(previous)));
        previous = *node_id;
    }
    out
}

pub fn decode_node_ids(bytes: &[u8]) -> Result<Vec<i64>, String> {
    let mut node_ids = Vec::new();
    let mut cursor = 0usize;
    let mut previous = 0i64;
    while cursor < bytes.len() {
        let delta = zigzag_decode(read_varint(bytes, &mut cursor)?);
        let node_id = previous.wrapping_add(delta);
        node_ids.push(node_id);
        previous = node_id;
    }
    Ok(node_ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(node_ids: &[i64]) {
        let encoded = encode_node_ids(node_ids);
        let decoded = decode_node_ids(&encoded).expect("decode");
        assert_eq!(decoded, node_ids, "round trip failed for {node_ids:?}");
    }

    #[test]
    fn round_trips_typical_way_geometry() {
        // Real OSM ids, ascending with small gaps — the case the encoding is
        // tuned for.
        round_trip(&[1_234_567_890, 1_234_567_891, 1_234_567_895, 1_234_567_900]);
    }

    #[test]
    fn round_trips_edge_shapes() {
        round_trip(&[]);
        round_trip(&[1]);
        round_trip(&[i64::MAX]);
        round_trip(&[i64::MIN]);
        round_trip(&[i64::MIN, i64::MAX, i64::MIN]);
        // Closed ways repeat their first node last.
        round_trip(&[42, 43, 44, 42]);
        // Ids are not monotonic in practice; deltas go both ways.
        round_trip(&[900, 100, 5_000, 20]);
        // Repeats are legal geometry and must not be collapsed.
        round_trip(&[7, 7, 7]);
    }

    #[test]
    fn preserves_order_including_duplicates() {
        let node_ids = vec![5, 3, 5, 9, 3];
        let decoded = decode_node_ids(&encode_node_ids(&node_ids)).unwrap();
        assert_eq!(decoded, node_ids);
    }

    #[test]
    fn typical_geometry_is_far_smaller_than_json() {
        let node_ids: Vec<i64> = (0..10).map(|index| 1_234_567_890 + index * 3).collect();
        let json = serde_json::to_string(&node_ids).unwrap();
        let packed = encode_node_ids(&node_ids);
        assert!(
            packed.len() * 4 < json.len(),
            "packed {} bytes vs json {} bytes",
            packed.len(),
            json.len()
        );
    }

    #[test]
    fn a_truncated_blob_is_an_error_not_a_short_way() {
        // Silently returning fewer nodes would produce a subtly wrong geometry
        // rather than a failure, which is the worse outcome.
        let encoded = encode_node_ids(&[1_234_567_890, 1_234_567_999]);
        let truncated = &encoded[..encoded.len() - 1];
        let error = decode_node_ids(truncated).expect_err("expected an error");
        assert!(error.contains("mid-varint"), "unexpected error: {error}");
    }

    #[test]
    fn an_over_long_varint_is_rejected() {
        // All continuation bits set: a corrupt blob that would otherwise shift
        // past the width of a u64.
        let bytes = vec![0xffu8; 12];
        let error = decode_node_ids(&bytes).expect_err("expected an error");
        assert!(error.contains("over-long"), "unexpected error: {error}");
    }

    #[test]
    fn zigzag_is_its_own_inverse() {
        for value in [0i64, 1, -1, 2, -2, i64::MAX, i64::MIN, 123_456_789] {
            assert_eq!(zigzag_decode(zigzag_encode(value)), value);
        }
    }
}
