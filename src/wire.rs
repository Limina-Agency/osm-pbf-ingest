//! Protobuf wire reading for the few messages a pass walks itself instead of
//! decoding a buffa view: the top level of a `PrimitiveBlock`, a group that
//! holds only ways, and those ways (see `osm_block::PlainWay`).
//!
//! A buffa view decodes every packed field of every object into its own
//! `Vec` up front. Pass 2 needs a way's tags and node IDs once each, mostly
//! to drop the way, so it reads them straight from the wire instead. These
//! readers only accept the plain encoding every PBF writer produces; for
//! anything else they report [`Unusual`] and the caller decodes the view,
//! which accepts all of protobuf.

/// The encoding is valid protobuf, or might be, but not the plain encoding
/// these readers handle: the caller falls back to a buffa view, which decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Unusual;

/// One field: its number and its value, a varint or length-delimited bytes.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Value<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
    /// A fixed-width value, which no field these readers want has.
    Fixed,
}

/// Reads a varint at `*pos`, advancing past it. `None` on a truncated or
/// over-long varint.
#[inline(always)]
pub(crate) fn varint(bytes: &[u8], pos: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *bytes.get(*pos)?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Some(value);
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
}

/// Calls `each` with every field of a message, in wire order. Groups (wire
/// types 3 and 4) and malformed framing are [`Unusual`].
pub(crate) fn for_each_field<'a>(
    bytes: &'a [u8],
    mut each: impl FnMut(u32, Value<'a>) -> Result<(), Unusual>,
) -> Result<(), Unusual> {
    let mut pos = 0;
    while pos < bytes.len() {
        let key = varint(bytes, &mut pos).ok_or(Unusual)?;
        let field = u32::try_from(key >> 3).map_err(|_| Unusual)?;
        let value = match key & 7 {
            0 => Value::Varint(varint(bytes, &mut pos).ok_or(Unusual)?),
            1 => {
                pos = pos
                    .checked_add(8)
                    .filter(|&end| end <= bytes.len())
                    .ok_or(Unusual)?;
                Value::Fixed
            }
            2 => {
                let len = usize::try_from(varint(bytes, &mut pos).ok_or(Unusual)?)
                    .map_err(|_| Unusual)?;
                let end = pos
                    .checked_add(len)
                    .filter(|&end| end <= bytes.len())
                    .ok_or(Unusual)?;
                let value = &bytes[pos..end];
                pos = end;
                Value::Bytes(value)
            }
            5 => {
                pos = pos
                    .checked_add(4)
                    .filter(|&end| end <= bytes.len())
                    .ok_or(Unusual)?;
                Value::Fixed
            }
            _ => return Err(Unusual),
        };
        each(field, value)?;
    }
    Ok(())
}

/// Whether a packed run of varints is well framed at its end: its last byte
/// ends a varint. Each value is read when it is used, and a varint inside the
/// run that runs on too long is an error then.
#[inline]
pub(crate) fn packed_ends_cleanly(packed: &[u8]) -> bool {
    packed.last().is_none_or(|&byte| byte < 0x80)
}

/// The values of a packed run of varints.
pub(crate) struct Packed<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Packed<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
}

impl Iterator for Packed<'_> {
    /// `Err` for an over-long varint, after which the run ends.
    type Item = Result<u64, String>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.bytes.len() {
            return None;
        }
        match varint(self.bytes, &mut self.pos) {
            Some(value) => Some(Ok(value)),
            None => {
                self.pos = self.bytes.len();
                Some(Err("malformed packed varint".to_string()))
            }
        }
    }
}

/// A zigzag-encoded `sint64`.
#[inline(always)]
pub(crate) fn zigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_and_zigzag_round_trip() {
        for value in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut bytes = Vec::new();
            let mut rest = value;
            while rest >= 0x80 {
                bytes.push((rest as u8) | 0x80);
                rest >>= 7;
            }
            bytes.push(rest as u8);
            let mut pos = 0;
            assert_eq!(varint(&bytes, &mut pos), Some(value));
            assert_eq!(pos, bytes.len());
        }
        assert_eq!(varint(&[0x80, 0x80], &mut 0), None);
        assert_eq!(varint(&[0xff; 11], &mut 0), None);
        for (encoded, decoded) in [(0, 0), (1, -1), (2, 1), (3, -2), (u64::MAX, i64::MIN)] {
            assert_eq!(zigzag(encoded), decoded);
        }
    }

    #[test]
    fn fields_are_read_in_order_and_bad_framing_is_unusual() {
        // 1: varint 150, 2: bytes "hi", 3: fixed64, 4: fixed32.
        let bytes = [
            0x08, 0x96, 0x01, 0x12, 0x02, b'h', b'i', 0x19, 1, 2, 3, 4, 5, 6, 7, 8, 0x25, 1, 2, 3,
            4,
        ];
        let mut seen = Vec::new();
        for_each_field(&bytes, |field, value| {
            seen.push(match value {
                Value::Varint(value) => (field, value as usize),
                Value::Bytes(value) => (field, value.len()),
                Value::Fixed => (field, 0),
            });
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, [(1, 150), (2, 2), (3, 0), (4, 0)]);
        // A length past the end, a start-group field, a truncated fixed64.
        for bad in [&[0x12, 0x05, b'h'][..], &[0x0b], &[0x19, 1, 2]] {
            assert_eq!(for_each_field(bad, |_, _| Ok(())), Err(Unusual));
        }
    }

    #[test]
    fn packed_runs_report_a_bad_varint() {
        let values = Packed::new(&[0x01, 0x96, 0x01, 0x7f]).collect::<Result<Vec<_>, _>>();
        assert_eq!(values.unwrap(), [1, 150, 127]);
        assert!(packed_ends_cleanly(&[0x01, 0x7f]));
        assert!(!packed_ends_cleanly(&[0x01, 0x80]));
        assert!(Packed::new(&[0xff; 11]).next().unwrap().is_err());
    }
}
