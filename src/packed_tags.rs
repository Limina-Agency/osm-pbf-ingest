//! Dictionary encoding for OSM tag sets.
//!
//! Tags stored as JSON repeat their keys on every object — `highway` appears
//! millions of times — and every read pays a JSON parse. Measured on a
//! region-sized extract, 8.8M way tag pairs used only 2,387 distinct keys and
//! 108,144 distinct values, and the values that appeared exactly once totalled
//! barely a megabyte. Both sides of a pair therefore intern extremely well.
//!
//! A tag set encodes to: varint pair count, then a `(key_id, value_id)` pair of
//! varints per tag. Ids index a dictionary shared by the whole store, so a
//! typical two-tag way costs about five bytes instead of sixty.
//!
//! Ids are assigned in first-seen order rather than by frequency. Frequency
//! order would pack slightly better, but it needs the whole corpus before any
//! id can be issued, and ingest is streaming. First-seen order is a good proxy
//! anyway: the common keys appear in the first blocks read, so they land on
//! one-byte ids.

use rustc_hash::FxHashMap;
use std::collections::BTreeMap;

/// Interns tag strings while a store is being written.
///
/// The lookup map uses `FxHash` rather than the standard hasher: this is an
/// internal dictionary over short strings with no adversarial input, and the
/// hit path runs about twice per tag pair — tens of millions of times on a
/// region-sized extract — so the DoS-resistant default is pure overhead here.
#[derive(Debug, Default)]
pub struct TagDictionaryBuilder {
    ids: FxHashMap<String, u32>,
    strings: Vec<String>,
    /// Reused across calls so encoding a tag set allocates nothing.
    scratch: Vec<(u32, u32)>,
}

impl TagDictionaryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn intern(&mut self, value: &str) -> u32 {
        if let Some(id) = self.ids.get(value) {
            return *id;
        }
        let id = self.strings.len() as u32;
        self.strings.push(value.to_string());
        self.ids.insert(value.to_string(), id);
        id
    }

    /// Strings in id order, ready to be persisted.
    pub fn strings(&self) -> &[String] {
        &self.strings
    }

    pub fn len(&self) -> usize {
        self.strings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }

    /// Encode a tag set, interning any strings not yet seen.
    ///
    /// Pairs are written sorted by key id, so the same tag set always encodes
    /// to the same bytes regardless of the order the caller iterated in — the
    /// caller does not have to pre-sort, and does not have to build an ordered
    /// map just to get a stable encoding.
    pub fn encode<'a, I>(&mut self, tags: I) -> Vec<u8>
    where
        I: IntoIterator<Item = (&'a str, &'a str)>,
    {
        // Taken out and put back so `intern` can borrow `self` mutably while
        // the scratch buffer is in hand.
        let mut pairs = std::mem::take(&mut self.scratch);
        pairs.clear();
        for (key, value) in tags {
            let key_id = self.intern(key);
            let value_id = self.intern(value);
            pairs.push((key_id, value_id));
        }
        pairs.sort_unstable_by_key(|(key_id, _)| *key_id);

        let mut out = Vec::with_capacity(1 + pairs.len() * 3);
        write_varint(&mut out, pairs.len() as u64);
        for (key_id, value_id) in pairs.iter() {
            write_varint(&mut out, u64::from(*key_id));
            write_varint(&mut out, u64::from(*value_id));
        }
        self.scratch = pairs;
        out
    }
}

/// Resolves tag ids back to strings when reading a store.
#[derive(Debug, Clone, Default)]
pub struct TagDictionary {
    strings: Vec<String>,
}

impl TagDictionary {
    /// Build from strings in id order — index 0 is id 0.
    pub fn from_strings(strings: Vec<String>) -> Self {
        Self { strings }
    }

    pub fn get(&self, id: u32) -> Option<&str> {
        self.strings.get(id as usize).map(String::as_str)
    }

    /// Id of an exact string, if the store ever wrote it.
    ///
    /// `None` means no object in the store carries that key or value, so a
    /// caller filtering on it can skip its scan entirely.
    pub fn id_of(&self, value: &str) -> Option<u32> {
        self.strings
            .iter()
            .position(|candidate| candidate == value)
            .map(|index| index as u32)
    }

    pub fn len(&self) -> usize {
        self.strings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.strings.is_empty()
    }

    /// Ids whose string contains `needle`, which is matched case-insensitively.
    ///
    /// This is what replaces `lower(tags_json) LIKE '%needle%'`: resolve the
    /// query against the dictionary once, then look for those ids in the
    /// encoded tag sets. The dictionary is a few megabytes where the JSON it
    /// replaced was hundreds.
    pub fn ids_containing(&self, needle: &str) -> Vec<u32> {
        let needle = needle.to_lowercase();
        self.strings
            .iter()
            .enumerate()
            .filter(|(_, value)| value.to_lowercase().contains(&needle))
            .map(|(index, _)| index as u32)
            .collect()
    }

    /// Decode a tag set into owned strings.
    pub fn decode(&self, bytes: &[u8]) -> Result<BTreeMap<String, String>, String> {
        let mut tags = BTreeMap::new();
        for (key_id, value_id) in decode_pairs(bytes)? {
            let key = self
                .get(key_id)
                .ok_or_else(|| format!("tag key id {key_id} is not in the dictionary"))?;
            let value = self
                .get(value_id)
                .ok_or_else(|| format!("tag value id {value_id} is not in the dictionary"))?;
            tags.insert(key.to_string(), value.to_string());
        }
        Ok(tags)
    }
}

/// Read the raw id pairs without resolving them.
///
/// Callers testing for the presence of a known id — a tag-key prefilter, or a
/// search that has already resolved its query — can skip building strings
/// entirely.
pub fn decode_pairs(bytes: &[u8]) -> Result<Vec<(u32, u32)>, String> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut cursor = 0usize;
    let count = read_varint(bytes, &mut cursor)? as usize;
    let mut pairs = Vec::with_capacity(count.min(64));
    for index in 0..count {
        let key_id = u32::try_from(read_varint(bytes, &mut cursor)?)
            .map_err(|_| format!("tag key id at pair {index} exceeds u32"))?;
        let value_id = u32::try_from(read_varint(bytes, &mut cursor)?)
            .map_err(|_| format!("tag value id at pair {index} exceeds u32"))?;
        pairs.push((key_id, value_id));
    }
    if cursor != bytes.len() {
        return Err(format!(
            "tag blob has {} trailing bytes after {count} pairs",
            bytes.len() - cursor
        ));
    }
    Ok(pairs)
}

/// True when the blob holds no tags. Cheaper than decoding, and the blob-side
/// equivalent of the old `tags_json != '{}'` test.
pub fn is_empty_tag_blob(bytes: &[u8]) -> bool {
    bytes.is_empty() || bytes[0] == 0
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
            .ok_or_else(|| format!("tag blob ends mid-varint at byte {}", *cursor))?;
        *cursor += 1;
        if shift >= 64 {
            return Err(format!(
                "tag blob has an over-long varint at byte {}",
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn round_trip(pairs: &[(&str, &str)]) {
        let expected = tags(pairs);
        let mut builder = TagDictionaryBuilder::new();
        let encoded = builder.encode(expected.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        let dictionary = TagDictionary::from_strings(builder.strings().to_vec());
        assert_eq!(dictionary.decode(&encoded).expect("decode"), expected);
    }

    #[test]
    fn round_trips_typical_tag_sets() {
        round_trip(&[("highway", "residential"), ("name", "Grodzka")]);
        round_trip(&[("building", "yes")]);
        round_trip(&[]);
    }

    #[test]
    fn round_trips_awkward_strings() {
        round_trip(&[("name", "Kraków — Główny \"stacja\"")]);
        round_trip(&[("name:ja", "クラクフ"), ("note", "")]);
        round_trip(&[("description", "a value with , and : and {} in it")]);
    }

    #[test]
    fn repeated_strings_are_stored_once() {
        let mut builder = TagDictionaryBuilder::new();
        for _ in 0..100 {
            builder.encode([("highway", "residential")]);
        }
        // Two strings total, however many objects carry them.
        assert_eq!(builder.len(), 2);
    }

    #[test]
    fn a_shared_dictionary_decodes_every_object() {
        let mut builder = TagDictionaryBuilder::new();
        let first = builder.encode([("highway", "residential"), ("surface", "asphalt")]);
        let second = builder.encode([("highway", "service"), ("surface", "asphalt")]);
        let dictionary = TagDictionary::from_strings(builder.strings().to_vec());

        assert_eq!(
            dictionary.decode(&first).unwrap(),
            tags(&[("highway", "residential"), ("surface", "asphalt")])
        );
        assert_eq!(
            dictionary.decode(&second).unwrap(),
            tags(&[("highway", "service"), ("surface", "asphalt")])
        );
    }

    #[test]
    fn encoding_is_deterministic_for_the_same_tag_set() {
        // Two builders that have seen the same strings in the same order must
        // agree, so a rebuild produces byte-identical rows.
        let build = || {
            let mut builder = TagDictionaryBuilder::new();
            builder.encode(
                tags(&[("b", "2"), ("a", "1")])
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect::<Vec<_>>(),
            )
        };
        assert_eq!(build(), build());
    }

    #[test]
    fn typical_tags_are_far_smaller_than_json() {
        let tag_set = tags(&[("highway", "residential"), ("surface", "asphalt")]);
        let json = serde_json::to_string(&tag_set).unwrap();
        let mut builder = TagDictionaryBuilder::new();
        let packed = builder.encode(tag_set.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        assert!(
            packed.len() * 5 < json.len(),
            "packed {} bytes vs json {} bytes",
            packed.len(),
            json.len()
        );
    }

    #[test]
    fn empty_tag_blobs_are_recognised_without_decoding() {
        let mut builder = TagDictionaryBuilder::new();
        assert!(is_empty_tag_blob(&builder.encode([])));
        assert!(is_empty_tag_blob(&[]));
        assert!(!is_empty_tag_blob(&builder.encode([("a", "b")])));
    }

    #[test]
    fn id_of_matches_exactly_and_reports_absence() {
        let mut builder = TagDictionaryBuilder::new();
        builder.encode([("highway", "residential")]);
        let dictionary = TagDictionary::from_strings(builder.strings().to_vec());

        assert_eq!(
            dictionary.get(dictionary.id_of("highway").unwrap()),
            Some("highway")
        );
        assert!(
            dictionary.id_of("Highway").is_none(),
            "must not match case-insensitively"
        );
        assert!(dictionary.id_of("barrier").is_none());
    }

    #[test]
    fn ids_containing_finds_case_insensitive_substrings() {
        let mut builder = TagDictionaryBuilder::new();
        builder.encode([("name", "Grodzka"), ("highway", "residential")]);
        let dictionary = TagDictionary::from_strings(builder.strings().to_vec());

        let ids = dictionary.ids_containing("grodz");
        assert_eq!(ids.len(), 1);
        assert_eq!(dictionary.get(ids[0]), Some("Grodzka"));
        assert!(dictionary.ids_containing("nothing here").is_empty());
    }

    #[test]
    fn an_id_outside_the_dictionary_is_an_error_not_a_missing_tag() {
        // Dropping the pair would silently lose a tag, which downstream reads
        // as "this way has no surface" rather than as corruption.
        let mut builder = TagDictionaryBuilder::new();
        let encoded = builder.encode([("highway", "residential")]);
        let empty = TagDictionary::default();
        let error = empty.decode(&encoded).expect_err("expected an error");
        assert!(error.contains("not in the dictionary"), "got: {error}");
    }

    #[test]
    fn a_truncated_blob_is_rejected() {
        let mut builder = TagDictionaryBuilder::new();
        let encoded = builder.encode([("highway", "residential"), ("name", "X")]);
        let error = decode_pairs(&encoded[..encoded.len() - 1]).expect_err("expected an error");
        assert!(error.contains("mid-varint"), "got: {error}");
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut builder = TagDictionaryBuilder::new();
        let mut encoded = builder.encode([("highway", "residential")]);
        encoded.push(0);
        let error = decode_pairs(&encoded).expect_err("expected an error");
        assert!(error.contains("trailing"), "got: {error}");
    }
}
