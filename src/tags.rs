//! Owned tags, and borrowed tag access for filters.
//!
//! A pass decides whether to keep an object from its tags before building the
//! owned [`Tags`] it hands out. [`TagsRef`] lets the filter read the tags
//! straight out of the block's string table, so an object the filter rejects
//! costs no allocation at all.

use compact_str::CompactString;
use std::slice;

/// An object's tags, owned: the `tags` of the nodes, ways and relations the
/// passes hand out.
///
/// The pairs are sorted by key and each key appears once. Keys and values up
/// to 24 bytes are stored inline, without a heap allocation of their own.
///
/// Collecting pairs into `Tags` keeps the first value of a repeated key;
/// [`insert`](Self::insert) replaces the value of a key already present.
#[derive(Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tags(Vec<(CompactString, CompactString)>);

impl Tags {
    pub fn new() -> Self {
        Self::default()
    }

    /// The value of `key`, or `None` when there is no such tag.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.position(key)
            .ok()
            .map(|index| self.0[index].1.as_str())
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.position(key).is_ok()
    }

    /// Whether there is a tag `key` with this `value`.
    pub fn contains(&self, key: &str, value: &str) -> bool {
        self.get(key) == Some(value)
    }

    /// Sets `key` to `value`, replacing the value it had.
    pub fn insert(&mut self, key: &str, value: &str) {
        match self.position(key) {
            Ok(index) => self.0[index].1 = value.into(),
            Err(index) => self.0.insert(index, (key.into(), value.into())),
        }
    }

    /// The key/value pairs, sorted by key.
    pub fn iter(&self) -> TagsIter<'_> {
        TagsIter(self.0.iter())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }

    fn position(&self, key: &str) -> Result<usize, usize> {
        self.0.binary_search_by(|(k, _)| k.as_str().cmp(key))
    }
}

impl<K: AsRef<str>, V: AsRef<str>> FromIterator<(K, V)> for Tags {
    /// Collects the pairs, keeping the first value of a key that repeats.
    fn from_iter<I: IntoIterator<Item = (K, V)>>(pairs: I) -> Self {
        let mut pairs: Vec<(CompactString, CompactString)> = pairs
            .into_iter()
            .map(|(key, value)| (key.as_ref().into(), value.as_ref().into()))
            .collect();
        // A stable sort keeps a repeated key's pairs in their given order, and
        // `dedup_by` removes all but the first of each run.
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs.dedup_by(|later, earlier| later.0 == earlier.0);
        Self(pairs)
    }
}

impl<'a> IntoIterator for &'a Tags {
    type Item = (&'a str, &'a str);
    type IntoIter = TagsIter<'a>;

    fn into_iter(self) -> TagsIter<'a> {
        self.iter()
    }
}

impl std::fmt::Debug for Tags {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_map().entries(self.iter()).finish()
    }
}

/// The pairs of a [`Tags`], sorted by key.
#[derive(Clone)]
pub struct TagsIter<'a>(slice::Iter<'a, (CompactString, CompactString)>);

impl<'a> Iterator for TagsIter<'a> {
    type Item = (&'a str, &'a str);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl DoubleEndedIterator for TagsIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|(k, v)| (k.as_str(), v.as_str()))
    }
}

impl ExactSizeIterator for TagsIter<'_> {}

/// An object's tags, borrowed from the block that holds it. Filters such as
/// [`PrepareOptions::keep_nodes`](crate::PrepareOptions::keep_nodes) receive
/// one.
///
/// [`get`](Self::get) answers exactly as the owned [`Tags`] built for the same
/// object would, including for a key the file repeats. [`iter`](Self::iter)
/// yields the pairs in file order, repeats included.
#[derive(Clone, Copy)]
pub struct TagsRef<'a> {
    pairs: &'a [(&'a str, &'a str)],
    /// Which value a repeated key resolves to. As in osmpbfreader, the last
    /// one wins for dense nodes and the first for every other object.
    repeat: Repeat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Repeat {
    FirstWins,
    LastWins,
}

impl<'a> TagsRef<'a> {
    pub(crate) fn new(pairs: &'a [(&'a str, &'a str)], repeat: Repeat) -> Self {
        Self { pairs, repeat }
    }

    /// The value of `key`, or `None` when the object has no such tag.
    pub fn get(&self, key: &str) -> Option<&'a str> {
        let matches = |&&(k, _): &&(&str, &str)| k == key;
        let found = match self.repeat {
            Repeat::FirstWins => self.pairs.iter().find(matches),
            Repeat::LastWins => self.pairs.iter().rev().find(matches),
        };
        found.map(|&(_, value)| value)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.pairs.iter().any(|&(k, _)| k == key)
    }

    /// The key/value pairs in file order. A key the file repeats appears once
    /// per occurrence.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&'a str, &'a str)> + 'a {
        self.pairs.iter().copied()
    }

    /// The number of pairs in the file, repeats included.
    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// The owned tags a pass hands out for this object.
    pub fn to_tags(&self) -> Tags {
        let mut tags: Tags = match self.repeat {
            Repeat::FirstWins => self.pairs.iter().copied().collect(),
            // Collecting keeps the first of a repeated key, so collect the
            // pairs back to front to keep the last.
            Repeat::LastWins => self.pairs.iter().rev().copied().collect(),
        };
        // Only a repeated key leaves spare capacity.
        tags.shrink_to_fit();
        tags
    }
}

/// Which objects a pass keeps, judged by their tags.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TagFilter {
    All,
    /// The filter of a deprecated entry point, which reads owned tags.
    Owned(fn(&Tags) -> bool),
    Borrowed(fn(&TagsRef<'_>) -> bool),
}

impl TagFilter {
    /// The owned tags of an object this keeps, or `None` when it rejects it.
    pub(crate) fn keep(self, pairs: &[(&str, &str)], repeat: Repeat) -> Option<Tags> {
        let tags = TagsRef::new(pairs, repeat);
        match self {
            Self::All => Some(tags.to_tags()),
            Self::Owned(keep) => {
                let owned = tags.to_tags();
                keep(&owned).then_some(owned)
            }
            Self::Borrowed(keep) => keep(&tags).then(|| tags.to_tags()),
        }
    }
}

impl std::fmt::Debug for TagsRef<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_map().entries(self.iter()).finish()
    }
}

/// Read access shared by [`Tags`] and [`TagsRef`], so one helper can serve a
/// pass filter and the code that later reads the kept objects' owned tags.
///
/// ```
/// use osm_pbf_ingest::TagLookup;
///
/// fn is_road(tags: &(impl TagLookup + ?Sized)) -> bool {
///     tags.has_tag("highway") || tags.tag("route") == Some("ferry")
/// }
/// ```
pub trait TagLookup {
    /// The value of `key`, or `None` when there is no such tag.
    fn tag(&self, key: &str) -> Option<&str>;

    fn has_tag(&self, key: &str) -> bool {
        self.tag(key).is_some()
    }
}

impl TagLookup for Tags {
    fn tag(&self, key: &str) -> Option<&str> {
        self.get(key)
    }

    fn has_tag(&self, key: &str) -> bool {
        self.contains_key(key)
    }
}

/// Whether `ours` holds the same pairs, in the same order, as the tags
/// osmpbfreader builds.
#[cfg(test)]
pub(crate) fn same_as_osmpbfreader(ours: &Tags, theirs: &osmpbfreader::Tags) -> bool {
    ours.iter()
        .eq(theirs.iter().map(|(k, v)| (k.as_str(), v.as_str())))
}

impl TagLookup for TagsRef<'_> {
    fn tag(&self, key: &str) -> Option<&str> {
        self.get(key)
    }

    fn has_tag(&self, key: &str) -> bool {
        self.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const REPEATED: &[(&str, &str)] = &[("highway", "primary"), ("name", "A"), ("name", "B")];

    #[test]
    fn get_resolves_a_repeated_key_as_the_owned_tags_do() {
        for repeat in [Repeat::FirstWins, Repeat::LastWins] {
            let tags = TagsRef::new(REPEATED, repeat);
            let owned = tags.to_tags();
            assert_eq!(tags.get("name"), owned.tag("name"), "{repeat:?}");
            assert_eq!(tags.get("highway"), Some("primary"));
            assert_eq!(tags.get("missing"), None);
            assert_eq!(owned.len(), 2);
        }
        assert_eq!(
            TagsRef::new(REPEATED, Repeat::FirstWins).get("name"),
            Some("A")
        );
        assert_eq!(
            TagsRef::new(REPEATED, Repeat::LastWins).get("name"),
            Some("B")
        );
    }

    /// The pairs osmpbfreader builds an object's tags from: out of order, with
    /// repeated keys, and long enough values to leave the inline storage.
    const SHUFFLED: &[(&str, &str)] = &[
        ("name", "A"),
        ("highway", "primary"),
        ("name", "B"),
        ("addr:street", "a street name longer than twenty-four bytes"),
        ("amenity", "cafe"),
        ("highway", "secondary"),
        ("name", "C"),
    ];

    #[test]
    fn to_tags_matches_osmpbfreader_construction() {
        for pairs in [REPEATED, SHUFFLED] {
            // Every object but a dense node: osmpbfreader collects the pairs.
            let collected: osmpbfreader::Tags =
                pairs.iter().map(|&(k, v)| (k.into(), v.into())).collect();
            let ours = TagsRef::new(pairs, Repeat::FirstWins).to_tags();
            assert!(same_as_osmpbfreader(&ours, &collected), "{ours:?}");

            // A dense node: osmpbfreader inserts them one by one.
            let mut inserted = osmpbfreader::Tags::new();
            for &(k, v) in pairs {
                inserted.insert(k.into(), v.into());
            }
            let ours = TagsRef::new(pairs, Repeat::LastWins).to_tags();
            assert!(same_as_osmpbfreader(&ours, &inserted), "{ours:?}");
        }
    }

    #[test]
    fn owned_tags_read_like_a_sorted_map() {
        let mut tags: Tags = SHUFFLED.iter().copied().collect();
        assert_eq!(tags.len(), 4);
        assert_eq!(
            tags.iter().map(|(k, _)| k).collect::<Vec<_>>(),
            ["addr:street", "amenity", "highway", "name"]
        );
        assert_eq!(tags.get("name"), Some("A"));
        assert!(tags.contains("highway", "primary"));
        assert!(!tags.contains("highway", "secondary"));
        assert!(!tags.contains_key("missing"));

        tags.insert("name", "D");
        tags.insert("barrier", "gate");
        assert_eq!(tags.get("name"), Some("D"));
        assert_eq!(tags.len(), 5);
        assert_eq!((&tags).into_iter().nth(1), Some(("amenity", "cafe")));
        assert_eq!(tags.iter().next_back(), Some(("name", "D")));
        assert_eq!(
            format!("{:?}", Tags::from_iter([("a", "1")])),
            r#"{"a": "1"}"#
        );
        assert!(Tags::new().is_empty());
    }

    #[test]
    fn tag_lookup_reads_owned_and_borrowed_tags_alike() {
        fn is_cafe(tags: &(impl TagLookup + ?Sized)) -> bool {
            tags.tag("amenity") == Some("cafe") && tags.has_tag("name")
        }
        let pairs = [("amenity", "cafe"), ("name", "A")];
        let borrowed = TagsRef::new(&pairs, Repeat::FirstWins);
        assert!(is_cafe(&borrowed));
        assert!(is_cafe(&borrowed.to_tags()));
        assert!(!is_cafe(&TagsRef::new(&pairs[..1], Repeat::FirstWins)));
    }

    #[test]
    fn iter_and_len_keep_file_order_and_repeats() {
        let tags = TagsRef::new(REPEATED, Repeat::FirstWins);
        assert_eq!(tags.len(), 3);
        assert!(!tags.is_empty());
        assert_eq!(tags.iter().collect::<Vec<_>>(), REPEATED);
        assert!(TagsRef::new(&[], Repeat::FirstWins).is_empty());
    }
}
