//! Borrowed tag access for filters.
//!
//! A pass decides whether to keep an object from its tags before building the
//! owned [`Tags`] it hands out. [`TagsRef`] lets the filter read the tags
//! straight out of the block's string table, so an object the filter rejects
//! costs no allocation at all.

use osmpbfreader::Tags;

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
    /// Which value a repeated key resolves to. osmpbfreader builds dense-node
    /// tags by inserting pairs in order, so the last one wins there; every
    /// other object's tags are collected, which keeps the first.
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
            Repeat::FirstWins => self
                .pairs
                .iter()
                .map(|&(key, value)| (key.into(), value.into()))
                .collect(),
            Repeat::LastWins => {
                let mut tags = Tags::new();
                for &(key, value) in self.pairs {
                    tags.insert(key.into(), value.into());
                }
                tags
            }
        };
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
        self.get(key).map(|value| value.as_str())
    }
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

    #[test]
    fn to_tags_matches_osmpbfreader_construction() {
        let collected: Tags = REPEATED
            .iter()
            .map(|&(k, v)| (k.into(), v.into()))
            .collect();
        assert_eq!(
            TagsRef::new(REPEATED, Repeat::FirstWins).to_tags(),
            collected
        );

        let mut inserted = Tags::new();
        for &(k, v) in REPEATED {
            inserted.insert(k.into(), v.into());
        }
        assert_eq!(TagsRef::new(REPEATED, Repeat::LastWins).to_tags(), inserted);
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
