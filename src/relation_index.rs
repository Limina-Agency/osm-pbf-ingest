/// Way ID to relation IDs, packed: the distinct way IDs in order, where each
/// one's relations end in `relation_ids`, and the relation IDs themselves, all
/// at their exact size: 16 bytes per way plus 8 per membership.
#[derive(Debug, Default, Clone)]
pub struct PackedRelationIndex {
    pending: Vec<(i64, i64)>,
    way_ids: Vec<i64>,
    /// `ends[i]` is one past the last of `way_ids[i]`'s relations; the first
    /// way's start at 0.
    ends: Vec<u64>,
    relation_ids: Vec<i64>,
    finalized: bool,
}

#[derive(Debug, Default)]
pub struct PackedRelationIndexBuilder {
    pending: Vec<(i64, i64)>,
}

impl PackedRelationIndexBuilder {
    pub fn extend<I>(&mut self, pairs: I)
    where
        I: IntoIterator<Item = (i64, i64)>,
    {
        self.pending.extend(pairs);
    }

    pub fn finish(self) -> PackedRelationIndex {
        let mut index = PackedRelationIndex {
            pending: self.pending,
            ..PackedRelationIndex::default()
        };
        index.finalize();
        index
    }
}

impl PackedRelationIndex {
    #[cfg(test)]
    pub fn push(&mut self, way_id: i64, relation_id: i64) {
        self.pending.extend(
            self.way_ids
                .iter()
                .enumerate()
                .flat_map(|(index, &way)| {
                    let start = index.checked_sub(1).map_or(0, |i| self.ends[i] as usize);
                    self.relation_ids[start..self.ends[index] as usize]
                        .iter()
                        .map(move |&relation| (way, relation))
                })
                .collect::<Vec<_>>(),
        );
        self.way_ids.clear();
        self.ends.clear();
        self.relation_ids.clear();
        self.pending.push((way_id, relation_id));
        self.finalized = false;
    }

    pub fn finalize(&mut self) {
        if self.finalized {
            return;
        }

        self.pending.sort_unstable();
        self.pending.dedup();
        let ways = self.pending.chunk_by(|a, b| a.0 == b.0).count();
        let mut way_ids = Vec::with_capacity(ways);
        let mut ends = Vec::with_capacity(ways);
        let mut relation_ids = Vec::with_capacity(self.pending.len());
        for run in self.pending.chunk_by(|a, b| a.0 == b.0) {
            way_ids.push(run[0].0);
            relation_ids.extend(run.iter().map(|&(_, relation)| relation));
            ends.push(relation_ids.len() as u64);
        }
        self.way_ids = way_ids;
        self.ends = ends;
        self.relation_ids = relation_ids;

        self.pending = Vec::new();
        self.finalized = true;
    }

    pub fn get(&self, way_id: i64) -> &[i64] {
        let Ok(index) = self.way_ids.binary_search(&way_id) else {
            return &[];
        };
        let start = index.checked_sub(1).map_or(0, |i| self.ends[i] as usize);
        &self.relation_ids[start..self.ends[index] as usize]
    }

    pub fn contains(&self, way_id: i64) -> bool {
        self.way_ids.binary_search(&way_id).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::PackedRelationIndex;

    #[test]
    fn packed_relation_index_returns_all_relation_ids_for_way() {
        let mut index = PackedRelationIndex::default();
        index.push(100, 7);
        index.push(100, 9);
        index.push(200, 11);
        index.finalize();

        assert_eq!(index.get(100), &[7, 9]);
        assert_eq!(index.get(200), &[11]);
        assert!(index.get(300).is_empty());
    }

    #[test]
    fn packed_relation_index_sorts_and_deduplicates_entries() {
        let mut index = PackedRelationIndex::default();
        index.push(100, 9);
        index.push(100, 7);
        index.push(100, 7);
        index.push(100, 8);
        index.finalize();

        assert_eq!(index.get(100), &[7, 8, 9]);
    }

    #[test]
    fn finished_builder_finds_each_ways_relations_and_nothing_between() {
        let mut builder = super::PackedRelationIndexBuilder::default();
        builder.extend([(300, 1), (100, 4), (200, 2), (100, 3), (300, 1), (300, 5)]);
        let index = builder.finish();
        assert_eq!(index.get(100), &[3, 4]);
        assert_eq!(index.get(200), &[2]);
        assert_eq!(index.get(300), &[1, 5]);
        for missing in [0, 150, 250, 400] {
            assert!(index.get(missing).is_empty());
            assert!(!index.contains(missing));
        }
        assert!(index.contains(200));
        assert!(super::PackedRelationIndex::default().get(1).is_empty());
    }
}
