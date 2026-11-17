#[derive(Debug, Default, Clone)]
pub struct PackedRelationIndex {
    pending: Vec<(i64, i64)>,
    way_ranges: Vec<WayRange>,
    relation_ids: Vec<i64>,
    finalized: bool,
}

#[derive(Debug, Clone, Copy)]
struct WayRange {
    way_id: i64,
    start: usize,
    end: usize,
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
        self.pending.push((way_id, relation_id));
        self.finalized = false;
    }

    pub fn finalize(&mut self) {
        if self.finalized {
            return;
        }

        self.pending.sort_unstable();
        self.pending.dedup();
        self.way_ranges.clear();
        self.relation_ids.clear();

        let mut index = 0usize;
        while index < self.pending.len() {
            let way_id = self.pending[index].0;
            let start = self.relation_ids.len();
            while index < self.pending.len() && self.pending[index].0 == way_id {
                self.relation_ids.push(self.pending[index].1);
                index += 1;
            }
            let end = self.relation_ids.len();
            self.way_ranges.push(WayRange { way_id, start, end });
        }

        self.pending.clear();
        self.pending.shrink_to_fit();
        self.finalized = true;
    }

    pub fn get(&self, way_id: i64) -> &[i64] {
        let Ok(index) = self
            .way_ranges
            .binary_search_by_key(&way_id, |range| range.way_id)
        else {
            return &[];
        };
        let range = self.way_ranges[index];
        &self.relation_ids[range.start..range.end]
    }

    pub fn contains(&self, way_id: i64) -> bool {
        !self.get(way_id).is_empty()
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
}
