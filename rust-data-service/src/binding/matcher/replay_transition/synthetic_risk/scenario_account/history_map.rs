//! Persistent ordered storage for immutable scenario history rows.
use std::borrow::Borrow;
use std::fmt;
use std::ops::Index;

use rpds::RedBlackTreeMapSync;

#[derive(Clone, PartialEq, Eq)]
struct PersistentTree<K: Ord, V>(RedBlackTreeMapSync<K, V>);

#[derive(Clone, PartialEq, Eq)]
pub(super) struct HistoryMap<K: Ord, V>(PersistentTree<K, V>);

impl<K: Ord, V> HistoryMap<K, V> {
    pub(super) fn new() -> Self {
        Self(PersistentTree(RedBlackTreeMapSync::new_sync()))
    }

    pub(super) fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0 .0.get(key)
    }

    pub(super) fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0 .0.contains_key(key)
    }

    pub(super) fn insert(&mut self, key: K, value: V) {
        self.0 .0.insert_mut(key, value);
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.0 .0.iter()
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &V> {
        self.0 .0.values()
    }

    pub(super) fn len(&self) -> usize {
        self.0 .0.size()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0 .0.is_empty()
    }

    #[cfg(test)]
    pub(super) fn clear(&mut self) {
        self.0 = PersistentTree(RedBlackTreeMapSync::new_sync());
    }
}

impl<K: Ord, V> Default for HistoryMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, Q, V> Index<&Q> for HistoryMap<K, V>
where
    K: Ord + Borrow<Q>,
    Q: Ord + ?Sized,
{
    type Output = V;

    fn index(&self, key: &Q) -> &Self::Output {
        self.get(key).expect("history map key not found")
    }
}

impl<K: Ord, V> fmt::Debug for HistoryMap<K, V>
where
    K: fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_map().entries(self.0 .0.iter()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::HistoryMap;
    use std::collections::BTreeMap;
    use std::fmt::Debug;

    fn assert_reference<K, V>(actual: &HistoryMap<K, V>, expected: &BTreeMap<K, V>)
    where
        K: Ord + Debug,
        V: PartialEq + Debug,
    {
        assert_eq!(actual.len(), expected.len());
        assert_eq!(actual.is_empty(), expected.is_empty());
        assert_eq!(
            actual.iter().collect::<Vec<_>>(),
            expected.iter().collect::<Vec<_>>()
        );
        assert_eq!(
            actual.values().collect::<Vec<_>>(),
            expected.values().collect::<Vec<_>>()
        );
        assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
        for (key, value) in expected {
            assert!(actual.contains_key(key));
            assert_eq!(actual.get(key), Some(value));
        }
    }

    #[test]
    fn ordered_insert_replace_and_lookup_match_btree_map() {
        for keys in [
            (0..128).collect::<Vec<_>>(),
            (0..128).rev().collect::<Vec<_>>(),
            (0..128).map(|key| (key * 53) % 128).collect::<Vec<_>>(),
        ] {
            let mut actual = HistoryMap::new();
            let mut expected = BTreeMap::new();
            for key in keys {
                actual.insert(key, key * 2);
                expected.insert(key, key * 2);
            }
            for key in (0..128).step_by(3) {
                actual.insert(key, -key);
                expected.insert(key, -key);
            }
            assert_reference(&actual, &expected);
            assert_eq!(actual, actual.clone());
        }
    }

    #[test]
    fn shared_clone_branches_and_failed_draft_drop_preserve_owned_snapshots() {
        let mut a = HistoryMap::new();
        let mut model_a = BTreeMap::new();
        for key in (0..96).rev() {
            a.insert(key, vec![key, key + 1]);
            model_a.insert(key, vec![key, key + 1]);
        }
        let snapshot_a = model_a.clone();
        let mut b = a.clone();
        let mut model_b = model_a.clone();
        b.insert(10, vec![900]);
        model_b.insert(10, vec![900]);
        b.insert(200, vec![200]);
        model_b.insert(200, vec![200]);
        let snapshot_b = model_b.clone();

        {
            let mut c = b.clone();
            let mut model_c = model_b.clone();
            c.insert(11, vec![901]);
            model_c.insert(11, vec![901]);
            c.insert(201, vec![201]);
            model_c.insert(201, vec![201]);
            assert_reference(&c, &model_c);
        }

        assert_reference(&a, &snapshot_a);
        assert_reference(&b, &snapshot_b);
        assert_eq!(a, a.clone());
        assert_eq!(b, b.clone());
    }
}
