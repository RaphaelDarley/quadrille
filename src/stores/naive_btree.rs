use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{KVStore, QuadrilleError};

#[derive(Default)]
pub struct NaiveBTree(BTreeMap<Vec<u8>, Vec<u8>>);

impl KVStore for NaiveBTree {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.0.get(key).cloned()
    }

    fn insert(&self, key: Vec<u8>, val: Vec<u8>) -> (Self, bool) {
        let mut new = self.0.clone();
        let found = new.insert(key, val).is_some();
        (NaiveBTree(new), found)
    }

    fn resolve(_basis: Arc<Self>, _prev: Arc<Self>) -> Result<Arc<Self>, QuadrilleError> {
        Err(QuadrilleError::KeyConflict)
    }
}
