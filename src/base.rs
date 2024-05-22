use std::collections::HashMap;
use std::io;
use std::io::ErrorKind;

use crate::map::Map;
use crate::{ChunkHash, Database, Segment};

/// Simple in-memory hashmap-based storage.
#[derive(Default)]
pub struct HashMapBase<Hash: ChunkHash, V> {
    segment_map: HashMap<Hash, V>, // hashmap<Hash, RefCell<Vec<u8>> for referencing
}

impl<Hash: ChunkHash> Database<Hash> for HashMapBase<Hash, Vec<u8>> {
    fn save(&mut self, segments: Vec<Segment<Hash>>) -> io::Result<()> {
        for segment in segments {
            self.segment_map.entry(segment.hash).or_insert(segment.data);
        }
        Ok(())
    }

    // vec<result>?
    fn retrieve(&self, request: Vec<Hash>) -> io::Result<Vec<Vec<u8>>> {
        request
            .into_iter()
            .map(|hash| {
                self.segment_map
                    .get(&hash)
                    .cloned() // can be done without cloning
                    .ok_or(ErrorKind::NotFound.into())
            })
            .collect()
    }
}

impl<Hash: ChunkHash, V: Clone> Map<Hash, V> for HashMap<Hash, V> {
    fn insert(&mut self, key: Hash, value: V) -> io::Result<()> {
        self.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &Hash) -> io::Result<V> {
        self.get(key).cloned().ok_or(ErrorKind::NotFound.into())
    }

    fn remove(&mut self, key: &Hash) {
        self.remove(key);
    }
}

impl<Hash: ChunkHash, V: Clone> Map<Hash, V> for HashMapBase<Hash, V> {
    fn insert(&mut self, key: Hash, value: V) -> io::Result<()> {
        self.segment_map.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &Hash) -> io::Result<V> {
        self.segment_map
            .get(key)
            .cloned()
            .ok_or(ErrorKind::NotFound.into())
    }

    fn remove(&mut self, key: &Hash) {
        self.segment_map.remove(key);
    }
}
