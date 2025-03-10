use std::collections::HashMap;
use std::{fs, io};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use crate::{ChunkHash};

/// Serves as base functionality for storing the actual data as key-value pairs.
///
/// Supports inserting and getting values by key, checking if the key is present in the storage.
pub trait Database<K, V> {
    fn init(storage_path: &str) -> Result<Self, io::Error>
    where
        Self: Sized;

    /// Inserts a key-value pair into the storage.
    fn insert(&mut self, key: K, value: V) -> io::Result<()>;

    /// Retrieves a value by a given key. Note that it returns a value, not a reference.
    ///
    /// # Errors
    /// Should return [ErrorKind::NotFound], if the key-value pair
    /// was not found in the storage.
    fn get(&mut self, key: &K) -> io::Result<V>;

    /// Inserts multiple key-value pairs into the storage.
    fn insert_multi(&mut self, pairs: Vec<(K, V)>) -> io::Result<()> {
        for (key, value) in pairs.into_iter() {
            self.insert(key, value)?;
        }
        Ok(())
    }

    /// Retrieves a multitude of values, corresponding to the keys, in the correct order.
    fn get_multi(&mut self, keys: &[K]) -> io::Result<Vec<V>> {
        keys.iter().map(|key| self.get(key)).collect()
    }

    /// Returns `true` if the database contains a value for the specified key.
    fn contains(&self, key: &K) -> bool;
}

/// Allows iteration over database contents.
pub trait IterableDatabase<K, V>: Database<K, V> {
    /// Returns a simple immutable iterator over values.
    fn iterator(&self) -> Box<dyn Iterator<Item=(&K, &V)> + '_>;

    /// Returns an iterator that can mutate values but not keys.
    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item=(&K, &mut V)> + '_>;

    /// Returns an immutable iterator over keys.
    fn keys<'a>(&'a self) -> Box<dyn Iterator<Item=&'a K> + 'a>
    where
        V: 'a,
    {
        Box::new(self.iterator().map(|(k, _)| k))
    }

    /// Returns an immutable iterator over values.
    fn values<'a>(&'a self) -> Box<dyn Iterator<Item=&'a V> + 'a>
    where
        K: 'a,
    {
        Box::new(self.iterator().map(|(_, v)| v))
    }

    /// Returns a mutable iterator over values.
    fn values_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item=&'a mut V> + 'a>
    where
        K: 'a,
    {
        Box::new(self.iterator_mut().map(|(_, v)| v))
    }

    /// Clears the database, removing all contained key-value pairs.
    fn clear(&mut self) -> io::Result<()>;
}

impl<Hash: ChunkHash, V: Clone> Database<Hash, V> for HashMap<Hash, V> {
    fn init(_: &str) -> Result<Self, io::Error> {
        Ok(HashMap::new())
    }

    fn insert(&mut self, key: Hash, value: V) -> io::Result<()> {
        self.entry(key).or_insert(value);
        Ok(())
    }

    fn get(&mut self, key: &Hash) -> io::Result<V> {
        (&*self).get(key).ok_or(io::ErrorKind::NotFound.into()).cloned()
    }

    fn contains(&self, key: &Hash) -> bool {
        self.contains_key(key)
    }
}

impl<Hash: ChunkHash, V: Clone> IterableDatabase<Hash, V> for HashMap<Hash, V> {
    fn iterator(&self) -> Box<dyn Iterator<Item=(&Hash, &V)> + '_> {
        Box::new(self.iter())
    }

    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item=(&Hash, &mut V)> + '_> {
        Box::new(self.iter_mut())
    }

    fn clear(&mut self) -> io::Result<()> {
        HashMap::clear(self);
        Ok(())
    }
}

#[derive(Clone)]
struct DataInfo {
    segment_number: u64,
    data_length: u64,
}

const SEGMENT_SIZE: u64 = 4096;

struct DiskDatabase<K, V> {
    device: File,
    bitmap: Vec<u64>,
    map: HashMap<K, DataInfo>,
    total_size: u64,
    bitmap_size: u64,
    map_size: u64,
    segments_number: u64,
    data_type: PhantomData<V>,
}

impl<K, V> DiskDatabase<K, V> {
    // finds free k segments in a row and marks them with 1 in bitmap
    fn find_and_mark_k_segments(&mut self, k: u64) -> Option<u64> {
        let mut start_segment: u64 = 0;
        let mut free_bits_count = 0;
        //  looking for k free bits in a row
        'outer: for (i, &interval) in self.bitmap.iter().enumerate() {
            let i = i as u64;
            for bit in 0..64 {
                if (interval & (1 << (63 - bit))) == 0 { // is bit = 0
                    if free_bits_count == 0 {
                        start_segment = i * 64 + bit;
                    }
                    free_bits_count += 1;
                    if free_bits_count == k {
                        break 'outer;
                    }
                } else {
                    free_bits_count = 0;
                }
            }
        }

        if free_bits_count == k {
            for j in 0..k {
                let bit_pos = start_segment + j;
                let interval_index = bit_pos / 64;
                let bit_in_interval = 63 - (bit_pos % 64);
                self.bitmap[interval_index as usize] |= 1 << bit_in_interval; // set bit to 1
            }
            return Some(start_segment);
        }
        None
    }
}

impl<K: ChunkHash, V> Database<K, V> for DiskDatabase<K, V>
where
        for<'a> V: Clone + Deserialize<'a> + Serialize,
{
    fn init(path: &str) -> Result<Self, io::Error> {
        let device = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        let total_size = fs::metadata(&path)?.len();
        let segments_number = total_size * 63 / 64 / SEGMENT_SIZE;
        let bitmap_size = segments_number / 8;
        let map_size = total_size / 64 * 63 / 64;
        let bitmap = vec![0; (segments_number / 64) as usize];
        let map = HashMap::new();
        Ok(Self { device, bitmap, map, total_size, bitmap_size, map_size, segments_number, data_type: PhantomData })
    }

    fn insert(&mut self, key: K, value: V) -> io::Result<()> {
        if self.map.contains(&key) {
            return Ok(());
        }
        let encoded = serialize(&value).unwrap();
        let data_length = encoded.len() as u64;
        let segments_number = data_length.div_ceil(SEGMENT_SIZE);

        let start_segment = self.find_and_mark_k_segments(segments_number);
        if start_segment.is_none() {
            return Err(io::Error::new(ErrorKind::OutOfMemory, "out of free segments"));
        }
        let start_segment = start_segment.unwrap();

        self.device.seek(SeekFrom::Start(self.bitmap_size + self.map_size + start_segment * SEGMENT_SIZE))?;
        self.device.write_all(&encoded)?;
        self.map.insert(key, DataInfo { segment_number: start_segment, data_length: data_length });
        Ok(())
    }

    fn get(&mut self, key: &K) -> io::Result<V> {
        let data_info = self.map.get(key).ok_or(ErrorKind::NotFound)?;
        self.device.seek(SeekFrom::Start(self.bitmap_size + self.map_size + data_info.segment_number * SEGMENT_SIZE))?;

        let mut data = Vec::with_capacity(data_info.data_length as usize);
        self.device.read(&mut data)?;
        let data = deserialize(&data).unwrap();
        Ok(data)
    }

    fn contains(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }
}

#[cfg(test)]
mod tests {
    use sha2::digest::Output;
    use sha2::Sha256;
    use crate::system::database::Database;
    use crate::system::database::DiskDatabase;

    #[test]
    fn find_free_segments() {
        let mut db: DiskDatabase<Output<Sha256>, Vec<u8>> = DiskDatabase::init("./testdb").unwrap();
        db.bitmap = vec![0b11100100001 | (u64::MAX << 11)]; // 1...1 11100100001

        assert_eq!(db.find_and_mark_k_segments(3), Some(59));
        assert_eq!(db.bitmap, vec![0b11100111101 | (u64::MAX << 11)])
    }

    #[test]
    fn find_free_segments_on_intersection() {
        let mut db: DiskDatabase<Output<Sha256>, Vec<u8>> = DiskDatabase::init("./testdb").unwrap();
        db.bitmap = vec![u64::MAX, 0b11100111100 | (u64::MAX << 11), u64::MAX >> 3]; // 1...1 1...111100111100 0001...1

        assert_eq!(db.find_and_mark_k_segments(4), Some(126));
        assert_eq!(db.bitmap, vec![u64::MAX, 0b11100111111 | (u64::MAX << 11), (0b1101 << 60) + (u64::MAX >> 4)]) // 1...1 1...111100111111 1101...1
    }

    #[test]
    fn cant_find_free_segments() {
        let mut db: DiskDatabase<Output<Sha256>, Vec<u8>> = DiskDatabase::init("./testdb").unwrap();
        db.bitmap = vec![u64::MAX, 0b11100111100 | (u64::MAX << 11), u64::MAX >> 3]; // 1...1 1...111100111100 0001...1

        assert_eq!(db.find_and_mark_k_segments(6), None);
        assert_eq!(db.bitmap, vec![u64::MAX, 0b11100111100 | (u64::MAX << 11), u64::MAX >> 3]) // same bitmap
    }
}
