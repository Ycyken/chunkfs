use crate::ChunkHash;
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Error, ErrorKind, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::io;

/// Serves as base functionality for storing the actual data as key-value pairs.
///
/// Supports inserting and getting values by key, checking if the key is present in the storage.
pub trait Database<K, V> {
    /// Inserts a key-value pair into the storage.
    fn insert(&mut self, key: K, value: V) -> io::Result<()>;

    /// Retrieves a value by a given key. Note that it returns a value, not a reference.
    ///
    /// # Errors
    /// Should return [ErrorKind::NotFound], if the key-value pair
    /// was not found in the storage.
    fn get(&self, key: &K) -> io::Result<V>;

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
    fn iterator(&self) -> Box<dyn Iterator<Item=(K, V)> + '_>;

    /// Returns an iterator that can mutate values but not keys.
    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item=(&K, &mut V)> + '_>;

    /// Returns an immutable iterator over keys.
    fn keys(&self) -> Box<dyn Iterator<Item=K> + '_>;

    /// Returns an immutable iterator over values.
    fn values(&self) -> Box<dyn Iterator<Item=V> + '_>;

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
    fn insert(&mut self, key: Hash, value: V) -> io::Result<()> {
        self.entry(key).or_insert(value);
        Ok(())
    }

    fn get(&self, key: &Hash) -> io::Result<V> {
        self.get(key).ok_or(ErrorKind::NotFound.into()).cloned()
    }

    fn contains(&self, key: &Hash) -> bool {
        self.contains_key(key)
    }
}

impl<Hash: ChunkHash, V: Clone> IterableDatabase<Hash, V> for HashMap<Hash, V> {
    fn iterator(&self) -> Box<dyn Iterator<Item=(Hash, V)> + '_> {
        Box::new(self.iter().map(|(k, v)| (k.clone(), v.clone())))
    }

    fn iterator_mut(&mut self) -> Box<dyn Iterator<Item=(&Hash, &mut V)> + '_> {
        Box::new(self.iter_mut())
    }

    fn keys(&self) -> Box<dyn Iterator<Item=Hash> + '_> {
        Box::new(self.keys().map(|k| k.clone()))
    }

    fn values(&self) -> Box<dyn Iterator<Item=V> + '_> {
        Box::new(self.values().map(|v| v.clone()))
    }

    fn clear(&mut self) -> io::Result<()> {
        HashMap::clear(self);
        Ok(())
    }
}

// impl<K, V> DiskDatabase<K, V> {
// finds free k segments in a row and marks them with 1 in bitmap
// fn find_and_mark_k_segments(&mut self, k: u64) -> Option<u64> {
//     let mut start_segment: u64 = 0;
//     let mut free_bits_count = 0;
//     //  looking for k free bits in a row
//     'outer: for (i, &interval) in self.bitmap.iter().enumerate() {
//         let i = i as u64;
//         for bit in 0..64 {
//             if (interval & (1 << (63 - bit))) == 0 { // is the bit = 0
//                 if free_bits_count == 0 {
//                     start_segment = i * 64 + bit;
//                 }
//                 free_bits_count += 1;
//                 if free_bits_count == k {
//                     break 'outer;
//                 }
//             } else {
//                 free_bits_count = 0;
//             }
//         }
//     }
//
//     if free_bits_count == k {
//         for j in 0..k {
//             let bit_pos = start_segment + j;
//             let interval_index = bit_pos / 64;
//             let bit_in_interval = 63 - (bit_pos % 64);
//             self.bitmap[interval_index as usize] |= 1 << bit_in_interval; // set bit to 1
//         }
//         return Some(start_segment);
//     }
//     None
// }
// }

pub trait ScrubDatabase<K1, V1, K2, V2>: Database<K1, V1> {
    /// Inserts a key-value pair into the database.
    fn db_insert(&mut self, key: K1, value: V1) -> io::Result<()>;

    /// Retrieves a value by a given key in the database. Note that it returns a value, not a reference.
    ///
    /// # Errors
    /// Should return [ErrorKind::NotFound], if the key-value pair
    /// was not found in the database.
    fn db_get(&self, key: &K1) -> io::Result<V1>;

    /// Inserts multiple key-value pairs into the database.
    fn db_insert_multi(&mut self, pairs: Vec<(K1, V1)>) -> io::Result<()> {
        for (key, value) in pairs.into_iter() {
            self.db_insert(key, value)?;
        }
        Ok(())
    }

    /// Retrieves a multitude of values from the database, corresponding to the keys, in the correct order.
    fn db_get_multi(&self, keys: &[K1]) -> io::Result<Vec<V1>> {
        keys.iter().map(|key| self.db_get(key)).collect()
    }

    /// Returns `true` if the database contains a value for the specified key.
    fn db_contains(&self, key: &K1) -> bool;


    /// Inserts a key-value pair into the target_map.
    fn target_map_insert(&mut self, key: K2, value: V2) -> io::Result<()>;

    /// Retrieves a value by a given key in the target_map. Note that it returns a value, not a reference.
    ///
    /// # Errors
    /// Should return [ErrorKind::NotFound], if the key-value pair
    /// was not found in the database.
    fn target_map_get(&self, key: &K2) -> io::Result<V2>;

    /// Inserts multiple key-value pairs into the target_map.
    fn target_map_insert_multi(&mut self, pairs: Vec<(K2, V2)>) -> io::Result<()> {
        for (key, value) in pairs.into_iter() {
            self.target_map_insert(key, value)?;
        }
        Ok(())
    }

    /// Retrieves a multitude of values from the database, corresponding to the keys, in the correct order.
    fn target_map_get_multi(&self, keys: &[K2]) -> io::Result<Vec<V2>> {
        keys.iter().map(|key| self.target_map_get(key)).collect()
    }

    /// Returns `true` if the database contains a value for the specified key.
    fn target_map_contains(&self, key: &K2) -> bool;
}

pub trait IterableScrubDatabase<K1, V1, K2, V2>: ScrubDatabase<K1, V1, K2, V2> {
    /// Returns a simple immutable iterator over key-value pairs of the database.
    fn db_iterator(&self) -> Box<dyn Iterator<Item=(K1, V1)> + '_>;

    /// Returns an immutable iterator over keys of the database.
    fn db_keys(&self) -> Box<dyn Iterator<Item=K1> + '_>;

    /// Returns an immutable iterator over values of the database.
    fn db_values<'a>(&'a self) -> Box<dyn Iterator<Item=V1> + 'a>
    where
        K1: 'a,
    {
        Box::new(self.db_keys().map(|k| self.db_get(&k).unwrap()))
    }

    /// Returns a simple immutable iterator over key-value pairs of the target map.
    fn target_map_iterator(&self) -> Box<dyn Iterator<Item=(K2, V2)> + '_>;

    /// Returns an immutable iterator over keys of the target map.
    fn target_map_keys(&self) -> Box<dyn Iterator<Item=K2> + '_>;

    /// Returns an immutable iterator over values of the target map.
    fn target_map_values<'a>(&'a self) -> Box<dyn Iterator<Item=V2> + 'a>
    where
        K2: 'a,
    {
        Box::new(self.target_map_keys().map(|k| self.target_map_get(&k).unwrap()))
    }

    /// Clears the database, removing all contained key-value pairs.
    fn clear(&mut self) -> io::Result<()>;
}

#[derive(Clone)]
struct DataInfo {
    start_block: u64,
    data_length: u64,
}

const BLKGETSIZE64: u64 = 0x80081272;
const BLKSSZGET: u64 = 0x1268;

pub struct DiskDatabase<K1, V1, K2, V2>
where
    K1: ChunkHash,
    K2: ChunkHash,
    V1: Clone + Serialize + for<'a> Deserialize<'a>,
    V2: Clone + Serialize + for<'a> Deserialize<'a>,
{
    device: File,
    // bitmap: Vec<u64>,
    database: HashMap<K1, DataInfo>,
    target_map: HashMap<K2, DataInfo>,
    total_size: u64,
    // bitmap_size: u64,
    block_size: u64,
    blocks_number: u64,
    used_blocks: u64,
    _data_types: PhantomData<(V1, V2)>,
}

impl<K1, V1, K2, V2> DiskDatabase<K1, V1, K2, V2>
where
    K1: ChunkHash,
    K2: ChunkHash,
    V1: Clone + Serialize + for<'a> Deserialize<'a>,
    V2: Clone + Serialize + for<'a> Deserialize<'a>,
{
    pub fn init_on_regular_file(file_path: &str, total_size: u64) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(file_path)?;
        file.set_len(total_size)?;

        let block_size = 512u64;
        let blocks_number = total_size / block_size;

        let database = HashMap::new();
        let target_map = HashMap::new();

        Ok(Self {
            device: file,
            database,
            target_map,
            total_size,
            block_size: 512,
            blocks_number,
            used_blocks: 0,
            _data_types: PhantomData,
        })
    }

    pub fn init(blkdev_path: &str) -> Result<Self, Error> {
        let device = OpenOptions::new()
            .read(true)
            .write(true)
            .open(blkdev_path)?;
        let _fd = device.as_raw_fd();

        let mut total_size: u64 = 0;
        let mut block_size: u64 = 0;
        if -1 == unsafe { libc::ioctl(_fd, BLKGETSIZE64, &mut total_size) } {
            return Err(Error::last_os_error());
        };
        if -1 == unsafe { libc::ioctl(_fd, BLKSSZGET, &mut block_size) } {
            return Err(Error::last_os_error());
        };
        if block_size == 0 {
            return Err(Error::new(ErrorKind::InvalidData, "block size cannot be 0"));
        }
        let blocks_number = total_size / block_size;

        let database = HashMap::new();
        let target_map = HashMap::new();

        Ok(Self {
            device,
            database,
            target_map,
            total_size,
            block_size,
            blocks_number,
            used_blocks: 0,
            _data_types: PhantomData {},
        })
    }

    fn write<T: Serialize>(&mut self, value: T) -> io::Result<DataInfo> {
        let encoded = serialize(&value).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        let data_length = encoded.len() as u64;
        let blocks_number = data_length.div_ceil(self.block_size);

        if self.used_blocks * self.block_size + data_length >= self.total_size {
            return Err(Error::new(ErrorKind::OutOfMemory, "out of memory"));
        }

        self.device.seek(SeekFrom::Start(self.used_blocks * self.block_size))?;
        self.device.write_all(&encoded)?;

        let data_info = DataInfo { start_block: self.used_blocks, data_length };
        self.used_blocks += blocks_number;
        Ok(data_info)
    }

    fn read<T: for<'a> Deserialize<'a>>(&self, data_info: DataInfo) -> io::Result<T> {
        let mut data = vec![0u8; data_info.data_length as usize];

        self.device.read_at(&mut data, data_info.start_block * self.block_size)?;
        let data = deserialize(&data).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        Ok(data)
    }
}

impl<K1, V1, K2, V2> ScrubDatabase<K1, V1, K2, V2> for DiskDatabase<K1, V1, K2, V2>
where
    K1: ChunkHash,
    K2: ChunkHash,
    V1: Clone + Serialize + for<'a> Deserialize<'a>,
    V2: Clone + Serialize + for<'a> Deserialize<'a>,
{
    fn db_insert(&mut self, key: K1, data: V1) -> io::Result<()> {
        if self.database.contains_key(&key) {
            return Ok(());
        }
        let data_info = self.write(data)?;
        self.database.insert(key, data_info);
        Ok(())
    }

    fn db_get(&self, key: &K1) -> io::Result<V1> {
        let data_info = self.database.get(key).ok_or(ErrorKind::NotFound)?;
        self.read(data_info.clone())
    }

    fn db_contains(&self, key: &K1) -> bool {
        self.database.contains_key(key)
    }

    fn target_map_insert(&mut self, key: K2, data: V2) -> io::Result<()> {
        if self.target_map.contains_key(&key) {
            return Ok(());
        }
        let data_info = self.write(data)?;
        self.target_map.insert(key, data_info);
        Ok(())
    }

    fn target_map_get(&self, key: &K2) -> io::Result<V2> {
        let data_info = self.target_map.get(key).ok_or(ErrorKind::NotFound)?;
        self.read(data_info.clone())
    }

    fn target_map_contains(&self, key: &K2) -> bool {
        self.target_map.contains_key(key)
    }
}

impl<K1, V1, K2, V2> IterableScrubDatabase<K1, V1, K2, V2> for DiskDatabase<K1, V1, K2, V2>
where
    K1: ChunkHash,
    K2: ChunkHash,
    V1: Clone + Serialize + for<'a> Deserialize<'a>,
    V2: Clone + Serialize + for<'a> Deserialize<'a>,
{
    fn db_iterator(&self) -> Box<dyn Iterator<Item=(K1, V1)> + '_> {
        Box::new(self.database.keys().map(|k| (k.clone(), self.db_get(k).unwrap())))
    }

    fn db_keys(&self) -> Box<dyn Iterator<Item=K1> + '_>
    {
        Box::new(self.database.keys().cloned())
    }

    fn target_map_iterator(&self) -> Box<dyn Iterator<Item=(K2, V2)> + '_> {
        Box::new(self.target_map.keys().map(|k| (k.clone(), self.target_map_get(k).unwrap())))
    }

    fn target_map_keys(&self) -> Box<dyn Iterator<Item=K2> + '_> {
        Box::new(self.target_map.keys().cloned())
    }

    fn clear(&mut self) -> io::Result<()> {
        self.database.clear();
        self.target_map.clear();
        self.used_blocks = 0;
        Ok(())
    }
}

pub struct DatabasePair<D1, D2, K1, V1, K2, V2>
where
    D1: Database<K1, V1>,
    D2: Database<K2, V2>,
    K1: ChunkHash,
    K2: ChunkHash,
    V1: Clone,
    V2: Clone,
{
    database: D1,
    target_map: D2,
    _marker: PhantomData<(K1, V1, K2, V2)>,
}

impl<D1, D2, K1, V1, K2, V2> DatabasePair<D1, D2, K1, V1, K2, V2>
where
    D1: Database<K1, V1>,
    D2: Database<K2, V2>,
    K1: ChunkHash,
    K2: ChunkHash,
    V1: Clone,
    V2: Clone,
{
    pub fn new(database: D1, target_map: D2) -> Self { Self { database, target_map, _marker: PhantomData } }
}

impl<D1, D2, K1, V1, K2, V2> Database<K1, V1> for DatabasePair<D1, D2, K1, V1, K2, V2>
where
    D1: Database<K1, V1>,
    D2: Database<K2, V2>,
    K1: ChunkHash,
    K2: ChunkHash,
    V1: Clone,
    V2: Clone,
{
    fn insert(&mut self, key: K1, value: V1) -> io::Result<()> {
        self.database.insert(key, value)
    }

    fn get(&self, key: &K1) -> io::Result<V1> {
        self.database.get(key)
    }

    fn contains(&self, key: &K1) -> bool {
        self.database.contains(key)
    }
}

impl<D1, D2, K1, V1, K2, V2> ScrubDatabase<K1, V1, K2, V2> for DatabasePair<D1, D2, K1, V1, K2, V2>
where
    D1: Database<K1, V1>,
    D2: Database<K2, V2>,
    K1: ChunkHash,
    K2: ChunkHash,
    V1: Clone,
    V2: Clone,
{
    fn db_insert(&mut self, key: K1, value: V1) -> io::Result<()> {
        self.database.insert(key, value)
    }

    fn db_get(&self, key: &K1) -> io::Result<V1> {
        self.database.get(key)
    }

    fn db_contains(&self, key: &K1) -> bool {
        self.database.contains(key)
    }

    fn target_map_insert(&mut self, key: K2, value: V2) -> io::Result<()> {
        self.target_map.insert(key, value)
    }

    fn target_map_get(&self, key: &K2) -> io::Result<V2> {
        self.target_map.get(key)
    }
    fn target_map_contains(&self, key: &K2) -> bool {
        self.target_map.contains(key)
    }
}

impl<D1, D2, K1, V1, K2, V2> IterableScrubDatabase<K1, V1, K2, V2> for DatabasePair<D1, D2, K1, V1, K2, V2>
where
    D1: IterableDatabase<K1, V1>,
    D2: IterableDatabase<K2, V2>,
    K1: ChunkHash,
    K2: ChunkHash,
    V1: Clone,
    V2: Clone,
{
    fn db_iterator(&self) -> Box<dyn Iterator<Item=(K1, V1)> + '_> {
        self.database.iterator()
    }

    fn db_keys(&self) -> Box<dyn Iterator<Item=K1> + '_> {
        self.database.keys()
    }

    fn target_map_iterator(&self) -> Box<dyn Iterator<Item=(K2, V2)> + '_> {
        self.target_map.iterator()
    }

    fn target_map_keys(&self) -> Box<dyn Iterator<Item=K2> + '_> {
        self.target_map.keys()
    }

    fn clear(&mut self) -> io::Result<()> {
        self.database.clear()?;
        self.target_map.clear()?;
        Ok(())
    }
}

impl<K1, V1, K2, V2> Database<K1, V1> for DiskDatabase<K1, V1, K2, V2>
where
    K1: ChunkHash,
    K2: ChunkHash,
    V1: Clone + Serialize + for<'a> Deserialize<'a>,
    V2: Clone + Serialize + for<'a> Deserialize<'a>,
{
    fn insert(&mut self, key: K1, value: V1) -> io::Result<()> {
        self.db_insert(key, value)
    }

    fn get(&self, key: &K1) -> io::Result<V1> {
        self.db_get(key)
    }

    fn contains(&self, key: &K1) -> bool {
        self.db_contains(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::system::database::{DiskDatabase, ScrubDatabase};
    use crate::KB;
    use chunkfs::hashers::Sha256Hasher;
    use chunkfs::Hasher;

    #[test]
    fn diskdb_write_and_read() {
        let file_path = "pseudo_dev";
        let file_size = 1024 * 1024 * 12;

        let mut scrub_db: DiskDatabase<[u8; 32], Vec<u8>, [u8; 32], Vec<u8>> =
            DiskDatabase::init_on_regular_file(file_path, file_size).unwrap();
        let v1: Vec<u8> = vec![1; 8 * KB + 30];
        let v2: Vec<u8> = vec![2; 8 * KB + 70];

        let mut hasher = Sha256Hasher::default();
        let k1 = hasher.hash(&v1);
        let k2 = hasher.hash(&v2);

        scrub_db.db_insert(k1, v1.clone()).unwrap();
        scrub_db.target_map_insert(k2, v2.clone()).unwrap();
        let actual1 = scrub_db.db_get(&k1).unwrap();
        let actual2 = scrub_db.target_map_get(&k2).unwrap();
        assert_eq!(actual1, v1);
        assert_eq!(actual2, v2);
    }

    // #[test]
    // fn find_free_segments() {
    //     let mut db: DiskDatabase<Output<Sha256>, Vec<u8>> = DiskDatabase::init("/dev/nvme0n1p5").unwrap();
    //     db.bitmap = vec![0b11100100001 | (u64::MAX << 11)]; // 1...1 11100100001
    //
    //     assert_eq!(db.find_and_mark_k_segments(3), Some(59));
    //     assert_eq!(db.bitmap, vec![0b11100111101 | (u64::MAX << 11)])
    // }
    //
    // #[test]
    // fn find_free_segments_on_intersection() {
    //     let mut db: DiskDatabase<Output<Sha256>, Vec<u8>> = DiskDatabase::init("/dev/nvme0n1p5").unwrap();
    //     db.bitmap = vec![u64::MAX, 0b11100111100 | (u64::MAX << 11), u64::MAX >> 3]; // 1...1 1...111100111100 0001...1
    //
    //     assert_eq!(db.find_and_mark_k_segments(4), Some(126));
    //     assert_eq!(db.bitmap, vec![u64::MAX, 0b11100111111 | (u64::MAX << 11), (0b1101 << 60) + (u64::MAX >> 4)]) // 1...1 1...111100111111 1101...1
    // }
    //
    // #[test]
    // fn cant_find_free_segments() {
    //     let mut db: DiskDatabase<Output<Sha256>, Vec<u8>> = DiskDatabase::init("/dev/nvme0n1p5").unwrap();
    //     db.bitmap = vec![u64::MAX, 0b11100111100 | (u64::MAX << 11), u64::MAX >> 3]; // 1...1 1...111100111100 0001...1
    //
    //     assert_eq!(db.find_and_mark_k_segments(6), None);
    //     assert_eq!(db.bitmap, vec![u64::MAX, 0b11100111100 | (u64::MAX << 11), u64::MAX >> 3]) // same bitmap
    // }

    // #[test]
    // fn insert_get_some_data() {
    //     let mut db: DiskDatabase<Output<Sha256>, Vec<u8>> = DiskDatabase::init("/dev/nvme0n1p5").unwrap();
    //     let v1: Vec<u8> = vec![1; 8 * KB + 30];
    //     let v2: Vec<u8> = vec![2; 8 * KB + 70];
    //     let v3: Vec<u8> = vec![1; 8 * KB + 30];
    //
    //     let mut hasher = Sha256Hasher::default();
    //     let k1 = hasher.hash(&v1);
    //     let k2 = hasher.hash(&v2);
    //     let k3 = hasher.hash(&v3);
    //     let values = vec![v1.clone(), v2.clone(), v3.clone()];
    //     let keys = vec![k1, k2, k3];
    //
    //     db.insert_multi(vec![(k1, v1), (k2, v2), (k3, v3)]).unwrap();
    //     assert_eq!(db.get_multi(&keys).unwrap(), values)
    // }
}
