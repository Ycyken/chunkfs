use std::fmt::{Debug, Formatter};
use std::hash;
use std::ops::{Add, AddAssign, Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use system::database::{Database, IterableDatabase};
pub use system::disk_database::DiskDatabase;
pub use system::scrub::{CopyScrubber, Scrub, ScrubMeasurements};
pub use system::storage::{Data, DataContainer};
pub use system::{create_cdc_filesystem, FileSystem};

#[cfg(feature = "bench")]
pub mod bench;
#[cfg(feature = "chunkers")]
pub mod chunkers;
#[cfg(feature = "hashers")]
pub mod hashers;
mod system;

/// Trait for a CDC hash, combining several other traits: [hash::Hash], [Clone], [Eq], [PartialEq], [Default].
///
/// Auto-implemented for those structures that implement all the listed traits.
pub trait ChunkHash: hash::Hash + Clone + Eq + PartialEq + Default {}

impl<T: hash::Hash + Clone + Eq + PartialEq + Default> ChunkHash for T {}

/// One kilobyte.
pub const KB: usize = 1024;

/// One megabyte.
pub const MB: usize = 1024 * KB;

/// One gigabyte.
pub const GB: usize = 1024 * MB;

/// Block size, used by [`read`][crate::FileSystem::read_from_file]
/// and [`write`][crate::FileSystem::write_to_file] methods in the [`FileSystem`].
/// Blocks given to the user or by them must be of this size.
const SEG_SIZE: usize = MB; // 1MB

/// A chunk of the processed data. Doesn't store any data,
/// only contains offset and length of the chunk.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    offset: usize,
    length: usize,
}

impl Chunk {
    pub fn new(offset: usize, length: usize) -> Self {
        Self { offset, length }
    }

    /// Effective range of the chunk in the data.
    pub fn range(&self) -> std::ops::Range<usize> {
        self.offset..self.offset + self.length
    }

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn offset(&self) -> usize {
        self.offset
    }
}

/// Base functionality for objects that split given data into chunks.
/// Doesn't modify the given data or do anything else.
///
/// Chunks that are found are returned by [`chunk_data`][Chunker::chunk_data] method.
/// If some contents were cut because the end of `data` and not the end of the chunk was reached,
/// it must be returned with [`rest`][Chunker::rest] instead of storing it in the [`chunk_data`][Chunker::chunk_data]'s output.
pub trait Chunker: Debug {
    /// Goes through whole `data` and finds chunks. If last chunk is not actually a chunk but a leftover,
    /// it is returned via [`rest`][Chunker::rest] method and is not contained in the vector.
    ///
    /// `empty` is an empty vector whose capacity is determined by [`estimate_chunk_count`][Chunker::estimate_chunk_count].
    /// Resulting chunks should be written right to it, and it should be returned as result.
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk>;

    /// Returns an estimate amount of chunks that will be created once the algorithm runs through the whole
    /// data buffer. Used to pre-allocate the buffer with the required size so that allocation times are not counted
    /// towards total chunking time.
    fn estimate_chunk_count(&self, data: &[u8]) -> usize;
}

/// Reference to a chunker that can be re-used.
#[derive(Clone)]
pub struct ChunkerRef(Arc<Mutex<dyn Chunker>>);

impl Deref for ChunkerRef {
    type Target = Arc<Mutex<dyn Chunker>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ChunkerRef {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<Arc<Mutex<dyn Chunker>>> for ChunkerRef {
    fn from(value: Arc<Mutex<dyn Chunker>>) -> Self {
        Self(value)
    }
}

impl<C> From<C> for ChunkerRef
where
    C: Chunker + 'static,
{
    fn from(chunker: C) -> Self {
        ChunkerRef(Arc::new(Mutex::new(chunker)))
    }
}

impl Debug for ChunkerRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.lock().unwrap().fmt(f)
    }
}

/// Functionality for an object that hashes the input.
pub trait Hasher {
    /// Hash type that would be returned by the hasher.
    type Hash: ChunkHash;

    /// Takes some `data` and returns its `hash`.
    fn hash(&mut self, data: &[u8]) -> Self::Hash;

    /// Returns length of the given hash.
    fn len(&self, hash: &Self::Hash) -> usize;
}

impl<H, Hash> From<H> for Box<dyn Hasher<Hash = Hash>>
where
    H: Hasher<Hash = Hash> + 'static,
{
    fn from(hasher: H) -> Self {
        Box::new(hasher)
    }
}

/// Measurements that are received after writing data to a file.
/// Contain time spent for chunking and for hashing.
#[derive(Debug, PartialEq, Default, Clone, Copy)]
pub struct WriteMeasurements {
    save_time: Duration,
    chunk_time: Duration,
    hash_time: Duration,
}

impl WriteMeasurements {
    pub(crate) fn new(save_time: Duration, chunk_time: Duration, hash_time: Duration) -> Self {
        Self {
            save_time,
            chunk_time,
            hash_time,
        }
    }

    pub fn save_time(&self) -> Duration {
        self.save_time
    }

    pub fn chunk_time(&self) -> Duration {
        self.chunk_time
    }

    pub fn hash_time(&self) -> Duration {
        self.hash_time
    }
}

impl Add for WriteMeasurements {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            save_time: self.save_time + rhs.save_time,
            chunk_time: self.chunk_time + rhs.chunk_time,
            hash_time: self.hash_time + rhs.hash_time,
        }
    }
}

impl AddAssign for WriteMeasurements {
    fn add_assign(&mut self, rhs: Self) {
        self.save_time += rhs.save_time;
        self.chunk_time += rhs.chunk_time;
        self.hash_time += rhs.hash_time;
    }
}
