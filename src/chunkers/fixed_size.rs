use std::cmp::min;
use std::fmt::{Debug, Formatter};

use crate::{Chunk, Chunker};

/// Chunker that utilizes Fixed Sized Chunking (FSC) algorithm,
/// splitting file into even-sized chunks.
///
/// Default chunk size is 4096 bytes.
pub struct FSChunker {
    chunk_size: usize,
}

impl FSChunker {
    pub fn new(chunk_size: usize) -> Self {
        Self { chunk_size }
    }
}

impl Debug for FSChunker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Fixed size chunking, chunk size: {}", self.chunk_size)
    }
}

impl Default for FSChunker {
    fn default() -> Self {
        Self::new(4096)
    }
}

impl Chunker for FSChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let mut offset = 0;
        let mut chunks = empty;
        while offset < data.len() {
            let chunk = Chunk::new(offset, min(self.chunk_size, data.len() - offset));
            chunks.push(chunk);
            offset += self.chunk_size;
        }

        chunks
    }

    fn estimate_chunk_count(&self, data: &[u8]) -> usize {
        data.len() / self.chunk_size + 1
    }
}
