use std::fmt::{Debug, Formatter};

use cdc_chunkers::SizeParams;

use crate::{Chunk, Chunker};

/// Chunker that utilizes Leap-based CDC algorithm.
pub struct LeapChunker {
    sizes: SizeParams,
}

impl LeapChunker {
    pub fn new(sizes: SizeParams) -> Self {
        Self { sizes }
    }
}

impl Default for LeapChunker {
    fn default() -> Self {
        Self::new(SizeParams::leap_default())
    }
}

impl Debug for LeapChunker {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LeapCDC, sizes: {:?}", self.sizes)
    }
}

impl Chunker for LeapChunker {
    fn chunk_data(&mut self, data: &[u8], empty: Vec<Chunk>) -> Vec<Chunk> {
        let chunker = cdc_chunkers::leap_based::Chunker::new(data, self.sizes);
        let mut chunks = empty;
        for chunk in chunker {
            chunks.push(Chunk::new(chunk.pos, chunk.len));
        }

        chunks
    }

    fn estimate_chunk_count(&self, data: &[u8]) -> usize {
        data.len() / self.sizes.min
    }
}
