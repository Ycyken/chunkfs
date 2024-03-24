use std::collections::HashMap;
use std::io::ErrorKind;

use crate::storage::Span;
use crate::{VecHash, SEG_SIZE};

/// Hashed span, starting at `offset`
#[derive(Debug, PartialEq, Eq, Default)]
pub struct FileSpan {
    hash: VecHash,
    offset: usize,
}

pub struct File {
    name: String,
    spans: Vec<FileSpan>,
}

/// Layer that contains all files
#[derive(Default)]
pub struct FileLayer {
    files: HashMap<String, File>,
}

/// Handle for an opened file
#[derive(Debug, PartialEq)]
pub struct FileHandle {
    // can't make file_name a reference
    // or have a reference to File,
    // or it would count as an immutable reference for FileSystem
    file_name: String,
    // store offset of the file, so that reading/writing can be done by blocks
    offset: usize,
}

impl File {
    fn new(name: String, spans: Vec<FileSpan>) -> Self {
        File { name, spans }
    }
}

impl FileHandle {
    fn new(file: &File) -> Self {
        FileHandle {
            file_name: file.name.clone(),
            offset: 0,
        }
    }
}

impl FileLayer {
    /// Creates a file and returns its `FileHandle`
    pub fn create(&mut self, name: String) -> std::io::Result<FileHandle> {
        if self.files.contains_key(&name) {
            return Err(std::io::Error::from(ErrorKind::AlreadyExists));
        }

        let file = File::new(name.clone(), vec![]);
        let file = self.files.entry(name).or_insert(file);
        Ok(FileHandle::new(file))
    }

    /// Opens a file based on its name and returns its `FileHandle`
    pub fn open(&self, name: &str) -> Option<FileHandle> {
        self.files.get(name).map(FileHandle::new)
    }

    fn find_file(&self, handle: &FileHandle) -> &File {
        self.files.get(&handle.file_name).unwrap()
    }

    fn find_file_mut(&mut self, handle: &FileHandle) -> &mut File {
        self.files.get_mut(&handle.file_name).unwrap()
    }

    /// Reads all hashes of the file, from beginning to end
    pub fn read_complete(&self, handle: &FileHandle) -> Vec<VecHash> {
        let file = self.find_file(handle);
        file.spans
            .iter()
            .map(|span| span.hash.clone()) // cloning hashes
            .collect()
    }

    /// Writes spans to the end of the file
    pub fn write(&mut self, handle: &mut FileHandle, spans: Vec<Span>) {
        let file = self.find_file_mut(handle);
        for span in spans {
            file.spans.push(FileSpan {
                hash: span.hash,
                offset: handle.offset,
            });
            handle.offset += span.length;
        }
    }

    pub fn read(&mut self, handle: &mut FileHandle) -> Vec<VecHash> {
        let file = self.find_file(handle);

        let mut bytes_read = 0;
        let mut last_offset = handle.offset;
        let hashes = file
            .spans
            .iter()
            .skip_while(|span| span.offset < handle.offset)
            .take_while(|span| {
                bytes_read += span.offset - last_offset;
                last_offset = span.offset;
                bytes_read < SEG_SIZE
            })
            .map(|span| span.hash.clone())
            .collect();
        handle.offset += bytes_read;
        hashes
    }
}

#[cfg(test)]
mod tests {
    use std::io::ErrorKind;

    use crate::file_layer::FileLayer;

    #[test]
    fn file_layer_create_file() {
        let mut fl = FileLayer::default();
        let name = "hello".to_string();
        fl.create(name.clone()).unwrap();

        assert_eq!(fl.files.get(&name).unwrap().name, "hello");
        assert_eq!(fl.files.get(&name).unwrap().spans, vec![]);
    }

    #[test]
    fn cant_create_two_files_with_same_name() {
        let mut fl = FileLayer::default();
        fl.create("hello".to_string()).unwrap();

        let result = fl.create("hello".to_string());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::AlreadyExists);
    }
}
