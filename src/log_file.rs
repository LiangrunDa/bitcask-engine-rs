use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use tracing::trace;
use crate::bitcask::{FileId};
use crate::error::BitCaskError;
use crate::log_entry::{Deserialize, DiskLogEntry};
use crate::memory_index::{MemIndex, MemIndexEntry};

pub(crate) struct DiskLogFile {
    pub(crate) file_id: FileId,
    pub(crate) path: PathBuf,
    pub(crate) file: std::fs::File,
}


impl DiskLogFile {
    pub(crate) const EXT: &'static str = "bitcask";

    // create a new file for writing
    pub(crate) fn new<T: Into<PathBuf>>(data_dir: T, file_id: FileId) -> Result<Self, BitCaskError> {
        let mut path: PathBuf = data_dir.into();
        path.push(file_id.to_string());
        path.set_extension(Self::EXT);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            file_id,
            path,
            file,
        })
    }

    // open an existing file for reading
    pub(crate) fn open(file_id: FileId, path: PathBuf, mem_index: &mut MemIndex) -> Result<Self, BitCaskError> {
        // Here all the files are opened in append mode, but we don't actually append anything except the last one
        trace!("opening disk log file: {:?}", path);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path)?;
        let file = Self {
            file_id,
            path,
            file,
        };
        file.populate_mem_index(mem_index)?;
        Ok(file)
    }

    fn populate_mem_index(&self, mem_index: &mut MemIndex) -> Result<(), BitCaskError> {
        let file_size = self.file.metadata()?.len();
        let mut buffered_reader = BufReader::new(&self.file);
        let mut cursor = 0 as u64;
        buffered_reader.seek(SeekFrom::Start(cursor))?;
        loop {
            if cursor >= file_size {
                break;
            }
            let entry = DiskLogEntry::deserialize(&mut buffered_reader)?;
            let entry_size = entry.total_byte_size();
            if entry.is_tombstone() {
                // if it is a tombstone, we don't need to store it in mem_index
                mem_index.delete(&entry.key);
            } else {
                let mem_log_entry = MemIndexEntry {
                    file_id: self.file_id,
                    value_offset: cursor,
                    value_size: entry.value_byte_size(),
                };
                mem_index.put(entry.key, mem_log_entry);
            }
            cursor += entry_size;
        }
        Ok(())
    }
}
