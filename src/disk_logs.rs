use std::ffi::OsStr;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use tracing::trace;
use crate::bitcask::{FileId, Key, Value};
use crate::error::BitCaskError;
use crate::log_entry::{DiskLogEntry, Serialize};
use crate::log_file::DiskLogFile;
use crate::memory_index::{MemIndex, MemIndexEntry};


pub(crate) struct DiskLog {
    files: Vec<DiskLogFile>,
}

impl DiskLog {

    /// create a new log file with file id 0.
    fn new<T: Into<PathBuf>>(data_dir: T) -> Result<Self, BitCaskError> {
        Ok(Self {
            files: vec![DiskLogFile::new(data_dir, 0)?],
        })
    }

    /// If the data directory is empty, create a new log file with file id 0.
    /// Otherwise, load all the log files from disk and populate the memory index.
    pub(crate) fn from_disk<T: Into<PathBuf>>(data_dir: T, mem_index: &mut MemIndex) -> Result<Self, BitCaskError> {
        let data_dir: PathBuf = data_dir.into();
        let mut files = std::fs::read_dir(&data_dir)?
            .filter_map(|path| {
                path.ok()
                    .map(|path| path.path())
                    .filter(|path| {
                        path.is_file() && path.extension() == Some(OsStr::new(DiskLogFile::EXT))
                    })
                    .and_then(|path| {
                        path.file_stem()
                            .and_then(|file_stem| file_stem.to_str())
                            .and_then(|file_stem| file_stem.parse::<FileId>().ok())
                            .map(|file_id| (file_id, path))
                    })
            })
            .map(|(file_id, path)| {
                DiskLogFile::open(file_id, path, mem_index)
                    .map(|disk_log_file| (file_id, disk_log_file))
            })
            .collect::<Result<Vec<(FileId, DiskLogFile)>, BitCaskError>>()?;

        files.sort_by_key(|(file_id, _)| *file_id);

        if (files.len()) == 0 {
            trace!("No disk log files found, starting from scratch");
            return Self::new(data_dir);
        }

        let files = files
            .into_iter()
            .map(|(_, disk_log_file)| disk_log_file)
            .collect();

        Ok(Self { files })
    }

    fn current_file(&mut self) -> (&mut DiskLogFile, FileId) {
        // the last file is always open for appending
        let file_id = self.files.len() - 1;
        (self.files.last_mut().unwrap(), file_id)
    }

    fn get_file(&self, file_id: FileId) -> &DiskLogFile {
        self.files.get(file_id as usize).unwrap()
    }

    pub(crate) fn get(&self, mem_index_entry: &MemIndexEntry) -> Result<Value, BitCaskError> {
        let MemIndexEntry {
            value_offset,
            value_size,
            file_id,
        } = mem_index_entry;
        let disk_log_file = self.get_file(*file_id);

        let mut buffered_reader =
            BufReader::with_capacity(*value_size as usize, &disk_log_file.file);
        buffered_reader.seek(SeekFrom::Start(*value_offset))?;
        let mut buf = vec![0u8; *value_size as usize];
        buffered_reader.read_exact(buf.as_mut())?;
        Ok(Value::from(buf))
    }

    pub(crate) fn put(&mut self, key: &Key, value: &Value) -> Result<MemIndexEntry, BitCaskError> {
        self.append(DiskLogEntry::new_entry(key.clone(), value.clone()))
    }

    pub(crate) fn delete(&mut self, key: &Key) -> Result<MemIndexEntry, BitCaskError> {
        self.append(DiskLogEntry::new_tombstone(key.clone()))
    }

    fn append(&mut self, entry: DiskLogEntry) -> Result<MemIndexEntry, BitCaskError> {
        let (disk_log_file, file_id) = self.current_file();
        let file = &mut disk_log_file.file;
        let value_offset = file.seek(SeekFrom::End(0))? + entry.value_byte_offset();
        entry.serialize(file)?;
        file.flush()?; // ensure persistency
        Ok(MemIndexEntry {
            file_id,
            value_offset,
            value_size: entry.value_byte_size(),
        })
    }
}