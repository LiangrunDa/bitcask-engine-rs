use crate::bitcask::{Key, PutOption, Value};
use crate::disk_logs::DiskLog;
use crate::error::BitCaskError;
use crate::log_entry::DiskLogEntry;
use crate::log_file::DiskLogFile;
use crate::memory_index::MemIndex;
use std::path::PathBuf;
use tracing::error;

pub struct LogIndexStorage {
    data_dir: PathBuf,
    disk_log: DiskLog,
    mem_index: MemIndex,
}

impl LogIndexStorage {
    pub fn new<T: Into<PathBuf>>(data_dir: T) -> Result<Self, BitCaskError> {
        let data_dir: PathBuf = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;
        let mut mem_index = MemIndex::new();
        // Populate mem_index from disk
        let disk_log = DiskLog::from_disk(&data_dir, &mut mem_index)?;
        Ok(Self {
            data_dir,
            disk_log,
            mem_index,
        })
    }

    pub(crate) fn prepare_compaction(&mut self) -> Result<Vec<PathBuf>, BitCaskError> {
        // step 0: create a new empty log file
        self.disk_log.create_new_file()?;
        // step 1: return the immutable files and the mem_index
        let immutable_files = self.disk_log.get_immutable_files();
        Ok(immutable_files)
    }

    pub(crate) fn finish_compaction(
        &mut self,
        immutable_files: Vec<PathBuf>,
        new_log_file_path: PathBuf,
    ) -> Result<(), BitCaskError> {
        // step 3: copy the files to the new directory except the immutable files
        self.disk_log
            .copy_files_to_new_dir(immutable_files, new_log_file_path.clone())?;
        // step 4: initialize a new DiskLog and MemIndex from the new log file
        let mut mem_index = MemIndex::new();
        let disk_log = DiskLog::from_disk(&new_log_file_path, &mut mem_index)?;
        self.disk_log = disk_log;
        self.mem_index = mem_index;
        self.data_dir = new_log_file_path.into();
        Ok(())
    }

    pub(crate) fn get(&self, key: &Key) -> Option<Value> {
        let mem_index_entry = self.mem_index.get(key);
        match mem_index_entry {
            Some(mem_index_entry) => {
                if mem_index_entry.is_tombstone() {
                    return None;
                }
                let res = self.disk_log.get(&mem_index_entry);
                match res {
                    Ok(value) => Some(value),
                    Err(e) => {
                        error!("Error while getting value from disk log: {:?}", e);
                        None
                    }
                }
            }
            None => None,
        }
    }

    pub(crate) fn put(&mut self, key: &Key, value: &Value, option: Option<PutOption>) -> Result<(), BitCaskError> {
        match option {
            Some(option) => {
                if option.nx {
                    return self.put_nx(key, value);
                }
                if option.xx {
                    return self.put_xx(key, value);
                }
                self.put_without_option(key, value)
            }
            None => self.put_without_option(key, value),
        }
    }

    pub(crate) fn put_without_option(&mut self, key: &Key, value: &Value) -> Result<(), BitCaskError> {
        let index_entry = self.disk_log.put(key, value)?;
        self.mem_index.put(key.clone(), index_entry);
        Ok(())
    }

    pub(crate) fn put_nx(&mut self, key: &Key, value: &Value) -> Result<(), BitCaskError> {
        let index_entry = self.mem_index.get(key);
        if let Some(index_entry) = index_entry {
            if !index_entry.is_tombstone() {
                return Err(BitCaskError::KeyExists);
            }
        }
        let index_entry = self.disk_log.put(key, value)?;
        self.mem_index.put(key.clone(), index_entry);
        Ok(())
    }

    pub(crate) fn put_xx(&mut self, key: &Key, value: &Value) -> Result<(), BitCaskError> {
        let index_entry = self.mem_index.get(key);
        if let Some(index_entry) = index_entry {
            if index_entry.is_tombstone() {
                return Err(BitCaskError::KeyNotFound);
            }
        } else {
            return Err(BitCaskError::KeyNotFound);
        }
        let index_entry = self.disk_log.put(key, value)?;
        self.mem_index.put(key.clone(), index_entry);
        Ok(())
    }

    pub(crate) fn delete(&mut self, key: &Key) -> Result<(), BitCaskError> {
        let index_entry = self.disk_log.delete(key)?;
        self.mem_index.put(key.clone(), index_entry);
        Ok(())
    }

    pub(crate) fn size(&self) -> usize {
        self.mem_index.size()
    }
}

pub(crate) fn start_compaction(
    immutable_files: Vec<PathBuf>,
    new_log_file_path: PathBuf,
) -> Result<(), BitCaskError> {
    // step 2: iterate through the mem_index, and write the entries to the new log file
    std::fs::create_dir_all(&new_log_file_path)?;
    let mut new_log_file = DiskLogFile::new(&new_log_file_path, 0)?;
    let mut mem_index = MemIndex::new();
    let disk_logs = DiskLog::immutable_initialization(immutable_files, &mut mem_index)?;
    let iter = mem_index.into_iter();
    for (key, mem_index_entry) in iter {
        let value = disk_logs.get(&mem_index_entry)?;
        let disk_log_entry = DiskLogEntry::new_entry(key, value);
        new_log_file.append_new_entry(disk_log_entry)?;
    }
    Ok(())
}
