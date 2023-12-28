use std::path::PathBuf;
use tracing::error;
use crate::bitcask::{Key, Value};
use crate::error::BitCaskError;
use crate::disk_logs::DiskLog;
use crate::memory_index::MemIndex;

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

    pub(crate) fn get(&self, key: &Key) -> Option<Value> {
        let mem_index_entry = self.mem_index.get(key);
        match mem_index_entry {
            Some(mem_index_entry) => {
                let res = self
                    .disk_log
                    .get(&mem_index_entry);
                return if let Err(e) = res {
                    error!("Error reading from disk log: {}", e);
                    None
                } else {
                    Some(res.unwrap())
                }
            },
            // if it is a tombstone, or the key does not exist, the mem_index will return None
            None => None,
        }
    }

    pub(crate) fn put(&mut self, key: &Key, value: &Value) -> Result<(), BitCaskError> {
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