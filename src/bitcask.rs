use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use crate::error::BitCaskError;
use crate::storage::LogIndexStorage;

pub(crate) type FileId = usize;
pub(crate) type ByteSize = u64;
pub(crate) type ByteOffset = u64;
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

pub trait KVStorage : Clone + Send + 'static {
    fn get(&self, key: &Key) -> Option<Value>;
    fn put(&mut self, key: &Key, value: &Value) -> Result<(), BitCaskError>;
    fn delete(&mut self, key: &Key) -> Result<(), BitCaskError>;
    fn size(&self) -> usize;
}

#[derive(Clone)]
pub struct BitCask {
    pub(crate) storage: Arc<RwLock<LogIndexStorage>>,
}

impl BitCask {
    pub fn new<T: Into<PathBuf>>(data_dir: T) -> Result<Self, BitCaskError> {
        let storage = LogIndexStorage::new(data_dir)?;
        Ok(Self {
            storage: Arc::new(RwLock::new(storage)),
        })
    }

    pub fn compact_from_disk<T: Into<PathBuf>>(old_data_dir: T, new_data_dir: T) -> Result<Self, BitCaskError> {
        todo!()
    }

}

impl KVStorage for BitCask {
    fn get(&self, key: &Key) -> Option<Value> {
        self.storage.read().unwrap().get(key)
    }

    fn put(&mut self, key: &Key, value: &Value) -> Result<(), BitCaskError> {
        self.storage.write().unwrap().put(key, value)
    }

    fn delete(&mut self, key: &Key) -> Result<(), BitCaskError> {
        self.storage.write().unwrap().delete(key)
    }

    fn size(&self) -> usize {
        self.storage.read().unwrap().size()
    }
}

// TODO: implement auto new file creation