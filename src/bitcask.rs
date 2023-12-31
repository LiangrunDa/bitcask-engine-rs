use crate::error::BitCaskError;
use crate::storage::{start_compaction, LogIndexStorage};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub(crate) type FileId = usize;
pub(crate) type ByteSize = u64;
pub(crate) type ByteOffset = u64;
pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

pub trait KVStorage: Clone + Send + 'static {
    fn get(&self, key: &Key) -> Option<Value>;
    fn put_with_option(&mut self, key: &Key, value: &Value, option: Option<PutOption>) -> Result<(), BitCaskError>;
    fn put(&mut self, key: &Key, value: &Value) -> Result<(), BitCaskError> {
        self.put_with_option(key, value, PutOption::none())
    }
    fn delete(&mut self, key: &Key) -> Result<(), BitCaskError>;
    fn size(&self) -> usize;
}

pub struct PutOption {
    pub nx: bool,
    pub xx: bool,
}

impl PutOption {

    pub fn none() -> Option<Self> {
        None
    }

    pub fn nx() -> Option<Self> {
        Some(Self {
            nx: true,
            xx: false,
        })
    }

    pub fn xx() -> Option<Self> {
        Some(Self {
            nx: false,
            xx: true,
        })
    }
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

    /// WARNING: this method is a blocking call, it will block the current thread until the compaction is finished.
    /// If you're using this method in an async context, you should spawn a blocking worker thread to call this method.
    pub fn compact_to_new_dir<T: Into<PathBuf>>(&self, data_dir: T) -> Result<(), BitCaskError> {
        let mut storage = self.storage.write().unwrap();
        let data_dir: PathBuf = data_dir.into();
        let immutable_files = storage.prepare_compaction()?;
        drop(storage);
        start_compaction(immutable_files.clone(), data_dir.clone())?;
        let mut storage = self.storage.write().unwrap();
        storage.finish_compaction(immutable_files, data_dir)
    }
}

impl KVStorage for BitCask {
    fn get(&self, key: &Key) -> Option<Value> {
        self.storage.read().unwrap().get(key)
    }

    fn put_with_option(&mut self, key: &Key, value: &Value, option: Option<PutOption>) -> Result<(), BitCaskError> {
        self.storage.write().unwrap().put(key, value, option)
    }

    fn delete(&mut self, key: &Key) -> Result<(), BitCaskError> {
        self.storage.write().unwrap().delete(key)
    }

    fn size(&self) -> usize {
        self.storage.read().unwrap().size()
    }
}
