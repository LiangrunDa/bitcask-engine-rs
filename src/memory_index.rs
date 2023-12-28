use std::collections::{BTreeMap, HashMap};
use crate::bitcask::{FileId, ByteSize, ByteOffset, Key};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemIndexEntry {
    pub(crate) file_id: FileId,
    pub(crate) value_offset: ByteOffset,
    pub(crate) value_size: ByteSize,
}

pub(crate) struct MemIndex {
    map: BTreeMap<Key, MemIndexEntry>,
}

impl MemIndex {
    pub(crate) fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }
    pub(crate) fn get(&self, key: &Key) -> Option<&MemIndexEntry> {
        self.map.get(key)
    }
    pub(crate) fn put(&mut self, key: Key, entry: MemIndexEntry) -> Option<MemIndexEntry> {
        self.map.insert(key, entry)
    }
    pub(crate) fn delete(&mut self, key: &Key) -> Option<MemIndexEntry> {
        self.map.remove(key)
    }
    pub(crate) fn size(&self) -> usize {
        self.map.len()
    }
}