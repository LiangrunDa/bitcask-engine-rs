use crate::bitcask::{ByteOffset, ByteSize, FileId, Key};
use std::collections::btree_map::{BTreeMap, IntoIter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemIndexEntry {
    pub(crate) file_id: FileId,
    pub(crate) value_offset: ByteOffset,
    pub(crate) value_size: ByteSize,
}

#[derive(Debug, Clone)]
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

pub(crate) struct MemIndexIterator {
    inner: IntoIter<Key, MemIndexEntry>,
}

impl IntoIterator for MemIndex {
    type Item = (Key, MemIndexEntry);
    type IntoIter = MemIndexIterator;

    fn into_iter(self) -> Self::IntoIter {
        let inner = self.map.into_iter();
        MemIndexIterator { inner }
    }
}

impl Iterator for MemIndexIterator {
    type Item = (Key, MemIndexEntry);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
