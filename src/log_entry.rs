use std::io::{Read, Write};
use crc::{Crc, CRC_32_CKSUM};
use crate::error::BitCaskError;
use crate::bitcask::{ByteSize, ByteOffset, Key, Value};

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_CKSUM);


/// Any object that is readable can be deserialized
pub(crate) trait Deserialize
{
    fn deserialize<T: Read>(buf: &mut T) -> Result<Self, BitCaskError> where Self: Sized;
}

/// Any object that is writable can be serialized to
pub(crate) trait Serialize {
    fn serialize<T: Write>(&self, buf: &mut T) -> Result<(), BitCaskError>;
}

/// DiskLogEntry is a memory representation of a key-value pair that is persisted in disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiskLogEntry {
    pub(crate) check_sum: u32,
    pub(crate) key: Key,
    pub(crate) value: Option<Value>, // None indicates a tombstone
}

impl DiskLogEntry {
    pub(crate) fn new_entry(key: Key, value: Value) -> Self {
        let check_sum = CRC32.checksum(&value);
        Self {
            check_sum,
            key,
            value: Some(value),
        }
    }
    pub(crate) fn new_tombstone(key: Key) -> Self {
        let check_sum = 0;
        Self { check_sum, key, value: None }
    }
    pub(crate) fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    fn is_valid(&self) -> bool {
        if let Some(value) = &self.value {
            self.check_sum == CRC32.checksum(value)
        } else {
            true
        }
    }

    const fn check_sum_byte_size() -> ByteSize { 4 }

    fn key_byte_size(&self) -> ByteSize { self.key.len() as u64 }
    pub(crate) fn value_byte_size(&self) -> ByteSize { self.value.as_ref().map(|v| v.len() as u64).unwrap_or(0) }
    const fn size_byte_len() -> ByteSize { ByteSize::BITS as u64 / 8 }
    pub(crate) fn value_byte_offset(&self) -> ByteOffset { Self::check_sum_byte_size() + Self::size_byte_len() * 2 + self.key_byte_size() }
    pub(crate) fn total_byte_size(&self) -> ByteSize { Self::check_sum_byte_size() + Self::size_byte_len() * 2 + self.key_byte_size() + self.value_byte_size() }
}

/// Disk layout
///  - Checksum (4 bytes long)
///  - Size of key in bytes (8 bytes long)
///  - Size of value in bytes (8 bytes long)
///  - Key
///  - Value (if tombstone, then value is None, and value size is 0)
impl Serialize for DiskLogEntry {
    fn serialize<T: Write>(&self, buf: &mut T) -> Result<(), BitCaskError> {
        let DiskLogEntry { check_sum, key, value } = self;
        // checksum
        buf.write_all(&check_sum.to_be_bytes())?;
        // key size and value size
        let key_size = self.key_byte_size();
        let value_size = self.value_byte_size();
        buf.write_all(&key_size.to_be_bytes())?;
        buf.write_all(&value_size.to_be_bytes())?;
        // key and value
        buf.write_all(key.as_ref())?;
        if let Some(value) = value {
            buf.write_all(value.as_ref())?;
        }
        Ok(())
    }
}


impl Deserialize for DiskLogEntry {
    fn deserialize<T: Read>(buf: &mut T) -> Result<Self, BitCaskError> {
        // 4 bytes long for holding checksum
        let mut check_sum_buf = [0u8; Self::check_sum_byte_size() as usize];
        buf.read_exact(&mut check_sum_buf)?;
        let check_sum = u32::from_be_bytes(check_sum_buf);
        // 8 bytes long for holding size
        let mut size_buf = [0u8; Self::size_byte_len() as usize];
        buf.read_exact(&mut size_buf)?;
        let key_size = ByteSize::from_be_bytes(size_buf);
        buf.read_exact(&mut size_buf)?;
        let value_size = ByteSize::from_be_bytes(size_buf);
        // read key
        let mut key_buf = vec![0 as u8; key_size as usize];
        buf.read_exact(&mut key_buf)?;
        let key = key_buf;
        // ready value, if tombstone, then value is None
        let value = if value_size > 0 {
            let mut value_buf = vec![0 as u8; value_size as usize];
            buf.read_exact(&mut value_buf)?;
            Some(value_buf)
        } else {
            None
        };
        // construct DiskLogEntry
        let entry = Self { check_sum, key, value };
        // validate checksum
        if entry.is_valid() {
            Ok(entry)
        } else {
            Err(BitCaskError::CorruptedData("invalid checksum".to_string()))
        }
    }
}
