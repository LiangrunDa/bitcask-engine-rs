pub mod bitcask;
mod log_entry;
mod error;
mod memory_index;
mod log_file;
mod disk_logs;
mod storage;


#[cfg(test)]
mod tests {
    use crate::bitcask;
    use crate::bitcask::KVStorage;

    #[test]
    fn it_works() {
        let mut bitcask = bitcask::BitCask::new("./data/bitcask").unwrap();
        bitcask.put(&vec![1, 2, 3], &vec![4, 5, 6]).unwrap();
        let res = bitcask.get(&vec![1, 2, 3]);
        assert_eq!(res, Some(vec![4, 5, 6]));
    }
}
