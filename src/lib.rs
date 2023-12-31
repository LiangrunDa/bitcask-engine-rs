pub mod bitcask;
mod disk_logs;
mod error;
mod log_entry;
mod log_file;
mod memory_index;
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

    #[test]
    fn compaction() {
        let mut bitcask = bitcask::BitCask::new("./data/bitcask").unwrap();
        bitcask.put(&vec![1, 2, 3], &vec![4, 5, 6]).unwrap();
        bitcask.put(&vec![1, 2], &vec![3, 4]).unwrap();
        bitcask.put(&vec![1, 2, 3], &vec![5, 6, 7]).unwrap();
        bitcask.compact_to_new_dir("./data/bitcask_new").unwrap();
        // the old bitcask handle automatically switches to the new directory
        assert_eq!(bitcask.get(&vec![1, 2, 3]), Some(vec![5, 6, 7]));
        assert_eq!(bitcask.get(&vec![1, 2]), Some(vec![3, 4]));
        // the new bitcask handle is also able to read the data
        let bitcask_new = bitcask::BitCask::new("./data/bitcask_new").unwrap();
        assert_eq!(bitcask_new.get(&vec![1, 2, 3]), Some(vec![5, 6, 7]));
        assert_eq!(bitcask_new.get(&vec![1, 2]), Some(vec![3, 4]));
    }
    
    #[test]
    fn test_put_nx() {
        let mut bitcask = bitcask::BitCask::new("./data/bitcask").unwrap();
        bitcask.put_nx(&vec![1, 2, 3], &vec![4, 5, 6]).unwrap();
        let res = bitcask.get(&vec![1, 2, 3]);
        assert_eq!(res, Some(vec![4, 5, 6]));
        bitcask.put_nx(&vec![1, 2, 3], &vec![4, 5, 6]).unwrap_err();
    }
}
