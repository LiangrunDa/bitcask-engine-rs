use rand::Rng;
use bitcask_rs::bitcask::{BitCask, KVStorage, PutOption};

#[test]
fn it_works() {
    let mut bitcask = generate_random_bitcask_instance();
    bitcask.put(&vec![1, 2, 3], &vec![4, 5, 6]).unwrap();
    let res = bitcask.get(&vec![1, 2, 3]);
    assert_eq!(res, Some(vec![4, 5, 6]));
}

#[test]
fn compaction() {
    let mut bitcask = generate_random_bitcask_instance();
    bitcask.put(&vec![1, 2, 3], &vec![4, 5, 6]).unwrap();
    bitcask.put(&vec![1, 2], &vec![3, 4]).unwrap();
    bitcask.put(&vec![1, 2, 3], &vec![5, 6, 7]).unwrap();
    let new_dir = format!("./data/{}", generate_random_name());
    bitcask.compact_to_new_dir(new_dir.clone()).unwrap();
    // the old bitcask handle automatically switches to the new directory
    assert_eq!(bitcask.get(&vec![1, 2, 3]), Some(vec![5, 6, 7]));
    assert_eq!(bitcask.get(&vec![1, 2]), Some(vec![3, 4]));
    // the new bitcask handle is also able to read the data
    let bitcask_new = BitCask::new(new_dir).unwrap();
    assert_eq!(bitcask_new.get(&vec![1, 2, 3]), Some(vec![5, 6, 7]));
    assert_eq!(bitcask_new.get(&vec![1, 2]), Some(vec![3, 4]));
}

#[test]
fn test_put_nx() {
    let mut bitcask = generate_random_bitcask_instance();
    bitcask.put_with_option(&vec![1, 2, 3], &vec![4, 5, 6], PutOption::nx()).unwrap();
    let res = bitcask.get(&vec![1, 2, 3]);
    assert_eq!(res, Some(vec![4, 5, 6]));
    bitcask.put_with_option(&vec![1, 2, 3], &vec![4, 5, 6], PutOption::nx()).unwrap_err();
}

#[test]
fn test_put_xx() {
    let mut bitcask = generate_random_bitcask_instance();
    bitcask.put_with_option(&vec![1, 2, 3], &vec![4, 5, 6], PutOption::xx()).unwrap_err();
    bitcask.put_with_option(&vec![1, 2, 3], &vec![4, 5, 6], PutOption::nx()).unwrap();
    bitcask.put_with_option(&vec![1, 2, 3], &vec![4, 5, 6], PutOption::xx()).unwrap();
}

fn generate_random_bitcask_instance() -> BitCask {
    let file_name = generate_random_name();
    let data_dir = format!("./data/{}", file_name);
    BitCask::new(data_dir).unwrap()
}

fn generate_random_name() -> String {
    let mut rng = rand::thread_rng();
    let rand_string: String = rng
        .sample_iter(rand::distributions::Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();
    rand_string
}