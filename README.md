# BitCask-rs

Rust implementation of [BitCask](https://riak.com/assets/bitcask-intro.pdf), a log-structured storage engine for key/value data.

Apart from the original paper, this implementation also supports the following features:

1. NX (not exist) for `put` operation
2. XX (exist) for `put` operation


## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
bitcask-rs = "0.1.0"
```

To use it in your project, you can initialize a `Bitcask` instance with a directory path:

```rust
use bitcask_rs::Bitcask;

fn main() {
    let mut bitcask = Bitcask::new("/tmp/bitcask").unwrap();
    bitcask.put(&vec![1, 2, 3], &vec![4, 5, 6]).unwrap();
    let res = bitcask.get(&vec![1, 2, 3]);
    assert_eq!(res, Some(vec![4, 5, 6]));
}
```

The `Bitcask` instance is thread-safe, so you can share it between threads.
```rust
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use bitcask_rs::bitcask::KVStorage;

#[tokio::main]
async fn main() {
    let bitcask = bitcask_rs::bitcask::BitCask::new("./data").unwrap();
    let listener = TcpListener::bind("127.0.0.1:8080").await.unwrap();
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bitcask_clone = bitcask.clone();
        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 {
                    return;
                }
                let key: Vec<u8> = buf[0..n].to_vec();
                let value: Vec<u8> = vec![1, 2];
                let res = bitcask_clone.put(&key, &value);
                println!("res: {:?}", res);
            }
        });
    }
}
```

## Related Projects

TODO

## License
The project is under the [MIT license](https://github.com/LiangrunDa/bitcask-rs/blob/main/LICENSE).