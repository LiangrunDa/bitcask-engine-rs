use thiserror::Error;

#[derive(Debug, Error)]
pub enum BitCaskError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Data is corrupted: {0}")]
    CorruptedData(String),
    #[error(transparent)]
    UnexpectedError(#[from] anyhow::Error),
    #[error("Key already exists")]
    KeyExists,
    #[error("Key does not exist")]
    KeyNotFound,
}
