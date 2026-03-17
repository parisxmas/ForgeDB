use thiserror::Error;

#[derive(Debug, Error)]
pub enum ForgeError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Buffer pool error: {0}")]
    BufferPool(String),

    #[error("Page error: {0}")]
    Page(String),

    #[error("Tuple error: {0}")]
    Tuple(String),

    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Index error: {0}")]
    Index(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Plan error: {0}")]
    Plan(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("WAL error: {0}")]
    Wal(String),
}

pub type Result<T> = std::result::Result<T, ForgeError>;
