//

use thiserror::Error;

/// Errors for `persist` package.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum Error {
    /// Error when create store
    #[error("Create store: {0}")]
    CreateStore(String),
    /// Store error
    #[error("Store error: {0}")]
    Store(String),
    #[error("Reading store error: {0}")]
    Reading(String),
    #[error("Writing store error: {0}")]
    Writing(String),
    #[error("Deleting store error: {0}")]
    Deleting(String),
    /// Error when the entity isn't found.
    #[error("Entry not found: {0}")]
    EntryNotFound(String),
    #[error("Serialization error: {0}")]
    Serialization(String),  
}