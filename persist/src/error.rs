//

use thiserror::Error;

/// Errors for `persist` package.
#[derive(Debug, Error)]
pub enum Error {
    /// Error when create store
    #[error("Creare store: {0}")]
    CreateStore(String),
    /// Error when the entity isn't found.
    #[error("Entry not found: {0}")]
    EntryNotFound(String),

}