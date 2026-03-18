//

//! Storage module for actors and other components in the system. This module provides a unified 
//! interface for storing and retrieving data, abstracting away the underlying storage 
//! mechanism. It supports various storage backends, such as in-memory, file-based, and database 
//! storage, allowing for flexibility and scalability in different use cases.
//! 

mod error;
mod journal;
mod persist;
mod snapshotter;
mod stores;

pub use error::Error;
pub use persist::PersistentActor;
pub use journal::{Journal, JournalMessage, JournalResponse};
pub use stores::Store;
pub use snapshotter::{Snapshotter, SnapshotMessage, SnapshotResponse};

#[cfg(feature = "fjall")]
pub use stores::fjall::FjallDbManager;

#[cfg(feature = "memory")]
pub use stores::memory::MemoryDbManager;

#[cfg(all(feature = "fjall", feature = "memory"))]
compile_error!("feature \"fjall\" and feature \"memory\" cannot be enabled at the same time");