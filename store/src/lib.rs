//

//! Storage module for actors and other components in the system. This module provides a unified 
//! interface for storing and retrieving data, abstracting away the underlying storage 
//! mechanism. It supports various storage backends, such as in-memory, file-based, and database 
//! storage, allowing for flexibility and scalability in different use cases.
//! 

mod error;
mod journal;
mod stores;

pub use error::Error;