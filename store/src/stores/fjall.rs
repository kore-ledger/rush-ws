//

//! This module provides an implementation of the `Store` trait using the Fjall database.
//! It is only compiled if the `fjall` feature is enabled.
//! 
//! The `FjallDbManager` struct is responsible for creating instances of `FjallStore`, which is 
//! the actual store implementation that interacts with the Fjall database.
//! 
//! The `FjallStore` struct implements the `Store` trait and provides methods for storing and 
//! retrieving data from the Fjall database. It uses a prefix-based key-value storage approach, 
//! where keys are prefixed with a string to allow for efficient retrieval of related data.
//! 

use super::{DbManager, Store, IteratorOptions};
use crate::Error;

use fjall::{Database, KeyspaceCreateOptions, Guard};

/// A database manager that creates Fjall stores. It's intended for production use and provides a 
/// persistent storage solution.
#[derive(Clone)]
pub struct FjallDbManager {
    db: Database,
}

impl FjallDbManager {
    /// Creates a new `FjallDbManager` with the specified database path.
    pub fn new(path: &str) -> Result<Self, Error> {
        let db = Database::builder(path).open()
            .map_err(|e| Error::Store(format!("failed to open Fjall database -> {}", e)))?;
        Ok(Self { db })
    }

}

impl DbManager<FjallStore> for FjallDbManager {

    fn create_store(&self, name: &str, prefix: &str) -> Result<FjallStore, Error> {
        let keyspace = self.db.keyspace(name, || {
            KeyspaceCreateOptions::default()
        }) 
            .map_err(|e| Error::Store(format!("failed to create fjallkeyspace -> {}", e)))?;
        Ok(FjallStore { name: name.to_owned(), keyspace, prefix: prefix.to_owned() })
    }
}

/// A store implementation that uses the Fjall database for persistent storage. It provides methods
/// for storing and retrieving data, as well as iterating over stored entries.
///
pub struct FjallStore {
    name: String,
    prefix: String,
    keyspace: fjall::Keyspace,
}

impl FjallStore {

    pub fn into_key_value(guard: Guard) -> Result<(u64, Vec<u8>), Error> {
        let (key, value) = guard.into_inner()
            .map_err(|e| Error::Reading(format!("failed to read key/value from fjall -> {}", e)))?;
        let key_str = String::from_utf8_lossy(&key);
        let key_num = key_str.split(':').last()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| Error::Reading(format!("failed to parse key from fjall -> {}", key_str)))?;
        Ok((key_num, value.to_vec()))
    }

    pub fn last_key(&self) -> u64 {
        let iter = self.keyspace.prefix(&self.prefix);
        iter.last()
            .and_then(|guard| {
                Self::into_key_value(guard).ok().map(|(key, _)| key)
            })
            .unwrap_or(0)
    }
}

impl Store for FjallStore {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn get(&self, key: u64) -> Result<Vec<u8>, Error> {
        let key = format!("{}:{:014}", self.prefix, key);
        let value = self.keyspace.get(key.as_bytes())
            .map_err(|e| Error::Reading(format!("failed to get value from fjall -> {}", e)))?;
        let value = value.map(|v| v.to_vec());
        match value {
            Some(v) => Ok(v),
            None => Err(Error::Reading(format!("key not found in fjall -> {}", key))),
        }
    }

    fn put(&mut self, key: u64, data: &[u8]) -> Result<(), Error> {
        let key = format!("{}:{:014}", self.prefix, key);
        self.keyspace.insert(key, data)
            .map_err(|e| Error::Writing(format!("failed to put value into fjall -> {}", e)))?;
        Ok(())
    }

    fn del(&mut self, key: u64) -> Result<(), Error> {
        let key = format!("{}:{:014}", self.prefix, key);
        self.keyspace.remove(key)
            .map_err(|e| Error::Writing(format!("failed to delete value from fjall -> {}", e)))?;
        Ok(())
    }

    fn last(&self) -> Option<(u64, Vec<u8>)> {
        let iter = self.keyspace.prefix(&self.prefix);
        match iter.last() {
            Some(guard) => {
                match Self::into_key_value(guard) {
                    Ok((key, value)) => Some((key, value)),
                    Err(_) => None,
                }
            },
            _ => {
                None
            },
        }
    }

    fn purge(&mut self) -> Result<(), Error> {
        let iter = self.keyspace.prefix(&self.prefix);
        for guard in iter {
            let key = guard.key()
                .map_err(|err| Error::Reading(
                    format!("failed to read key from fjall during purge -> {}", err)
                ))?;           
            self.keyspace.remove(key)
                .map_err(|e| Error::Writing(
                    format!("failed to delete value from fjall -> {}", e)
                ))?;
        }
        Ok(())
    }

    fn iter<'a>(
        &'a self,
        options: IteratorOptions,
    ) -> Box<dyn Iterator<Item = (u64, Vec<u8>)> + 'a> {
        let vec = self.keyspace.prefix(&self.prefix).collect::<Vec<_>>();
        let size = vec.len() as u64;
        match options {
            IteratorOptions::Forward { from, count } => {
                // Implement forward iteration logic here
                let from = from.unwrap_or(0);
                let count = count.unwrap_or(size);
                Box::new(
                    vec.into_iter().filter_map(move |guard| {
                        let (key, value) = Self::into_key_value(guard).ok()?;
                        if key <= from {
                            return None;
                        }
                        Some((key, value))
                    })
                    .take(count as usize)
                )
            },
            IteratorOptions::Reverse { from, count } => {
                // Implement reverse iteration logic here
                let from = from.unwrap_or(self.last_key());
                let count = count.unwrap_or(size);
                Box::new(
                    vec.into_iter().rev().filter_map(move |guard| {
                        let (key, value) = Self::into_key_value(guard).ok()?;
                        if key >= from {
                            return None;
                        }
                        Some((key, value))
                    })
                    .take(count as usize)
                )
            },
            IteratorOptions::Range { from, to } => {
                // Implement range iteration logic here
                let to = to.unwrap_or(self.last_key());
                Box::new(
                    vec.into_iter().filter_map(move |guard| {
                        let (key, value) = Self::into_key_value(guard).ok()?;
                        if key >= from && key <= to {
                            Some((key, value))
                        } else {
                            None
                        }
                    })
                )
            },
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::test_store_trait;
    use tempfile::TempDir;

    #[test]
    fn test_create_fjall_db_manager() {
        let temp_dir = TempDir::new().unwrap();
        let binding = temp_dir.path().join("fjall_db");
        let path = binding.to_str().unwrap();
        let manager = FjallDbManager::new(path);
        assert!(manager.is_ok());
    }

    impl Default for FjallDbManager {
        fn default() -> Self {
            let temp_dir = TempDir::new().unwrap();
            let binding = temp_dir.path().join("fjall_db");
            let path = binding.to_str().unwrap();
            Self::new(path).unwrap()
        }
    } 
    test_store_trait!{
        unit_test_fjall_store:crate::stores::fjall::FjallDbManager:crate::stores::fjall::FjallStore
    }
}