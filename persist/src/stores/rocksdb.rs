//

use super::{Store, DbManager, IteratorOptions};
use crate::Error;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options, ReadOptions, IteratorMode, SliceTransform};

use tracing::debug;
use std::{
    fs,
    path::Path,
    sync::Arc,
};

/// Type alias for RocksDB database instance with multi-threaded access.
type DB = DBWithThreadMode<MultiThreaded>;

// RocksDB store for persistent actor storage.
/// Manages RocksDB instances and provides factory methods for creating
/// column families for event storage and state snapshots.
///
#[derive(Clone)]
pub struct RocksDbManager {
    /// RocksDB options used for creating column families.
    opts: Options,
    /// Thread-safe shared RocksDB instance.
    db: Arc<DB>,
}

impl RocksDbManager {

    /// Creates a new RocksDB store.
    /// Opens or creates a RocksDB database at the specified path,
    /// loading all existing column families.
    ///
    /// # Arguments
    ///
    /// * `path` - Directory path where the RocksDB database will be created.
    ///
    /// # Returns
    ///
    /// Returns a new RocksDbStore instance.
    ///
    /// # Errors
    ///
    /// Returns Error::CreateStore if:
    /// - The directory cannot be created
    /// - The RocksDB database cannot be opened
    ///
    /// # Behavior
    ///
    /// - Creates the directory if it doesn't exist
    /// - Lists and opens all existing column families
    /// - Enables "create_if_missing" option
    ///
    pub fn new(path: &str) -> Result<Self, Error> {
        debug!("Creating RockDB database manager");
        if !Path::new(&path).exists() {
            debug!("Path does not exist, creating it");
            fs::create_dir_all(path).map_err(|e| {
                Error::CreateStore(format!(
                    "fail RockDB create directory: {}",
                    e
                ))
            })?;
        }

        let mut options = Options::default();

        // Set configuration options to compaction.
        options.set_max_bytes_for_level_multiplier(4.0); // Reduce size multiplier
        options.set_max_bytes_for_level_base(32 * 1024 * 1024); // 32 MB base size
       
        
        options.set_recycle_log_file_num(5);
        //options.set_compression_type(rocksdb::DBCompressionType::Snappy);

        options.create_if_missing(true);

        // Set prefix extractor for column families to optimize prefix-based queries
        options.set_prefix_extractor(SliceTransform::create_fixed_prefix(45));

        let cfs = match DB::list_cf(&options, path) {
            Ok(cf_names) => cf_names,
            Err(_) => vec!["default".to_string()], // Si la base de datos no existe, usamos solo `default`
        };

        // Crear descriptores para cada column family
        let cf_descriptors: Vec<_> = cfs
            .iter()
            .map(|cf| ColumnFamilyDescriptor::new(cf, Options::default()))
            .collect();

        // Abrir la base de datos con las column families existentes
        let db = DB::open_cf_descriptors(&options, path, cf_descriptors)
            .map_err(|e| {
                Error::CreateStore(format!("Can not open RockDB: {}", e))
            })?
;
        Ok(Self {
            opts: options,
            db: Arc::new(db),
        })
    }
}

impl DbManager<RocksDbStore> for RocksDbManager {

    fn create_store(&self, name: &str, prefix: &str) -> Result<RocksDbStore, Error> {
        if self.db.cf_handle(name).is_none() {
            self.db
                .create_cf(name, &self.opts)
                .map_err(|e| Error::CreateStore(format!("{:?}", e)))?;
        }
        Ok(RocksDbStore {
            name: name.to_owned(),
            prefix: prefix.to_owned(),
            store: self.db.clone(),
        })
    }
}

/// RocksDB store that implements both Collection and State traits.
/// Stores key-value pairs in a RocksDB column family with prefix-based keys.
///
/// # Storage Layout
///
/// - **Column Family**: Separate namespace identified by `name`
/// - **Keys**: Prefixed with actor identifier for isolation
/// - **Values**: Raw bytes (serialized data)
///
/// # Thread Safety
///
/// Uses Arc<DB> for safe concurrent access across multiple stores.
///
pub struct RocksDbStore {
    /// Column family name.
    name: String,
    /// Prefix for keys (actor namespace).
    prefix: String,
    /// Shared RocksDB instance.
    store: Arc<DB>,
}

impl Store for RocksDbStore {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn get(&self, key: u64) -> Result<Vec<u8>, Error> {
        let full_key = format!("{}:{:014}", self.prefix, key);
        if let Some(cf) = self.store.cf_handle(&self.name) {
            match self.store.get_cf(&cf, full_key.as_bytes()) {
                Ok(Some(value)) => Ok(value),
                Ok(None) => Err(Error::EntryNotFound(format!(
                    "Key not found: {}",
                    full_key
                ))),
                Err(e) => Err(Error::Reading(format!(
                    "Failed to get key {}: {:?}",
                    full_key, e
                ))),
            }
        } else {
            Err(Error::Store(format!(
                "Column family not found: {}",
                self.name
            )))
        }
    }

    fn put(&mut self, key: u64, data: &[u8]) -> Result<(), Error> {
        let full_key = format!("{}:{:014}", self.prefix, key);
        if let Some(cf) = self.store.cf_handle(&self.name) {
            match self.store.put_cf(&cf, full_key.as_bytes(), data) {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::Writing(format!(
                    "Failed to put key {}: {:?}",
                    full_key, e
                ))),
            }
        } else {
            Err(Error::Store(format!(
                "Column family not found: {}",
                self.name
            )))
        }
    }

    fn del(&mut self, key: u64) -> Result<(), Error> {
        let full_key = format!("{}:{:014}", self.prefix, key);
        if let Some(cf) = self.store.cf_handle(&self.name) {
            match self.store.delete_cf(&cf, full_key.as_bytes()) {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::Deleting(format!(
                    "Failed to delete key {}: {:?}",
                    full_key, e
                ))),
            }
        } else {
            Err(Error::Store(format!(
                "Column family not found: {}",
                self.name
            )))
        }
    }

    fn purge(&mut self) -> Result<(), Error> {
        let from = format!("{}:", self.prefix);
        let to = format!("{}:~", self.prefix); // '~' is higher than any valid character in keys
        if let Some(cf) = self.store.cf_handle(&self.name) {
            match self.store.delete_range_cf(&cf, from.as_bytes(), to.as_bytes()) {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::Deleting(format!(
                    "Failed to purge keys with prefix {}: {:?}",
                    self.prefix, e
                ))),
            }
        } else {
            Err(Error::Store(format!(
                "Column family not found: {}",
                self.name
            )))
        }
    }

    fn last(&self) -> Option<(u64, Vec<u8>)> {
        if let Some(cf) = self.store.cf_handle(&self.name) {
            let iter = self.store.iterator_cf(&cf, rocksdb::IteratorMode::End);
            for (key, value) in iter.flatten() {
                let key_str = String::from_utf8_lossy(&key);
                if key_str.starts_with(&self.prefix) 
                    && let Some(id_str) = key_str.split(':').nth(1) 
                        && let Ok(id) = id_str.parse::<u64>() {
                            return Some((id, value.to_vec()));
                        }                
            }
        }
        None
    }

    fn iter<'a>(
        &'a self,
        options: IteratorOptions,
    ) -> Box<dyn Iterator<Item = (u64, Vec<u8>)> + 'a> {
        if let Some(cf) = self.store.cf_handle(&self.name) {
            let mut opt = ReadOptions::default();
            opt.set_prefix_same_as_start(true);
            let mut max_elements = 0u64;
            let mut counter = 0u64;
            let mode = match options {
                IteratorOptions::Forward { from, count } => {
                    let start_key = if let Some(from) = from {
                        format!("{}:{:014}", self.prefix, from)
                    } else {
                        format!("{}:", self.prefix)
                    };
                    max_elements = count.unwrap_or(0);
                    opt.set_iterate_lower_bound(start_key.as_bytes());
                    IteratorMode::Start
                }
                IteratorOptions::Reverse { from, count } => {
                    let end_key = if let Some(from) = from {
                        format!("{}:{:014}", self.prefix, from +1)
                    } else {
                        format!("{}:~", self.prefix) // '~' is higher than any valid character in keys
                    };
                    max_elements = count.unwrap_or(0);
                    opt.set_iterate_upper_bound(end_key.as_bytes());
                    IteratorMode::End
                }
                IteratorOptions::Range { from, to } => {
                    let start_key = format!("{}:{:014}", self.prefix, from);
                    let end_key = if let Some(to) = to {
                        format!("{}:{:014}", self.prefix, to +1)
                    } else {
                        format!("{}:~", self.prefix) // '~' is higher than any valid character in keys
                    };
                    opt.set_iterate_lower_bound(start_key.as_bytes());
                    opt.set_iterate_upper_bound(end_key.as_bytes());
                    IteratorMode::Start
                }
            };
            let iter = self.store.iterator_cf_opt(&cf, opt, mode);

            Box::new(iter.filter_map(move |item| {
                if let Ok((key, value)) = item {
                    if max_elements > 0  {
                        counter += 1;
                        if counter > max_elements {
                            return None; // Stop iteration after reaching the count limit
                        }
                    }
                    let key_str = String::from_utf8_lossy(&key);
                    if key_str.starts_with(&self.prefix) 
                        && let Some(id_str) = key_str.split(':').nth(1)
                            && let Ok(id) = id_str.parse::<u64>() {
                                return Some((id, value.to_vec()));
                            }
                }
                None
            }))
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn flush(&self) -> Result<(), Error> {
        if let Some(cf) = self.store.cf_handle(&self.name) {
            match self.store.flush_cf(&cf) {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::Store(format!(
                    "Failed to flush column family {}: {:?}",
                    self.name, e
                ))),
            }
        } else {
            Err(Error::Store(format!(
                "Column family not found: {}",
                self.name
            )))
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_store_trait;

    impl Default for RocksDbManager {
        fn default() -> Self {
            let dir = tempfile::tempdir()
                .expect("Can not create temporal directory.");
            Self::new(dir.path().to_str().unwrap()).expect("Failed to create RocksDbManager")
        }
    }

    #[test]
    fn test_new_store() {
        let dir = tempfile::tempdir()
            .expect("Can not create temporal directory.");
        let manager = RocksDbManager::new(dir.path().to_str().unwrap());
        assert!(manager.is_ok(), "Failed to create RocksDbManager: {:?}", manager.err());
    }

    // Use the test macro to generate tests for RocksDbStore
    test_store_trait! {
        unit_test_rocksdb_manager:crate::stores::rocksdb::RocksDbManager:crate::stores::rocksdb::RocksDbStore
    }

}