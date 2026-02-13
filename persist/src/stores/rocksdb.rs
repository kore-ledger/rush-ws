//

use super::{Collection, State, DbManager};
use crate::Error;
use rocksdb::{Options, DB, ColumnFamilyDescriptor};

use tracing::{debug, error};
use std::{
    fs,
    path::Path,
    sync::Arc,
};

// RocksDB store for persistent actor storage.
/// Manages RocksDB instances and provides factory methods for creating
/// column families for event storage and state snapshots.
///
/// # Storage Model
///
/// - **Collections**: RocksDB column families for event storage
/// - **State**: RocksDB column families for state snapshots
/// - **Connection**: Thread-safe shared DB instance using Arc<DB>
/// - **Column Families**: Separate namespaces for different actors
///
#[derive(Clone)]
pub struct RocksDbManager {
    /// RocksDB configuration options.
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
            })?;

        Ok(Self {
            opts: options,
            db: Arc::new(db),
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

impl RocksDbStore {

    /// Create new `RocksDbStore.
    /// 
    pub fn new(name: &str, prefix: &str, store: Arc<DB>) -> Self {
        Self {name: name.to_owned(), prefix: prefix.to_owned(), store}
    }
}

