//

#[cfg(feature = "rocksdb")]
mod rocksdb;

//#[cfg(feature = "rocksdb")]



use crate::Error;

use tracing::debug;

/// A trait representing a store that creates collections and state storage.
/// Implementations of this trait provide the factory methods for creating
/// persistent storage backends used by actors for event sourcing and state snapshots.
///
/// # Type Parameters
///
/// * `S` - The store type that stores key-value pairs (events).
///
pub trait DbManager<S: Store>: Sync + Send + Clone
{
    /// Creates a new store for storing key-value pairs (typically events).
    /// Stores are used for event sourcing where multiple events
    /// are stored with unique keys (usually sequence numbers).
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the store (e.g., table name, column family).
    /// * `prefix` - A prefix for filtering/namespacing values within the store.
    ///
    /// # Returns
    ///
    /// Returns a store instance if successful.
    ///
    /// # Errors
    ///
    /// Returns an error if the store could not be created.
    ///
    fn create_store(&mut self, name: &str, prefix: &str) -> Result<S, Error>;

    /// Stops the store and performs cleanup.
    /// Default implementation does nothing. Override this to implement
    /// connection closing, flushing, or other cleanup operations.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if cleanup was successful.
    ///
    /// # Errors
    ///
    /// Returns an error if cleanup failed.
    ///
    fn stop(self) -> Result<(), Error> {
        Ok(())
    }
}

/// A trait representing a collection of key-value pairs in a database.
///
pub trait Store: Sync + Send + 'static {
    /// Retrieve the name of the store.
    ///
    /// # Returns
    ///     
    /// The name of the store.
    ///
    fn name(&self) -> &str;

    /// Retrieves the value associated with the given key.
    ///
    /// # Arguments
    ///
    /// - key: The key to retrieve the value for.
    ///
    /// # Returns
    ///
    /// The value associated with the given key.
    ///
    /// # Errors
    ///
    /// - If the operation failed.
    ///
    fn get(&self, key: u64) -> Result<Vec<u8>, Error>;

    /// Associates the given value with the given key.
    ///
    /// # Arguments
    ///
    /// - key: The key to associate the value with.
    /// - data: The value to associate with the key.
    ///
    /// # Returns
    ///
    /// An error if the operation failed.
    ///
    /// # Errors
    ///
    /// - If the operation failed.
    ///
    fn put(&mut self, key: &str, data: &[u8]) -> Result<(), Error>;

    /// Removes the value associated with the given key.
    ///
    /// # Arguments
    ///
    /// - key: The key to remove the value for.
    ///
    /// # Returns
    ///
    /// An error if the operation failed.
    ///
    fn del(&mut self, key: &str) -> Result<(), Error>;

    /// Returns the last value in the store.
    ///
    /// # Returns
    ///
    /// The last key / value in the store.
    ///
    /// # Errors
    ///
    /// - If the operation failed.
    ///
    fn last(&self) -> Option<(String, Vec<u8>)>;

    /// Removes all values from the store.
    ///
    /// # Returns
    ///
    /// An error if the operation failed.
    ///
    fn purge(&mut self) -> Result<(), Error>;

    /// Returns an iterator over the key-value pairs in the store.
    ///
    /// # Arguments
    ///
    /// - options: The iterator options specifying the iteration behavior.
    ///
    /// # Returns
    ///
    /// An iterator over the key-value pairs in the store.
    ///
    fn iter<'a>(
        &'a self,
        options: IteratorOptions,
    ) -> Box<dyn Iterator<Item = (String, Vec<u8>)> + 'a>;

    /// Flush store.
    ///
    fn flush(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Options for iterating over a store's key-value pairs.
pub enum IteratorOptions {
    /// Iterate in reverse order.
    Reverse {from: Option<u64>, count: Option<i64>},
    /// Iterate in forward order.
    Forward{from: Option<u64>, count: Option<i64>},
    /// Iterate over a range of keys.
    IdRange { from: u64, to: u64 },
    /// Iterate over a range of timestamps.
    TimeStampRange { from: u64, to: u64 },
}

/* 
/// Macro for test stores
/// 
#[macro_export]
macro_rules! test_store_trait {
    ($name:ident: $type:ty: $type2:ty) => {
        #[cfg(test)]
        mod $name {
            use super::*;
            use $crate::error::Error;

            #[test]
            fn test_create_collection() {
                let manager = <$type>::default();
                let store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                assert_eq!(Collection::name(&store), "test");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_create_state() {
                let manager = <$type>::default();
                let store: $type2 =
                    manager.create_state("test", "test").unwrap();
                assert_eq!(State::name(&store), "test");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_put_get_collection() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key", b"value").unwrap();
                assert_eq!(Collection::get(&store, "key").unwrap(), b"value");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_put_get_state() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_state("test", "test").unwrap();
                State::put(&mut store, b"value").unwrap();
                assert_eq!(State::get(&store).unwrap(), b"value");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_del_collection() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key", b"value").unwrap();
                Collection::del(&mut store, "key").unwrap();
                assert_eq!(
                    Collection::get(&store, "key"),
                    Err(Error::EntryNotFound(
                        "Query returned no rows".to_owned()
                    ))
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_del_state() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_state("test", "test").unwrap();
                State::put(&mut store, b"value").unwrap();
                State::del(&mut store).unwrap();
                assert_eq!(
                    State::get(&store),
                    Err(Error::EntryNotFound(
                        "Query returned no rows".to_owned()
                    ))
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_iter() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                let mut iter = store.iter(false);
                assert_eq!(
                    iter.next(),
                    Some(("key1".to_string(), b"value1".to_vec()))
                );
                assert_eq!(
                    iter.next(),
                    Some(("key2".to_string(), b"value2".to_vec()))
                );
                assert_eq!(
                    iter.next(),
                    Some(("key3".to_string(), b"value3".to_vec()))
                );
                assert_eq!(iter.next(), None);
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_iter_reverse() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                let mut iter = store.iter(true);
                assert_eq!(
                    iter.next(),
                    Some(("key3".to_string(), b"value3".to_vec()))
                );
                assert_eq!(
                    iter.next(),
                    Some(("key2".to_string(), b"value2".to_vec()))
                );
                assert_eq!(
                    iter.next(),
                    Some(("key1".to_string(), b"value1".to_vec()))
                );
                assert_eq!(iter.next(), None);
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_last() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                let last = store.last();
                assert_eq!(
                    last,
                    Some(("key3".to_string(), b"value3".to_vec()))
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_get_by_range() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                let result = store.get_by_range(None, 2).unwrap();
                assert_eq!(
                    result,
                    vec![b"value1".to_vec(), b"value2".to_vec()]
                );
                let result =
                    store.get_by_range(Some("key3".to_string()), -2).unwrap();
                assert_eq!(
                    result,
                    vec![b"value2".to_vec(), b"value1".to_vec()]
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_purge_collection() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_collection("test", "test").unwrap();
                Collection::put(&mut store, "key1", b"value1").unwrap();
                Collection::put(&mut store, "key2", b"value2").unwrap();
                Collection::put(&mut store, "key3", b"value3").unwrap();
                assert_eq!(
                    Collection::get(&store, "key1"),
                    Ok(b"value1".to_vec())
                );
                assert_eq!(
                    Collection::get(&store, "key2"),
                    Ok(b"value2".to_vec())
                );
                assert_eq!(
                    Collection::get(&store, "key3"),
                    Ok(b"value3".to_vec())
                );
                Collection::purge(&mut store).unwrap();
                assert_eq!(
                    Collection::get(&store, "key1"),
                    Err(Error::EntryNotFound(
                        "Query returned no rows".to_owned()
                    ))
                );
                assert_eq!(
                    Collection::get(&store, "key2"),
                    Err(Error::EntryNotFound(
                        "Query returned no rows".to_owned()
                    ))
                );
                assert_eq!(
                    Collection::get(&store, "key3"),
                    Err(Error::EntryNotFound(
                        "Query returned no rows".to_owned()
                    ))
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_purge_state() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_state("test", "test").unwrap();
                State::put(&mut store, b"value1").unwrap();
                assert_eq!(State::get(&store), Ok(b"value1".to_vec()));
                State::purge(&mut store).unwrap();
                assert_eq!(
                    State::get(&store),
                    Err(Error::EntryNotFound(
                        "Query returned no rows".to_owned()
                    ))
                );

                State::put(&mut store, b"value2").unwrap();
                assert_eq!(State::get(&store), Ok(b"value2".to_vec()));
                State::purge(&mut store).unwrap();
                assert_eq!(
                    State::get(&store),
                    Err(Error::EntryNotFound(
                        "Query returned no rows".to_owned()
                    ))
                );
                assert!(manager.stop().is_ok())
            }
        }
    };
}
*/