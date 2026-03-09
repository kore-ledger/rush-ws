//
//

#[cfg(feature = "memory")]
pub mod memory;
#[cfg(feature = "rocksdb")]
pub mod rocksdb;
#[cfg(feature = "sqlite")]
pub mod sqlite;


use crate::Error;

/// A trait representing a store that creates collections and state storage.
/// Implementations of this trait provide the factory methods for creating
/// persistent storage backends used by actors for event sourcing and state snapshots.
///
/// # Type Parameters
///
/// * `S` - The store type that stores key-value pairs (events).
///
pub trait DbManager<S: Store + 'static>: Sync + Send + Clone
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
    fn create_store(&self, name: &str, prefix: &str) -> Result<S, Error>;

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
    fn put(&mut self, key: u64, data: &[u8]) -> Result<(), Error>;

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
    fn del(&mut self, key: u64) -> Result<(), Error>;

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
    fn last(&self) -> Option<(u64, Vec<u8>)>;

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
    ) -> Box<dyn Iterator<Item = (u64, Vec<u8>)> + 'a>;

    /// Flush store.
    ///
    fn flush(&self) -> Result<(), Error> {
        Ok(())
    }
}

/// Options for iterating over a store's key-value pairs.
pub enum IteratorOptions {
    /// Iterate in reverse order.
    Reverse {from: Option<u64>, count: Option<u64>},
    /// Iterate in forward order.
    Forward{from: Option<u64>, count: Option<u64>},
    /// Iterate over a range of keys.
    Range { from: u64, to: Option<u64> },
}


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
            fn test_create_store() {
                let manager = <$type>::default();
                let store: $type2 =
                    manager.create_store("test", "test").unwrap();
                assert_eq!(Store::name(&store), "test");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_put_get_store() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_store("test", "test").unwrap();
                Store::put(&mut store, 1, b"value").unwrap();
                assert_eq!(Store::get(&store, 1).unwrap(), b"value");
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_del_store() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_store("test", "test").unwrap();
                Store::put(&mut store, 1, b"value").unwrap();
                Store::del(&mut store, 1).unwrap();
                assert_eq!(
                    Store::get(&store, 1),
                    Err(Error::EntryNotFound(
                        "Key not found: test:00000000000001".to_owned()
                    ))
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_iter_reverse() {
                let manager = <$type>::default();
                let mut store1: $type2 =
                    manager.create_store("test", "test1").unwrap();
                Store::put(&mut store1, 1, b"value1").unwrap();
                Store::put(&mut store1, 2, b"value2").unwrap();
                Store::put(&mut store1, 3, b"value3").unwrap();
                let mut store2: $type2 =
                    manager.create_store("test", "test2").unwrap();
                Store::put(&mut store2, 4, b"value4").unwrap();
                Store::put(&mut store2, 5, b"value5").unwrap();
                Store::put(&mut store2, 6, b"value6").unwrap();
                let mut iter = store1.iter(IteratorOptions::Reverse { from: None, count: None });
                let mut iter2 = store2.iter(IteratorOptions::Reverse { from: Some(5), count: Some(1) });
                assert_eq!(
                    iter.next(),
                    Some((3, b"value3".to_vec())) 
                );
                assert_eq!(
                    iter.next(),
                    Some((2, b"value2".to_vec()))
                );
                assert_eq!(
                    iter.next(),
                    Some((1, b"value1".to_vec()))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(
                    iter2.next(),
                    Some((5, b"value5".to_vec()))
                );
                assert_eq!(iter2.next(), None);
                let mut iter = store1.iter(IteratorOptions::Reverse { from: Some(2), count: None });
                let mut iter2 = store2.iter(IteratorOptions::Reverse { from: None, count: Some(1) });
                assert_eq!(
                    iter.next(),
                    Some((2, b"value2".to_vec())) 
                );
                assert_eq!(
                    iter.next(),
                    Some((1, b"value1".to_vec()))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(
                    iter2.next(),
                    Some((6, b"value6".to_vec()))
                );
                assert_eq!(iter2.next(), None);
                assert!(manager.stop().is_ok())
            }
 
            #[test]
            fn test_iter_forward() {
                let manager = <$type>::default();
                let mut store1: $type2 =
                    manager.create_store("test", "test1").unwrap();
                Store::put(&mut store1, 1, b"value1").unwrap();
                Store::put(&mut store1, 2, b"value2").unwrap();
                Store::put(&mut store1, 3, b"value3").unwrap();
                let mut store2: $type2 =
                    manager.create_store("test", "test2").unwrap();
                Store::put(&mut store2, 4, b"value4").unwrap();
                Store::put(&mut store2, 5, b"value5").unwrap();
                Store::put(&mut store2, 6, b"value6").unwrap();
                let mut iter = store1.iter(IteratorOptions::Forward { from: None, count: None });
                let mut iter2 = store2.iter(IteratorOptions::Forward { from: Some(5), count: Some(1) });
                assert_eq!(
                    iter.next(),
                    Some((1, b"value1".to_vec())) 
                );
                assert_eq!(
                    iter.next(),
                    Some((2, b"value2".to_vec()))
                );
                assert_eq!(
                    iter.next(),
                    Some((3, b"value3".to_vec()))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(
                    iter2.next(),
                    Some((5, b"value5".to_vec()))
                );
                assert_eq!(iter2.next(), None);
                let mut iter = store1.iter(IteratorOptions::Forward { from: None, count: Some(2) });
                let mut iter2 = store2.iter(IteratorOptions::Forward { from: Some(5), count: None});
                assert_eq!(
                    iter.next(),
                    Some((1, b"value1".to_vec())) 
                );
                assert_eq!(
                    iter.next(),
                    Some((2, b"value2".to_vec()))
                );
                assert_eq!(iter.next(), None);
                assert_eq!(
                    iter2.next(),
                    Some((5, b"value5".to_vec()))
                );
                assert_eq!(iter2.next(),
                    Some((6, b"value6".to_vec()))
                );
                assert_eq!(iter2.next(), None);
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_iter_range() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_store("test", "test").unwrap();
                Store::put(&mut store, 1, b"value1").unwrap();
                Store::put(&mut store, 2, b"value2").unwrap();
                Store::put(&mut store, 3, b"value3").unwrap();
                let mut iter = store.iter(IteratorOptions::Range { from: 2, to: Some(4) });
                assert_eq!(
                    iter.next(),
                    Some((2, b"value2".to_vec())) 
                );
                assert_eq!(
                    iter.next(),
                    Some((3, b"value3".to_vec()))
                );
                assert_eq!(iter.next(), None);
                let mut iter = store.iter(IteratorOptions::Range { from: 3, to: None });
                assert_eq!(
                    iter.next(),
                    Some((3, b"value3".to_vec()))
                );
                assert_eq!(iter.next(), None);
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_last() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_store("test", "test1").unwrap();
                Store::put(&mut store, 1, b"value1").unwrap();
                Store::put(&mut store, 2, b"value2").unwrap();
                Store::put(&mut store, 3, b"value3").unwrap();
                let mut store2: $type2 =
                    manager.create_store("test", "test2").unwrap();
                Store::put(&mut store2, 4, b"value4").unwrap();
                let last = store.last();
                assert_eq!(
                    last,
                    Some((3, b"value3".to_vec()))
                );
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_purge() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_store("test", "test").unwrap();
                Store::put(&mut store, 1, b"value1").unwrap();
                Store::put(&mut store, 2, b"value2").unwrap();
                Store::put(&mut store, 3, b"value3").unwrap();
                assert_eq!(
                    Store::get(&store, 1),
                    Ok(b"value1".to_vec())
                );
                assert_eq!(
                    Store::get(&store, 2),
                    Ok(b"value2".to_vec())
                );
                assert_eq!(
                    Store::get(&store, 3),
                    Ok(b"value3".to_vec())
                );
                Store::purge(&mut store).unwrap();
                let mut iter = store.iter(IteratorOptions::Forward { from: None, count: None });
                assert_eq!(iter.next(), None);
                
                assert!(manager.stop().is_ok())
            }

            #[test]
            fn test_flush() {
                let manager = <$type>::default();
                let mut store: $type2 =
                    manager.create_store("test", "test").unwrap();
                Store::put(&mut store, 1, b"value1").unwrap();
                Store::put(&mut store, 2, b"value2").unwrap();
                Store::put(&mut store, 3, b"value3").unwrap();
                assert!(store.flush().is_ok());
                assert!(manager.stop().is_ok())
            }
        }
    };
}
