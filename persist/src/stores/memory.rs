//
use super::{DbManager, Store, IteratorOptions};

use crate::Error;

use std::collections::{HashMap, BTreeMap};
use std::sync::{Arc, RwLock};

type MemoryData = Arc<
    RwLock<HashMap<(String, String), Arc<RwLock<BTreeMap<String, Vec<u8>>>>>>,
>;

/// A database manager that creates in-memory stores. It's only intended for testing and 
/// should not be used in production.
#[derive(Clone, Default)]
pub struct MemoryDbManager {
    data: MemoryData,
}

impl DbManager<MemoryStore> for MemoryDbManager {
    fn create_store(&self, name: &str, prefix: &str) -> Result<MemoryStore, crate::Error> {
        let mut data = self.data.write().unwrap();
        let key = (name.to_string(), prefix.to_string());
        let store_data = data.entry(key).or_insert_with(|| Arc::new(RwLock::new(BTreeMap::new())));
        Ok(MemoryStore {
            name: name.to_string(),
            prefix: prefix.to_string(),
            data: store_data.clone(),
        })
    }
}

/// A store implementation that stores data in memory.
///
#[derive(Default, Clone)]
pub struct MemoryStore {
    name: String,
    prefix: String,
    data: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,
}

impl Store for MemoryStore {
    fn name(&self) -> &str {
        self.name.as_str()
    }
    
    fn get(&self, key: u64) -> Result<Vec<u8>, crate::Error> {
        let key = format!("{}:{:014}", self.prefix, key);
        let lock = self
            .data
            .read()
            .map_err(|e| Error::Store(format!("Can not lock data: {}", e)))?;

        match lock.get(&key) {
            Some(value) => Ok(value.clone()),
            None => {
                Err(Error::EntryNotFound("Query returned no rows".to_owned()))
            }
        }       
    }
    
    fn put(&mut self, key: u64, data: &[u8]) -> Result<(), crate::Error> {
        let key = format!("{}:{:014}", self.prefix, key);
        let mut lock = self
            .data
            .write()
            .map_err(|e| Error::Store(format!("Can not lock data: {}", e)))?;
        lock.insert(key, data.to_vec());
        Ok(())
    }
    
    fn del(&mut self, key: u64) -> Result<(), crate::Error> {
        let key = format!("{}:{:014}", self.prefix, key);
        let mut lock = self
            .data
            .write()
            .map_err(|e| Error::Store(format!("Can not lock data: {}", e)))?;
        lock.remove(&key);
        Ok(())
    }
    
    fn last(&self) -> Option<(u64, Vec<u8>)> {
        let lock = self.data.read().ok()?;
        lock.iter().rev().next().map(|(k, v)| {
            let key = k.split(':').last().unwrap().parse::<u64>().unwrap();
            (key, v.clone())
        })
    }
    
    fn purge(&mut self) -> Result<(), crate::Error> {
        let mut lock = self
            .data
            .write()
            .map_err(|e| Error::Store(format!("Can not lock data: {}", e)))?;
        lock.clear();
        Ok(())
    }
    
    fn iter<'a>(
        &'a self,
        options: IteratorOptions,
    ) -> Box<dyn Iterator<Item = (u64, Vec<u8>)> + 'a> {
        let lock = self.data.read().unwrap();
        let size = lock.len() as u64;
        let iter: Box<dyn Iterator<Item = (u64, Vec<u8>)>> = match options {
            IteratorOptions::Forward { from, count } => {
                let mut from = from.unwrap_or(0);
                let count = count.unwrap_or(size);
                let key = format!("{}:{:014}", self.prefix, from); 
                Box::new(
                    lock.iter()
                        .filter_map(move |(k, v)| {
                            let rk = k.split(':').last().unwrap().parse::<u64>().unwrap();
                            if k >= &key {
                                Some((rk, v.clone()))
                            } else {
                                from += 1;
                                None
                            }
                        })
                        .take(count as usize),
                )
            },
            IteratorOptions::Reverse { from, count } => {
                let mut from = from.unwrap_or(0);
                let count = count.unwrap_or(size);
                let key = format!("{}:{:014}", self.prefix, from); 
                Box::new(
                    lock.iter()
                        .rev()
                        .filter_map(move |(k, v)| {
                            let rk = k.split(':').last().unwrap().parse::<u64>().unwrap();
                            if k <= &key {
                                Some((rk, v.clone()))
                            } else {
                                None
                            }
                        })
                        .take(count as usize),
                )
            },
            IteratorOptions::Range { from, to } => Box::new(
                lock.iter()
                    .filter_map(move |(k, v)| {
                        let key = k.split(':').last().unwrap().parse::<u64>().unwrap();
                        if key >= from && to.map_or(true, |t| key < t) {
                            Some((key, v.clone()))
                        } else {
                            None
                        }
                    }),
            ),
        };
        iter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_store_trait;

    test_store_trait!{
        unit_test_memory_store:crate::stores::memory::MemoryDbManager:crate::stores::memory::MemoryStore      
    }
}