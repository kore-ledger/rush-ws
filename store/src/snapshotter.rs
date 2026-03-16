//

use crate::stores::{IteratorOptions, Store};
#[cfg(feature = "fjall")]
use crate::stores::fjall::FjallStore;
#[cfg(feature = "memory")]
use crate::stores::memory::MemoryStore;

use actor::{Actor, ActorContext, ActorPath, DummyEvent, Error as ActorError, Message, Response};
use async_trait::async_trait;
use tracing::error;

#[cfg(feature = "memory")]
pub type Snapshotter = BaseSnapshotter<MemoryStore>;

#[cfg(feature = "fjall")]
pub type Snapshotter = BaseSnapshotter<FjallStore>;

pub struct BaseSnapshotter<S: Store> {
    store: S,    
}

impl<S: Store> BaseSnapshotter<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub fn save_snapshot(&mut self, key: u64, data: Vec<u8>) -> Result<(), ActorError> {
        self.store.put(key, &data)
            .map_err(|e| ActorError::Store(format!("Failed to save snapshot: {}", e)))
    }

    pub fn load_snapshot(&self, key: u64) -> Result<Vec<u8>, ActorError> {
        self.store.get(key)
            .map_err(|e| ActorError::Store(format!("Failed to load snapshot: {}", e)))
    }

    pub fn last_snapshot(&self) -> Option<(u64, Vec<u8>)> {
        self.store.last()
    }

}
