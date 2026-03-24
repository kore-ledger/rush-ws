//

use crate::stores::Store;
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

/// A snapshotter that stores snapshots for an event-sourced actor. The snapshotter is responsible 
/// for managing the persistence of snapshots, allowing actors to recover their state efficiently.
/// The snapshotter provides methods for saving and loading snapshots, as well as retrieving the 
/// last snapshot stored.
/// 
pub struct BaseSnapshotter<S: Store> {
    /// The underlying store used for persisting snapshots.
    store: S,    
}

impl<S: Store> BaseSnapshotter<S> {
    /// Create a new snapshotter with the given store.
    /// 
    /// # Arguments
    /// 
    /// * `store` - The underlying store used for persisting snapshots.
    /// 
    /// # Returns
    /// 
    /// * `Self` - A new instance of the snapshotter.
    ///
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// Saves a snapshot in the snapshotter by storing it in the underlying store. This method is
    /// used to save the state of an actor at a specific point in time, allowing for efficient 
    /// recovery later.
    /// 
    /// # Arguments
    /// 
    /// * `key` - A unique identifier for the snapshot, typically representing the sequence number or version of the snapshot.
    /// * `data` - The serialized data of the snapshot to be stored.
    /// 
    /// # Returns
    /// 
    /// * `Result<(), ActorError>` - Ok if the snapshot was saved successfully, or an error if there was a problem during saving.
    ///
    fn save_snapshot(&mut self, key: u64, data: Vec<u8>) -> Result<(), ActorError> {
        self.store.put(key, &data)
            .map_err(|e| ActorError::Store(format!("Failed to save snapshot: {}", e)))
    }

    /// Loads a snapshot from the snapshotter by retrieving it from the underlying store. This method is
    /// used to load the state of an actor at a specific point in time, allowing for efficient 
    /// recovery.
    /// 
    /// # Arguments
    /// 
    /// * `key` - A unique identifier for the snapshot, typically representing the sequence number or version of the snapshot.
    /// 
    /// # Returns
    /// 
    /// * `Option<Vec<u8>>` - Some with the snapshot data if found, or None if the snapshot was not found or there was an 
    ///   error during loading.
    ///
    fn load_snapshot(&self, key: u64) -> Option<Vec<u8>> {
        match self.store.get(key) {
            Ok(data) => Some(data),
            Err(e) => {
                error!("Failed to load snapshot for key {}: {}", key, e);
                None
            }
        }
    }

    /// Retrieves the last snapshot stored in the snapshotter. This method is used to get the most
    /// recent snapshot, allowing for efficient recovery of the actor's state.
    /// 
    /// # Returns
    /// 
    /// * `Option<(u64, Vec<u8>)>` - Some with the key and snapshot data if found, or None if no 
    ///   snapshot was found or there was an error during retrieval.
    ///
    fn last_snapshot(&self) -> Option<(u64, Vec<u8>)> {
        self.store.last()
    }

    /// Flushes the snapshotter's store to ensure that all pending writes are persisted. This 
    /// method is typically called during shutdown to ensure that all snapshots are saved properly.
    /// 
    /// # Returns
    /// 
    /// * `Result<(), ActorError>` - Ok if the flush was successful, or an error if there was a 
    ///   problem during flushing.
    ///
    fn flush(&mut self) -> Result<(), ActorError> {
        self.store.flush()
            .map_err(|e| {
                error!("Failed to flush store: {}", e);
                ActorError::Store(format!("Failed to flush store: {}", e))
            })
    }

}

/// A message type for interacting with the snapshotter actor. This enum defines the different
/// types of messages that can be sent to the snapshotter, including saving a snapshot, loading
/// a snapshot, and retrieving the last snapshot.
///
#[derive(Clone)]
pub enum SnapshotMessage {
    /// A message to save a snapshot, containing the key and data of the snapshot to be saved.
    SaveSnapshot { key: u64, data: Vec<u8> },
    /// A message to load a snapshot, containing the key of the snapshot to be loaded.
    LoadSnapshot { key: u64 },
    /// A message to retrieve the last snapshot stored in the snapshotter.
    LastSnapshot,
    /// A message to flush the snapshotter's store, ensuring all pending writes are persisted.
    Flush,
}

impl Message for SnapshotMessage {}

/// A response type for the snapshotter actor. This enum defines the different types of responses 
/// that can be sent by the snapshotter in reply to messages.
/// 
pub enum SnapshotResponse {
    /// A response containing the result of a load snapshot request.
    LoadResult(Option<Vec<u8>>),
    /// A response containing the result of a last snapshot request.
    LastResult(Option<(u64, Vec<u8>)>),
    /// A response indicating that no specific result is available.
    None,
}

impl Response for SnapshotResponse {}

#[async_trait]
impl<S: Store> Actor for BaseSnapshotter<S> {

    type Message = SnapshotMessage;
    type Response = SnapshotResponse;
    type Event = DummyEvent;

    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        _sender: &ActorPath,
        msg: Self::Message,
    ) -> Result<Self::Response, ActorError> {
        match msg {
            SnapshotMessage::SaveSnapshot { key, data } => {
                self.save_snapshot(key, data)?;
                Ok(SnapshotResponse::None)
            },
            SnapshotMessage::LoadSnapshot { key } => {
                let result = self.load_snapshot(key);
                Ok(SnapshotResponse::LoadResult(result))
                
            },
            SnapshotMessage::LastSnapshot => {
                let result = self.last_snapshot();
                Ok(SnapshotResponse::LastResult(result))
            },
            SnapshotMessage::Flush => {
                self.flush()?;
                Ok(SnapshotResponse::None)
            },
        }
    }

    async fn post_stop(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        self.store.flush()
            .map_err(|e| {
                error!("Failed to flush store on snapshotter shutdown: {}", e);
                ActorError::Store(format!("Failed to flush store on snapshotter shutdown: {}", e))
            })
    }
    
}

#[cfg(test)]
mod tests {
   use super::*;
    use crate::stores::{StoreManager, DbManager};
    use actor::{Config, System};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    #[serial_test::serial]
    async fn test_snapshotter() {
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token);
        let manager = StoreManager::default();
        let store = manager.create_store("test_snapshotter", "snapshot").unwrap();
        let snapshotter = Snapshotter::new(store);
        let snapshotter_ref = system.create_actor(snapshotter, "snapshotter").await.unwrap();

        // Test saving a snapshot
        let snapshot = "snapshot data".as_bytes().to_vec();
        snapshotter_ref.tell(SnapshotMessage::SaveSnapshot { key: 1, data: snapshot }).await.unwrap();

        // Test loading the snapshot
        let response = snapshotter_ref.ask(SnapshotMessage::LoadSnapshot { key: 1 }).await.unwrap();
        if let SnapshotResponse::LoadResult(Some(data)) = response {
            assert_eq!(data, "snapshot data".as_bytes().to_vec());
        } else {
            panic!("Failed to load snapshot");
        } 
        let snapshot = "last snapshot data".as_bytes().to_vec();
        snapshotter_ref.tell(SnapshotMessage::SaveSnapshot { key: 2, data: snapshot }).await.unwrap();

        // Test loading the last snapshot
        let response = snapshotter_ref.ask(SnapshotMessage::LastSnapshot).await.unwrap();
        if let SnapshotResponse::LastResult(Some((key, data))) = response {
            assert_eq!(key, 2);
            assert_eq!(data, "last snapshot data".as_bytes().to_vec());
        } else {
            panic!("Failed to load last snapshot");
        }

        // Test flushing the snapshotter
        let response = snapshotter_ref.tell(SnapshotMessage::Flush).await;
        assert!(response.is_ok());

        assert!(manager.drop().is_ok());       
    }
}
    