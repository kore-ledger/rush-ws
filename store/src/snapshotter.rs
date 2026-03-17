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

    pub fn load_snapshot(&self, key: u64) -> Option<Vec<u8>> {
        match self.store.get(key) {
            Ok(data) => Some(data),
            Err(e) => {
                error!("Failed to load snapshot for key {}: {}", key, e);
                None
            }
        }
    }

    pub fn last_snapshot(&self) -> Option<(u64, Vec<u8>)> {
        self.store.last()
    }

}

pub enum SnapshotMessage {
    SaveSnapshot { key: u64, data: Vec<u8> },
    LoadSnapshot { key: u64 },
    LastSnapshot,
}

impl Message for SnapshotMessage {}

pub enum SnapshotResponse {
    LoadResult(Option<Vec<u8>>),
    LastResult(Option<(u64, Vec<u8>)>),
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
        }
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

        assert!(manager.drop().is_ok());       
    }
}
    