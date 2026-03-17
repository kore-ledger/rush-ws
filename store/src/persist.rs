//

use crate::{
    Journal, JournalMessage, JournalResponse, 
    Snapshotter, SnapshotMessage, SnapshotResponse, 
    stores::{DbManager, StoreManager}
};

use actor::{Actor, ActorContext, ActorRef, ActorPath, Error as ActorError, Response};
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use serde_bare::{to_vec, from_slice};
use async_trait::async_trait;


#[async_trait]
pub trait PersistentActor: Actor + Serialize + DeserializeOwned {

    async fn apply_event(&mut self, event: &Self::Event) -> Result<(), ActorError>;

    async fn init_state (
        &mut self, 
        name: &str, 
        prefix: &str, 
        ctx: &mut ActorContext<Self>
    ) -> Result<(), ActorError> {
        self.create_stores(name, prefix, ctx).await?;
        let mut from = 0 as u64;
        let state = self.snapshotter(ctx).await
            .ok_or_else(|| ActorError::Store("Snapshotter not found".to_string()))?
            .ask(SnapshotMessage::LastSnapshot).await
            .map_err(|e| ActorError::Store(format!("Failed to get last snapshot: {}", e)))?;
        if let SnapshotResponse::LastResult(Some((snapshot_from, snapshot_data))) = state {
            from = snapshot_from;
            let deserialized_state: Self = from_slice(&snapshot_data)
                .map_err(|e| ActorError::Serialization(format!("Failed to deserialize snapshot: {}", e)))?;
            *self = deserialized_state;
        }
        let events = self.journal(ctx).await
            .ok_or_else(|| ActorError::Store("Journal not found".to_string()))?
            .ask(JournalMessage::Range(from, None)).await
            .map_err(|e| ActorError::Store(format!("Failed to get events from journal: {}", e)))?;
        if let JournalResponse::Events(events) = events {
            for (_, event_data) in events {
                let event: Self::Event = from_slice(&event_data)
                    .map_err(|e| ActorError::Serialization(format!("Failed to deserialize event: {}", e)))?;
                self.apply_event(&event).await?;
            }
        }
        Ok(())
    }

    async fn create_stores(
        &mut self,
        name: &str,
        prefix: &str,
        ctx: &mut ActorContext<Self>
    ) -> Result<(), ActorError> {
        let store_manager = ctx.get_helper::<StoreManager>("db_manager").await
            .ok_or_else(|| ActorError::Store("DB Manager not found".to_string()))?;
        let journal_store = store_manager.create_store(name, prefix)
            .map_err(|e| ActorError::Store(format!("Failed to create journal store: {}", e)))?;
        let snapshotter_store = store_manager.create_store(name, prefix)
            .map_err(|e| ActorError::Store(format!("Failed to create snapshotter store: {}", e)))?;
        ctx.create_child(Journal::new(journal_store), "journal").await
            .map_err(|e| ActorError::Store(format!("Failed to create journal actor: {}", e)))?;
        ctx.create_child(Snapshotter::new(snapshotter_store), "snapshotter").await
            .map_err(|e| ActorError::Store(format!("Failed to create snapshotter actor: {}", e)))?;
        Ok(())
    }

    async fn journal(&self, ctx: &mut ActorContext<Self>) -> Option<ActorRef<Journal>> {
        ctx.get_child("journal").await
    }

    async fn snapshotter(&self, ctx: &mut ActorContext<Self>) -> Option<ActorRef<Snapshotter>> {
        ctx.get_child("snapshotter").await
    }
    
    async fn persist(&mut self, event: &Self::Event, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        let serialized_event = to_vec(event)
            .map_err(|e| ActorError::Serialization(format!("Failed to serialize event: {}", e)))?;
        if let Some(journal) = self.journal(ctx).await {
            journal.tell(JournalMessage::Put(serialized_event)).await
                .map_err(|e| ActorError::Store(format!("Failed to send event to journal: {}", e)))?;
            Ok(())
        } else {
            Err(ActorError::Store("Journal not found".to_string()))
        }
    }
}

