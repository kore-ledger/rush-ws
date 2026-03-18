//

use crate::{
    Journal, JournalMessage, JournalResponse, 
    Snapshotter, SnapshotMessage, SnapshotResponse, 
    stores::{DbManager, StoreManager}
};

use actor::{Actor, ActorContext, ActorRef, Error as ActorError};
use serde::{Serialize, de::DeserializeOwned};
use serde_bare::{to_vec, from_slice};
use async_trait::async_trait;

/// A trait for actors that can persist their state using a journal and snapshotter.
/// Actors implementing this trait must define an associated type `Event` that represents the 
/// events that can be persisted. The actor must implement the `apply_event` method to update
/// its state based on the events, and the `init_state` method to initialize its state from 
/// the journal and snapshotter when it starts.
/// 
/// The `persist` method can be used to persist an event to the journal, and the `create_stores` 
/// method is responsible for creating the necessary stores for the journal and snapshotter.
#[async_trait]
pub trait PersistentActor: Actor + Serialize + DeserializeOwned {

    /// Apply an event to the actor's state. 
    /// 
    /// # Arguments
    /// 
    /// * `event` - The event to apply to the actor's state.
    /// 
    /// # Returns
    /// 
    /// * `Result<(), ActorError>` - Ok if the event was applied successfully, or an error if 
    ///   there was a problem applying the event.
    /// 
    async fn apply_event(&mut self, event: &Self::Event) -> Result<(), ActorError>;

    /// Initialize the actor's state from the journal and snapshotter. This method is called 
    /// when the actor starts, and is responsible for loading the last snapshot and replaying any
    /// events from the journal that occurred after the snapshot.
    /// 
    /// # Arguments
    /// 
    /// * `name` - The name of the actor, used to create the stores for the journal and 
    ///   snapshotter.
    /// * `prefix` - A prefix for the store names, used to differentiate between different actors.
    /// * `ctx` - The actor context, used to create child actors for the journal and snapshotter, 
    ///   and to access the stores.
    ///
    /// # Returns
    /// 
    /// * `Result<(), ActorError>` - Ok if the state was initialized successfully, or an error if 
    ///   there was a problem initializing the state.
    ///
    async fn init_state (
        &mut self, 
        name: &str, 
        prefix: &str, 
        ctx: &mut ActorContext<Self>
    ) -> Result<(), ActorError> {
        self.create_stores(name, prefix, ctx).await?;
        let mut from = 0_u64;
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

    /// Create the necessary stores for the journal and snapshotter, and create child actors for
    /// the journal and snapshotter.
    /// 
    /// # Arguments
    /// 
    /// * `name` - The name of the actor type, used to create the stores for the journal and 
    ///   snapshotter.
    /// * `prefix` - A prefix for the store names, used to differentiate between different actors.
    /// * `ctx` - The actor context, used to create child actors for the journal and snapshotter.
    ///
    /// # Returns
    /// 
    /// * `Result<(), ActorError>` - Ok if the stores were created successfully, or an error if 
    ///   there was a problem creating the stores or child actors.
    ///
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

    /// Get a reference to the journal actor.
    /// 
    /// # Arguments
    /// 
    /// * `ctx` - The actor context, used to access the child actors.
    /// 
    /// # Returns
    /// 
    /// * `Option<ActorRef<Journal>>` - A reference to the journal actor, or None if the journal 
    ///   actor was not found.
    ///
    async fn journal(&self, ctx: &mut ActorContext<Self>) -> Option<ActorRef<Journal>> {
        ctx.get_child("journal").await
    }

    /// Get a reference to the snapshotter actor.
    /// 
    /// # Arguments
    /// 
    /// * `ctx` - The actor context, used to access the child actors.
    /// 
    /// # Returns
    /// 
    /// * `Option<ActorRef<Snapshotter>>` - A reference to the snapshot
    ///  actor, or None if the snapshotter actor was not found.
    /// 
    async fn snapshotter(&self, ctx: &mut ActorContext<Self>) -> Option<ActorRef<Snapshotter>> {
        ctx.get_child("snapshotter").await
    }

    /// Persist an event to the journal.
    /// 
    /// # Arguments
    /// 
    /// * `event` - The event to persist.
    /// * `ctx` - The actor context, used to access the child actors.
    /// 
    /// # Returns
    /// 
    /// * `Result<(), ActorError>` - Ok if the event was persisted successfully, or an error if
    ///   there was a problem persisting the event.
    ///
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

#[cfg(test)]
mod tests {

    use super::*;
    use actor::{
        Actor, ActorContext, ActorRef, ActorPath, System, Config,
        Event, Message, Response, Error as ActorError
    };
    use serde::Deserialize;
    use tokio_util::sync::CancellationToken;

    pub struct TestMessage(String);

    impl Message for TestMessage {}
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestEvent {
        value: String,
    }

    impl Event for TestEvent {}

    struct DummyResponse;

    impl Response for DummyResponse {}

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestActor {
        state: Vec<String>,
    }

    #[async_trait]
    impl Actor for TestActor {
        type Message = TestMessage;
        type Response = DummyResponse;
        type Event = TestEvent;

        async fn pre_start(
            &mut self, ctx: 
            &mut ActorContext<Self>
        ) -> Result<(), ActorError> {
            self.init_state("TestActor", "test_actor", ctx).await
        }

        async fn handle(
            &mut self, 
            _ctx: &mut ActorContext<Self>,
            _sender: &ActorPath,
            msg: Self::Message,
        ) -> Result<Self::Response, ActorError> {
            let event = TestEvent { value: msg.0 };
            self.apply_event(&event).await?;
            Ok(DummyResponse)
        }
    }

    #[async_trait]
    impl PersistentActor for TestActor {
        async fn apply_event(&mut self, event: &Self::Event) -> Result<(),ActorError> {
            self.state.push(event.value.clone());
            Ok(())
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_persistent_actor() {
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token);
        let manager = StoreManager::default();
        system.add_helper("storage", manager.clone()).await;

        let actor = TestActor { state: Vec::new() };

        let actor_ref = system.create_actor(actor, "test_actor").await.unwrap();
    }

}