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
use tracing::{debug, error};

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

    /// Get the type name of the actor, used for creating stores. This method is used to 
    /// determine the name of the stores for the journal and snapshotter, allowing different 
    /// actor types to have their own separate stores.
    /// 
    /// # Returns
    /// 
    /// * `&'static str` - The type name of the actor, used for creating stores.
    ///
    fn type_name() -> &'static str where Self: Sized {
        let type_name = std::any::type_name::<Self>();
        type_name.rsplit("::").next().unwrap_or(type_name)
    }

    /// Get a unique identifier for the actor instance, used for creating stores. This method is
    /// used to determine the prefix of the stores for the journal and snapshotter, allowing 
    /// different actor instances to have their own separate stores.
    /// 
    /// # Returns
    /// 
    /// * `String` - A unique identifier for the actor instance, used for creating stores.
    ///
    fn id(&self) -> String;

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
        ctx: &mut ActorContext<Self>
    ) -> Result<(), ActorError> {
        self.create_stores(ctx).await?;
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
        ctx: &mut ActorContext<Self>
    ) -> Result<(), ActorError> {
        let store_manager = ctx.get_helper::<StoreManager>("storage").await
            .ok_or_else(|| ActorError::Store("DB Manager not found".to_string()))?;
        let journal_store = store_manager.create_store(Self::type_name(), &self.id())
            .map_err(|e| ActorError::Store(format!("Failed to create journal store: {}", e)))?;
        let snapshotter_store = store_manager.create_store(Self::type_name(), &self.id())
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
                .map_err(|e| {
                    error!("Failed to send event to journal: {}", e);
                    ActorError::Store(format!("Failed to send event to journal: {}", e))
                })?;
            debug!("Persisted event to journal");
            self.apply_event(event).await?;
            debug!("Applied event to state");
            Ok(())
        } else {
            error!("Journal actor not found, failed to persist event");
            Err(ActorError::Store("Journal not found".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use actor::{
        Actor, ActorContext, ActorPath, System, Config,
        Event, Message, Response, Error as ActorError
    };
    use serde::Deserialize;
    use tokio_util::sync::CancellationToken;

     enum TestMessage {
        Add(String),
        GetAll,
    }

    impl Message for TestMessage {}
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestEvent {
        value: String,
    }

    impl Event for TestEvent {}

    enum TestResponse {
        All(Vec<String>),
        None,
    }

    impl Response for TestResponse {}

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestActor {
        state: Vec<String>,
    }

    #[async_trait]
    impl Actor for TestActor {
        type Message = TestMessage;
        type Response = TestResponse;
        type Event = TestEvent;

        async fn pre_start(
            &mut self, ctx: 
            &mut ActorContext<Self>
        ) -> Result<(), ActorError> {
            self.init_state(ctx).await
        }

        async fn handle(
            &mut self, 
            _ctx: &mut ActorContext<Self>,
            _sender: &ActorPath,
            msg: Self::Message,
        ) -> Result<Self::Response, ActorError> {
            match msg {
                TestMessage::Add(value) => {
                    let event = TestEvent { value: value.clone() };
                    self.persist(&event, _ctx).await?;
                    Ok(TestResponse::None)
                },
                TestMessage::GetAll => {
                    Ok(TestResponse::All(self.state.clone()))
                },
            }
        }
    }

    #[async_trait]
    impl PersistentActor for TestActor {
        async fn apply_event(&mut self, event: &Self::Event) -> Result<(),ActorError> {
            self.state.push(event.value.clone());
            Ok(())
        }

        fn id(&self) -> String {
            "test_actor".to_string()
        }
    }

    #[tokio::test]
    #[tracing_test::traced_test]
    #[serial_test::serial]
    async fn test_persistent_actor() {
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token);
        let manager = StoreManager::default();
        system.add_helper("storage", manager.clone()).await;

        // Create the actor.
        let actor = TestActor { state: Vec::new() };
        let actor_ref = system.create_actor(actor, "test_actor").await.unwrap();

        // Send some messages to the actor.
        actor_ref.tell(TestMessage::Add("event1".to_string())).await.unwrap();
        actor_ref.tell(TestMessage::Add("event2".to_string())).await.unwrap();

        // Get the state from the actor.
        let response = actor_ref.ask(TestMessage::GetAll).await.unwrap();
        if let TestResponse::All(state) = response {
            assert_eq!(state, vec!["event1".to_string(), "event2".to_string()]);
        } else {
            panic!("Unexpected response");
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        manager.drop().unwrap();
    }

}