//

use crate::{
    Journal, JournalMessage, JournalResponse, SnapshotMessage, SnapshotResponse, Snapshotter, stores::{DbManager, StoreManager}
};

use actor::{Actor, ActorContext, ActorRef, Error as ActorError};
use serde::{Serialize, de::DeserializeOwned};
use serde_bare::{to_vec, from_slice};
use async_trait::async_trait;
use tracing::{debug, error};
use std::fmt::Debug;

/// A trait for actors that can persist their state using a journal and snapshotter.
/// Actors implementing this trait must define an associated type `Event` that represents the 
/// events that can be persisted. The actor must implement the `apply_event` method to update
/// its state based on the events, and the `init_state` method to initialize its state from 
/// the journal and snapshotter when it starts.
/// 
/// The `persist` method can be used to persist an event to the journal, and the `create_stores` 
/// method is responsible for creating the necessary stores for the journal and snapshotter.
#[async_trait]
pub trait PersistentActor: Actor + Debug + Serialize + DeserializeOwned {

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
            debug!("Initialized state from snapshot with sequence number {}", snapshot_from);
        }
        let events = self.journal(ctx).await
            .ok_or_else(|| ActorError::Store("Journal not found".to_string()))?
            .ask(JournalMessage::Range(from + 1, None)).await
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
        let type_name = Self::type_name();
        let store_manager = ctx.get_helper::<StoreManager>("storage").await
            .ok_or_else(|| ActorError::Store("DB Manager not found".to_string()))?;
        let journal_name = format!("{}-Journal", type_name);
        let journal_store = store_manager.create_store(&journal_name, &self.id())
            .map_err(|e| ActorError::Store(format!("Failed to create journal store: {}", e)))?;
        let snapshotter_name = format!("{}-Snapshotter", type_name);
        let snapshotter_store = store_manager.create_store(&snapshotter_name, &self.id())
            .map_err(|e| ActorError::Store(format!("Failed to create snapshotter store: {}", e)))?;
        if let Err(e) = ctx.create_child(Journal::new(journal_store), "journal").await {
            //println!("Failed to create journal actor: {}", e);
            error!("Failed to create journal actor: {}", e);
            return Err(ActorError::Store(format!("Failed to create journal actor: {}", e)));
        }
        if let Err(e) = ctx.create_child(Snapshotter::new(snapshotter_store), "snapshotter").await {
            //println!("Failed to create snapshotter actor: {}", e);
            error!("Failed to create snapshotter actor: {}", e);
            return Err(ActorError::Store(format!("Failed to create snapshotter actor: {}", e)));
        }
        /*ctx.create_child(Journal::new(journal_store), "journal").await
            .map_err(|e| ActorError::Store(format!("Failed to create journal actor: {}", e)))?;
        ctx.create_child(Snapshotter::new(snapshotter_store), "snapshotter").await
            .map_err(|e| ActorError::Store(format!("Failed to create snapshotter actor: {}", e)))?;*/
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

    /// Get the last sequence number of the events in the journal. 
    /// 
    /// # Arguments
    /// 
    /// * `ctx` - The actor context, used to access the child actors.
    /// 
    /// # Returns
    /// 
    /// * `Option<u64>` - The last sequence number of the events in the journal, or None if the journal
    ///   actor was not found or an error occurred.
    ///
    async fn last_sequence(&self, ctx: &mut ActorContext<Self>) -> Option<u64> {
        if let Some(journal) = self.journal(ctx).await {
            let response: JournalResponse = journal.ask(JournalMessage::LastSequence).await.ok()?;
            if let JournalResponse::LastSequence(seq) = response {
                Some(seq)
            } else {
                None
            }
        } else {
            None
        }
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
    async fn persist(&mut self, event: &Self::Event, ctx: &mut ActorContext<Self>) -> Result<u64, ActorError> {
        let serialized_event = to_vec(event)
            .map_err(|e| ActorError::Serialization(format!("Failed to serialize event: {}", e)))?;
        if let Some(journal) = self.journal(ctx).await {
            let response = journal.ask(JournalMessage::Put(serialized_event)).await
                .map_err(|e| {
                    error!("Failed to send event to journal: {}", e);
                    ActorError::Store(format!("Failed to send event to journal: {}", e))
                })?;
            let sn = if let JournalResponse::LastSequence(seq) = response {
                seq
            } else {
                return Err(ActorError::Store("Unexpected response when putting event in journal".to_string()));
            };
            debug!("Persisted event to journal");
            self.apply_event(event).await?;
            debug!("Applied event to state");
            Ok(sn)
        } else {
            error!("Journal actor not found, failed to persist event");
            Err(ActorError::Store("Journal not found".to_string()))
        }
    }

    /// Flush the journal and snapshotter to ensure all data is persisted to the underlying stores.
    /// This method is used to ensure that all events and snapshots are written to the underlying 
    /// storage, providing durability guarantees for the actor's state.
    /// 
    /// # Arguments
    /// 
    /// * `ctx` - The actor context, used to access the child actors.
    /// 
    /// # Returns
    /// 
    /// * `Result<(), ActorError>` - Ok if the flush was successful, or an error if there was a 
    ///   problem during flushing.
    ///
    async fn flush(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        // Snapshot the current state
        self.snapshot(ctx).await?;

        // Get journal and snapshotter references
        let journal = self.journal(ctx).await
            .ok_or_else(|| ActorError::Store("Journal not found".to_string()))?;
        let snapshotter = self.snapshotter(ctx).await
            .ok_or_else(|| ActorError::Store("Snapshotter not found".to_string()))?;

        // Flush the journal and snapshotter
        journal.tell(JournalMessage::Flush).await
            .map_err(|e| {
                error!("Failed to send flush message to journal: {}", e);
                ActorError::Store(format!("Failed to send flush message to journal: {}", e))
            })?;
        snapshotter.tell(SnapshotMessage::Flush).await
            .map_err(|e| {
                error!("Failed to send flush message to snapshotter: {}", e);
                ActorError::Store(format!("Failed to send flush message to snapshotter: {}", e))
            })?;
        debug!("Flushed journal and snapshotter");

        Ok(())
    }
    /// Persist a snapshot of the actor's state to the snapshotter.
    /// 
    /// # Arguments
    /// 
    /// * `ctx` - The actor context, used to access the child actors.
    /// 
    /// # Returns
    /// 
    /// * `Result<(), ActorError>` - Ok if the snapshot was persisted successfully, or an error if
    ///   there was a problem persisting the snapshot.
    ///
    async fn snapshot(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        let sn = self.last_sequence(ctx).await
            .ok_or_else(|| {
                error!("Failed to get last journal sequence, cannot create snapshot");
                ActorError::Store("Failed to get last journal sequence".to_string())
            })?;
        let serialized_state = to_vec(self)
            .map_err(|e| ActorError::Serialization(format!("Failed to serialize state: {}", e)))?;
        if let Some(snapshotter) = self.snapshotter(ctx).await {
            snapshotter.tell(SnapshotMessage::SaveSnapshot { key: sn, data: serialized_state }).await
                .map_err(|e| {
                    error!("Failed to send snapshot to snapshotter: {}", e);
                    ActorError::Store(format!("Failed to send snapshot to snapshotter: {}", e))
                })?;
            debug!("Persisted snapshot to snapshotter");
            Ok(())
        } else {
            error!("Snapshotter actor not found, failed to persist snapshot");
            Err(ActorError::Store("Snapshotter not found".to_string()))
        }
    }

    /// Retrieve the last snapshot stored in the snapshotter, along with its sequence number. This
    /// method is used to get the most recent snapshot of the actor's state, allowing for 
    /// efficient recovery of the actor's state.
    /// 
    /// # Arguments
    /// 
    /// * `ctx` - The actor context, used to access the child actors.
    /// 
    /// # Returns
    /// 
    /// * `Option<(u64, Self)>` - Some with the sequence number and deserialized state if a 
    ///   snapshot was found, or None if no snapshot was found or there was an error during 
    ///   retrieval.
    ///
    async fn last_snapshot(&self, ctx: &mut ActorContext<Self>) -> Option<(u64, Self)> {
        if let Some(snapshotter) = self.snapshotter(ctx).await {
            let response = snapshotter.ask(SnapshotMessage::LastSnapshot).await.ok()?;
            if let SnapshotResponse::LastResult(Some((sn, data))) = response {
                let deserialized_state: Self = from_slice(&data).ok()?;
                Some((sn, deserialized_state))
            } else {
                None
            }
        } else {
            None
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
        LastSnapshot,
        Snapshot,
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
        Snapshot(Option<(u64, TestActor)>),
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
            &mut self, 
            ctx: &mut ActorContext<Self>
        ) -> Result<(), ActorError> {
            self.init_state(ctx).await
        }

        async fn pre_stop(&mut self, 
            ctx: &mut ActorContext<Self>
        ) -> Result<(), ActorError> {
            self.flush(ctx).await?;
            Ok(())
        }

        async fn handle(
            &mut self, 
            ctx: &mut ActorContext<Self>,
            _sender: &ActorPath,
            msg: Self::Message,
        ) -> Result<Self::Response, ActorError> {
            match msg {
                TestMessage::Add(value) => {
                    let event = TestEvent { value: value.clone() };
                    let sn = self.persist(&event, ctx).await?;
                    assert_eq!(sn, self.state.len() as u64);
                    Ok(TestResponse::None)
                },
                TestMessage::GetAll => {
                    Ok(TestResponse::All(self.state.clone()))
                },
                TestMessage::LastSnapshot => {
                    Ok(TestResponse::Snapshot(self.last_snapshot(ctx).await))
                }
                TestMessage::Snapshot => {
                    //let sn = self.state.len() as u64;
                    self.snapshot(ctx).await?;
                    Ok(TestResponse::None)
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

        // Do snapshot
        let response = actor_ref.ask(TestMessage::Snapshot).await.unwrap();
        assert!(matches!(response, TestResponse::None));

        // Get the last snapshot
        let response = actor_ref.ask(TestMessage::LastSnapshot).await.unwrap();
        if let TestResponse::Snapshot(Some((sn, snapshot))) = response {
            assert_eq!(sn, 2);
            assert_eq!(snapshot.state, vec!["event1".to_string(), "event2".to_string()]);
        } else {
            panic!("Failed to get last snapshot");
        }

        // Stop the actor and create a new instance to test state recovery.
        system.stop_actor("test_actor").await.unwrap();

        // Wait a bit to ensure the actor has stopped and the state is flushed to the stores.

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        
        let actor = TestActor { state: Vec::new() };
        let actor_ref = system.create_actor(actor, "test_actor").await.unwrap();
        let response = actor_ref.ask(TestMessage::GetAll).await.unwrap();
        if let TestResponse::All(state) = response {
            assert_eq!(state, vec!["event1".to_string(), "event2".to_string()]);
        } else {
            panic!("Unexpected response");
        }

        // Add another event and check state.
        actor_ref.tell(TestMessage::Add("event3".to_string())).await.unwrap();
        let response = actor_ref.ask(TestMessage::GetAll).await.unwrap();
        if let TestResponse::All(state) = response {
            assert_eq!(state, vec!["event1".to_string(), "event2".to_string(), "event3".to_string()]);
        } else {
            panic!("Unexpected response");
        }

        system.stop_children().await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        manager.drop().unwrap();
    }

}