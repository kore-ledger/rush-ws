//

use crate::{
    Journal, JournalMessage, JournalResponse, SnapshotMessage, SnapshotResponse, Snapshotter,
    stores::{DbManager, StoreManager},
};

use actor::{Actor, ActorContext, ActorRef, Error as ActorError, ResultExt};
use async_trait::async_trait;
use rmp_serde::{from_slice, to_vec};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;
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
pub trait PersistentActor: Actor + Debug + Serialize + DeserializeOwned {
    /// Get the type name of the actor, used for creating stores. This method is used to
    /// determine the name of the stores for the journal and snapshotter, allowing different
    /// actor types to have their own separate stores.
    ///
    /// # Returns
    ///
    /// * `&'static str` - The type name of the actor, used for creating stores.
    ///
    fn type_name() -> &'static str
    where
        Self: Sized,
    {
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
    async fn init_state(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        self.create_stores(ctx).await?;
        let mut from = 0_u64;
        let state = self
            .snapshotter(ctx)
            .await
            .ok_or_else(|| ActorError::Store("Snapshotter not found".to_string()))?
            .ask(SnapshotMessage::LastSnapshot)
            .await
            .actor_store("Failed to get last snapshot")?;
        if let SnapshotResponse::LastResult(Some((snapshot_from, snapshot_data))) = state {
            from = snapshot_from;
            let deserialized_state: Self = from_slice(&snapshot_data)
                .actor_deserialization("Failed to deserialize snapshot")?;
            *self = deserialized_state;
            debug!(
                "Initialized state from snapshot with sequence number {}",
                snapshot_from
            );
        }
        let events = self
            .journal(ctx)
            .await
            .ok_or_else(|| ActorError::Store("Journal not found".to_string()))?
            .ask(JournalMessage::Range(from + 1, None))
            .await
            .actor_store("Failed to get events from journal")?;
        if let JournalResponse::Events(events) = events {
            for (_, event_data) in events {
                let event: Self::Event =
                    from_slice(&event_data).actor_deserialization("Failed to deserialize event")?;
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
    async fn create_stores(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        let type_name = Self::type_name();
        let store_manager = ctx
            .get_helper::<StoreManager>("storage")
            .await
            .ok_or_else(|| ActorError::Store("DB Manager not found".to_string()))?;
        let journal_name = format!("{}-Journal", type_name);
        let journal_store = store_manager
            .create_store(&journal_name, &self.id())
            .actor_store("Failed to create journal store")?;
        let snapshotter_name = format!("{}-Snapshotter", type_name);
        let snapshotter_store = store_manager
            .create_store(&snapshotter_name, &self.id())
            .actor_store("Failed to create snapshotter store")?;
        ctx.create_child(Journal::new(journal_store), "journal")
            .await
            .actor_store("Failed to create journal actor")
            .inspect_err(|e| error!("Failed to create journal actor: {}", e))?;
        ctx.create_child(Snapshotter::new(snapshotter_store), "snapshotter")
            .await
            .actor_store("Failed to create snapshotter actor")
            .inspect_err(|e| error!("Failed to create snapshotter actor: {}", e))?;
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
    /// * `Result<u64, ActorError>` - The last sequence number of the events in the journal, or
    ///   an error if it could not be retrieved.
    ///
    async fn last_sequence(&self, ctx: &mut ActorContext<Self>) -> Result<u64, ActorError> {
        if let Some(journal) = self.journal(ctx).await {
            let response: JournalResponse = journal.ask(JournalMessage::LastSequence).await?;
            if let JournalResponse::LastSequence(seq) = response {
                return Ok(seq);
            }
        }
        Err(ActorError::Store("Journal not found".to_string()))
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
    async fn persist(
        &mut self,
        event: &Self::Event,
        ctx: &mut ActorContext<Self>,
    ) -> Result<u64, ActorError> {
        let serialized_event = to_vec(event).actor_serialization("Failed to serialize event")?;
        if let Some(journal) = self.journal(ctx).await {
            let response = journal
                .ask(JournalMessage::Put(serialized_event))
                .await
                .actor_store("Failed to send event to journal")
                .inspect_err(|e| error!("Failed to send event to journal: {}", e))?;
            let sn = if let JournalResponse::LastSequence(seq) = response {
                seq
            } else {
                return Err(ActorError::Store(
                    "Unexpected response when putting event in journal".to_string(),
                ));
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

    /// Get the last persisted event and its sequence number from the journal.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The actor context, used to access the child actors.
    ///
    /// # Returns
    ///
    /// * `Result<(u64, Self::Event), ActorError>` - Ok with the sequence number and event if
    ///   successful, or an error if there was a problem retrieving the last event.
    ///
    async fn last_event(&mut self, ctx: &mut ActorContext<Self>) -> Option<(u64, Self::Event)> {
        if let Some(journal) = self.journal(ctx).await {
            let response = journal.ask(JournalMessage::Last).await.ok()?;
            if let JournalResponse::Last(seq, bytes) = response {
                match from_slice(&bytes) {
                    Ok(event) => Some((seq, event)),
                    Err(e) => {
                        println!("Deserialization error: {}", e);
                        error!("Failed to deserialize last event from journal: {}", e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            error!("Journal actor not found, failed to get last sequence");
            None
        }
    }

    /// Insert an event into the journal at a specific sequence number.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The actor context, used to access the child actors.
    /// * `sequence` - The sequence number at which to insert the new event.
    /// * `new_event` - The event to be inserted.
    ///
    /// # Returns
    ///
    /// * `Result<(), ActorError>` - Ok if the insertion was successful, or an error if there was
    ///   a problem.
    ///
    async fn insert_event(
        &mut self,
        ctx: &mut ActorContext<Self>,
        sequence: u64,
        new_event: Self::Event,
    ) -> Result<(), ActorError> {
        if let Some(journal) = self.journal(ctx).await {
            let bytes = to_vec(&new_event).actor_serialization("Failed to serialize event")?;
            let response = journal
                .ask(JournalMessage::Insert(sequence, bytes))
                .await
                .actor_store("Failed to send insert request to journal")
                .inspect_err(|e| error!("Failed to send insert request to journal: {}", e))?;
            if let JournalResponse::None = response {
                Ok(())
            } else {
                Err(ActorError::Store(
                    "Unexpected response when requesting insert to journal".to_string(),
                ))
            }
        } else {
            error!("Journal actor not found, failed to insert event");
            Err(ActorError::Store("Journal not found".to_string()))
        }
    }

    /// Get a range of events from the journal.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The actor context, used to access the child actors.
    /// * `from_sequence` - The starting sequence number of the events to retrieve.
    /// * `to_sequence` - The optional ending sequence number of the events to retrieve.
    ///
    /// # Returns
    ///
    /// * `Result<Vec<Self::Event>, ActorError>` - Ok with the list of events if successful, or an
    ///   error if there was a problem retrieving the events.
    ///
    async fn get_events(
        &mut self,
        ctx: &mut ActorContext<Self>,
        from_sequence: u64,
        to_sequence: Option<u64>,
    ) -> Result<Vec<Self::Event>, ActorError> {
        if let Some(journal) = self.journal(ctx).await {
            let response = journal
                .ask(JournalMessage::Range(from_sequence, to_sequence))
                .await
                .actor_store("Failed to send get events request to journal")
                .inspect_err(|e| error!("Failed to send get events request to journal: {}", e))?;
            if let JournalResponse::Events(records) = response {
                let mut events = Vec::new();
                for (_, bytes) in records {
                    let event =
                        from_slice(&bytes).actor_deserialization("Failed to deserialize event")?;
                    events.push(event);
                }
                Ok(events)
            } else {
                Err(ActorError::Store(
                    "Unexpected response when requesting events from journal".to_string(),
                ))
            }
        } else {
            error!("Journal actor not found, failed to get events");
            Err(ActorError::Store("Journal not found".to_string()))
        }
    }

    async fn get_event(
        &mut self,
        ctx: &mut ActorContext<Self>,
        sequence: u64,
    ) -> Result<Self::Event, ActorError> {
        if let Some(journal) = self.journal(ctx).await {
            let response = journal
                .ask(JournalMessage::Get(sequence))
                .await
                .actor_store("Failed to send get event request to journal")
                .inspect_err(|e| error!("Failed to send get event request to journal: {}", e))?;
            if let JournalResponse::Event(bytes) = response {
                let event =
                    from_slice(&bytes).actor_deserialization("Failed to deserialize event")?;
                Ok(event)
            } else {
                Err(ActorError::Store(
                    "Unexpected response when requesting event from journal".to_string(),
                ))
            }
        } else {
            error!("Journal actor not found, failed to get event");
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
        let journal = self
            .journal(ctx)
            .await
            .ok_or_else(|| ActorError::Store("Journal not found".to_string()))?;
        let snapshotter = self
            .snapshotter(ctx)
            .await
            .ok_or_else(|| ActorError::Store("Snapshotter not found".to_string()))?;

        // Flush the journal and snapshotter
        journal
            .tell(JournalMessage::Flush)
            .await
            .actor_store("Failed to send flush message to journal")
            .inspect_err(|e| error!("Failed to send flush message to journal: {}", e))?;
        snapshotter
            .tell(SnapshotMessage::Flush)
            .await
            .actor_store("Failed to send flush message to snapshotter")
            .inspect_err(|e| error!("Failed to send flush message to snapshotter: {}", e))?;
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
        let sn = self
            .last_sequence(ctx)
            .await
            .actor_store("Failed to get last journal sequence")
            .inspect_err(|e| error!("Failed to get last journal sequence, cannot create snapshot: {}", e))?;
        let serialized_state = to_vec(self).actor_serialization("Failed to serialize state")?;
        if let Some(snapshotter) = self.snapshotter(ctx).await {
            snapshotter
                .tell(SnapshotMessage::SaveSnapshot {
                    key: sn,
                    data: serialized_state,
                })
                .await
                .actor_store("Failed to send snapshot to snapshotter")
                .inspect_err(|e| error!("Failed to send snapshot to snapshotter: {}", e))?;
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
        Actor, ActorContext, ActorPath, Config, Error as ActorError, Event, Message, Response,
        System,
    };
    use serde::Deserialize;
    use tokio_util::sync::CancellationToken;

    #[derive(Clone)]
    enum TestMessage {
        Add(String),
        Insert(u64, String),
        Get(u64),
        LastSnapshot,
        Snapshot,
        GetAll,
        Range {
            from_sequence: u64,
            to_sequence: Option<u64>,
        },
        LastEvent,
    }

    impl Message for TestMessage {}

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestEvent {
        pub value: String,
    }

    impl Event for TestEvent {}

    enum TestResponse {
        All(Vec<String>),
        Snapshot(Option<(u64, TestActor)>),
        None,
        LastEvent(Option<(u64, TestEvent)>),
        Events(Vec<TestEvent>),
        Event(TestEvent),
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

        async fn pre_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            self.init_state(ctx).await
        }

        async fn pre_stop(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
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
                    let event = TestEvent {
                        value: value.clone(),
                    };
                    let sn = self.persist(&event, ctx).await?;
                    assert_eq!(sn + 1, self.state.len() as u64);
                    Ok(TestResponse::None)
                }
                TestMessage::GetAll => Ok(TestResponse::All(self.state.clone())),
                TestMessage::LastSnapshot => {
                    Ok(TestResponse::Snapshot(self.last_snapshot(ctx).await))
                }
                TestMessage::Snapshot => {
                    //let sn = self.state.len() as u64;
                    self.snapshot(ctx).await?;
                    Ok(TestResponse::None)
                }
                TestMessage::LastEvent => Ok(TestResponse::LastEvent(self.last_event(ctx).await)),
                TestMessage::Range {
                    from_sequence,
                    to_sequence,
                } => Ok(TestResponse::Events(
                    self.get_events(ctx, from_sequence, to_sequence).await?,
                )),
                TestMessage::Insert(sequence, new_event) => {
                    self.insert_event(ctx, sequence, TestEvent { value: new_event })
                        .await?;
                    Ok(TestResponse::None)
                }
                TestMessage::Get(sequence) => {
                    let event = self.get_event(ctx, sequence).await?;
                    Ok(TestResponse::Event(event))
                }
            }
        }
    }

    #[async_trait]
    impl PersistentActor for TestActor {
        async fn apply_event(&mut self, event: &Self::Event) -> Result<(), ActorError> {
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
        let path = tempfile::tempdir().unwrap();
        let manager = StoreManager::new(path.path().to_str().unwrap()).unwrap();
        system.add_helper("storage", manager.clone()).await;

        // Create the actor.
        let actor = TestActor { state: Vec::new() };
        let actor_ref = system.create_actor(actor, "test_actor").await.unwrap();

        // Send some messages to the actor.
        actor_ref
            .tell(TestMessage::Add("event1".to_string()))
            .await
            .unwrap();
        actor_ref
            .tell(TestMessage::Add("event2".to_string()))
            .await
            .unwrap();

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
            assert_eq!(sn, 1);
            assert_eq!(
                snapshot.state,
                vec!["event1".to_string(), "event2".to_string()]
            );
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
        actor_ref
            .tell(TestMessage::Add("event3".to_string()))
            .await
            .unwrap();
        let response = actor_ref.ask(TestMessage::GetAll).await.unwrap();
        if let TestResponse::All(state) = response {
            assert_eq!(
                state,
                vec![
                    "event1".to_string(),
                    "event2".to_string(),
                    "event3".to_string()
                ]
            );
        } else {
            panic!("Unexpected response");
        }

        // Get the last persisted event and its sequence number.
        let response = actor_ref.ask(TestMessage::LastEvent).await.unwrap();
        if let TestResponse::LastEvent(Some((sn, event))) = response {
            assert_eq!(sn, 2);
            assert_eq!(event.value, "event3".to_string());
        } else {
            panic!("Failed to get last event");
        }

        // Get a range of events from the journal.
        let response = actor_ref
            .ask(TestMessage::Range {
                from_sequence: 0,
                to_sequence: None,
            })
            .await
            .unwrap();
        if let TestResponse::Events(events) = response {
            assert_eq!(events.len(), 3);
            assert_eq!(events[0].value, "event1".to_string());
            assert_eq!(events[1].value, "event2".to_string());
            assert_eq!(events[2].value, "event3".to_string());
        } else {
            panic!("Failed to get events range");
        }

        // Insert a new event at a specific sequence number and verify it.
        let response = actor_ref
            .ask(TestMessage::Insert(2, "event1.5".to_string()))
            .await
            .unwrap();
        if let TestResponse::None = response {
        } else {
            panic!("Failed to insert event");
        }

        // Verify the insertion by getting the event
        let response = actor_ref.ask(TestMessage::Get(2)).await.unwrap();
        if let TestResponse::Event(event) = response {
            assert_eq!(event.value, "event1.5".to_string());
        } else {
            panic!("Failed to get inserted event");
        }

        // Stop the system and clean up.
        system.stop_children().await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        manager.drop().unwrap();
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_persist_error_1() {
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token);
        let path = tempfile::tempdir().unwrap();
        let manager = StoreManager::new(path.path().to_str().unwrap()).unwrap();
        system.add_helper("storage", manager.clone()).await;

        // Create the actor.
        let actor = TestActor { state: Vec::new() };
        let actor_ref = system.create_actor(actor, "test_actor").await.unwrap();

        // Initially, the actor should have no events.
        let response = actor_ref
            .ask(TestMessage::Range {
                from_sequence: 0,
                to_sequence: None,
            })
            .await
            .unwrap();
        if let TestResponse::Events(events) = response {
            assert_eq!(events.len(), 0);
        } else {
            panic!("Unexpected response");
        }

        // Send a message.
        actor_ref
            .tell(TestMessage::Add("event1".to_string()))
            .await
            .unwrap();

        // Get all events.
        let response = actor_ref
            .ask(TestMessage::Range {
                from_sequence: 0,
                to_sequence: None,
            })
            .await
            .unwrap();
        if let TestResponse::Events(events) = response {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].value, "event1".to_string());
        } else {
            panic!("Failed to get events range");
        }
    }
}
