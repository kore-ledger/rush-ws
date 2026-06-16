//

use crate::{
    SnapshotMessage, SnapshotResponse, Snapshotter, stores::{DbManager, StoreManager}
};

use actor::{Actor, ActorContext, ActorRef, Error as ActorError};
use serde::{Serialize, de::DeserializeOwned};
use serde_bare::{to_vec, from_slice};
use async_trait::async_trait;
use tracing::{debug, error};
use std::fmt::Debug;

#[async_trait]
pub trait SnapshotActor: Actor + Debug + Serialize + DeserializeOwned {

    /// Get the type name of the actor, used for creating stores. This method is used to 
    /// determine the name of the stores for the snapshotter, allowing different 
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
    /// used to determine the prefix of the stores for the snapshotter, allowing 
    /// different actor instances to have their own separate stores.
    /// 
    /// # Returns
    /// 
    /// * `String` - A unique identifier for the actor instance, used for creating stores.
    ///
    fn id(&self) -> String;

    /// Save the current state of the actor to a snapshot. 
    /// 
    /// # Arguments
    /// 
    /// * `ctx` - The actor context, used to access the snapshotter actor and other child actors.
    /// 
    /// # Returns
    /// 
    /// * `Result<(), ActorError>` - Ok if the snapshot was saved successfully, or an error if 
    ///   there was a problem during saving.
    ///
    async fn save(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        if let Some(snapshotter) = self.snapshotter(ctx).await {
            let serialized_state = to_vec(self)
                .map_err(|e| ActorError::Store(format!("Failed to serialize snapshot: {}", e)))?;
            snapshotter.tell(SnapshotMessage::SaveSnapshot { key: 0, data: serialized_state }).await
                .map_err(|e| ActorError::Store(format!("Failed to save snapshot: {}", e)))
        } else {
            Err(ActorError::Store("Snapshotter actor not found".to_string()))
        }
    }

    /// Load the state of the actor from a snapshot. This method is used to recover the state of
    /// the actor after a restart or failure.
    /// 
    /// # Arguments
    /// 
    /// * `ctx` - The actor context, used to access the snapshotter actor and other child actors.
    /// 
    /// # Returns
    /// 
    /// * `Result<(), ActorError>` - Ok if the snapshot was loaded successfully, or an error if 
    ///   there was a problem during loading.
    ///
    async fn load(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        if let Some(snapshotter) = self.snapshotter(ctx).await {
            match snapshotter.ask(SnapshotMessage::LoadSnapshot { key: 0 }).await {
                Ok(SnapshotResponse::LoadResult(data)) => {
                    if let Some(data) = data {
                        debug!("Loaded snapshot for actor {}: {} bytes", Self::type_name(), data.len());
                        let deserialized_state: Self = from_slice(&data)
                            .map_err(|e| ActorError::Store(format!("Failed to deserialize snapshot: {}", e)))?;
                        *self = deserialized_state;
                        Ok(())
                    } else {
                        debug!("No snapshot found for actor {}", Self::type_name());
                        Err(ActorError::Store("Snapshot not found".to_string()))
                    }
                },
                _ => Err(ActorError::Store("Failed to load snapshot".to_string())),
            }
        } else {
            Err(ActorError::Store("Snapshotter actor not found".to_string()))
        }
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

    /// Flush the snapshotter to ensure that all pending snapshots are saved. This method is used 
    /// to ensure that all snapshots are persisted before the actor shuts down, allowing for 
    /// proper recovery later.
    /// 
    /// # Arguments
    /// 
    /// * `ctx` - The actor context, used to access the snapshotter actor and other child actors.
    /// 
    /// # Returns
    /// 
    /// * `Result<(), ActorError>` - Ok if the snapshotter was flushed successfully, or an error if
    ///  there was a problem during flushing.
    ///  
    async fn flush(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
        if let Some(snapshotter) = self.snapshotter(ctx).await {
            snapshotter.tell(SnapshotMessage::Flush).await
                .map_err(|e| ActorError::Store(format!("Failed to flush snapshot: {}", e)))
        } else {
            Err(ActorError::Store("Snapshotter actor not found".to_string()))
        }
    }

    /// Initialize the state of the actor. This method is used to set up the initial state of the
    /// actor, including creating the snapshotter store and loading any existing snapshots.
    /// 
    /// # Arguments
    /// 
    /// * `ctx` - The actor context, used to access the store manager and create the snapshotter 
    ///   store.
    /// 
    /// # Returns
    /// 
    /// * `Result<(), ActorError>` - Ok if the state was initialized successfully, or an error if
    ///   there was a problem during initialization.
    ///
    async fn init_state (
        &mut self, 
        ctx: &mut ActorContext<Self>
    ) -> Result<(), ActorError> {
        self.create_store(ctx).await?;
        if let Err(e) = self.load(ctx).await {
            debug!("Failed to load snapshot for actor {}: {}", Self::type_name(), e);
        }
        Ok(())
    }

    /// Create a new store for the snapshotter actor. This method is used to initialize the
    /// snapshotter with a dedicated store for saving snapshots. The store is created based on the
    /// actor type name and the actor ID, allowing for separate stores for different actor types
    /// and instances.
    /// 
    /// # Arguments
    /// 
    /// * `ctx` - The actor context, used to access the store manager and create the snapshotter 
    ///   store.
    /// 
    /// # Returns
    /// 
    /// * `Result<(), ActorError>` - Ok if the store was created successfully, or an error if there
    ///   was a problem during store creation.
    ///
    async fn create_store(
        &mut self,
        ctx: &mut ActorContext<Self>
    ) -> Result<(), ActorError> {
        // Get the store manager from the actor context. This is used to create a new store for the 
        // snapshotter.
        let store_manager = ctx.get_helper::<StoreManager>("storage").await
            .ok_or_else(|| ActorError::Store("DB Manager not found".to_string()))?;

        // Create a new store for the snapshotter. The store name is based on the actor type name 
        // and the actor ID.
        let snapshotter_name = format!("{}-Snapshotter", Self::type_name());
        let snapshotter_store = store_manager.create_store(&snapshotter_name, &self.id())
            .map_err(|e| ActorError::Store(format!("Failed to create snapshotter store: {}", e)))?;

        // Create the snapshotter actor as a child of the current actor. This allows the snapshotter to
        // manage snapshots for the current actor.
        let snapshotter = Snapshotter::new(snapshotter_store);
        if let Err(e) = ctx.create_child(
            snapshotter, "snapshotter"
        ).await {
            error!("Failed to create snapshotter actor: {}", e);
            return Err(ActorError::Store(format!("Failed to create snapshotter actor: {}", e)));
        }

        Ok(())
    } 

}


#[cfg(test)]
mod tests {

    use super::*;

    use crate::StoreManager;
    use actor::{Actor, Message, ActorPath, Response, DummyEvent, System, Config};
    use tokio_util::sync::CancellationToken;
    use serde::Deserialize;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct TestActor {
        pub id: String,
        pub value: u64,
    }

    #[derive(Clone)]
    pub enum TestMessage {
        Increment,
        GetValue,
    }

    impl Message for TestMessage {}

    pub struct TestResponse {
        pub value: u64,
    }

    impl Response for TestResponse {}

    #[async_trait]
    impl Actor for TestActor {
        type Message = TestMessage;
        type Response = TestResponse;
        type Event = DummyEvent;

        async fn handle(
            &mut self,
            ctx: &mut ActorContext<Self>,
            _sender: &ActorPath,
            msg: Self::Message,
        ) -> Result<Self::Response, ActorError> {
            match msg {
                TestMessage::Increment => {
                    self.value += 1;
                    self.save(ctx).await?;
                    Ok(TestResponse { value: self.value })
                },
                TestMessage::GetValue => Ok(TestResponse { value: self.value }),
            }
        }

        async fn pre_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            self.init_state(ctx).await
        }

        async fn post_stop(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), ActorError> {
            self.flush(ctx).await
        }
    }

    impl SnapshotActor for TestActor {
        fn id(&self) -> String {
            self.id.clone()
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_snapshot_actor() {
        // Initialize the actor system and store manager for testing.
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token);
        let manager = StoreManager::default();
        system.add_helper("storage", manager.clone()).await;

        // Create a test actor and send messages to it.
        let actor = TestActor { id: "test_actor".to_string(), value: 0 };
        let actor_ref = system.create_actor(actor, "test_actor").await.unwrap();
        let response = actor_ref.ask(TestMessage::Increment).await.unwrap();
        assert_eq!(response.value, 1);

        // Stop the actor
        system.stop_actor("test_actor").await.unwrap();

        // End the actor system and wait for it to finish.
        system.stop_children().await.unwrap();

        // Wait for a moment to ensure that the snapshotter has time to flush the 
        // snapshot before the store is dropped.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Create the actor again and check if the state was restored from the snapshot.
        let actor = TestActor { id: "test_actor".to_string(), value: 0 };
        let actor_ref = system.create_actor(actor, "test_actor").await.unwrap();
        let response = actor_ref.ask(TestMessage::GetValue).await.unwrap();
        assert_eq!(response.value, 1);
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        manager.drop().unwrap();
    }
}