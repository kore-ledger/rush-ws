//

use crate::{
    event_sourcing::{Journal, JournalMessage},
    stores::Store,
};
use actor::{Actor, ActorRef, ActorContext, Message};
use postcard::{to_vec, from_bytes};
use serde::{Serialize, de::DeserializeOwned};

pub trait PersistentActor: Actor {
    /// The type of the state that the actor will manage.
    type State: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Provides access to the journal for persisting events. This method should return an
    /// `ActorRef` to the journal actor that the persistent actor can use to persist events
    /// related to its state changes.
    /// 
    /// # Returns
    /// 
    /// An `ActorRef` to the journal actor that the persistent actor can use for event persistence.
    async fn journal<S: Store>(&self, ctx: &mut ActorContext<Self>) -> ActorRef<Journal<S>> {
        ctx.get_child("journal").await
            .expect("Missing journal actor")
    }

    /// Persists an event to the journal. This method takes an event of type `Self::Event` and
    /// persists it to the journal. The event is serialized and sent to the journal actor for
    /// persistence.
    /// 
    /// # Arguments
    /// 
    /// * `ctx` - A mutable reference to the actor context, which can be used to access the journal 
    /// actor.
    /// * `event` - The event of type `Self::Event` that should be persisted to the journal.
    /// 
    /// # Returns
    /// 
    /// A `Result` indicating whether the event was successfully persisted or if an error occurred. 
    /// On success, it returns `Ok(())`. On failure, it returns an `Err` containing an 
    /// `actor::Error` that describes the error that occurred during persistence.
    /// 
    async fn persist(
        &mut self,
        ctx: &mut ActorContext<Self>,
        event: Self::Event,
    ) -> Result<(), actor::Error> {
        let journal = self.journal(ctx).await;
        let serialized_event = to_vec(&event)
            .map_err(|e| actor::Error::Serialization(e.to_string()))?
            .as_slice()
            .to_vec();
        journal.tell(JournalMessage::PersistEvent(serialized_event)).await
            .map_err(|e| actor::Error::SendMessage(e.to_string()))?;    
        Ok(())
    }
}