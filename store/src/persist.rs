//

use crate::{Journal, JournalMessage, JournalResponse, Snapshotter};

use actor::{Actor, ActorContext, ActorRef, ActorPath, Error as ActorError, Response};
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use serde_bare::{to_vec, from_slice};
use async_trait::async_trait;


#[async_trait]
pub trait PersistentActor: Actor + Serialize + DeserializeOwned {

    async fn journal(&self, ctx: &mut ActorContext<Self>) -> Option<ActorRef<Journal>> {
        ctx.get_child("journal").await
    }

    /*async fn snapshotter(&self, ctx: &mut ActorContext<Self>) -> Option<ActorRef<Snapshotter>> {
        ctx.get_child("snapshotter").await
    }*/
    
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

