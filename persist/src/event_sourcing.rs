//

use crate::stores::{Store, IteratorOptions};
use actor::{Actor, Event, Message, Response, DummyEvent};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// A journal that stores events for an event-sourced actor. The journal is responsible for
/// persisting events and providing them to the actor when it needs to replay its state. The 
/// journal also keeps track of the latest sequence number for the events, allowing actors 
/// to know up to which point they have replayed.
pub struct Journal<S: Store> {
    /// The name of the journal, typically associated with the actor or entity it belongs to.
    name: String,
    /// The underlying store used for persisting events.
    store: S,
    /// The latest sequence number of the events.
    latest_sequence: u64,    
}

impl<S: Store> Journal<S> {
    /// Creates a new journal with the given store.
    /// 
    /// # Arguments
    /// 
    /// * `name` - A string slice that holds the name of the journal.
    /// * `store` - An instance of a store that implements the `Store` trait
    /// 
    /// # Returns
    /// 
    /// A new instance of `Journal` initialized with the provided name and store, and the latest
    /// sequence set to 0.
    /// 
    pub fn new(name: &str, store: S) -> Self {
        Self {
            name: name.to_owned(),
            store,
            latest_sequence: 0,
        }
    }
}

/// Messages that can be sent to the journal for processing. These messages include commands to 
/// persist events, retrieve events within a specific sequence range, and get the latest event in
/// the journal for a given actor.
pub enum JournalMessage {
    /// A message to persist an event in the journal. The event is wrapped in the `PersistEvent` 
    /// variant.
    PersistEvent(Vec<u8>),
    /// A message to retrieve events within a specific sequence range. The `from_sequence` and 
    /// `to_sequence` fieldsspecify the range of events to retrieve.
    GetEvents { from_sequence: u64, to_sequence: Option<u64> },
    /// A message to get the latest event in the journal.
    LastEvent,     
}

impl Message for JournalMessage {}

/// Responses that the journal can send back after processing messages. These responses include a
/// vector of events retrieved from the journal or the latest event in the journal.
pub enum JournalResponse {
    /// A response containing a vector of events retrieved from the journal.
    Events(Vec<(u64, Vec<u8>)>),
    /// A response containing the latest event in the journal, if it exists.
    LastEvent(Option<(u64, Vec<u8>)>),
    /// No response, used for messages that do not require a response (e.g., persisting an event).
    None,
}
    
impl Response for JournalResponse {}

#[async_trait]
impl<S: Store> Actor for Journal<S> {
    type Message = JournalMessage;
    type Response = JournalResponse;
    type Event = DummyEvent;

    async fn handle(
        &mut self,
        context: &mut actor::ActorContext<Self>,
        _path: &actor::ActorPath,
        message: Self::Message,
    ) -> Result<Self::Response, actor::Error> {
        match message {
            JournalMessage::PersistEvent(event) => {
                // Persist the event in the store and update the latest sequence number
                self.latest_sequence += 1;
                if let Err(e) = self.store.put(self.latest_sequence, &event).map_err(|e| {
                    actor::Error::SendEvent(format!("Failed to persist event {} with error: {:?}", self.latest_sequence, e))
                }) {
                    context.emit_fault(&e).await?;
                }
                Ok(JournalResponse::None)
            }
            JournalMessage::GetEvents { from_sequence, to_sequence } => {
                // Retrieve events from the store within the specified sequence range
                let opt = IteratorOptions::Range { from: from_sequence, to: to_sequence };
                let events = self.store.iter(opt).collect();
                Ok(JournalResponse::Events(events))
            }
            JournalMessage::LastEvent => {
                // Get the latest event from the store
                let last_event = self.store.last();
                Ok(JournalResponse::LastEvent(last_event))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    //use crate::stores::{DbManager, rocksdb::{RocksDbStore, RocksDbManager}};

    #[tokio::test]
    async fn test_journal() {
        //let manager = RocksDbManager::new("test_db");
        
    }
}
