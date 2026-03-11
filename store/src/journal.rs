//

use crate::stores::{Store, IteratorOptions};
use actor::{Actor, Message, Response, DummyEvent};
use async_trait::async_trait;

/// A journal that stores events for an event-sourced actor. The journal is responsible for
/// persisting events and providing them to the actor when it needs to replay its state. The 
/// journal also keeps track of the latest sequence number for the events, allowing actors 
/// to know up to which point they have replayed.
pub struct Journal<S: Store> {
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
    pub fn new(store: S) -> Self {
        Self {
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
    use crate::stores::{DbManager, memory::MemoryDbManager};
    use actor::{Config, System};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn test_journal() {
        let manager = MemoryDbManager::default();
        let store = manager.create_store("test_journal", "event").unwrap();
        let journal = Journal::new(store);

        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token.clone());
        let journal_ref = system.create_actor(journal, "test_journal").await.unwrap();

        // Persist some events
        for i in 1..=5 {
            let event_data = format!("event_{}", i).into_bytes();
            let response = journal_ref.tell(JournalMessage::PersistEvent(event_data)).await;
            assert!(response.is_ok());
        }

        // Retrieve events from the journal
        let response = journal_ref.ask(JournalMessage::GetEvents { from_sequence: 1, to_sequence: Some(5) }).await.unwrap();
        if let JournalResponse::Events(events   ) = response {
            assert_eq!(events.len(), 5);
            for (i, (seq, data)) in events.into_iter().enumerate() {
                assert_eq!(seq, (i + 1) as u64);
                assert_eq!(data, format!("event_{}", i + 1).into_bytes());
            }
        } else {
            panic!("Expected JournalResponse::Events");
        }

        // Get the latest event from the journal
        let response = journal_ref.ask(JournalMessage::LastEvent).await.unwrap();
        if let JournalResponse::LastEvent(Some((seq, data))) = response {
            assert_eq!(seq, 5);
            assert_eq!(data, format!("event_5").into_bytes());
        } else {
            panic!("Expected JournalResponse::LastEvent with Some event");
        }

        token.cancel();
    }
}
