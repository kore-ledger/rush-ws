//

use crate::Event;

use async_trait::async_trait;
use tokio::sync::broadcast::{Receiver as EventReceiver, error::RecvError};

use tracing::debug;

/// 
pub struct EventManager<E: Event> {
    /// The event handler that will be called for each received event.
    handler: Box<dyn EventHandler<E>>,
    /// Receiver for incoming events. The EventManager will listen to this receiver and 
    /// invoke the handler for each received event.
    receiver: EventReceiver<E>,
}

impl<E: Event> EventManager<E> {
    /// Creates a new EventManager with the given event handler and receiver.
    ///
    /// # Arguments
    ///
    /// * `handler` - The event handler to process incoming events.
    /// * `receiver` - The broadcast receiver to listen for events.
    ///
    pub fn new(handler: impl EventHandler<E>, receiver: EventReceiver<E>) -> Self {

        Self { handler: Box::new(handler), receiver }
    }

    /// Starts the event loop, listening for incoming events and invoking the handler.
    /// This method will run indefinitely until the receiver is closed or an error occurs.
    pub async fn start(&mut self) {
        debug!("EventManager started, waiting for events...");
        loop {
            match self.receiver.recv().await {
                Ok(event) => {
                    debug!("Received event: {:?}", event);
                    self.handler.notify(event).await;
                }
                Err(RecvError::Closed) => {
                    debug!("Event channel closed, stopping EventManager.");
                    break;
                }
                Err(RecvError::Lagged(count)) => {
                    debug!("Missed {} events due to lag.", count);
                }
            }
        }
    }
}

/// Trait for types that can receive and process actor events.
/// Implement this trait to define custom event processing logic
/// that will be invoked by a Sink for each event received.
///
/// # Type Parameters
///
/// * `E` - The event type this handler can process.
///
#[async_trait]
pub trait EventHandler<E: Event>: Send + Sync + 'static {
    /// Called when an event is received by the sink.
    /// This method should contain the logic for processing the event.
    ///
    /// # Arguments
    ///
    /// * `event` - The event to process.
    ///
    async fn notify(&self, event: E);
}