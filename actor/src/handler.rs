//! # Message Handler
//!
//! This module manages the sending and receiving of messages between actors,
//! providing the mailbox and helpers for communication.

use crate::{Actor, ActorContext, ActorPath, Error};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error};

use std::fmt::Debug;

/// Actor message.
///
pub struct ActorMessage<A: Actor> {
    /// The actor path of the sender.
    sender: ActorPath,
    /// The message to be processed by the actor.
    msg: A::Message,
    /// Optional channel to send the response back to the sender.
    resp: Option<oneshot::Sender<Result<A::Response, Error>>>,
}

impl<A: Actor> ActorMessage<A> {
    /// Creates a new ActorMessage.
    ///
    /// # Arguments
    ///
    /// * `sender` - The ActorPath of the sender.
    /// * `msg` - The message to be processed by the actor.
    /// * `resp` - Optional oneshot sender for the response.
    ///
    pub fn new(
        sender: ActorPath,
        msg: A::Message,
        resp: Option<oneshot::Sender<Result<A::Response, Error>>>,
    ) -> Self {
        Self { sender, msg, resp }
    }

    /// Handles the actor message.
    ///
    /// # Arguments
    ///
    /// * `actor` - The actor to handle the message.
    /// * `ctx` - The context of the actor.
    ///
    /// # Returns
    ///
    /// A boolean indicating whether the actor should stop.
    ///
    pub async fn handle(self, actor: &mut A, ctx: &mut ActorContext<A>) {
        debug!("Handling message from {:?}", self.sender);
        let result = actor.handle(ctx, &self.sender, self.msg).await;
        if let Some(resp) = self.resp
            && let Err(_e) = resp.send(result)
        {
            error!("Failed to send response back to sender");
        }
    }
}

impl<A: Actor> Debug for ActorMessage<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorMessage")
            .field("sender", &self.sender)
            .field("msg", &"<message>")
            .field("resp", &self.resp.as_ref().map(|_| "<oneshot::Sender>"))
            .finish()
    }
}

/// Type aliases for mailbox sender.
pub type MailboxSender<A> = mpsc::Sender<ActorMessage<A>>;
/// Type aliases for mailbox receiver.
pub type MailboxReceiver<A> = mpsc::Receiver<ActorMessage<A>>;
/// Type aliases for mailbox.
pub type Mailbox<A> = (MailboxSender<A>, MailboxReceiver<A>);

/// Creates a new  mailbox for an actor.
///
/// # Arguments
///
/// * `buffer` - The size of the mailbox buffer.
///
/// # Returns
///
/// Returns a tuple of (sender, receiver) for the actor's mailbox.
///
pub fn mailbox<A: Actor>(buffer: usize) -> Mailbox<A> {
    mpsc::channel(buffer)
}

/// Helper struct for actor message handling.
pub struct HandlerHelper<A: Actor> {
    sender: MailboxSender<A>,
}

impl<A: Actor> HandlerHelper<A> {
    /// Creates a new HandlerHelper.
    ///
    /// # Arguments
    ///
    /// * `sender` - The mailbox sender for the actor.
    ///
    pub fn new(sender: MailboxSender<A>) -> Self {
        debug!("Creating new handle reference.");
        Self { sender }
    }

    /// Sends a message to the actor without expecting a response (fire-and-forget).
    /// This is the "tell" pattern in actor terminology.
    ///
    /// # Arguments
    ///
    /// * `sender` - The path of the actor sending the message.
    /// * `message` - The message to send.
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if the message was queued successfully.
    ///
    /// # Errors
    ///
    /// Returns Error::Send if the actor's mailbox is closed or full.
    ///
    pub(crate) async fn tell(&self, sender: ActorPath, message: A::Message) -> Result<(), Error> {
        debug!("Telling message to actor from handle reference.");
        let msg = ActorMessage::new(sender, message, None);
        if let Err(error) = self.sender.send(msg).await {
            debug!("Failed to tell message! {}", error.to_string());
            Err(Error::SendMessage(error.to_string()))
        } else {
            debug!("Message sent successfully.");
            Ok(())
        }
    }

    /// Sends a message to the actor and waits for a response (request-response).
    /// This is the "ask" pattern in actor terminology.
    ///
    /// # Arguments
    ///
    /// * `sender` - The path of the actor sending the message.
    /// * `message` - The message to send.
    ///
    /// # Returns
    ///
    /// Returns the actor's response if successful.
    ///
    /// # Errors
    ///
    /// Returns Error::Send if the message couldn't be sent or if
    /// the response channel was closed before receiving a response.
    ///
    pub(crate) async fn ask(
        &self,
        sender: ActorPath,
        message: A::Message,
    ) -> Result<A::Response, Error> {
        debug!("Asking message to actor from handle reference.");
        let (response_sender, response_receiver) = oneshot::channel();
        let msg = ActorMessage::new(sender, message, Some(response_sender));
        if let Err(error) = self.sender.send(msg).await {
            error!("Failed to ask message! {}", error.to_string());
            Err(Error::SendMessage(error.to_string()))
        } else {
            response_receiver
                .await
                .map_err(|error| Error::SendMessage(error.to_string()))?
        }
    }
}

impl<A: Actor> Clone for HandlerHelper<A> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Actor, ActorPath, Event, Message, Response, system::Config};
    use tokio::time::{Duration, sleep};
    use tracing_test::traced_test;

    struct TestActor;

    impl Message for String {}

    impl Response for String {}

    impl Event for () {}

    #[async_trait::async_trait]
    impl Actor for TestActor {
        type Message = String;
        type Response = String;
        type Event = ();

        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _sender: &ActorPath,
            msg: Self::Message,
        ) -> Result<Self::Response, Error> {
            Ok(format!("Received: {}", msg))
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_tell_and_ask() {
        let (sender, mut receiver) = mailbox::<TestActor>(10);
        let handler = HandlerHelper::new(sender);

        // Spawn a task to process messages
        tokio::spawn(async move {
            let mut actor = TestActor;
            let config = Config::default();
            let mut ctx = ActorContext::new(
                ActorPath::from("test_actor"),
                // Dummy supervision handler
                crate::system::Supervisor::default(),
                tokio::sync::broadcast::channel(10).0,
                None,
                &config,
            );
            while let Some(msg) = receiver.recv().await {
                msg.handle(&mut actor, &mut ctx).await;
            }
        });

        // Test tell
        handler
            .tell(ActorPath::from("sender_actor"), "Hello".to_string())
            .await
            .unwrap();

        // Allow some time for the message to be processed
        sleep(Duration::from_millis(100)).await;

        // Test ask
        let response = handler
            .ask(ActorPath::from("sender_actor"), "World".to_string())
            .await
            .unwrap();
        assert_eq!(response, "Received: World");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_concurrent_messages() {
        let (sender, mut receiver) = mailbox::<TestActor>(100);
        let handler = HandlerHelper::new(sender);

        // Spawn a task to process messages
        tokio::spawn(async move {
            let mut actor = TestActor;
            let config = Config::default();
            let mut ctx = ActorContext::new(
                ActorPath::from("test_actor"),
                crate::system::Supervisor::default(),
                tokio::sync::broadcast::channel(100).0,
                None,
                &config,
            );
            while let Some(msg) = receiver.recv().await {
                msg.handle(&mut actor, &mut ctx).await;
            }
        });

        // Send multiple messages concurrently
        let mut handles = vec![];
        for i in 0..20 {
            let handler_clone = handler.clone();
            let handle = tokio::spawn(async move {
                handler_clone
                    .ask(ActorPath::from("sender"), format!("Message {}", i))
                    .await
            });
            handles.push(handle);
        }

        // Wait for all responses
        for (i, handle) in handles.into_iter().enumerate() {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), format!("Received: Message {}", i));
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_handler_clone() {
        let (sender, _receiver) = mailbox::<TestActor>(10);
        let handler = HandlerHelper::new(sender);
        let cloned = handler.clone();

        // Both handlers should be able to send messages
        let result1 = handler
            .tell(ActorPath::from("sender"), "Test1".to_string())
            .await;
        let result2 = cloned
            .tell(ActorPath::from("sender"), "Test2".to_string())
            .await;

        assert!(result1.is_ok());
        assert!(result2.is_ok());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_handler_errors() {
        let (sender, receiver) = mailbox::<TestActor>(1); // Small buffer to trigger send failure
        let handler = HandlerHelper::new(sender);

        drop(receiver); // Drop receiver to close the channel

        let result = handler
            .tell(ActorPath::from("sender"), "Test".to_string())
            .await;
        assert!(result.is_err());

        let result = handler
            .ask(ActorPath::from("sender"), "Test".to_string())
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_actor_message_debug() {
        let msg = ActorMessage::<TestActor>::new(
            ActorPath::from("sender"),
            "Test message".to_string(),
            None,
        );
        let debug_str = format!("{:?}", msg);
        assert!(debug_str.contains("ActorMessage"));
        assert!(debug_str.contains("sender"));
        assert!(debug_str.contains("<message>"));
        assert!(debug_str.contains("None"));
    }

    #[tokio::test]
    #[traced_test]
    #[serial_test::serial]
    async fn test_actor_message_sender_error() {
        let (sender, receiver) = oneshot::channel::<Result<String, Error>>();
        let msg = ActorMessage::<TestActor>::new(
            ActorPath::from("sender"),
            "Test message".to_string(),
            Some(sender),
        );
        drop(receiver); // Drop receiver to close the channel
        let mut ctx = ActorContext::new(
            ActorPath::from("test_actor"),
            crate::system::Supervisor::default(),
            tokio::sync::broadcast::channel(100).0,
            None,
            &Config::default(),
        );

        msg.handle(&mut TestActor, &mut ctx).await;

        // Check that the error was logged
        assert!(logs_contain("Failed to send response back to sender"));
    }
}
