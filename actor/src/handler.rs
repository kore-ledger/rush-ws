//

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
            error!("Failed to send response");
        }
        // ctx.should_stop()
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
    pub(crate) async fn tell(
        &self,
        sender: ActorPath,
        message: A::Message,
    ) -> Result<(), Error> {
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
/* 
    use super::*;
    use crate::{
        Actor, ActorContext, ActorPath, Error, Event, Message, Response,
    };
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use tokio::sync::broadcast;

    use async_trait::async_trait;

    #[derive(Debug)]
    struct TestActor {
        pub received_messages: Arc<Mutex<VecDeque<String>>>,
    }

    impl Message for String {}
    impl Event for () {}
    impl Response for String {}

    #[async_trait]
    impl Actor for TestActor {
        type Message = String;
        type Event = ();
        type Response = String;
        async fn handle<A: Actor>(
            &mut self,
            _ctx: &mut ActorContext<A>,
            _sender: &ActorPath,
            msg: Self::Message,
        ) -> Result<Self::Response, Error> {
            let mut messages = self.received_messages.lock().unwrap();
            messages.push_back(msg.clone());
            Ok(format!("Received: {}", msg))
        }
    }

    #[test]
    fn text_debug_actor_message() {
        let actor_path = ActorPath::from("test_actor");
        let (resp_sender, _resp_receiver) = oneshot::channel();
        let msg = ActorMessage::<TestActor>::new(
            actor_path.clone(),
            "Hello, Actor!".to_string(),
            Some(resp_sender),
        );
        let debug_str = format!("{:?}", msg);
        assert!(debug_str.contains("test_actor"));
        assert!(debug_str.contains("<message>"));
        assert!(debug_str.contains("<oneshot::Sender>"));
    }

    #[tokio::test]
    async fn test_tell_and_ask() {
        //tracing_subscriber::fmt::init();
        let (sender, mut receiver) = mailbox::<TestActor>(10);
        let (signal_sender, _signal_receiver) = mpsc::channel(10);
        let (event_sender, _event_receiver) =
            broadcast::channel::<<TestActor as Actor>::Event>(10);
        let handler = HandlerHelper::new(sender);
        let received_messages = Arc::new(Mutex::new(VecDeque::new()));
        let mut actor = TestActor {
            received_messages: received_messages.clone(),
        };
        let actor_path = ActorPath::from("test_actor");
        // Test tell
        handler
            .tell(actor_path.clone(), "Hello, Actor!".to_string())
            .await
            .unwrap();
        // Process the message
        if let Some(msg) = receiver.recv().await {
            msg.handle(
                &mut actor,
                &mut ActorContext::new(
                    actor_path.clone(),
                    None,
                    signal_sender.clone(),
                    event_sender.clone(),
                    None,
                ),
            )
            .await;
        }
        {
            let messages = received_messages.lock().unwrap();
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0], "Hello, Actor!");
        }
        // Test ask
        let value = actor_path.clone();
        tokio::spawn(async move {
            if let Some(msg) = receiver.recv().await {
                msg.handle(
                    &mut actor,
                    &mut ActorContext::new(
                        value,
                        None,
                        signal_sender.clone(),
                        event_sender.clone(),
                        None,
                    ),
                )
                .await;
            }
        });
        let response = handler
            .ask(actor_path.clone(), "How are you?".to_string())
            .await
            .unwrap();
        assert_eq!(response, "Received: How are you?");
        {
            let messages = received_messages.lock().unwrap();
            assert_eq!(messages.len(), 2);
            assert_eq!(messages[1], "How are you?");
        }
    }

    #[tokio::test]
    async fn test_tell_error() {
        //tracing_subscriber::fmt::init();
        let (sender, receiver) = mailbox::<TestActor>(10);
        //let (signal_sender, _signal_receiver) = mpsc::channel(10);
        let handler = HandlerHelper::new(sender);
        let actor_path = ActorPath::from("test_actor");
        // Drop the receiver to simulate closed mailbox
        drop(receiver);
        let result = handler
            .tell(actor_path.clone(), "Hello, Actor!".to_string())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ask_no_response() {
        //tracing_subscriber::fmt::init();
        let (sender, receiver) = mailbox::<TestActor>(10);
        let handler = HandlerHelper::new(sender);
        let received_messages = Arc::new(Mutex::new(VecDeque::new()));
        let _actor = TestActor {
            received_messages: received_messages.clone(),
        };
        let actor_path = ActorPath::from("test_actor");
        // Drop the receiver to simulate no response scenario
        drop(receiver);
        let result = handler
            .ask(actor_path.clone(), "Will I get a response?".to_string())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_message_handle_error() {
        //tracing_subscriber::fmt::init();
        struct ErrorActor;
        #[async_trait]
        impl Actor for ErrorActor {
            type Message = String;
            type Event = ();
            type Response = String;
            async fn handle<A: Actor>(
                &mut self,
                _ctx: &mut ActorContext<A>,
                _sender: &ActorPath,
                _msg: Self::Message,
            ) -> Result<Self::Response, Error> {
                Err(Error::SendMessage("Intentional error".to_string()))
            }
        }
        let (sender, mut receiver) = mailbox::<ErrorActor>(10);
        let (event_sender, _event_receiver) =
            broadcast::channel::<<TestActor as Actor>::Event>(10);
        let (signal_sender, _signal_receiver) = mpsc::channel(10);
        let handler = HandlerHelper::new(sender);
        let actor_path = ActorPath::from("error_actor");
        let value = actor_path.clone();
        tokio::spawn(async move {
            let mut actor = ErrorActor;
            if let Some(msg) = receiver.recv().await {
                let _ = msg
                    .handle(
                        &mut actor,
                        &mut ActorContext::new(
                            value,
                            None,
                            signal_sender.clone(),
                            event_sender.clone(),
                            None,
                        ),
                    )
                    .await;
            }
        });
        let result = handler
            .ask(actor_path.clone(), "Trigger error".to_string())
            .await;
        assert!(result.is_err());
    }
    */
}
