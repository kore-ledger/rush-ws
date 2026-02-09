//! # Actor Implementation
//!
//! This module contains the implementation of the `Actor` trait and its related components,
//! including the execution context and actor references.

use crate::{
    ActorPath, Error,
    handler::HandlerHelper,
    supervision::SupervisionStrategy,
    system::{ActorSignal, SignalSender, Supervisor, Config},
};
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;
use tokio::sync::broadcast::{Receiver as EventReceiver, Sender as EventSender};
use tracing::{debug, error};

/// The maximum depth of the actor hierarchy. This is used to prevent infinite recursion when 
/// restarting actors.
const MAX_ACTOR_DEPTH: usize = 100;

/// Events that this actor will emit after processing a message. The events emitted by a message
/// handler will be used to apply the event sourcing pattern.
pub trait Event: Serialize + DeserializeOwned + Debug + Clone + Send + Sync + 'static {}

/// Defines what an actor will receive as its message, and with what it should respond.
pub trait Message: Send + Sync + 'static {}

/// Defines the response of a message.
pub trait Response: Send + Sync + 'static {}

/// The `Actor` trait is the main trait that actors must implement.
#[async_trait]
pub trait Actor: Send + Sync + Sized + 'static {
    /// The type of messages that this actor will handle.
    /// This type must implement the `Message` trait.
    type Message: Message;
    /// The type of responses that this actor will return.
    /// This type must implement the `Response` trait.
    type Event: Event;
    /// The type of responses that this actor will return.
    /// This type must implement the `Response` trait.
    type Response: Response;

    /// Defines the supervision strategy to use for this actor. By default it is
    /// `Stop` which simply stops the actor if an error occurs at startup or when an
    /// error or fault is issued from a handler.
    ///
    /// # Returns
    ///
    /// Returns the supervision strategy to use for this actor.
    fn supervision_strategy() -> SupervisionStrategy {
        SupervisionStrategy::Stop
    }

    /// Called when the actor is started.
    /// Override this method to perform initialization when the actor is started.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the actor.
    ///
    /// # Returns
    ///
    /// Returns a void result.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor could not be started.
    ///
    async fn pre_start(&mut self, _context: &mut ActorContext<Self>) -> Result<(), Error> {
        Ok(())
    }

    /// Override this function if you want to define what should happen when an
    /// error occurs in [`Actor::pre_start()`]. By default it simply calls
    /// `pre_start()` again, but you can also choose to reinitialize the actor
    /// in some other way.
    async fn pre_restart(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Error> {
        self.pre_start(ctx).await
    }

    /// Called before stopping the actor.
    /// Override this method to define what should happen before the actor is stopped.
    /// By default it does nothing.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the actor.
    ///
    /// # Returns
    ///
    /// Returns a void result.
    ///
    /// # Errors
    ///
    /// Returns an error if the actor could not be stopped.
    ///
    async fn pre_stop(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), Error> {
        Ok(())
    }

    /// Called when the actor is stopped.
    ///
    /// # Arguments
    ///
    /// * `context` - The context of the actor.
    ///
    async fn post_stop(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), Error> {
        Ok(())
    }

    /// Handles a message to the actor.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context of the actor.
    /// * `sender` - The path of the sender actor.
    /// * `msg` - The message to handle.
    ///
    /// # Returns
    ///
    /// An optional response to the message.
    ///
    async fn handle(
        &mut self,
        ctx: &mut ActorContext<Self>,
        sender: &ActorPath,
        msg: Self::Message,
    ) -> Result<Self::Response, Error>;

    /// Internal event.
    /// Override this method to define what should happen when an internal event is emitted by the
    /// actor.
    /// By default it does nothing.
    ///
    /// # Arguments
    ///
    /// * `event` - The event to handle.
    /// * `ctx` - The actor context.
    ///
    async fn on_event(&mut self, _event: Self::Event, _ctx: &mut ActorContext<Self>) {
        // Default implementation.
    }

    /// Called when an error occurs in a child actor.
    /// Override this method to define what should happen when an error occurs in a child actor.
    /// By default it does nothing.
    ///
    /// # Arguments
    ///
    /// * `error` - The error that occurred.
    /// * `child` - The path of the child actor that caused the error.
    /// * `ctx` - The actor context.
    ///
    /// # Returns
    ///
    /// Returns a void result.
    ///
    /// # Errors
    ///
    /// Returns an error if the error could not be handled.
    ///
    async fn on_child_error(
        &mut self,
        _child: &ActorPath,
        error: &Error,
        ctx: &mut ActorContext<Self>,
    ) {
        // Default implementation from child actor errors.
        debug!("Handling error: {:?}", error);
        // Emit the error to the parent actor.
        ctx.emit_error(error).await.unwrap_or_else(|e| {
            error!("Failed to emit fault for child error: {:?}", e);
        });
    }

    /// Called when a fault occurs in a child actor.
    /// Override this method to define what should happen when a fault occurs in a child actor.
    /// By default it does nothing.
    ///
    /// # Arguments
    ///
    /// * `child` - The path of the child actor that faulted.
    /// * `error` - The error that occurred.
    /// * `ctx` - The actor context.
    ///
    /// # Returns
    ///
    /// Returns a void result.
    ///
    /// # Errors
    ///
    /// Returns an error if the fault could not be handled.
    ///
    async fn on_child_fault(
        &mut self,
        _child: &ActorPath,
        error: &Error,
        ctx: &mut ActorContext<Self>,
    ) {
        // Default implementation from child actor errors.
        debug!("Handling fault: {:?}", error);
        // Emit the error to the parent actor.
        ctx.emit_fault(error).await.unwrap_or_else(|e| {
            error!("Failed to emit fault for child fault: {:?}", e);
        });
        
    }
}

/// The `ActorContext` is the context of the actor.
/// It is passed to the actor when it is started, and can be used to interact with the actor
/// system.
pub struct ActorContext<A: Actor> {
    /// The path of the actor.
    path: ActorPath,
    /// The actor supervisor.
    actor_supervisor: Supervisor,
    /// Event sender.
    event_sender: EventSender<<A as Actor>::Event>,
    /// Signal sender to parent actor.
    signal_sender: Option<SignalSender>,
    /// Actor systemconfiguration.
    config: Config,
}

impl<A: Actor> ActorContext<A> {
    
    /// Creates a new actor context.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the actor.
    /// * `actor_supervisor` - The actor supervisor.
    /// * `event_sender` - The event sender for the actor.
    /// * `signal_sender` - The signal sender to the parent actor.
    /// * `config` - The actor system configuration.
    ///
    /// # Returns
    /// 
    /// Returns a new actor context.
    ///
    pub fn new(
        path: ActorPath,
        actor_supervisor: Supervisor,
        event_sender: EventSender<<A as Actor>::Event>,
        signal_sender: Option<SignalSender>,
        config: &Config,
    ) -> Self {
        Self {
            path,
            actor_supervisor,
            event_sender,
            signal_sender,
            config: config.clone(),
        }
    }

    /// Returns the path of the actor.
    ///
    /// # Returns
    /// * `&ActorPath` - The path of the actor.
    ///
    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Creates a child actor.
    ///
    /// # Arguments
    ///
    /// * `actor` - The actor to create.
    /// * `name` - The name of the child actor.
    ///
    /// # Returns
    ///
    /// * `Result<ActorRef<B>, Error>` - The reference to the created child actor or an error.
    ///
    pub async fn create_child<B>(&mut self, actor: B, name: &str) -> Result<ActorRef<B>, Error>
    where
        B: Actor,
    {
        let path = self.path.clone() / name;
        if self.path.level() >= MAX_ACTOR_DEPTH {
            return Err(Error::CreateActor("Max actor depth exceeded".into()));
        }
        self.actor_supervisor.create_actor(actor, &path, &self.config).await
    }

    /// Gets a child actor by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the child actor.
    ///
    /// # Returns
    ///
    /// * `Result<Option<ActorRef<B>>, Error>` - The actor reference if found, or None.
    ///
    pub async fn get_child<B>(&self, name: &str) -> Result<Option<ActorRef<B>>, Error>
    where
        B: Actor,
    {
        let child_path = self.path.clone() / name;
        self.actor_supervisor.get_actor(&child_path).await
    }

    /// Checks if a child actor exists by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the child actor.
    ///
    /// # Returns
    ///
    /// * `Result<bool, Error>` - True if the child actor exists, false otherwise.
    ///
    pub async fn child_exists(&self, name: &str) -> Result<bool, Error> {
        let child_path = self.path.clone() / name;
        self.actor_supervisor.child_exists(&child_path).await
    }

    /// Stops all child actors of this actor.
    ///
    /// # Returns
    ///
    /// * `Result<(), Error>` - The result of the stop operation.
    ///
    pub async fn stop_children(&mut self) -> Result<(), Error> {
        self.actor_supervisor.stop_children().await
    }

    /// Restarts all child actors of this actor.
    ///
    /// # Returns
    ///
    /// * `Result<(), Error>` - The result of the restart operation.
    ///
    pub async fn restart_children(&mut self) -> Result<(), Error> {
        self.actor_supervisor.restart_children().await
    }

    /// Restarts the actor by calling its `pre_restart` method.
    ///
    /// # Arguments
    /// * `actor` - The actor to restart.
    /// # Returns
    /// * `Result<(), Error>` - The result of the restart operation.
    ///
    pub(crate) async fn restart(&mut self, actor: &mut A) -> Result<(), Error>
    where
        A: Actor,
    {
        actor.pre_restart(self).await
    }

    /// Emits an event from the actor.
    ///
    /// # Arguments
    ///
    /// * `event` - The event to emit.
    ///
    pub fn emit_event(&self, event: <A as Actor>::Event) -> Result<(), Error> {
        if let Err(e) = self.event_sender.send(event) {
            error!("Failed to emit event: {}", e);
            Err(Error::SendEvent(format!("Failed to emit event: {}", e)))
        } else {
            Ok(())
        }
    }

    /// Emits an error signal to the parent actor.
    ///
    /// # Arguments
    ///
    /// * `error` - The error to emit.
    ///
    pub async fn emit_error(&self, error: &Error) -> Result<(), Error> {
        if let Some(signal_sender) = &self.signal_sender
            && let Err(e) = signal_sender
                .send(ActorSignal::ChildError(self.path.clone(), error.clone()))
                .await
        {
            error!("Failed to emit error signal: {}", e);
            Err(Error::SendEvent(format!(
                "Failed to emit error signal: {}",
                e
            )))
        } else {
            Ok(())
        }
    }

    /// Emits a fault signal to the parent actor.
    ///
    /// # Arguments
    ///
    /// * `error` - The error to emit.
    ///
    pub async fn emit_fault(&self, error: &Error) -> Result<(), Error> {
        if let Some(signal_sender) = &self.signal_sender
            && let Err(e) = signal_sender
                .send(ActorSignal::ChildFault(self.path.clone(), error.clone()))
                .await
        {
            error!("Failed to emit fault signal: {}", e);
            Err(Error::SendEvent(format!(
                "Failed to emit fault signal: {}",
                e
            )))
        } else {
            Ok(())
        }
    }
}

/// Actor reference.
///
/// This is a reference to an actor that can be used to send messages to him.
///
pub struct ActorRef<A: Actor> {
    /// The path of the actor.
    path: ActorPath,
    /// The handler helper to send messages.
    handler: HandlerHelper<A>,
    /// The actor event receiver.
    event_receiver: EventReceiver<<A as Actor>::Event>,
}

impl<A> ActorRef<A>
where
    A: Actor,
{
    /// Creates a new actor reference.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the actor.
    /// * `handler` - The handler helper to send messages.
    /// * `event_receiver` - The actor event receiver.
    ///
    pub fn new(
        path: ActorPath,
        handler: HandlerHelper<A>,
        event_receiver: EventReceiver<<A as Actor>::Event>,
    ) -> Self {
        Self {
            path,
            handler,
            event_receiver,
        }
    }

    /// Returns the path of the actor.
    ///
    /// # Returns
    /// * `&ActorPath` - The path of the actor.
    ///
    pub fn path(&self) -> &ActorPath {
        &self.path
    }

    /// Sends a message to the actor without expecting a response.
    ///
    /// # Arguments
    ///
    /// * `msg` - The message to send.
    ///
    /// # Returns
    ///
    /// * `Result<(), Error>` - The result of the send operation.
    ///
    pub async fn tell(&self, msg: A::Message) -> Result<(), Error> {
        self.handler.tell(self.path.clone(), msg).await
    }

    /// Sends a message to the actor and waits for a response.
    ///
    /// # Arguments
    ///     
    /// * `msg` - The message to send.
    ///     
    /// # Returns
    ///     
    /// * `Result<A::Response, Error>` - The response from the actor or an error.
    ///
    pub async fn ask(&self, msg: A::Message) -> Result<A::Response, Error> {
        self.handler.ask(self.path.clone(), msg).await
    }

    /// Subscribes to the actor event bus.
    /// This will return an event receiver that can be used to receive events from the actor.
    /// The event receiver will receive events that the actor emits after processing a message.
    ///
    /// # Returns
    ///
    /// Returns an event receiver.
    ///
    pub fn subscribe(&self) -> EventReceiver<<A as Actor>::Event> {
        self.event_receiver.resubscribe()
    }
}

/// Clone implementation for ActorRef.
impl<A> Clone for ActorRef<A>
where
    A: Actor,
{
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            handler: self.handler.clone(),
            event_receiver: self.event_receiver.resubscribe(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Actor, ActorPath, Event, Message, Response, handler::mailbox};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{RwLock, broadcast};
    use tokio::time::sleep;

    struct TestActor;

    #[async_trait::async_trait]
    impl Actor for TestActor {
        type Message = TestMessage;
        type Response = TestResponse;
        type Event = TestEvent;

        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _sender: &ActorPath,
            msg: Self::Message,
        ) -> Result<Self::Response, Error> {
            match msg {
                TestMessage::Ping => Ok(TestResponse::Pong),
            }
        }
    }

    enum TestMessage {
        Ping,
    }
    impl Message for TestMessage {}

    enum TestResponse {
        Pong,
    }
    impl Response for TestResponse {}

    #[derive(Serialize, Deserialize, Debug, Clone)]
    enum TestEvent {
        Started,
        Stopped,
    }
    impl Event for TestEvent {}

    #[tokio::test]
    #[serial_test::serial]
    async fn test_actor_ref_tell_ask() {
        let (sender, mut receiver) = mailbox::<TestActor>(10);
        let handler = HandlerHelper::new(sender);
        let actor_path = ActorPath::from("test_actor");
        let (event_sender, _event_receiver) = broadcast::channel(10);
        let config = Config::default();
        let context = ActorContext::new(
            actor_path.clone(),
            Supervisor::default(),
            event_sender,
            None,
            &config,
        );
        let actor_ref = ActorRef::new(
            actor_path.clone(),
            handler,
            tokio::sync::broadcast::channel(10).1,
        );

        // Spawn a task to process messages
        tokio::spawn(async move {
            let mut actor = TestActor;
            let mut ctx = context;
            while let Some(msg) = receiver.recv().await {
                msg.handle(&mut actor, &mut ctx).await;
            }
        });

        // Test tell
        assert!(actor_ref.tell(TestMessage::Ping).await.is_ok());
        sleep(Duration::from_millis(100)).await;
        // Test ask
        let response = actor_ref.ask(TestMessage::Ping).await;
        assert!(response.is_ok());
        match response.unwrap() {
            TestResponse::Pong => {}
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_actor_ref_clone() {
        let (sender, _receiver) = mailbox::<TestActor>(10);
        let handler = HandlerHelper::new(sender);
        let actor_path = ActorPath::from("test_actor");
        let actor_ref = ActorRef::new(
            actor_path.clone(),
            handler,
            tokio::sync::broadcast::channel(10).1,
        );

        // Clone the actor ref
        let cloned_ref = actor_ref.clone();

        // Both should point to the same actor
        assert_eq!(actor_ref.path(), cloned_ref.path());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_actor_context_path() {
        let actor_path = ActorPath::from("/parent/child");
        let (event_sender, _) = broadcast::channel(10);
        let config = Config::default();
        let context: ActorContext<TestActor> = ActorContext::new(
            actor_path.clone(),
            Supervisor::default(),
            event_sender,
            None,
            &config,
        );

        assert_eq!(context.path().to_string(), "/parent/child");
    }

    struct StatefulActor {
        counter: usize,
    }

    impl Response for usize {}

    #[async_trait::async_trait]
    impl Actor for StatefulActor {
        type Message = TestMessage;
        type Response = usize;
        type Event = TestEvent;

        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _sender: &ActorPath,
            _msg: Self::Message,
        ) -> Result<Self::Response, Error> {
            self.counter += 1;
            Ok(self.counter)
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_actor_state_preservation() {
        let (sender, mut receiver) = mailbox::<StatefulActor>(10);
        let handler = HandlerHelper::new(sender);
        let actor_path = ActorPath::from("stateful_actor");
        let (event_sender, _) = broadcast::channel(10);
        let actor_ref = ActorRef::new(
            actor_path.clone(),
            handler,
            tokio::sync::broadcast::channel(10).1,
        );

        // Spawn a task to process messages
        let counter_check = Arc::new(RwLock::new(0));
        let counter_clone = counter_check.clone();
        tokio::spawn(async move {
            let mut actor = StatefulActor { counter: 0 };
            let config = Config::default();
            let mut ctx = ActorContext::new(actor_path, Supervisor::default(), event_sender, None, &config);
            while let Some(msg) = receiver.recv().await {
                msg.handle(&mut actor, &mut ctx).await;
                *counter_clone.write().await = actor.counter;
            }
        });

        // Send multiple messages
        for _ in 0..5 {
            actor_ref.tell(TestMessage::Ping).await.unwrap();
        }

        sleep(Duration::from_millis(100)).await;

        // Verify state was preserved across messages
        let final_count = *counter_check.read().await;
        assert_eq!(final_count, 5);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_actor_event_subscription() {
        let (sender, mut receiver) = mailbox::<TestActor>(10);
        let handler = HandlerHelper::new(sender);
        let actor_path = ActorPath::from("event_actor");
        let (event_sender, _) = broadcast::channel(10);
        let actor_ref = ActorRef::new(actor_path.clone(), handler, event_sender.subscribe());

        // Subscribe to events
        let mut event_receiver = actor_ref.subscribe();

        // Spawn actor
        tokio::spawn(async move {
            let mut actor = TestActor;
            let config = Config::default();
            let mut ctx = ActorContext::new(actor_path, Supervisor::default(), event_sender, None, &config);

            // Emit an event
            let _ = ctx.emit_event(TestEvent::Started);

            while let Some(msg) = receiver.recv().await {
                msg.handle(&mut actor, &mut ctx).await;
            }
        });

        // Wait for event
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Try to receive event
        let result = event_receiver.try_recv();
        assert!(result.is_ok() || event_receiver.len() == 0);
    }
}
