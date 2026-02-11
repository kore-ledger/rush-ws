//! # Actor System
//!
//! This module provides the main actor system, responsible for
//! managing actors, supervision and lifecycle signals.

use crate::{Actor, ActorContext, ActorPath, ActorRef, Error, runner::ActorRunner};
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use std::{any::Any, collections::HashMap, sync::Arc};

/// Actions that can be taken on a child actor.
pub enum ChildAction {
    Stop,
    Restart,
}

/// Type aliases for action sender.
pub type ActionSender = mpsc::Sender<ChildAction>;
/// Type aliases for action receiver.
pub type ActionReceiver = mpsc::Receiver<ChildAction>;
/// Creates a new action channel for child actor management.
pub fn action_channel(buffer: usize) -> (ActionSender, ActionReceiver) {
    mpsc::channel(buffer)
}

/// Actor signals for supervision and lifecycle management.
pub enum ActorSignal {
    /// Signal indicating a child actor encountered an error.
    ChildError(ActorPath, Error),
    /// Signal indicating a child actor encountered a fault.
    ChildFault(ActorPath, Error),
}

impl ActorSignal {
    /// Handles the actor signal by invoking the appropriate handler on the parent actor.
    ///
    /// # Arguments
    ///
    /// * `actor` - The parent actor that will handle the signal.
    /// * `ctx` - The actor context for the parent actor.
    ///
    pub async fn handle<A: Actor>(&self, actor: &mut A, ctx: &mut ActorContext<A>) {
        match self {
            ActorSignal::ChildFault(path, error) => {
                actor.on_child_fault(path, error, ctx).await;
            }
            ActorSignal::ChildError(path, error) => {
                actor.on_child_error(path, error, ctx).await;
            }
        }
    }
}

/// Type aliases for signal sender.
pub type SignalSender = mpsc::Sender<ActorSignal>;
/// Type aliases for signal receiver.
pub type SignalReceiver = mpsc::Receiver<ActorSignal>;

/// Creates a new signal channel for actor supervision.
///
/// # Arguments
///
/// * `buffer` - The size of the signal channel buffer.
///
/// # Returns
///
/// Returns a tuple of (sender, receiver) for the signal channel.
///
pub fn signal_channel(buffer: usize) -> (SignalSender, SignalReceiver) {
    mpsc::channel(buffer)
}

/// The actor system responsible for managing actors and supervision.
///
#[derive(Clone)]
pub struct System {
    root_path: ActorPath,
    system_supervisor: Supervisor,
    config: Config,
}

impl System {
    /// Creates a new actor system with an empty registry and system handler.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration for the actor system, including mailbox and buffer sizes.
    /// * `cancellation_token` - The cancellation token for the system.
    ///
    /// # Returns
    ///
    /// * `System` - The newly created actor system.
    ///
    pub fn new(config: Config, cancellation_token: CancellationToken) -> Self {
        let registry: ActorRegistry = Arc::new(RwLock::new(HashMap::new()));
        let (child_signal_sender, child_signal_receiver) =
            signal_channel(config.signal_buffer_size);
        let system_supervisor = Supervisor::new(registry, child_signal_sender);
        let root_path = ActorPath::from("/user");
        let system = System {
            system_supervisor,
            root_path: root_path.clone(),
            config,
        };
        let system_runner =
            SystemRunner::new(system.clone(), child_signal_receiver, cancellation_token);
        system_runner.run();
        debug!("Actor system created with root path: {:?}", root_path);
        system
    }

    /// Creates a new child actor under the system's supervision.
    ///
    /// # Arguments
    ///     
    /// * `actor` - The actor instance to create.
    /// * `name` - The name of the child actor.
    ///
    /// # Returns
    ///     
    /// * `Result<ActorRef<A>, Error>` - The reference to the created actor or an error.
    ///
    pub async fn create_actor<A>(&mut self, actor: A, name: &str) -> Result<ActorRef<A>, Error>
    where
        A: Actor,
    {
        let path = self.root_path.clone() / name;
        self.system_supervisor
            .create_actor(actor, &path, &self.config)
            .await
    }

    /// Retrieves a child actor by name.
    ///
    /// # Arguments
    ///     
    /// * `name` - The name of the child actor to retrieve.
    ///
    /// # Returns
    ///     
    /// * `Result<Option<ActorRef<A>>, Error>` - The actor reference if found, or None.
    ///
    pub async fn get_actor<A>(&self, name: &str) -> Result<Option<ActorRef<A>>, Error>
    where
        A: Actor,
    {
        let child_path = self.root_path.clone() / name;
        self.system_supervisor.get_actor(&child_path).await
    }

    /// Checks if a child actor exists by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the  actor to check.
    ///
    /// # Returns
    ///
    /// * `Result<bool, Error>` - True if the child exists, false otherwise.
    ///
    pub async fn actor_exists(&self, name: &str) -> Result<bool, Error> {
        let child_path = self.root_path.clone() / name;
        self.system_supervisor.child_exists(&child_path).await
    }

    /// Handles a child actor error signal.
    ///     
    /// # Arguments
    ///
    /// * `path` - The path of the child actor that encountered the error.
    /// * `error` - The error encountered by the child actor.
    ///
    pub async fn on_child_error(&mut self, path: &ActorPath, error: &Error) -> Result<(), Error> {
        error!("System received ChildError from {:?}: {:?}", path, error);
        self.system_supervisor.stop_children().await?;
        Ok(())
    }

    /// Handles a child actor fault signal.
    ///     
    /// # Arguments
    ///
    /// * `path` - The path of the child actor that encountered the fault.
    /// * `error` - The fault encountered by the child actor.
    ///
    pub async fn on_child_fault(&mut self, path: &ActorPath, error: &Error) -> Result<(), Error> {
        error!("System received ChildFault from {:?}: {:?}", path, error);
        self.system_supervisor.stop_children().await?;
        Ok(())
    }

    /// Stops all child actors under the system's supervision.
    ///
    pub async fn stop_children(&mut self) -> Result<(), Error> {
        debug!("System stopped all actors.");
        self.system_supervisor.stop_children().await
    }
}

/// Runner for the actor system to handle signals and lifecycle events.
///
struct SystemRunner {
    /// The actor system instance to manage.
    system: System,
    signal_receiver: SignalReceiver,
    cancellation_token: CancellationToken,
}

impl SystemRunner {
    pub fn new(
        system: System,
        signal_receiver: SignalReceiver,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            system,
            signal_receiver,
            cancellation_token,
        }
    }

    pub fn run(mut self) {
        debug!(
            "SystemRunner started for actor system with root path: {:?}",
            self.system.root_path
        );
        let mut receiver = self.signal_receiver;
        // Spawn a task to handle system-level signals
        tokio::spawn(async move {
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    debug!("SystemRunner received cancellation signal, shutting down.");
                    let _ = self.system.stop_children().await;
                }
                Some(signal) = receiver.recv() => {
                    debug!("SystemRunner received signal.");
                    // Handle system-level signals here
                    match signal {
                        ActorSignal::ChildError(path, error) => {
                            let _ = self.system.on_child_error(&path, &error).await;
                        }
                        ActorSignal::ChildFault(path, error) => {
                            let _ = self.system.on_child_fault(&path, &error).await;
                        }
                    }
                }
            }
        });
    }
}

/// Type alias for the actor registry.
pub type ActorRegistry = Arc<RwLock<HashMap<ActorPath, Box<dyn Any + Send + Sync + 'static>>>>;
/// Type alias for action senders registry.
pub type ActionSendersRegistry = Arc<RwLock<HashMap<ActorPath, ActionSender>>>;

/// Supervision handler for managing child actors.
///
#[derive(Clone)]
pub struct Supervisor {
    /// The actor registry for managing child actors.
    registry: ActorRegistry,
    /// The action senders registry for managing child actors.
    action_senders: ActionSendersRegistry,
    /// The signal sender to share with child actors.
    child_signal_sender: SignalSender,
}

impl Supervisor {
    /// Creates a new supervision handler.
    ///
    /// # Arguments
    ///
    /// * `registry` - The actor registry for managing child actors.
    /// * `child_signal_sender` - The signal sender for child actors.
    ///
    /// # Returns
    ///
    /// * `Supervisor` - The newly created supervision handler.
    ///
    pub fn new(registry: ActorRegistry, child_signal_sender: SignalSender) -> Self {
        Self {
            action_senders: Arc::new(RwLock::new(HashMap::new())),
            child_signal_sender,
            registry,
        }
    }

    /// Creates a new child actor under supervision.
    ///
    /// # Arguments
    ///    
    /// * `actor` - The actor instance to create.
    /// * `path` - The path for the new child actor.
    ///
    /// # Returns
    ///
    /// * `Result<ActorRef<A>, Error>` - The reference to the created actor or an error.
    ///
    pub async fn create_actor<A>(
        &mut self,
        actor: A,
        path: &ActorPath,
        conf: &Config,
    ) -> Result<ActorRef<A>, Error>
    where
        A: Actor,
    {
        // Check if an actor with the same name already exists
        if self.registry.read().await.contains_key(path) {
            return Err(Error::Supervision(format!(
                "Actor with path '{}' already exists.",
                path
            )));
        }

        // Contruct the actor runner and reference
        let (mut runner, actor_ref, action_sender) = ActorRunner::new(
            actor,
            path.clone(),
            Some(self.child_signal_sender.clone()),
            self.registry.clone(),
            conf,
        );

        // Insert the actor reference into the registry before initialization
        // to allow for recursive actor creation
        self.registry
            .write()
            .await
            .insert(path.clone(), Box::new(actor_ref.clone()));

        // Init the actor
        let (init_sender, init_receiver) = oneshot::channel();
        tokio::spawn(async move {
            runner.init(Some(init_sender)).await;
        });

        // Wait for initialization to complete
        let init_result = init_receiver
            .await
            .expect("Sender should not be dropped before initialization completes or fails.");

        if let Err(e) = init_result {
            // Remove the actor reference from the registry if initialization fails
            self.registry.write().await.remove(path);
            return Err(e);
        }

        // Insert the child into supervision
        self.action_senders
            .write()
            .await
            .insert(path.clone(), action_sender);
        debug!("Actor '{}' created successfully.", path);
        Ok(actor_ref)
    }

    /// Retrieves a child actor by name.
    ///
    /// # Arguments
    ///     
    /// * `path` - The path of the child actor to retrieve.
    ///
    /// # Returns
    ///
    /// * `Result<Option<ActorRef<A>>, Error>` - The actor reference if found, or None.
    ///
    pub async fn get_actor<A>(&self, path: &ActorPath) -> Result<Option<ActorRef<A>>, Error>
    where
        A: Actor,
    {
        self.registry
            .read()
            .await
            .get(path)
            .and_then(|any| any.downcast_ref::<ActorRef<A>>().cloned())
            .ok_or_else(|| Error::Supervision(format!("Actor '{}' not found.", path)))
            .map(Some)
    }

    /// Checks if a child actor exists by name.
    ///
    /// # Arguments
    ///     
    /// * `path` - The path of the child actor to check.
    ///
    /// # Returns
    ///     
    /// * `Result<bool, Error>` - True if the child exists, false otherwise.
    ///
    pub async fn child_exists(&self, path: &ActorPath) -> Result<bool, Error> {
        Ok(self.registry.read().await.contains_key(path))
    }

    /// Restarts a child actor by name.
    ///
    /// # Arguments
    ///     
    /// * `path` - The path of the child actor to restart.
    ///
    /// # Returns
    ///    
    /// * `Result<(), Error>` - Ok if restarted successfully, error otherwise.
    ///
    pub async fn restart_child(&self, path: &ActorPath) -> Result<(), Error> {
        if let Some(action_sender) = self.action_senders.read().await.get(path) {
            action_sender
                .send(ChildAction::Restart)
                .await
                .map_err(|e| {
                    Error::Supervision(format!(
                        "Failed to send restart action to child '{}': {:?}",
                        path, e
                    ))
                })?;
            Ok(())
        } else {
            Err(Error::Supervision(format!(
                "Child actor '{}' not found for restarting.",
                path
            )))
        }
    }

    /// Stops all child actors under supervision.
    ///
    /// # Returns
    ///
    /// * `Result<(), Error>` - Ok if all children stopped successfully, error otherwise.
    ///
    pub async fn stop_children(&mut self) -> Result<(), Error> {
        let action_senders = self.action_senders.read().await;
        for (path, action_sender) in action_senders.iter() {
            if let Err(e) = action_sender.send(ChildAction::Stop).await {
                error!("Failed to send stop action to child '{}': {:?}", path, e);
                return Err(Error::Supervision(format!(
                    "Failed to send stop action to child '{}': {:?}",
                    path, e
                )));
            } else {
                let _ = self.registry.write().await.remove(path);
            }
        }
        Ok(())
    }

    /// Restarts all child actors under supervision.
    ///
    /// # Returns
    ///
    /// * `Result<(), Error>` - Ok if all children restarted successfully, error otherwise.
    ///
    pub async fn restart_children(&self) -> Result<(), Error> {
        let action_senders = self.action_senders.read().await;
        for (path, action_sender) in action_senders.iter() {
            if let Err(e) = action_sender.send(ChildAction::Restart).await {
                error!("Failed to send restart action to child '{}': {:?}", path, e);
                return Err(Error::Supervision(format!(
                    "Failed to send restart action to child '{}': {:?}",
                    path, e
                )));
            }
        }
        Ok(())
    }
}

/// Default implementation for Supervisor with an empty registry and a dummy signal sender.
impl Default for Supervisor {
    fn default() -> Self {
        let registry: ActorRegistry = Arc::new(RwLock::new(HashMap::new()));
        let (child_signal_sender, _child_signal_receiver) = signal_channel(10);
        Supervisor::new(registry, child_signal_sender)
    }
}

/// Configuration for the actor system, including mailbox and buffer sizes.
#[derive(Clone)]
pub struct Config {
    /// The size of the mailbox for each actor.
    pub mailbox_size: usize,
    /// The size of the event buffer for each actor.
    pub event_buffer_size: usize,
    /// The size of the signal buffer for each actor.
    pub signal_buffer_size: usize,
    /// The size of the action buffer for each actor.
    pub action_buffer_size: usize,
}

/// Default configuration for the actor system with reasonable defaults for mailbox and buffer
/// sizes.
impl Default for Config {
    fn default() -> Self {
        Self {
            mailbox_size: 10_000,
            event_buffer_size: 10_000,
            signal_buffer_size: 100_000,
            action_buffer_size: 10_000,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{Actor, ActorContext, Error, Event};
    use tracing_test::traced_test;

    struct TestActor;

    impl Event for String {}

    #[async_trait::async_trait]
    impl Actor for TestActor {
        type Message = String;
        type Response = String;
        type Event = String;

        async fn handle(
            &mut self,
            ctx: &mut ActorContext<Self>,
            _sender: &ActorPath,
            msg: Self::Message,
        ) -> Result<Self::Response, Error> {
            let _ = self.on_event("event".to_owned(), ctx);
            Ok(format!("Received: {}", msg))
        }

        async fn pre_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Error> {
            debug!("TestActor pre_start called.");
            let result = ctx.create_child(TestChild, "child").await;
            assert!(result.is_ok());
            Ok(())
        }

        fn on_event(&mut self, event: Self::Event, _ctx: &mut ActorContext<Self>) {
            assert_eq!(event, "event".to_owned());
        }
    }

    struct TestChild;

    #[async_trait::async_trait]
    impl Actor for TestChild {
        type Message = String;
        type Response = String;
        type Event = ();

        async fn handle(
            &mut self,
            ctx: &mut ActorContext<Self>,
            _sender: &ActorPath,
            msg: Self::Message,
        ) -> Result<Self::Response, Error> {
            if msg == "fail" {
                let _ = ctx
                    .emit_fault(&Error::Supervision("Test fault".into()))
                    .await;
            } else if msg == "error" {
                let _ = ctx
                    .emit_error(&Error::Supervision("Test error".into()))
                    .await;
            }
            Ok(format!("Child received: {}", msg))
        }

        async fn post_stop(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), Error> {
            debug!("TestChild post_stop called.");
            Ok(())
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn test_system() {
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token.clone());
        assert!(logs_contain(
            "SystemRunner started for actor system with root path: /user"
        ));
        assert!(logs_contain("Actor system created with root path: /user"));

        // Create an actor.
        let actor_ref = system.create_actor(TestActor, "test_actor").await;
        assert!(actor_ref.is_ok());
        let actor_ref = actor_ref.unwrap();
        assert_eq!(actor_ref.path().to_string(), "/user/test_actor");
        assert!(logs_contain("Creating new handle reference."));
        assert!(logs_contain("Initializing actor /user/test_actor runner."));
        assert!(logs_contain("TestActor pre_start called."));
        assert!(logs_contain(
            "Actor '/user/test_actor' created successfully."
        ));

        // Test exists
        let exists_result = system.actor_exists("test_actor").await;
        assert!(exists_result.is_ok());
        assert!(exists_result.unwrap());

        // Create an actor with the same name and verify error
        let duplicate_result = system.create_actor(TestActor, "test_actor").await;
        assert!(duplicate_result.is_err());

        // Retrieve the actor.
        let retrieved_ref: Option<ActorRef<TestActor>> = system
            .get_actor("test_actor")
            .await
            .expect("Failed to get actor");
        assert!(retrieved_ref.is_some());
        let retrieved_ref = retrieved_ref.unwrap();
        assert_eq!(&retrieved_ref.path().to_string(), "/user/test_actor");

        // Stop the system and verify shutdown logs
        token.cancel();

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await; // Wait for shutdown logs
    }

    struct TestErrorActor;

    #[async_trait::async_trait]
    impl Actor for TestErrorActor {
        type Message = String;
        type Response = String;
        type Event = ();

        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _sender: &ActorPath,
            _msg: Self::Message,
        ) -> Result<Self::Response, Error> {
            Err(Error::Supervision("Test error".into()))
        }

        async fn pre_start(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), Error> {
            Err(Error::Supervision("Pre-start error".into()))
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_system_multiple_actors() {
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token.clone());

        // Create multiple actors
        let actor1 = system.create_actor(TestActor, "actor1").await.unwrap();
        let actor2 = system.create_actor(TestActor, "actor2").await.unwrap();
        let actor3 = system.create_actor(TestActor, "actor3").await.unwrap();

        assert_eq!(actor1.path().to_string(), "/user/actor1");
        assert_eq!(actor2.path().to_string(), "/user/actor2");
        assert_eq!(actor3.path().to_string(), "/user/actor3");

        // Verify all actors can be retrieved
        assert!(
            system
                .get_actor::<TestActor>("actor1")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            system
                .get_actor::<TestActor>("actor2")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            system
                .get_actor::<TestActor>("actor3")
                .await
                .unwrap()
                .is_some()
        );

        // Clean up
        token.cancel();

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await; // Wait for shutdown logs
    }

    #[tokio::test]
    async fn test_error_actor_fails_to_start() {
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token.clone());

        // Try to create an actor that fails in pre_start
        let result = system.create_actor(TestErrorActor, "error_actor").await;
        assert!(result.is_err());

        // Verify the actor was not added to the system
        let retrieved = system.get_actor::<TestErrorActor>("error_actor").await;
        assert!(retrieved.is_err());

        // Clean up
        token.cancel();

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await; // Wait for shutdown logs
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_concurrent_actor_operations() {
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token.clone());
        let actor_ref = system.create_actor(TestActor, "concurrent").await.unwrap();

        // Send multiple concurrent messages
        let mut handles = vec![];
        for i in 0..10 {
            let actor_clone = actor_ref.clone();
            let handle =
                tokio::spawn(async move { actor_clone.ask(format!("Message {}", i)).await });
            handles.push(handle);
        }

        // Wait for all operations to complete
        for (i, handle) in handles.into_iter().enumerate() {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), format!("Received: Message {}", i));
        }

        token.cancel();
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await; // Wait for shutdown logs
    }

    struct CounterActor {
        count: usize,
    }

    #[async_trait::async_trait]
    impl Actor for CounterActor {
        type Message = String;
        type Response = usize;
        type Event = ();

        async fn handle(
            &mut self,
            _ctx: &mut ActorContext<Self>,
            _sender: &ActorPath,
            _msg: Self::Message,
        ) -> Result<Self::Response, Error> {
            self.count += 1;
            Ok(self.count)
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_actor_state() {
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token.clone());
        let actor_ref = system
            .create_actor(CounterActor { count: 0 }, "counter")
            .await
            .unwrap();

        // Send multiple messages
        for i in 1..=5 {
            let count = actor_ref.ask("increment".to_string()).await.unwrap();
            assert_eq!(count, i);
        }

        token.cancel();

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await; // Wait for shutdown logs
    }

    #[tokio::test]
    #[traced_test]
    #[serial_test::serial]
    async fn test_child_error() {
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token.clone());
        let _ = system.create_actor(TestActor, "parent").await.unwrap();
        assert!(system.actor_exists("parent").await.unwrap());

        // Retrieve the child actor reference .
        let child_ref = system
            .get_actor::<TestChild>("/parent/child")
            .await
            .unwrap();
        assert!(child_ref.is_some());
        let child_ref = child_ref.unwrap();
        // Send a message that causes the child to emit an error.
        let _ = child_ref.ask("error".to_string()).await;
        assert!(logs_contain(
            "Actor /user/parent received child error from /user/parent/child"
        ));
        assert!(logs_contain("System received ChildError"));
        assert!(logs_contain("Actor /user/parent received stop action."));
        assert!(logs_contain("Actor /user/parent stopped."));
        assert!(logs_contain("Actor /user/parent terminated."));
        assert!(logs_contain(
            "Actor /user/parent/child received stop action."
        ));
        assert!(logs_contain("Actor /user/parent/child stopped."));
        assert!(logs_contain("TestChild post_stop called."));
        assert!(logs_contain("Actor /user/parent/child terminated."));

        // Stop the system.
        token.cancel();

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await; // Wait for shutdown logs
    }

    #[tokio::test]
    #[traced_test]
    #[serial_test::serial]
    async fn test_child_fail() {
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token.clone());
        let _ = system.create_actor(TestActor, "parent").await.unwrap();
        assert!(system.actor_exists("parent").await.unwrap());

        // Retrieve the child actor reference .
        let child_ref = system
            .get_actor::<TestChild>("/parent/child")
            .await
            .unwrap();
        assert!(child_ref.is_some());
        let child_ref = child_ref.unwrap();
        // Send a message that causes the child to emit an fail.
        let _ = child_ref.ask("fail".to_string()).await;
        assert!(logs_contain(
            "Actor /user/parent received child fault from /user/parent/child"
        ));
        assert!(logs_contain("System received ChildFault"));
        assert!(logs_contain("Actor /user/parent received stop action."));
        assert!(logs_contain("Actor /user/parent stopped."));
        assert!(logs_contain("Actor /user/parent terminated."));
        assert!(logs_contain(
            "Actor /user/parent/child received stop action."
        ));
        assert!(logs_contain("Actor /user/parent/child stopped."));
        assert!(logs_contain("TestChild post_stop called."));
        assert!(logs_contain("Actor /user/parent/child terminated."));

        // Stop the system.
        token.cancel();

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await; // Wait for shutdown logs
    }

    #[tokio::test]
    #[traced_test]
    #[serial_test::serial]
    async fn test_on_event() {
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token.clone());
        let actor_ref = system.create_actor(TestActor, "event_test").await.unwrap();

        // Send a message to trigger on_event.
        let result = actor_ref.ask("trigger event".to_string()).await;

        assert!(result.is_ok());
    }

    struct TestRestartParent;

    #[async_trait::async_trait]
    impl Actor for TestRestartParent {
        type Message = String;
        type Response = String;
        type Event = ();

        async fn handle(
            &mut self,
            ctx: &mut ActorContext<Self>,
            _sender: &ActorPath,
            msg: Self::Message,
        ) -> Result<Self::Response, Error> {
            if msg == "restart" {
                let _ = ctx.restart_children().await;
            }
            Ok(format!("Received: {}", msg))
        }

        async fn pre_start(&mut self, ctx: &mut ActorContext<Self>) -> Result<(), Error> {
            debug!("TestRestartParent pre_start called.");
            let result = ctx.create_child(TestRestartChild, "child").await;
            assert!(result.is_ok());
            Ok(())
        }

        async fn on_child_error(&mut self, path: &ActorPath, error: &Error, ctx: &mut ActorContext<Self>) {
            debug!("TestRestartParent received child error from {:?}: {:?}", path, error);
            ctx.restart_children().await.unwrap();
        }
    }

    struct TestRestartChild;

    #[async_trait::async_trait]
    impl Actor for TestRestartChild {
        type Message = String;
        type Response = String;
        type Event = ();

        async fn handle(
            &mut self,
            ctx: &mut ActorContext<Self>,
            _sender: &ActorPath,
            msg: Self::Message,
        ) -> Result<Self::Response, Error> {
            if msg == "fail" {
                let _ = ctx
                    .emit_error(&Error::Supervision("Test error".into()))
                    .await;
            }
            Ok(format!("Child received: {}", msg))
        }
        
    }

    #[tokio::test]
    #[traced_test]
    #[serial_test::serial]
    async fn test_child_restart() {
        let token = CancellationToken::new();
        let mut system = System::new(Config::default(), token.clone());
        let _ = system.create_actor(TestRestartParent, "parent").await.unwrap();  

        // Retrieve the child actor reference .
        let child_ref = system
            .get_actor::<TestRestartChild>("/parent/child")
            .await
            .unwrap();
        assert!(child_ref.is_some());
        let child_ref = child_ref.unwrap();
        // Send a message that causes the child to emit an error.  
        let _ = child_ref.ask("fail".to_string()).await;
        assert!(logs_contain(
            "TestRestartParent received child error from /user/parent/child"
        ));
        assert!(logs_contain("Actor /user/parent/child received restart action."));
    }

}
