//

use crate::{
    Actor, ActorContext, ActorPath, ActorRef, Error,
    runner::ActorRunner, 
};
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use std::{
    any::Any, collections::HashMap, sync::Arc 
};


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
    pub async fn handle<A: Actor>(
        &self,
        actor: &mut A,
        ctx: &mut ActorContext<A>,
    ) {
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

#[derive(Clone)]
pub struct System {
    root_path: ActorPath,
    system_handler: SupervisionHandler,
}

impl System {

    /// Creates a new actor system with an empty registry and system handler.
    /// 
    /// # Arguments
    /// 
    /// * `cancellation_token` - The cancellation token for the system.
    /// 
    /// # Returns
    /// 
    /// * `System` - The newly created actor system.
    /// 
    pub fn new(cancellation_token: CancellationToken) -> Self {
        let registry: ActorRegistry = Arc::new(RwLock::new(HashMap::new()));
        let (child_signal_sender, child_signal_receiver) = signal_channel(10);
        let system_handler = SupervisionHandler::new(registry, child_signal_sender);
        let root_path = ActorPath::from("/user");
        let system = System { system_handler, root_path: root_path.clone() };
        let system_runner = SystemRunner::new(system.clone(), child_signal_receiver, cancellation_token);
        system_runner.run();
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
    pub async fn create_actor<A>(
        &mut self,
        actor: A,
        name: &str,
    ) -> Result<ActorRef<A>, Error>
    where
        A: Actor,
    {
        let path = self.root_path.clone() / name;
        self.system_handler
            .create_actor(actor, &path)
            .await
    }

    pub async fn get_actor<A>(&self, name: &str) -> Result<Option<ActorRef<A>>, Error>
    where
        A: Actor,
    {
        let child_path = self.root_path.clone() / name;
        self.system_handler.get_actor(&child_path).await
    }

    pub async fn on_child_error(
        &mut self,
        path: &ActorPath,
        error: &Error,
    ) {
        error!("System received ChildError from {:?}: {:?}", path, error);
        // Handle system-level child error
    }

    pub async fn on_child_fault(
        &mut self,
        path: &ActorPath,
        error: &Error,
    ) {
        error!("System received ChildFault from {:?}: {:?}", path, error);
        // Handle system-level child fault
    }

    /// Stops all child actors under supervision.
    ///
    pub async fn stop_all_children(&self) {
        self.system_handler.stop_all_children().await;
    }

    /// Restarts all child actors under supervision.
    ///
    pub async fn restart_all_children(&self) {
        self.system_handler.restart_all_children().await;
    }
}

struct SystemRunner {
    system: System,
    signal_receiver: SignalReceiver,
    cancellation_token: CancellationToken,
}

impl SystemRunner {

    pub fn new(system: System,signal_receiver: SignalReceiver, cancellation_token: CancellationToken) -> Self {
        Self { system, signal_receiver, cancellation_token }
    }

    pub fn run(mut self) {       
        // Spawn a task to handle system-level signals
        let _ = tokio::spawn(async move {
            let mut receiver = self.signal_receiver;
            tokio::select! {
                _ = self.cancellation_token.cancelled() => {
                    debug!("SystemRunner received cancellation signal, shutting down.");
                    self.system.stop_all_children().await;
                    return;
                }
                Some(signal) = receiver.recv() => {
                    debug!("SystemRunner received signal.");
                    // Handle system-level signals here
                    match signal {
                        ActorSignal::ChildError(path, error) => {
                            self.system.on_child_error(&path, &error).await;
                        }
                        ActorSignal::ChildFault(path, error) => {
                            self.system.on_child_fault(&path, &error).await;
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
pub struct SupervisionHandler {
    /// The actor registry for managing child actors.
    registry: ActorRegistry,
    /// The action senders registry for managing child actors.
    action_senders: ActionSendersRegistry,
    /// The signal sender to share with child actors.
    child_signal_sender: SignalSender,
}

impl SupervisionHandler {

    /// Creates a new supervision handler.
    /// 
    /// # Arguments
    /// 
    /// * `registry` - The actor registry for managing child actors.
    /// * `child_signal_sender` - The signal sender for child actors.
    ///
    /// # Returns
    /// 
    /// * `SupervisionHandler` - The newly created supervision handler.
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
    ) -> Result<ActorRef<A>, Error>
    where
        A: Actor,
    {
        // Check if an actor with the same name already exists
        if self.registry.read().await.contains_key(&(path)) {
            return Err(Error::Supervision(format!(
                "Actor with path '{}' already exists.",
                path
            )));
        }

        // Contruct the actor runner and reference
       let (mut runner, actor_ref, action_sender) =
            ActorRunner::new(actor, path.clone(), Some(self.child_signal_sender.clone()), self.registry.clone());
        
        // Init the actor
        let (init_sender, init_receiver) = oneshot::channel();
        tokio::spawn(async move {
            runner.init(Some(init_sender)).await;
        });
        match init_receiver.await {
            Ok(Ok(())) => {
                debug!("Child actor '{}' created successfully.", path);
                // Insert the child into supervision
                self.action_senders.write().await.insert(path.clone(), action_sender);
                self.registry.write().await.insert(
                    path.clone(),
                    Box::new(actor_ref.clone()),
                );
                Ok(actor_ref)

            }
            Ok(Err(e))  => {
                error!("Failed to initialize child actor '{}': {:?}", path, e);
                Err(Error::Supervision(format!(
                    "Failed to initialize child actor '{}': {:?}",
                    path, e
                )))
            }
            Err(e) => {
                error!("Initialization channel error for child actor '{}': {:?}", path, e);
                Err(Error::Supervision(format!(
                    "Initialization channel error for child actor '{}': {:?}",
                    path, e
                )))
            }
        }
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
        self.registry.read().await.get(path)
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

    /// Removes a child actor by name.
    /// 
    /// # Arguments
    ///     
    /// * `path` - The path of the child actor to remove.
    /// 
    /// # Returns
    ///     
    /// * `Result<(), Error>` - Ok if removed successfully, error otherwise.
    /// 
    pub async fn remove_child<A: Actor>(&self, path: &ActorPath) -> Option<ActorRef<A>> {
        self.registry.write().await.remove(path)
            .and_then(|any| any.downcast::<ActorRef<A>>().ok())
            .map(|boxed_ref| *boxed_ref)}

    /// Stops a child actor by name.
    /// 
    /// # Arguments
    ///     
    /// * `path` - The path of the child actor to stop.
    ///
    /// # Returns
    ///
    /// * `Result<(), Error>` - Ok if stopped successfully, error otherwise.
    ///
    pub async fn stop_child(&self, path: &ActorPath) -> Result<(), Error> {
        if let Some(action_sender) = self.action_senders.read().await.get(path) {
            action_sender.send(ChildAction::Stop).await.map_err(|e| {
                Error::Supervision(format!(
                    "Failed to send stop action to child '{}': {:?}",
                    path, e
                ))
            })?;
            Ok(())
        } else {
            Err(Error::Supervision(format!(
                "Child actor '{}' not found for stopping.",
                path
            )))
        }
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
            action_sender.send(ChildAction::Restart).await.map_err(|e| {
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
    pub async fn stop_all_children(&self) {
        let action_senders = self.action_senders.read().await;
        for (path, action_sender) in action_senders.iter() {
            if let Err(e) = action_sender.send(ChildAction::Stop).await {
                error!(
                    "Failed to send stop action to child '{}': {:?}",
                    path, e
                );
            }
        }
    }

    /// Restarts all child actors under supervision.
    ///
    pub async fn restart_all_children(&self) {
        let action_senders = self.action_senders.read().await;
        for (path, action_sender) in action_senders.iter() {
            if let Err(e) = action_sender.send(ChildAction::Restart).await {
                error!(
                    "Failed to send restart action to child '{}': {:?}",
                    path, e
                );
            }
        }
    }   
}

impl Default for SupervisionHandler {
    fn default() -> Self {
        let registry: ActorRegistry = Arc::new(RwLock::new(HashMap::new()));
        let (child_signal_sender, _child_signal_receiver) = signal_channel(10);
        SupervisionHandler::new(registry, child_signal_sender)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{Actor, ActorContext, Error};

    struct TestActor;

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
    async fn test_create_actor() {
        let mut system = System::new(CancellationToken::new());
        let actor_ref = system
            .create_actor(TestActor, "test_actor")
            .await
            .expect("Failed to create actor");
        assert_eq!(actor_ref.path().to_string(), "/user/test_actor");
    }
}