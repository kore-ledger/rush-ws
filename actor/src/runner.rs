//! # Actor Runner
//!
//! This module manages the lifecycle and execution of actors,
//! including startup, restart, shutdown and supervision handling.

use crate::{
    Actor, ActorContext, ActorPath, ActorRef, Error,
    handler::{HandlerHelper, MailboxReceiver, mailbox},
    supervision::{RetryStrategy, SupervisionStrategy},
    system::{
        ActionReceiver, ActionSender, ActorRegistry, ActorSignal, ChildAction, SignalReceiver,
        SignalSender, Supervisor, action_channel, signal_channel,
    },
};

use tokio::sync::{broadcast, oneshot};
use tracing::{debug, error};

/// Runner responsible for managing the lifecycle and execution of an actor.
///
pub(crate) struct ActorRunner<A: Actor> {
    actor: A,
    actor_path: ActorPath,
    context: ActorContext<A>,
    lifecycle: ActorLifecycle,
    receiver: MailboxReceiver<A>,
    signal_receiver: SignalReceiver,
    action_receiver: ActionReceiver,
}

impl<A> ActorRunner<A>
where
    A: Actor,
{
    /// Creates a new actor runner and its corresponding actor reference.
    ///
    /// # Arguments
    ///
    /// * `actor` - The actor instance to be managed.
    /// * `actor_path` - The path of the actor.
    /// * `signal_sender` - The signal sender for actor supervision.
    ///
    /// # Returns
    ///
    /// * `(ActorRunner<A>, ActorRef<A>)` - A tuple containing the actor runner and its reference.
    ///
    pub fn new(
        actor: A,
        actor_path: ActorPath,
        signal_sender: Option<SignalSender>,
        registry: ActorRegistry,
    ) -> (Self, ActorRef<A>, ActionSender) {
        let (sender, receiver) = mailbox::<A>(10000);
        let (event_sender, event_receiver) = broadcast::channel(10000);
        let handler = HandlerHelper::new(sender);
        let (child_signal_sender, signal_receiver) = signal_channel(100000);
        let (action_sender, action_receiver) = action_channel(10000);
        let system_handler = Supervisor::new(registry, child_signal_sender.clone());
        let context = ActorContext::new(
            actor_path.clone(),
            system_handler,
            event_sender,
            signal_sender,
        );
        let actor_ref = ActorRef::new(actor_path.clone(), handler, event_receiver);
        let runner = Self {
            actor,
            actor_path,
            context,
            lifecycle: ActorLifecycle::Created,
            receiver,
            signal_receiver,
            action_receiver,
        };
        (runner, actor_ref, action_sender)
    }

    pub async fn init(&mut self, mut init_sender: Option<oneshot::Sender<Result<(), Error>>>) {
        debug!("Initializing actor {} runner.", &self.actor_path);

        // Main loop of the actor.
        let mut retries = 0;
        //let mut lifecycle = ActorLifecycle::default();
        loop {
            match self.lifecycle {
                ActorLifecycle::Created => {
                    debug!("Actor {} created.", &self.actor_path);
                    if let Err(e) = self.actor.pre_start(&mut self.context).await {
                        error!("Actor {} pre_start failed: {:?}", &self.actor_path, e);
                        self.context.set_current_error(e);
                        self.lifecycle = ActorLifecycle::Failed;
                    } else {
                        debug!("Actor {} pre_start succeeded.", &self.actor_path);
                        self.lifecycle = ActorLifecycle::Started;
                    }
                }
                ActorLifecycle::Started => {
                    debug!("Actor {} started.", &self.actor_path);
                    // Start processing messages.
                    if let Some(sender) = init_sender.take()
                        && let Err(e) = sender.send(Ok(()))
                    {
                        error!(
                            "Failed to send init completion for actor {}: {:?}",
                            &self.actor_path, e
                        );
                    }
                    self.lifecycle = self.run().await;
                }
                ActorLifecycle::Restarted => {
                    debug!("Actor {} restarted.", &self.actor_path);
                    self.apply_supervision_strategy(A::supervision_strategy(), &mut retries)
                        .await;
                }
                ActorLifecycle::Failed => {
                    error!("Actor {} failed.", &self.actor_path);
                    self.lifecycle = ActorLifecycle::Stopped;
                }
                ActorLifecycle::Stopped => {
                    debug!("Actor {} stopped.", &self.actor_path);
                    if let Err(e) = self.actor.post_stop(&mut self.context).await {
                        error!("Actor {} post_stop failed: {:?}", &self.actor_path, e);
                    }
                    self.lifecycle = ActorLifecycle::Terminated;
                }
                ActorLifecycle::Terminated => {
                    debug!("Actor {} terminated.", &self.actor_path);
                    break;
                }
            }
        }
    }

    /// Runs the actor, processing incoming messages and signals.
    /// This function enters a loop where it listens for messages and signals,
    /// and delegates handling to the actor.
    ///
    async fn run(&mut self) -> ActorLifecycle {
        debug!("Running actor {}.", &self.actor_path);
        loop {
            tokio::select! {
                // Handle incoming messages.
                Some(message) = self.receiver.recv() => {
                    // Handle incoming messages.
                    message.handle(&mut self.actor, &mut self.context).await;
                }
                // Handle incoming signals from child actors.
                Some(signal) = self.signal_receiver.recv() => {
                    // Handle incoming signals.
                    match signal {
                        ActorSignal::ChildError(child_path, error) => {
                            error!("Actor {} received child error from {}: {:?}", &self.actor_path, child_path, error);
                            self.actor.on_child_error(&child_path, &error, &mut self.context).await;
                        },
                        ActorSignal::ChildFault(child_path, error) => {
                            error!("Actor {} received child fault from {}.", &self.actor_path, child_path);
                            self.actor.on_child_fault(&child_path, &error, &mut self.context).await;
                        }
                    }
                }
                // Handle child actions (stop/restart) sent by the supervisor.
                Some(action) = self.action_receiver.recv() => {
                    match action {
                        ChildAction::Stop => {
                            debug!("Actor {} received stop action.", &self.actor_path);
                            // Stop child actors first
                            if let Err(e) = self.context.stop_children().await {
                                error!("Failed to stop child actors of {}: {:?}", &self.actor_path, e);
                                if let Err(e) = self.context.emit_fault(e).await {
                                    error!("Failed to emit fault for {}: {:?}", &self.actor_path, e);
                                    self.context.set_current_error(e);
                                    return ActorLifecycle::Failed;
                                }
                            }
                            return ActorLifecycle::Stopped;
                        },
                        ChildAction::Restart => {
                            debug!("Actor {} received restart action.", &self.actor_path);
                            // Restart child actors first
                            if let Err(e) = self.context.restart_children().await {
                                error!("Failed to restart child actors of {}: {:?}", &self.actor_path, e);
                                if let Err(e) = self.context.emit_fault(e).await {
                                    error!("Failed to emit fault for {}: {:?}", &self.actor_path, e);
                                    self.context.set_current_error(e);
                                    return ActorLifecycle::Failed;
                                }
                            }
                            return ActorLifecycle::Restarted;
                        },
                    }
                }
                else => {
                    // No more messages or signals to process.
                    debug!("Actor {} has no more messages or signals to process.", &self.actor_path);
                    break;
                }

            }
        }
        ActorLifecycle::Stopped
    }

    /// Apply supervision strategy.
    /// If the actor fails, the strategy is applied.
    ///
    async fn apply_supervision_strategy(
        &mut self,
        strategy: SupervisionStrategy,
        retries: &mut usize,
    ) {
        match strategy {
            SupervisionStrategy::Stop => {
                error!("Actor '{}' failed to start!", &self.actor_path);
                // Stop childs first

                self.lifecycle = ActorLifecycle::Stopped;
            }
            SupervisionStrategy::Retry(mut retry_strategy) => {
                debug!(
                    "Restarting actor with retry strategy: {:?}",
                    &retry_strategy
                );
                if *retries < retry_strategy.max_retries() {
                    debug!("retries: {}", &retries);
                    if let Some(duration) = retry_strategy.next_backoff() {
                        debug!("Backoff for {:?}", &duration);
                        tokio::time::sleep(duration).await;
                    }
                    *retries += 1;
                    //let error = ctx.current_error();
                    match self.context.restart(&mut self.actor).await {
                        Ok(_) => {
                            self.lifecycle = ActorLifecycle::Started;
                            *retries = 0;
                        }
                        Err(err) => {
                            self.context.set_current_error(err);
                        }
                    }
                } else {
                    self.lifecycle = ActorLifecycle::Stopped;
                }
            }
        }
    }
}

/// The `Actor` lifecycle enum
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ActorLifecycle {
    /// The actor is created.
    #[default]
    Created,
    /// The actor is started.
    Started,
    /// The actor is restarted.
    Restarted,
    /// The actor is failed.
    Failed,
    /// The actor is stopped.
    Stopped,
    /// The actor is terminated.
    Terminated,
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::supervision::{FixedIntervalStrategy, Strategy};
    use crate::{Actor, ActorContext, ActorPath, Error};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

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
            _msg: Self::Message,
        ) -> Result<Self::Response, Error> {
            //println!("Handling message: {}", _msg);
            assert_eq!(_msg, "Hello, Actor!");
            Ok("ok".to_string())
        }

        async fn pre_start(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), Error> {
            //println!("Pre-start called");
            Ok(())
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_actor_runner_lifecycle() {
        let actor = TestActor;
        let actor_path = ActorPath::from("test_actor");
        let registry: ActorRegistry = Arc::new(RwLock::new(HashMap::new()));
        let (mut runner, actor_ref, _action_sender) =
            ActorRunner::new(actor, actor_path, None, registry);

        // Initialize the actor runner.
        let (init_sender, init_receiver) = oneshot::channel();
        tokio::spawn(async move {
            runner.init(Some(init_sender)).await;
        });
        // Wait for initialization to complete.
        let init_result = init_receiver.await.unwrap();
        assert!(init_result.is_ok());
        // Send a message to the actor.
        actor_ref.tell("Hello, Actor!".to_string()).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_actor_retry_strategy() {
        // Test that a retry strategy is configured correctly
        let strategy = SupervisionStrategy::Retry(Strategy::FixedInterval(
            FixedIntervalStrategy::new(3, std::time::Duration::from_millis(10)),
        ));

        match strategy {
            SupervisionStrategy::Retry(mut s) => {
                assert_eq!(s.max_retries(), 3);
                assert_eq!(s.next_backoff(), Some(std::time::Duration::from_millis(10)));
            }
            _ => panic!("Expected Retry strategy"),
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_actor_message_handling() {
        let actor = TestActor;
        let actor_path = ActorPath::from("msg_handler");
        let registry: ActorRegistry = Arc::new(RwLock::new(HashMap::new()));
        let (mut runner, actor_ref, _action_sender) =
            ActorRunner::new(actor, actor_path, None, registry);

        let (init_sender, init_receiver) = oneshot::channel();
        tokio::spawn(async move {
            runner.init(Some(init_sender)).await;
        });

        init_receiver.await.unwrap().unwrap();

        // Test single message that TestActor expects
        let response = actor_ref.ask("Hello, Actor!".to_string()).await.unwrap();
        assert_eq!(response, "ok".to_string());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn test_actor_stop_action() {
        let actor = TestActor;
        let actor_path = ActorPath::from("stop_actor");
        let registry: ActorRegistry = Arc::new(RwLock::new(HashMap::new()));
        let (mut runner, _actor_ref, action_sender) =
            ActorRunner::new(actor, actor_path, None, registry);

        let (init_sender, init_receiver) = oneshot::channel();
        let runner_handle = tokio::spawn(async move {
            runner.init(Some(init_sender)).await;
        });

        init_receiver.await.unwrap().unwrap();

        // Send stop action
        action_sender.send(ChildAction::Stop).await.unwrap();

        // Wait for runner to complete
        tokio::time::timeout(tokio::time::Duration::from_secs(1), runner_handle)
            .await
            .expect("Runner should stop")
            .unwrap();
    }
}
