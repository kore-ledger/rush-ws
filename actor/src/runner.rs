//

use crate::{
    Actor, ActorContext, ActorPath, ActorRef, Error,
    handler::{HandlerHelper, MailboxReceiver, mailbox},
    system::{
        SupervisionHandler, ActorRegistry, 
        action_channel, ChildAction, ActionSender, ActionReceiver,
        signal_channel, SignalSender, SignalReceiver, ActorSignal,
    },
    supervision::{RetryStrategy, SupervisionStrategy},
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
        let system_handler = SupervisionHandler::new(registry, child_signal_sender.clone());
        let context = ActorContext::new(
            actor_path.clone(),
            system_handler,
            event_sender,
            signal_sender,
        );
        let actor_ref =
            ActorRef::new(actor_path.clone(), handler, event_receiver);
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

    pub async fn init(
        &mut self,
        mut init_sender: Option<oneshot::Sender<Result<(), Error>>>,
    ) {
        debug!("Initializing actor {} runner.", &self.actor_path);

        // Main loop of the actor.
        let mut retries = 0;
        //let mut lifecycle = ActorLifecycle::default();
        loop {
            match self.lifecycle {
                ActorLifecycle::Created => {
                    debug!("Actor {} created.", &self.actor_path);
                    if let Err(e) =
                        self.actor.pre_start(&mut self.context).await
                    {
                        error!(
                            "Actor {} pre_start failed: {:?}",
                            &self.actor_path, e
                        );
                        self.context.set_current_error(e);
                        self.lifecycle = ActorLifecycle::Failed;
                    } else {
                        debug!(
                            "Actor {} pre_start succeeded.",
                            &self.actor_path
                        );
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
                    self.apply_supervision_strategy(
                        A::supervision_strategy(),
                        &mut retries,
                    )
                    .await;
                }
                ActorLifecycle::Failed => {
                    error!("Actor {} failed.", &self.actor_path);
                    self.lifecycle = ActorLifecycle::Stopped;
                }
                ActorLifecycle::Stopped => {
                    debug!("Actor {} stopped.", &self.actor_path);
                    if let Err(e) =
                        self.actor.post_stop(&mut self.context).await
                    {
                        error!(
                            "Actor {} post_stop failed: {:?}",
                            &self.actor_path, e
                        );
                    }
                    self.lifecycle = ActorLifecycle::Terminated;
                }
                ActorLifecycle::Terminated => {
                    debug!("Actor {} terminated.", &self.actor_path);
                    break;
                }
            }
        }

        unimplemented!()
    }

    /// Runs the actor, processing incoming messages and signals.
    /// This function enters a loop where it listens for messages and signals,
    /// and delegates handling to the actor.
    ///
    async fn run(&mut self) -> ActorLifecycle {
        debug!("Running actor {}.", &self.actor_path);
        loop {
            tokio::select! {
                Some(message) = self.receiver.recv() => {
                    // Handle incoming messages.
                    message.handle(&mut self.actor, &mut self.context).await;
                }
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
                Some(action) = self.action_receiver.recv() => {
                    match action {
                        ChildAction::Stop => {
                            debug!("Actor {} received stop action.", &self.actor_path);
                            return ActorLifecycle::Stopped;
                        },
                        ChildAction::Restart => {
                            debug!("Actor {} received restart action.", &self.actor_path);
                            return ActorLifecycle::Restarted;
                        }
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
