//! # Error Types
//!
//! Defines the error types that can occur in the actor system.

use thiserror::Error;

/// Errors that can occur in the actor system.
/// This enum defines the various errors that can occur when working with actors,
/// such as when sending messages, starting actors, or handling messages.
#[derive(Error, Debug, Clone)]
pub enum Error {
    /// Error when an actor is not found.
    #[error("Actor not found: {0}")]
    ActorNotFound(String),  
    /// Error when sending a message to an actor fails.
    #[error("Failed to send message to actor: {0}")]
    SendMessage(String),
    /// Error when starting an actor fails.
    #[error("Failed to start actor: {0}")]
    StartActor(String),
    /// Error when handling a message fails.
    #[error("Failed to create actor: {0}")]
    CreateActor(String),
    /// Error when emitting an event fails.
    #[error("Sending event failed: {0}")]
    SendEvent(String),
    /// Supervision error.
    #[error("Supervision error: {0}")]
    Supervision(String),
    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(String),
    /// Deserialization error.
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    /// Store error.
    #[error("Store error: {0}")]
    Store(String),
    /// Retry limit exceeded error.
    #[error("Retry limit exceeded")]
    RetryLimitExceeded,
    #[error("Unhandled message: {0}")]
    UnhandledMessage(String),
    #[error("Invalid response")]
    InvalidResponse,
}
