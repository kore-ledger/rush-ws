//! # Actor System
//!
//! This module provides an actor system for Rust, enabling concurrency
//! based on the actor model with support for supervision, events and error handling.
//!
//! ## Main Components
//!
//! - `Actor`: Main trait that actors must implement
//! - `ActorContext`: Actor execution context
//! - `ActorRef`: Reference to communicate with an actor
//! - `System`: Actor management system
//! - `ActorPath`: Hierarchical path of an actor in the system

mod actor;
mod error;
mod event;
mod handler;
mod path;
mod runner;
mod supervision;
mod system;

pub use actor::{Actor, ActorContext, ActorRef, DummyEvent, Event, Message, Response};
pub use error::{Error, ResultExt};
pub use event::EventHandler;
pub use path::ActorPath;
pub use system::{Config, System};
