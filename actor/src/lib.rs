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
mod handler;
mod path;
mod runner;
mod supervision;
mod system;

pub use actor::{Actor, ActorContext, ActorRef, Event, Message, Response, DummyEvent};
pub use error::Error;
pub use path::ActorPath;
pub use system::{Config, System};
