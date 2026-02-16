//

mod error;
mod event_sourced;
mod snapshot;
mod stores;

pub use error::Error;

/// PersistentCounter is a trait that defines the interface for a counter that
/// can be persisted using event sourcing and snapshotting. It provides methods
/// to retrieve the current journal and snapshot values of the counter.
pub trait PersistentCounter {

    /// Retrieves the current journal value of the counter.
    /// Returns a `u64` representing the journal value.
    /// 
    /// # Returns
    /// 
    /// A `u64` representing the current journal value of the counter.
    /// 
    fn journal(&self) -> u64;

    /// Retrieves the current snapshot value of the counter.
    /// Returns a `u64` representing the snapshot value.
    /// 
    /// # Returns
    /// 
    /// A `u64` representing the current snapshot value of the counter.
    /// 
    fn snapshot(&self) -> u64;

    /// Increments the journal value of the counter by 1.
    /// This method is used to update the journal value when an event occurs.
    ///
    /// # Returns
    /// 
    /// A `u64` representing the updated journal value of the counter after incrementing.
    /// 
    fn increment_journal(&mut self) -> u64;

    /// Increments the snapshot value of the counter by 1.
    /// This method is used to update the snapshot value when a snapshot is taken.
    /// 
    /// # Returns
    /// 
    /// A `u64` representing the updated snapshot value of the counter after incrementing.
    /// 
    fn increment_snapshot(&mut self);
}