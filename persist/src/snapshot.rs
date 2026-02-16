//

use actor::{Actor, ActorRef};
use serde::{Serialize, Deserialize};

pub trait Snapshotter: Actor {

    fn delete(&mut self, id: u64);

    fn save<S: State>(&mut self, id: u64, state: S);

    fn load<S: State>(&mut self, id: u64) -> Option<S>;

    fn load_snapshots<S: State>(&mut self, filter: SnapshotFilter) -> Vec<(u64, S)>;

    fn delete_snapshots(&mut self, filter: SnapshotFilter);
    
}

pub enum SnapshotFilter {
    All,
    IdRange { from: u64, to: u64 },
    TimeStampRange { from: u64, to: u64 },    
}

pub trait State: Serialize + for<'de> Deserialize<'de> {}