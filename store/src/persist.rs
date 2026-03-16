//

use actor::{Actor, ActorContext, ActorRef, ActorPath, Error as ActorError, Response};
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use crate::{Store, Journal};
use async_trait::async_trait;


#[async_trait]
pub trait PersistentActor: Actor + Serialize + DeserializeOwned {


}

