use internal::config::{StaticConfig};
use internal::process::{Process, ProcessRouter};
use internal::protocol::{
  MASTER_PORT, REPLICA_PORT,
  MasterProcess, ReplicaProcess,
  ProtocolMsg, SharedData,
};

use sync::{Arc, RWLock};

pub struct Server {
  router: ProcessRouter<ProtocolMsg>,
}

impl Server {
  pub fn new() -> Server {
    let config = Arc::new(StaticConfig::new());
    let shared = Arc::new(RWLock::new(SharedData::new()));
    let master = ~MasterProcess::new(config.clone(), shared.clone());
    let replica = ~ReplicaProcess::new(config.clone(), shared.clone());
    let mut router = ProcessRouter::<ProtocolMsg>::new();
    router.register(MASTER_PORT, master);
    router.register(REPLICA_PORT, replica);
    Server{
      router: router,
    }
  }
}
