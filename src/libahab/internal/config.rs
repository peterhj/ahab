use internal::network::{HostAddr, HostId, HostInfo};

use collections::hashmap::{HashMap};

#[deriving(Clone)]
pub struct StaticConfig {
  identity: HostId,
  hosts: HashMap<HostId, HostInfo>,
}

impl StaticConfig {
  pub fn new() -> StaticConfig {
    let mut hosts = HashMap::<HostId, HostInfo>::new();
    hosts.insert(HostId(0), HostInfo{id: HostId(0), addr: HostAddr::new(127, 0, 0, 1, 9000)});
    hosts.insert(HostId(1), HostInfo{id: HostId(1), addr: HostAddr::new(127, 0, 0, 1, 9001)});
    hosts.insert(HostId(2), HostInfo{id: HostId(2), addr: HostAddr::new(127, 0, 0, 1, 9002)});
    StaticConfig{
      identity: HostId(0),
      hosts: hosts,
    }
  }

  pub fn quorum_min_size(&self) -> uint {
    3
  }

  pub fn quorum_max_size(&self) -> uint {
    5
  }
}
