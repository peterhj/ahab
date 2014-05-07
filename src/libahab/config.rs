use collections::hashmap::{HashMap};
use std::io::net::ip::{Ipv4Addr, SocketAddr};

#[deriving(Clone)]
#[deriving(Eq, TotalEq)]
#[deriving(Hash)]
#[deriving(Decodable, Encodable)]
pub struct HostId(u32);

#[deriving(Clone)]
pub struct AhabConfig {
  identity: HostId,
  host_addrs: HashMap<HostId, SocketAddr>,
}

impl AhabConfig {
  pub fn new() -> AhabConfig {
    let mut host_addrs = HashMap::<HostId, SocketAddr>::new();
    host_addrs.insert(HostId(0), SocketAddr{ip: Ipv4Addr(127, 0, 0, 1), port: 9000});
    host_addrs.insert(HostId(1), SocketAddr{ip: Ipv4Addr(127, 0, 0, 1), port: 9001});
    host_addrs.insert(HostId(2), SocketAddr{ip: Ipv4Addr(127, 0, 0, 1), port: 9002});
    AhabConfig{
      identity: HostId(0),
      host_addrs: host_addrs,
    }
  }
}
