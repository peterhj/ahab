use collections::hashmap::{HashMap};
use std::io::net::ip::{Ipv4Addr, SocketAddr};
use std::io::net::tcp::{TcpListener, TcpStream};

#[deriving(Clone, Eq, TotalEq, Ord, TotalOrd)]
#[deriving(Hash, Decodable, Encodable)]
pub struct HostId(u32);

#[deriving(Clone, Eq, TotalEq)]
#[deriving(Hash)]
pub struct HostAddr {
  sockaddr: SocketAddr,
}

impl HostAddr {
  pub fn new(ip0: u8, ip1: u8, ip2: u8, ip3: u8, port: u16) -> HostAddr {
    HostAddr{
      sockaddr: SocketAddr{ip: Ipv4Addr(ip0, ip1, ip2, ip3), port: port},
    }
  }

  pub fn default() -> HostAddr {
    HostAddr::new(127, 0, 0, 1, 6379)
  }
}

#[deriving(Clone)]
pub struct HostInfo {
  id: HostId,
  addr: HostAddr,
}

pub struct Quorum {
  hosts: HashMap<HostId, HostInfo>,
}

pub struct NetworkMsg {
  dest: HostAddr,
  value: ~[u8],
}
