use std::io::net::ip::{Ipv4Addr, SocketAddr};
use std::io::net::tcp::{TcpListener, TcpStream};

#[deriving(Eq, TotalEq)]
pub struct HostAddr {
  sockaddr: SocketAddr,
}

impl HostAddr {
  pub fn new(ip0: u8, ip1: u8, ip2: u8, ip3: u8, port: u16) -> HostAddr {
    HostAddr{
      sockaddr: SocketAddr{ip: Ipv4Addr(ip0, ip1, ip2, ip3), port: port},
    }
  }
}

pub struct NetworkMsg {
  dest: SocketAddr,
  value: ~[u8],
}
