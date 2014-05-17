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

#[deriving(Clone, Eq, TotalEq)]
#[deriving(Hash)]
pub struct ProcessPort(u32);

#[deriving(Clone, Eq, TotalEq)]
#[deriving(Hash)]
pub struct ProcessId {
  host: HostId,
  port: ProcessPort,
}

pub struct NetworkMsg {
  value: Vec<u8>,
}

pub struct NetworkSender {
  port: Receiver<NetworkMsg>,
}

pub struct NetworkReceiver {
  master_chan: Sender<NetworkMsg>,
  replica_chan: Sender<NetworkMsg>,
  listener: TcpListener,
}

pub trait Process<T> {
  fn send(&self, dest: &HostId, msg: T);
  fn recv(&self) -> (HostId, T);
  fn process(&mut self);

  fn recv_from(&self, exp_src: &HostId) -> T {
    loop {
      let (src, msg) = self.recv();
      if src == *exp_src {
        return msg;
      }
    }
  }
}

pub struct ProcessHelper {
  net_chan: Sender<NetworkMsg>,
  net_port: Receiver<NetworkMsg>,
}

impl ProcessHelper {
  pub fn new() -> ProcessHelper {
    let (chan, port) = channel::<NetworkMsg>();
    ProcessHelper{
      net_chan: chan,
      net_port: port,
    }
  }
}

pub struct ProcessRouter<T> {
  procs: HashMap<ProcessPort, ~Process<T>>,
}

impl<T: Send> ProcessRouter<T> {
}
