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
  bytes: ~[u8],
}

pub struct NetworkSender {
  port: Receiver<NetworkMsg>,
}

pub struct NetworkReceiver {
  listener: TcpListener,
  master_chan: Sender<NetworkMsg>,
  replica_chan: Sender<NetworkMsg>,
}

pub trait Process<T: Send> {
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

  pub fn send(&self, msg: NetworkMsg) {
  }

  pub fn recv(&self) -> NetworkMsg {
    NetworkMsg{bytes: ~[]}
  }
}

pub struct ProcessRouter<T> {
  procs: HashMap<ProcessPort, ~Process<T>>,
}

impl<T: Send> ProcessRouter<T> {
  pub fn new() -> ProcessRouter<T> {
    ProcessRouter{
      procs: HashMap::new(),
    }
  }

  pub fn register(&mut self, port: ProcessPort, process: ~Process<T>) {
    self.procs.insert(port, process);
  }
}
