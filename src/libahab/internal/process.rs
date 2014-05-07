use internal::network::{HostAddr, NetworkMsg};

pub type ProcessId = u32;

#[deriving(Eq, TotalEq)]
pub struct ProcessAddr {
  host: HostAddr,
  pid: ProcessId,
}

impl ProcessAddr {
  pub fn new() -> ProcessAddr {
    ProcessAddr{
      host: HostAddr::new(127, 0, 0, 1, 0),
      pid: 0,
    }
  }
}

pub trait Process<T> {
  fn send(&self, dest: &ProcessAddr, msg: T);
  fn recv(&self) -> (ProcessAddr, T);
  fn process(&mut self);

  fn recv_from(&self, exp_src: &ProcessAddr) -> T {
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
    let (net_chan, net_port) = channel::<NetworkMsg>();
    ProcessHelper{
      net_chan: net_chan,
      net_port: net_port,
    }
  }

  pub fn send(&self, dest: &ProcessAddr, msg: NetworkMsg) {
  }

  pub fn recv(&self) { // -> (ProcessAddr, NetworkMsg) {
  }
}
