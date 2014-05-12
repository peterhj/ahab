use internal::network::{HostId, NetworkMsg};

pub type ProcessId = u32;

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
    let (net_chan, net_port) = channel::<NetworkMsg>();
    ProcessHelper{
      net_chan: net_chan,
      net_port: net_port,
    }
  }

  pub fn send(&self, dest: &HostId, msg: NetworkMsg) {
  }

  pub fn recv(&self) -> (HostId, NetworkMsg) {
    (HostId(0), NetworkMsg{value: ~[]}) // FIXME
  }
}
