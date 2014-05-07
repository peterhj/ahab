use config::{AhabConfig};
//use internal::protocol::{MasterState, ProtocolMessage, SlaveState, master_process, slave_process};

pub struct Server;

impl Server {
}

/*pub enum ProcessMessage {
  Control(ControlMessage),
  Network(NetworkMessage),
  Protocol(ProtocolMessage),
}

pub enum ControlMessage {
  NetworkChan(Sender<ProcessMessage>),
  SlaveChan(Sender<ProcessMessage>),
  MasterChan(Sender<ProcessMessage>),
  UpgradeMaster,
}

pub enum NetworkMessage {
  Heartbeat,
  SendBlocking,
  RecvBlocking,
}

fn spawn_process<S: Send, T: Send>(state: S, process: fn (state: &mut S, msg: T)) -> Sender<T> {
  let (chan, port) = channel::<T>();
  spawn(proc () {
    let mut state = state;
    loop {
      match port.recv_opt() {
        Some(msg) => process(&mut state, msg),
        None => (),
      }
    }
  });
  chan
}

fn join_process<T: Send>(chan: Sender<T>) {  
}

struct NetworkState {
  ctrl_chan: Sender<ProcessMessage>,
}

impl NetworkState {
  pub fn new(config: AhabConfig, ctrl_chan: Sender<ProcessMessage>) -> NetworkState {
    NetworkState{
      ctrl_chan: ctrl_chan,
    }
  }
}

fn network_process(state: &mut NetworkState, msg: ProcessMessage) {
  /*match msg {
    Control(msg) => match msg {
      SlaveChan(chan) => (),
      MasterChan(chan) => (),
      _ => fail!("Warning! Network process received invalid Control!"),
    },
    Network(msg) => match msg {
      SendBlocking => (),
      RecvBlocking => (),
    },
    _ => fail!("Warning! Network process received invalid ProcessMessage!"),
  }*/
}

pub struct Server {
  ctrl_chan: Sender<ProcessMessage>,
  ctrl_port: Receiver<ProcessMessage>,
  network_chan: Option<Sender<ProcessMessage>>,
  slave_chan: Option<Sender<ProcessMessage>>,
  master_chan: Option<Sender<ProcessMessage>>,
}

impl Server {
  pub fn new() -> Server {
    let (ctrl_chan, ctrl_port) = channel::<ProcessMessage>();
    Server{
      ctrl_chan: ctrl_chan,
      ctrl_port: ctrl_port,
      network_chan: None,
      slave_chan: None,
      master_chan: None,
    }
  }

  pub fn runloop(&mut self) {
    let config = AhabConfig::new();
    self.network_chan = Some(spawn_process(NetworkState::new(config.clone(), self.ctrl_chan.clone()), network_process));
    self.slave_chan = Some(spawn_process(SlaveState::new(config.clone(), self.ctrl_chan.clone()), slave_process));
    loop {
      match self.ctrl_port.recv_opt() {
        Some(msg) => match msg {
          Control(msg) => match msg {
            UpgradeMaster => {
              self.master_chan = Some(spawn_process(MasterState::new(config.clone(), self.ctrl_chan.clone()), master_process));
            },
            _ => (),
          },
          _ => (),
        },
        None => break,
      }
    }
  }
}*/
