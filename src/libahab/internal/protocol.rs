use config::{AhabConfig, HostId};
use internal::process::{Process, ProcessAddr, ProcessHelper};

use serialize::{base64, Decodable, Decoder, Encodable, Encoder};
use serialize::base64::{FromBase64, ToBase64};
use sync::{Arc};

macro_rules! match_until (
  ($e:expr, $p:ident($($lhs:pat),*) => $rhs:expr) => ({
    let mut m;
    loop {
      m = $e;
      match m {
        $p(..) => break,
        _ => (),
      }
    }
    match m {
      $p($($lhs),*) => $rhs,
      _ => fail!(),
    }
  })
)

pub type Epoch = u64;

#[deriving(Clone, Decodable, Encodable)]
pub struct TxnId {
  epoch: Epoch,
  counter: u64,
}

#[deriving(Clone, Decodable, Encodable)]
pub enum TxnOp {
  Put(TxnPutOp),
  Delete(TxnDeleteOp),
}

#[deriving(Clone)]
pub struct TxnPutOp {
  key: ~str,
  value: ~[u8],
}

impl<E, S: Encoder<E>> Encodable<S, E> for TxnPutOp {
  fn encode(&self, s: &mut S) -> Result<(), E> {
    match s.emit_str(self.key) {
      Ok(()) => (),
      Err(e) => return Err(e),
    }
    let value: &[u8] = self.value;
    match s.emit_str(value.to_base64(base64::STANDARD)) {
      Ok(()) => (),
      Err(e) => return Err(e),
    }
    Ok(())
  }
}

impl<E, D: Decoder<E>> Decodable<D, E> for TxnPutOp {
  fn decode(d: &mut D) -> Result<TxnPutOp, E> {
    // FIXME handle errors.
    let key = d.read_str().ok().unwrap();
    let value = d.read_str().ok().unwrap()
        .from_base64().ok().unwrap();
    let op = TxnPutOp{key: key, value: value};
    Ok(op)
  }
}

#[deriving(Clone, Decodable, Encodable)]
pub struct TxnDeleteOp {
  key: ~str,
}

#[deriving(Clone, Decodable, Encodable)]
pub struct Txn {
  xid: TxnId,
  ops: Vec<TxnOp>,
}

#[deriving(Clone, Decodable, Encodable)]
pub enum ProtocolMsg {
  // Discovery phase.
  CurrentEpoch(Epoch),
  NewEpoch(Epoch, TxnId),
  AckEpoch(HostId, Vec<Txn>),
  // Synchronization phase.
  NewMaster(Epoch, Vec<Txn>),
  AckMaster(Epoch),
  CommitMaster(Epoch),
  // Broadcast phase.
  Propose(Epoch, Txn),
  Ack(Epoch),
  Commit(Epoch),
  Heartbeat,
}

enum ProtocolPhase {
  Discovery,
  Synchronization,
  Broadcast,
}

pub struct MasterProcess {
  helper: ProcessHelper,
  config: Arc<AhabConfig>,
  phase: ProtocolPhase,
}

impl MasterProcess {
  pub fn new(config: Arc<AhabConfig>) -> MasterProcess {
    MasterProcess{
      helper: ProcessHelper::new(),
      config: config,
      phase: Discovery,
    }
  }
}

impl Process<ProtocolMsg> for MasterProcess {
  fn send(&self, dest: &ProcessAddr, msg: ProtocolMsg) {
  }

  fn recv(&self) -> (ProcessAddr, ProtocolMsg) {
    self.helper.recv();
    (ProcessAddr::new(), Heartbeat)
  }

  fn process(&mut self) {
    match self.phase {
      Discovery => {
        // Receive from a quorum of followers CurrentEpoch(e).
        // Send to quorum NewEpoch(e') s.t. e' = max(e received from quorum) + 1.
        // Receive from quorum AckEpoch(m, diff).
        // Select a follower in quorum and its diff.
        // Transition to Synchronization phase.
      },
      Synchronization => {
        // Send to quorum NewMaster(e', diff).
        // Receive from quorum AckMaster(e', diff), send to quorum
        // CommitMaster(e', diff), and transition to Broadcast phase.
      },
      Broadcast => {
        // Receive from primary process a proposed txn.
        // Send to quorum Propose(e', txn).
        // Receive from quorum Ack(e', txn).
        // Send to all followers Commit(e', txn).
      },
    }
  }
}

pub struct SlaveProcess {
  helper: ProcessHelper,
  config: Arc<AhabConfig>,
  phase: ProtocolPhase,
  last_epoch: Epoch,
  last_master: Option<HostId>,
}

impl SlaveProcess {
  pub fn new(config: Arc<AhabConfig>) -> SlaveProcess {
    SlaveProcess{
      helper: ProcessHelper::new(),
      config: config,
      phase: Discovery,
      last_epoch: 0,
      last_master: None,
    }
  }

  pub fn infer_master(&self) -> HostId {
    HostId(0)
  }

  pub fn accept_txn(&mut self) {
  }

  pub fn deliver_txn(&mut self) {
  }
}

impl Process<ProtocolMsg> for SlaveProcess {
  fn send(&self, dest: &ProcessAddr, msg: ProtocolMsg) {
  }

  fn recv(&self) -> (ProcessAddr, ProtocolMsg) {
    self.helper.recv();
    (ProcessAddr::new(), Heartbeat)
  }

  fn process(&mut self) {
    let leader = ProcessAddr::new();
    let dummy_msg = Heartbeat;
    loop {
      match self.phase {
        Discovery => {
          // Send to leader CurrentEpoch(state.last_epoch).
          self.send(&leader, dummy_msg.clone());
          // Receive from leader NewEpoch(e).
          let e = match_until!(self.recv_from(&leader), NewEpoch(e, _) => e);
          // If state.last_epoch < e, then set state.last_epoch = e, send to
          // leader AckEpoch(state.last_master, diff), and transition to
          // Synchronization phase.
          if self.last_epoch < e {
            self.last_epoch = e;
            self.send(&leader, dummy_msg.clone());
            self.phase = Synchronization;
          }
        },
        Synchronization => {
          // Receive from leader NewMaster(e, diff).
          let (e, diff) = match_until!(self.recv_from(&leader), NewMaster(e, diff) => (e, diff));
          // If state.last_epoch != e, then transition to Discovery phase.
          if self.last_epoch != e {
            self.phase = Discovery;
            continue;
          }
          // Atomically:
          // 1. set state.last_epoch = e,
          // 2. for each txn in diff, accept txn.
          // FIXME
          // Send to leader AckMaster.
          self.send(&leader, dummy_msg.clone());
          // Receive from leader CommitMaster.
          let e = match_until!(self.recv_from(&leader), CommitMaster(e) => e);
          // For each txn in state.history, deliver txn.
          // Transition to Broadcast phase.
          self.phase = Broadcast;
          // If state.infer_master() is itself, invoke state.ready(e).
          if self.infer_master() == HostId(0) { // FIXME
          }
        },
        Broadcast => {
          // Forever: accept proposed txns from leader and append each txn to
          // state.history.
          // Upon receiving from leader Commit(e, txn), and when all txn' s.t.
          // txn' < txn are committed, commit txn.
        },
      }
    }
  }
}
