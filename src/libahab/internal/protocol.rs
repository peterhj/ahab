use config::{AhabConfig, HostId};
use internal::network::{HostAddr};
use internal::process::{Process, ProcessAddr, ProcessHelper};

use collections::hashmap::{HashMap, HashSet};
use collections::treemap::{TreeMap};
use serialize::{base64, Decodable, Decoder, Encodable, Encoder};
use serialize::base64::{FromBase64, ToBase64};
use sync::{Arc, Mutex};

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

#[deriving(Clone, Eq, TotalEq, Ord, TotalOrd)]
#[deriving(Decodable, Encodable)]
pub struct TxnId {
  epoch: Epoch,
  counter: u64,
}

#[deriving(Clone)]
#[deriving(Decodable, Encodable)]
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

#[deriving(Clone)]
#[deriving(Decodable, Encodable)]
pub struct TxnDeleteOp {
  key: ~str,
}

#[deriving(Clone)]
#[deriving(Decodable, Encodable)]
pub struct Txn {
  xid: TxnId,
  ops: Vec<TxnOp>,
}

impl Txn {
  pub fn acceptable(&self) -> bool {
    true
  }
}

#[deriving(Clone)]
#[deriving(Decodable, Encodable)]
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
  Ack(Epoch, TxnId),
  Commit(Epoch, TxnId),
  Heartbeat,
}

enum ProtocolPhase {
  Discovery,
  Synchronization,
  Broadcast,
}

enum ProtocolState {
  Electing,
  Following,
  Leading,
}

pub struct MasterProcess {
  helper: ProcessHelper,
  config: Arc<AhabConfig>,
  phase: ProtocolPhase,
  quorum: HashSet<HostAddr>,
}

impl MasterProcess {
  pub fn new(config: Arc<AhabConfig>) -> MasterProcess {
    MasterProcess{
      helper: ProcessHelper::new(),
      config: config,
      phase: Discovery,
      quorum: HashSet::new(),
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
  accepted_txns: TreeMap<TxnId, Txn>,
  committing_txns: TreeMap<TxnId, Txn>,
  history: Vec<Txn>,
}

impl SlaveProcess {
  pub fn new(config: Arc<AhabConfig>) -> SlaveProcess {
    SlaveProcess{
      helper: ProcessHelper::new(),
      config: config,
      phase: Discovery,
      last_epoch: 0,
      last_master: None,
      accepted_txns: TreeMap::new(),
      committing_txns: TreeMap::new(),
      history: Vec::new(),
    }
  }

  pub fn infer_master(&self) -> HostId {
    HostId(0)
  }

  pub fn commit_txn(&mut self, txn: Txn) {
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
    let identity = HostId(0);
    let leader = ProcessAddr::new();
    loop {
      match self.phase {
        Discovery => {
          // Send to leader CurrentEpoch(self.last_epoch).
          self.send(&leader, CurrentEpoch(self.last_epoch));
          // Receive from leader NewEpoch(e).
          let (e, last_xid) = match_until!(self.recv_from(&leader), NewEpoch(e, xid) => (e, xid));
          // If self.last_epoch < e, then set self.last_epoch = e, send to
          // leader AckEpoch(self.last_master, diff), and transition to
          // Synchronization phase.
          if self.last_epoch < e {
            self.last_epoch = e;
            self.send(&leader, AckEpoch(identity, Vec::new())); // FIXME
            self.phase = Synchronization;
          }
        },
        Synchronization => {
          // Receive from leader NewMaster(e, diff).
          // If self.last_epoch != e, then transition to Discovery phase.
          let (e, diff) = match_until!(self.recv_from(&leader), NewMaster(e, diff) => (e, diff));
          if e != self.last_epoch {
            self.phase = Discovery;
            continue;
          }

          // Atomically:
          // 1. set self.last_epoch = e,
          // 2. for each txn in diff, accept txn.
          // (If failure, then transition to Discovery phase.)
          let accepted_txns = {
            let mut accepted_txns = Vec::<Txn>::new();
            let mut diff_accepted = true;
            for txn in diff.move_iter() {
              if txn.acceptable() {
                accepted_txns.push(txn);
              } else {
                diff_accepted = false;
                break;
              }
            }
            if diff_accepted {
              self.last_epoch = e;
            } else {
              self.phase = Discovery;
              continue;
            }
            accepted_txns
          };

          // Send to leader AckMaster.
          self.send(&leader, AckMaster(self.last_epoch));

          // Receive from leader CommitMaster.
          // (If self.last_epoch != e, then transition to Discovery phase.)
          let e = match_until!(self.recv_from(&leader), CommitMaster(e) => e);
          if e != self.last_epoch {
            self.phase = Discovery;
            continue;
          }

          // For each accepted txn, deliver txn.
          for txn in accepted_txns.move_iter() {
            self.commit_txn(txn);
          }

          // Transition to Broadcast phase.
          // If self.infer_master() is itself, invoke self.ready(e).
          self.phase = Broadcast;
          if identity == self.infer_master() {
            // FIXME
          }
        },
        Broadcast => {
          match self.recv_from(&leader) {
            Propose(e, txn) => {
              // Accept proposed txns from leader and append each txn to
              // self.history.
              self.accepted_txns.insert(txn.xid, txn);
            },
            Commit(e, xid) => {
              // Upon receiving from leader Commit(e, txn), and when all txn'
              // s.t. txn' < txn are committed, commit txn.
              match self.accepted_txns.pop(&xid) {
                Some(txn) => {
                  self.committing_txns.insert(xid, txn);
                },
                None => (),
              }
              let mut to_commit_xids = Vec::<TxnId>::new();
              'commit_all: for (&xid, txn) in self.committing_txns.iter() {
                for (&acc_xid, acc_txn) in self.accepted_txns.iter() {
                  if acc_xid < xid {
                    break 'commit_all;
                  } else {
                    break;
                  }
                }
                to_commit_xids.push(xid);
              }
              for xid in to_commit_xids.iter() {
                let txn = self.committing_txns.pop(xid).unwrap();
                self.commit_txn(txn);
              }
            },
            _ => (),
          }
        },
      }
    }
  }
}
