use config::{AhabConfig, HostId};
use internal::network::{HostAddr};
use internal::process::{Process, HostAddr, ProcessHelper};

use collections::hashmap::{HashMap, HashSet};
use collections::treemap::{TreeMap};
use serialize::{base64, Decodable, Decoder, Encodable, Encoder};
use serialize::base64::{FromBase64, ToBase64};
use std::cmp::{max};
use sync::{Arc, RWLock};

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

#[deriving(Clone, TotalEq, TotalOrd)]
#[deriving(Decodable, Encodable)]
pub struct TxnId {
  epoch: Epoch,
  tick: u64,
}

impl TxnId {
  pub fn lower_bound() -> TxnId {
    TxnId{epoch: 0, tick: 0}
  }
}

impl Eq for TxnId {
  fn eq(&self, other: &TxnId) -> bool {
    self.epoch == other.epoch && self.tick == other.tick
  }
}

impl Ord for TxnId {
  fn lt(&self, other: &TxnId) -> bool {
    self.epoch < other.epoch || (self.epoch == other.epoch && self.tick < other.tick)
  }
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
  Heartbeat,
  Timeout,
  // Discovery phase.
  CurrentEpoch(Epoch),
  NewEpoch(Epoch, TxnId),
  AckEpoch(Epoch, TxnId),
  ReqEpochHistory(Epoch, TxnId),
  AckEpochHistory(Epoch, Vec<Txn>),
  // Synchronization phase.
  NewMaster(Epoch, Vec<Txn>),
  AckMaster(Epoch),
  CommitMaster(Epoch),
  // Broadcast phase.
  Propose(Epoch, Txn),
  Ack(Epoch, TxnId),
  Commit(Epoch, TxnId),
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

pub struct SharedState {
  identity: HostId,
  history: Vec<Txn>,
}

impl SharedState {
  pub fn new() -> SharedState {
    SharedState{
      identity: HostId(0),
      history: Vec::new(),
    }
  }
}

pub struct MasterProcess {
  helper: ProcessHelper,
  config: Arc<AhabConfig>,
  phase: ProtocolPhase,
  quorum: HashSet<HostAddr>,
  epoch: Epoch,
  shared: Arc<RWLock<SharedState>>,
}

impl MasterProcess {
  pub fn new(config: Arc<AhabConfig>) -> MasterProcess {
    MasterProcess{
      helper: ProcessHelper::new(),
      config: config,
      phase: Discovery,
      quorum: HashSet::new(),
      epoch: 0,
      shared: Arc::new(RWLock::new(SharedState::new())),
    }
  }

  pub fn quorum_min_size(&self) -> uint {
    3
  }

  pub fn quorum_max_size(&self) -> uint {
    5
  }
}

impl Process<ProtocolMsg> for MasterProcess {
  fn send(&self, dest: &HostAddr, msg: ProtocolMsg) {
  }

  fn recv(&self) -> (HostAddr, ProtocolMsg) {
    self.helper.recv();
    (HostAddr::default(), Heartbeat)
  }

  fn process(&mut self) {
    let identity = HostAddr::default();
    match self.phase {
      Discovery => {
        // Receive from a quorum of followers CurrentEpoch(e).
        let mut quorum_epochs = HashMap::<HostAddr, Epoch>::new();
        loop {
          match self.recv() {
            (addr, msg) => match msg {
              CurrentEpoch(e) => {
                self.quorum.insert(addr);
                quorum_epochs.insert(addr, e);
                if self.quorum.len() == self.quorum_max_size() {
                  break;
                }
              },
              Timeout => {
                if self.quorum.len() >= self.quorum_min_size() {
                  break;
                }
              },
              _ => (),
            },
          }
        }

        // Send to quorum NewEpoch(e') s.t. e' = max(e received from quorum) + 1.
        self.epoch = {
          let mut sup_epoch: Epoch = 0;
          for (_, &e) in quorum_epochs.iter() {
            sup_epoch = max(sup_epoch, e);
          }
          sup_epoch + 1
        };
        assert!(self.epoch != 0);
        for (addr, _) in quorum_epochs.iter() {
          // FIXME read txn history from master process?
          self.send(addr, NewEpoch(self.epoch, TxnId::lower_bound()));
        }

        // Receive from quorum AckEpoch(m, diff).
        let mut quorum_xids = HashMap::<HostAddr, TxnId>::new();
        loop {
          match self.recv() {
            (addr, msg) => match msg {
              AckEpoch(e, xid) => {
                if self.quorum.contains(&addr) {
                  quorum_xids.insert(addr, xid);
                }
                if quorum_xids.len() == self.quorum_max_size() {
                  break;
                }
              },
              Timeout => {
                if quorum_xids.len() >= self.quorum_min_size() {
                  break;
                }
              },
              _ => (),
            }
          }
        }

        // Select a follower in quorum and its diff.
        let chosen_host = {
          let mut chosen_host = identity;
          let mut sup_xid = TxnId::lower_bound();
          for (&addr, &xid) in quorum_xids.iter() {
            if xid > sup_xid {
              chosen_host = addr;
              sup_xid = xid;
            }
          }
          chosen_host
        };

        // (Send to chosen follower ReqEpochHistory(e', xid').)
        let last_xid = {
          let shared = self.shared.read();
          match shared.history.last() {
            Some(txn) => txn.xid,
            None => TxnId::lower_bound(),
          }
        };
        self.send(&chosen_host, ReqEpochHistory(self.epoch, last_xid));

        // (Receive from chosen follower AckEpochHistory(e', diff).)
        let (e, diff) = match_until!(self.recv_from(&chosen_host), AckEpochHistory(e, diff) => (e, diff));
        assert!(e == self.epoch);
        // TODO adjust the dual follower history w/ the diff.

        // Transition to Synchronization phase.
        self.phase = Synchronization;
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
  last_epoch_prop: Epoch,
  last_master_prop: Epoch,
  accepted_txns: TreeMap<TxnId, Txn>,
  committing_txns: TreeMap<TxnId, Txn>,
  shared: Arc<RWLock<SharedState>>,
}

impl SlaveProcess {
  pub fn new(config: Arc<AhabConfig>) -> SlaveProcess {
    SlaveProcess{
      helper: ProcessHelper::new(),
      config: config,
      phase: Discovery,
      last_epoch_prop: 0,
      last_master_prop: 0,
      accepted_txns: TreeMap::new(),
      committing_txns: TreeMap::new(),
      //history: Vec::new(),
      shared: Arc::new(RWLock::new(SharedState::new())),
    }
  }

  pub fn infer_master(&self) -> HostId {
    HostId(0)
  }
}

impl Process<ProtocolMsg> for SlaveProcess {
  fn send(&self, dest: &HostAddr, msg: ProtocolMsg) {
  }

  fn recv(&self) -> (HostAddr, ProtocolMsg) {
    self.helper.recv();
    (HostAddr::default(), Heartbeat)
  }

  fn process(&mut self) {
    let identity = HostId(0);
    let leader = HostAddr::default();
    loop {
      match self.phase {
        Discovery => {
          // Send to leader CurrentEpoch(self.last_epoch_prop).
          self.send(&leader, CurrentEpoch(self.last_epoch_prop));
          // Receive from leader NewEpoch(e).
          let (e, last_xid) = match_until!(self.recv_from(&leader), NewEpoch(e, xid) => (e, xid));
          // If self.last_epoch_prop < e, then set self.last_epoch_prop = e, send to
          // leader AckEpoch(self.last_master_prop, diff), and transition to
          // Synchronization phase.
          if self.last_epoch_prop < e {
            self.last_epoch_prop = e;
            self.send(&leader, AckEpoch(self.last_master_prop, TxnId::lower_bound())); // FIXME
            self.phase = Synchronization;
          }
        },
        Synchronization => {
          // Receive from leader NewMaster(e, diff).
          // If self.last_epoch_prop != e, then transition to Discovery phase.
          let (e, diff) = match_until!(self.recv_from(&leader), NewMaster(e, diff) => (e, diff));
          if e != self.last_epoch_prop {
            self.phase = Discovery;
            continue;
          }

          // Atomically:
          // 1. set self.last_epoch_prop = e,
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
              self.last_master_prop = e;
            } else {
              self.phase = Discovery;
              continue;
            }
            accepted_txns
          };

          // Send to leader AckMaster.
          self.send(&leader, AckMaster(self.last_epoch_prop));

          // Receive from leader CommitMaster.
          // (If self.last_epoch_prop != e, then transition to Discovery phase.)
          let e = match_until!(self.recv_from(&leader), CommitMaster(e) => e);
          if e != self.last_epoch_prop {
            self.phase = Discovery;
            continue;
          }

          // For each accepted txn, deliver txn.
          {
            let mut shared = self.shared.write();
            for txn in accepted_txns.move_iter() {
              shared.history.push(txn);
            }
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
              {
                let mut shared = self.shared.write();
                for xid in to_commit_xids.iter() {
                  let txn = self.committing_txns.pop(xid).unwrap();
                  shared.history.push(txn);
                }
              }
            },
            _ => (),
          }
        },
      }
    }
  }
}
