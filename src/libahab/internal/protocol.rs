use internal::config::{StaticConfig};
use internal::network::{HostId};
use internal::process::{Process, ProcessHelper};

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
enum Phase {
  Election,
  Discovery,
  Synchronization,
  Broadcast,
}

#[deriving(Clone)]
#[deriving(Decodable, Encodable)]
enum State {
  Voting,
  Following,
  Leading,
}

#[deriving(Clone)]
#[deriving(Decodable, Encodable)]
struct Vote {
  leader: HostId,
  xid: TxnId,
  epoch: Epoch,
}

struct SharedData {
  history: Vec<Txn>,
}

impl SharedData {
  pub fn new() -> SharedData {
    SharedData{
      history: Vec::new(),
    }
  }
}

#[deriving(Clone)]
#[deriving(Decodable, Encodable)]
pub enum ProtocolMsg {
  Timeout,
  TimeoutAll,
  Heartbeat,
  // Leader election.
  Notify(Epoch, State, Vote),
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

pub struct MasterProcess {
  helper: ProcessHelper,
  config: Arc<StaticConfig>,
  shared: Arc<RWLock<SharedData>>,
  state: State,
  phase: Phase,
  quorum: HashSet<HostId>,
  epoch: Epoch,
}

impl MasterProcess {
  pub fn new(config: Arc<StaticConfig>, shared: Arc<RWLock<SharedData>>) -> MasterProcess {
    MasterProcess{
      helper: ProcessHelper::new(),
      config: config,
      shared: shared,
      state: Voting,
      phase: Election,
      quorum: HashSet::new(),
      epoch: 0,
    }
  }

  fn discovery_process(&mut self) {
    let identity = HostId(0);

    // Receive from a quorum of followers CurrentEpoch(e).
    let mut quorum_epochs = HashMap::<HostId, Epoch>::new();
    loop {
      match self.recv() {
        (host, msg) => match msg {
          CurrentEpoch(e) => {
            self.quorum.insert(host);
            quorum_epochs.insert(host, e);
            if self.quorum.len() == self.config.quorum_max_size() {
              break;
            }
          },
          Timeout => {
            if self.quorum.len() >= self.config.quorum_min_size() {
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
    for (host, _) in quorum_epochs.iter() {
      // FIXME read txn history from master process?
      self.send(host, NewEpoch(self.epoch, TxnId::lower_bound()));
    }

    // Receive from quorum AckEpoch(m, diff).
    let mut quorum_xids = HashMap::<HostId, TxnId>::new();
    loop {
      match self.recv() {
        (host, msg) => match msg {
          AckEpoch(e, xid) => {
            if self.quorum.contains(&host) {
              quorum_xids.insert(host, xid);
            }
            if quorum_xids.len() == self.config.quorum_max_size() {
              break;
            }
          },
          Timeout => {
            if quorum_xids.len() >= self.config.quorum_min_size() {
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
      for (&host, &xid) in quorum_xids.iter() {
        if xid > sup_xid {
          chosen_host = host;
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
  }

  fn synchronization_process(&mut self) {
    // Send to quorum NewMaster(e', diff).
    // Receive from quorum AckMaster(e', diff), send to quorum
    // CommitMaster(e', diff), and transition to Broadcast phase.
  }

  fn broadcast_process(&mut self) {
    // Receive from primary process a proposed txn.
    // Send to quorum Propose(e', txn).
    // Receive from quorum Ack(e', txn).
    // Send to all followers Commit(e', txn).
  }
}

impl Process<ProtocolMsg> for MasterProcess {
  fn send(&self, dest: &HostId, msg: ProtocolMsg) {
  }

  fn recv(&self) -> (HostId, ProtocolMsg) {
    self.helper.recv();
    (HostId(0), Heartbeat) // FIXME
  }

  fn process(&mut self) {
    match self.phase {
      Election => {
        // XXX master should never run in election phase.
      },
      Discovery => {
        self.discovery_process();
      },
      Synchronization => {
        self.synchronization_process();
      },
      Broadcast => {
        self.broadcast_process();
      },
    }
  }
}

struct ElectionAtomicData {
  epoch: Epoch,
  state: State,
  //proposal: Vote,
}

impl ElectionAtomicData {
  pub fn new() -> ElectionAtomicData {
    ElectionAtomicData{
      epoch: 0,
      state: Voting,
    }
  }
}

pub struct SlaveProcess {
  helper: ProcessHelper,
  config: Arc<StaticConfig>,
  shared: Arc<RWLock<SharedData>>,
  state: State,
  phase: Phase,
  election: RWLock<ElectionAtomicData>,
  disc_epoch: Epoch,
  sync_epoch: Epoch,
  received_votes: HashMap<HostId, Vote>,
  out_of_election: HashMap<HostId, Vote>,
  accepted_txns: TreeMap<TxnId, Txn>,
  ready_txns: TreeMap<TxnId, Txn>,
}

impl SlaveProcess {
  pub fn new(config: Arc<StaticConfig>, shared: Arc<RWLock<SharedData>>) -> SlaveProcess {
    SlaveProcess{
      helper: ProcessHelper::new(),
      config: config,
      shared: shared,
      state: Voting,
      phase: Election,
      election: RWLock::new(ElectionAtomicData::new()),
      disc_epoch: 0,
      sync_epoch: 0,
      received_votes: HashMap::new(),
      out_of_election: HashMap::new(),
      accepted_txns: TreeMap::new(),
      ready_txns: TreeMap::new(),
    }
  }

  pub fn send_all(&self, msg: ProtocolMsg) {
    for (host, _) in self.config.hosts.iter() {
      self.send(host, msg.clone());
    }
  }

  pub fn recv_notify(&mut self) -> (HostId, ProtocolMsg) {
    loop {
      /*match self.recv() {
        // Receive vote notification from a peer.
        (host, Notify(vote)) => {
          let peer_is_looking = match vote.state {
            Voting => true,
            _ => false,
          };
          let peer_is_lagged = true; // FIXME
          match self.state {
            // If self is looking, the peer is looking, and the peer is
            // lagged, then respond with the leader candidate.
            Voting => {
              if peer_is_looking && peer_is_lagged {
                self.send(&host, Notify(self.vote()));
              }
            },
            // Otherwise, if the peer is looking, then respond with the
            // proposed leader.
            _ => {
              if peer_is_looking {
                self.send(&host, Notify(self.final_vote()));
              }
            },
          }
          return (host, Notify(vote));
        },
        _ => (),
      }*/
    }
  }

  pub fn predicate_total_order(&self, other: &Vote) -> bool {
    /*if false {
    //if weight of other == 0 {
      false
    } else {
      let vote = self.vote();
      other.leader_epoch > vote.leader_epoch ||
      (other.leader_epoch == vote.leader_epoch &&
       (other.leader_xid > vote.leader_xid &&
        other.leader > other.leader))
    }*/
    true
  }

  pub fn predicate_termination(&self, votes: &HashMap<HostId, Vote>) -> bool {
    true
  }

  pub fn predicate_valid_leader(&self, votes: &HashMap<HostId, Vote>, leader: HostId, epoch: Epoch) -> bool {
    /*if leader != self.config.identity {
      match votes.find(&leader) {
        Some(vote) => match vote.state {
          Leading => true,
          _ => false,
        },
        None => false,
      }
    } else if epoch != self.vote_epoch {
      false
    } else {
      true
    }*/
    true
  }

  pub fn infer_master(&self) -> HostId {
    HostId(0)
  }

  /*pub fn vote(&self) -> Vote {
    Vote{
      leader: HostId(0),
      xid: TxnId{epoch: 0, tick: 0},
      epoch: 0,
    }
  }

  pub fn final_vote(&self) -> Vote {
    Vote{
      leader: HostId(0),
      xid: TxnId{epoch: 0, tick: 0},
      epoch: 0,
    }
  }*/

  fn election_process(&mut self) {
    /*let (host, vote) = match self.recv_notify() {
      (host, Notify(vote)) => (host, vote),
      // If not enough notifications received, send more.
      (_, TimeoutAll) => {
        self.send_all(Notify(self.vote()));
        continue;
      },
      _ => continue,
    };
    match vote.state {
      Voting => {
        if vote.epoch > self.vote_epoch {
          // TODO
        } else if vote.epoch < self.vote_epoch {
          // TODO
        } else if true { // FIXME
        }
      },
      Following | Leading => {
        // Consider notifications from the same epoch.
        if vote.epoch == self.vote_epoch {
        }
        // Verify that a quorum is following the same leader.
        // TODO
      },
    }*/
  }

  fn discovery_process(&mut self) {
    let identity = HostId(0);
    let leader = HostId(0);
    // Send to leader CurrentEpoch(self.disc_epoch).
    self.send(&leader, CurrentEpoch(self.disc_epoch));
    // Receive from leader NewEpoch(e).
    let (e, last_xid) = match_until!(self.recv_from(&leader), NewEpoch(e, xid) => (e, xid));
    // If self.disc_epoch < e, then set self.disc_epoch = e, send to
    // leader AckEpoch(self.sync_epoch, diff), and transition to
    // Synchronization phase.
    if self.disc_epoch < e {
      self.disc_epoch = e;
      self.send(&leader, AckEpoch(self.sync_epoch, TxnId::lower_bound())); // FIXME
      self.phase = Synchronization;
    }
  }

  fn synchronization_process(&mut self) {
    let identity = HostId(0);
    let leader = HostId(0);

    // Receive from leader NewMaster(e, diff).
    // If self.disc_epoch != e, then transition to Discovery phase.
    let (e, diff) = match_until!(self.recv_from(&leader), NewMaster(e, diff) => (e, diff));
    if e != self.disc_epoch {
      self.phase = Discovery;
      return;
    }

    // Atomically:
    // 1. set self.disc_epoch = e,
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
        self.sync_epoch = e;
      } else {
        self.phase = Discovery;
        return;
      }
      accepted_txns
    };

    // Send to leader AckMaster.
    self.send(&leader, AckMaster(self.disc_epoch));

    // Receive from leader CommitMaster.
    // (If self.disc_epoch != e, then transition to Discovery phase.)
    let e = match_until!(self.recv_from(&leader), CommitMaster(e) => e);
    if e != self.disc_epoch {
      self.phase = Discovery;
      return;
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
  }

  fn broadcast_process(&mut self) {
    let identity = HostId(0);
    let leader = HostId(0);
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
            self.ready_txns.insert(xid, txn);
          },
          None => (),
        }
        let mut to_commit_xids = Vec::<TxnId>::new();
        'commit_all: for (&xid, txn) in self.ready_txns.iter() {
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
            let txn = self.ready_txns.pop(xid).unwrap();
            shared.history.push(txn);
          }
        }
      },
      _ => (),
    }
  }
}

impl Process<ProtocolMsg> for SlaveProcess {
  fn send(&self, dest: &HostId, msg: ProtocolMsg) {
  }

  fn recv(&self) -> (HostId, ProtocolMsg) {
    self.helper.recv();
    (HostId(0), Heartbeat) // FIXME
  }

  fn process(&mut self) {
    loop {
      match self.phase {
        Election => {
          self.election_process();
        },
        Discovery => {
          self.discovery_process();
        },
        Synchronization => {
          self.synchronization_process();
        },
        Broadcast => {
          self.broadcast_process();
        },
      }
    }
  }
}
