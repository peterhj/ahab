use internal::config::{StaticConfig};
use internal::process::{HostId, NetworkMsg, Process, ProcessHelper, ProcessId, ProcessPort};
use internal::txn::{Txn, TxnId};

use collections::hashmap::{HashMap, HashSet};
use collections::treemap::{TreeMap};
use serialize::json;
use serialize::json::{ToJson};
use std::cmp::{max};
use std::str;
use sync::{Arc, RWLock};

pub static MASTER_PORT: ProcessPort     = ProcessPort(1);
pub static REPLICA_PORT: ProcessPort    = ProcessPort(2);

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

#[deriving(Clone, Eq, TotalEq, Ord, TotalOrd)]
#[deriving(Decodable, Encodable)]
pub struct Epoch(u64);

impl Epoch {
  pub fn inc(self) -> Epoch {
    let curr: u64 = match self {
      Epoch(e) => e,
    };
    let next = curr + 1;
    assert!(next != 0);
    Epoch(next)
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

#[deriving(Clone, Eq, TotalEq)]
#[deriving(Decodable, Encodable)]
enum State {
  Voting,
  Following,
  Leading,
}

#[deriving(Clone, Eq, TotalEq)]
#[deriving(Decodable, Encodable)]
struct Vote {
  epoch: Epoch,
  state: State,
  leader: HostId,
  leader_epoch: Epoch,
  leader_xid: TxnId,
}

impl Vote {
  pub fn loosely_eq(&self, other: &Vote) -> bool {
    self.state == other.state &&
    self.leader == other.leader &&
    self.leader_epoch == other.leader_epoch
  }
}

#[deriving(Clone)]
#[deriving(Decodable, Encodable)]
pub enum ProtocolMsg {
  KeepAlive,
  Timeout,
  TimeoutAll,
  // Leader election.
  Notify(Vote),
  // Discovery phase.
  CurrentEpoch(Epoch),
  NewEpoch(Epoch, TxnId),
  AckEpoch(Epoch, TxnId),
  ReqEpochHistory(Epoch, TxnId),
  AckEpochHistory(Epoch, Vec<Txn>),
  // Synchronization phase.
  NewMaster(Epoch, Vec<Txn>),
  AckMaster(Epoch, TxnId),
  CommitMaster(Epoch, TxnId),
  // Broadcast phase.
  Submit(Txn),
  Propose(Epoch, Txn),
  Ack(Epoch, TxnId),
  Commit(Epoch, TxnId),
}

pub struct ProtocolHelper {
  inner: ProcessHelper,
  last_msg_times: HashMap<ProcessId, u32>,
}

impl ProtocolHelper {
  pub fn new() -> ProtocolHelper {
    ProtocolHelper{
      inner: ProcessHelper::new(),
      last_msg_times: HashMap::new(),
    }
  }

  pub fn send(&self, dest: &HostId, msg: ProtocolMsg) {
    let enc_bytes = json::Encoder::buffer_encode(&msg);
    let raw_msg = NetworkMsg{bytes: enc_bytes};
    self.inner.send(raw_msg);
  }

  pub fn recv(&self) -> (HostId, ProtocolMsg) {
    let raw_msg = self.inner.recv();
    let json_str = unsafe {
      str::raw::from_utf8_owned(raw_msg.bytes)
    };
    let json = json_str.to_json();
    let mut decoder = json::Decoder::new(json);
    // FIXME
    //let msg: ProtocolMsg = decoder.read_enum("ProtocolMsg", ||);
    (HostId(0), KeepAlive) // FIXME
  }
}

pub struct SharedData {
  history: Vec<Txn>,
}

impl SharedData {
  pub fn new() -> SharedData {
    SharedData{
      history: Vec::new(),
    }
  }
}

pub struct MasterProcess {
  helper: ProtocolHelper,
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
      helper: ProtocolHelper::new(),
      config: config,
      shared: shared,
      state: Voting,
      phase: Election,
      quorum: HashSet::new(),
      epoch: Epoch(0),
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
      let mut sup_epoch = Epoch(0);
      for (_, &e) in quorum_epochs.iter() {
        sup_epoch = max(sup_epoch, e);
      }
      sup_epoch.inc()
    };
    assert!(self.epoch != Epoch(0));
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
    // TODO adjust the colo follower history w/ the diff.
    // Transition to Synchronization phase.
    self.phase = Synchronization;
  }

  fn synchronization_process(&mut self) {
    // Send to quorum NewMaster(e', diff).
    let last_xid = TxnId::lower_bound(); // FIXME
    for host in self.quorum.iter() {
      let diff = Vec::new();
      self.send(host, NewMaster(self.epoch, diff)); // FIXME
    }
    // Receive from quorum AckMaster(e', diff), send to quorum
    // CommitMaster(e', diff), and transition to Broadcast phase.
    let mut quorum_acks = HashMap::<HostId, (Epoch, TxnId)>::new();
    loop {
      match self.recv() {
        (host, AckMaster(e, xid)) => {
          if self.quorum.contains(&host) {
            if e == self.epoch && xid == last_xid {
              quorum_acks.insert(host, (e, xid));
              if quorum_acks.len() >= self.config.quorum_min_size() {
                self.phase = Broadcast;
                return;
              }
            } else {
              self.phase = Discovery;
              return;
            }
          }
        },
        _ => (),
      }
    }
  }

  fn broadcast_process(&mut self) {
    loop {
      match self.recv() {
        (host, KeepAlive) => {
          // TODO
        },
        (host, Timeout) => {
          // TODO
        },
        (_, TimeoutAll) => {
          // TODO
        },
        // Receive from primary process a proposed txn.
        (host, Submit(txn)) => {
          // Send to quorum Propose(e', txn).
          for host in self.quorum.iter() {
            self.send(host, Propose(self.epoch, txn.clone()));
          }
        },
        // Receive from quorum Ack(e', txn).
        (host, Ack(e, txn)) => {
          // Send to all followers Commit(e', txn).
          if e == self.epoch {
            for host in self.quorum.iter() {
              self.send(host, Commit(self.epoch, txn.clone()));
            }
          }
        }
        (host, CurrentEpoch(e)) => {
          let last_xid = TxnId::lower_bound(); // FIXME
          self.send(&host, NewEpoch(self.epoch, last_xid));
        },
        (host, AckEpoch(e, xid)) => {
          if e == self.epoch {
            self.send(&host, NewMaster(self.epoch, Vec::new())); // FIXME
          }
        }
        _ => (),
      }
    }
  }
}

impl Process<ProtocolMsg> for MasterProcess {
  fn send(&self, dest: &HostId, msg: ProtocolMsg) {
  }

  fn recv(&self) -> (HostId, ProtocolMsg) {
    self.helper.recv();
    (HostId(0), KeepAlive) // FIXME
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

enum ElectionTerminationMode {
  Strict,
  Loose,
}

pub struct ReplicaProcess {
  helper: ProtocolHelper,
  config: Arc<StaticConfig>,
  shared: Arc<RWLock<SharedData>>,
  state: State,
  phase: Phase,
  election_proposal: RWLock<Vote>,
  accepted_epoch: Epoch,
  current_epoch: RWLock<Epoch>,
  accepted_txns: TreeMap<TxnId, Txn>,
  ready_txns: TreeMap<TxnId, Txn>,
}

impl ReplicaProcess {
  pub fn new(config: Arc<StaticConfig>, shared: Arc<RWLock<SharedData>>) -> ReplicaProcess {
    ReplicaProcess{
      helper: ProtocolHelper::new(),
      config: config,
      shared: shared,
      state: Voting,
      phase: Election,
      election_proposal: RWLock::new(Vote{ // FIXME
        epoch: Epoch(0),
        state: Voting,
        leader: HostId(0),
        leader_epoch: Epoch(0),
        leader_xid: TxnId::lower_bound(),
      }),
      accepted_epoch: Epoch(0),
      current_epoch: RWLock::new(Epoch(0)),
      accepted_txns: TreeMap::new(),
      ready_txns: TreeMap::new(),
    }
  }

  fn send_all(&self, msg: ProtocolMsg) {
    for (host, _) in self.config.hosts.iter() {
      // FIXME to itself too?
      self.send(host, msg.clone());
    }
  }

  fn infer_master(&self) -> HostId {
    HostId(0)
  }

  fn self_vote(&self) -> Vote {
    Vote{
      epoch: Epoch(0),
      state: Voting,
      leader: self.config.identity,
      leader_epoch: *self.current_epoch.read(),
      leader_xid: TxnId::lower_bound(),
    }
  }

  fn total_order_predicate(&self, new_vote: &Vote, curr_vote: &Vote) -> bool {
    new_vote.leader_epoch > curr_vote.leader_epoch ||
    (new_vote.leader_epoch == curr_vote.leader_epoch &&
     (new_vote.leader_xid > curr_vote.leader_xid &&
      new_vote.leader > curr_vote.leader))
  }

  fn termination_predicate(&self, votes: &HashMap<HostId, Vote>, fixed_vote: &Vote, mode: ElectionTerminationMode) -> bool {
    let mut potential_quorum = HashSet::<HostId>::new();
    for (&host, vote) in votes.iter() {
      match mode {
        Strict => {
          if vote == fixed_vote {
            potential_quorum.insert(host);
          }
        },
        Loose => {
          if vote.loosely_eq(fixed_vote) {
            potential_quorum.insert(host);
          }
        },
      }
    }
    potential_quorum.len() >= self.config.quorum_min_size()
  }

  fn valid_leader_predicate(&self, votes: &HashMap<HostId, Vote>, leader: HostId, maybe_epoch: Option<Epoch>) -> bool {
    let election_proposal = self.election_proposal.read();
    if leader != self.config.identity {
      match votes.find(&leader) {
        Some(vote) => match vote.state {
          Leading => true,
          _ => false,
        },
        None => false,
      }
    } else {
      match maybe_epoch {
        Some(epoch) => epoch == election_proposal.epoch,
        None => false,
      }
    }
  }

  fn election_recv(&mut self) -> (HostId, ProtocolMsg) {
    loop {
      match self.recv() {
        // Receive vote notification from a peer.
        (host, Notify(vote)) => {
          let peer_is_looking = match vote.state {
            Voting => true,
            _ => false,
          };
          let peer_is_lagged = vote.epoch < self.election_proposal.read().epoch;
          match self.state {
            // If self is looking, the peer is looking, and the peer is
            // lagged, then respond with the leader candidate.
            Voting => {
              if peer_is_looking && peer_is_lagged {
                self.send(&host, Notify(*self.election_proposal.read()));
              }
            },
            // Otherwise, if the peer is looking, then respond with the
            // proposed leader.
            _ => {
              if peer_is_looking {
                self.send(&host, Notify(*self.election_proposal.read()));
              }
            },
          }
          return (host, Notify(vote));
        },
        _ => (),
      }
    }
  }

  fn election_notify_all(&mut self) {
    self.send_all(Notify(*self.election_proposal.read()));
  }

  fn election_update_proposal(&mut self, new_vote: &Vote) {
    let mut election_proposal = self.election_proposal.write();
    election_proposal.state = self.state;
    election_proposal.leader = new_vote.leader;
    election_proposal.leader_epoch = new_vote.leader_epoch;
    election_proposal.leader_xid = new_vote.leader_xid;
  }

  fn election_leave(&mut self, final_vote: &Vote) {
    assert!(match self.state {
      Voting => false,
      Following | Leading => true,
    });
    self.phase = Discovery;
  }

  fn election_process(&mut self) {
    let mut received_set = HashMap::<HostId, Vote>::new();
    let mut out_of_election = HashMap::<HostId, Vote>::new();
    {
      let mut election_proposal = self.election_proposal.write();
      election_proposal.epoch = election_proposal.epoch.inc();
      election_proposal.state = Voting;
      election_proposal.leader = self.config.identity;
      election_proposal.leader_epoch = *self.current_epoch.read();
      election_proposal.leader_xid = TxnId::lower_bound();
    }
    loop {
      let (host, vote) = match self.election_recv() {
        (host, Notify(vote)) => (host, vote),
        // If not enough notifications received, send more.
        (_, TimeoutAll) => {
          self.election_notify_all();
          continue;
        },
        _ => continue,
      };
      let election_epoch = self.election_proposal.read().epoch;
      match vote.state {
        Voting => {
          // If notification > current, replace and send messages out.
          if vote.epoch > election_epoch {
            self.election_proposal.write().epoch = vote.epoch;
            received_set.clear();
            let self_vote = self.self_vote();
            if self.total_order_predicate(&vote, &self_vote) {
              self.election_update_proposal(&vote);
            } else {
              self.election_update_proposal(&self_vote);
            }
            self.election_notify_all();
          } else if vote.epoch < election_epoch {
            return;
          } else if self.total_order_predicate(&vote, &*self.election_proposal.read()) {
            self.election_update_proposal(&vote);
            self.election_notify_all();
          }
          received_set.insert(host, vote);
          if self.termination_predicate(&received_set, &*self.election_proposal.read(), Strict) {
            loop {
              match self.election_recv() {
                (host, Notify(vote)) => {
                  self.send(&self.config.identity, Notify(vote));
                  break;
                },
                (_, TimeoutAll) => {
                  if self.election_proposal.read().leader == self.config.identity {
                    self.state = Leading;
                  } else {
                    self.state = Following;
                  }
                  let election_proposal = *self.election_proposal.read();
                  return self.election_leave(&election_proposal);
                },
                _ => (),
              }
            }
          }
        },
        Following | Leading => {
          // Consider notifications from the same epoch.
          if vote.epoch == election_epoch {
            received_set.insert(host, vote);
            if self.termination_predicate(&received_set, &vote, Strict) &&
               self.valid_leader_predicate(&out_of_election, vote.leader, Some(vote.leader_epoch))
            {
              if vote.leader == self.config.identity {
                self.state = Leading;
              } else {
                self.state = Following;
              }
              return self.election_leave(&vote);
            }
          }
          // Verify that a quorum is following the same leader.
          out_of_election.insert(host, vote);
          if self.termination_predicate(&out_of_election, &vote, Loose) &&
             self.valid_leader_predicate(&out_of_election, vote.leader, None)
          {
            {
              let mut election_proposal = self.election_proposal.write();
              if vote.leader == self.config.identity {
                self.state = Leading;
              } else {
                self.state = Following;
              }
              election_proposal.epoch = vote.epoch;
            }
            return self.election_leave(&vote);
          }
        },
      }
    }
  }

  fn discovery_process(&mut self) {
    let identity = HostId(0);
    let leader = HostId(0);
    // Send to leader CurrentEpoch(self.accepted_epoch).
    self.send(&leader, CurrentEpoch(self.accepted_epoch));
    // Receive from leader NewEpoch(e).
    let (e, last_xid) = match_until!(self.recv_from(&leader), NewEpoch(e, xid) => (e, xid));
    // If self.accepted_epoch < e, then set self.accepted_epoch = e, send to
    // leader AckEpoch(self.current_epoch, diff), and transition to
    // Synchronization phase.
    if self.accepted_epoch < e {
      self.accepted_epoch = e;
      self.send(&leader, AckEpoch(*self.current_epoch.read(), TxnId::lower_bound())); // FIXME
      self.phase = Synchronization;
    }
  }

  fn synchronization_process(&mut self) {
    let identity = HostId(0);
    let leader = HostId(0);
    // Receive from leader NewMaster(e, diff).
    // If self.accepted_epoch != e, then transition to Discovery phase.
    let (e, diff) = match_until!(self.recv_from(&leader), NewMaster(e, diff) => (e, diff));
    if e != self.accepted_epoch {
      self.phase = Discovery;
      return;
    }
    // Atomically:
    // 1. set self.accepted_epoch = e,
    // 2. for each txn in diff, accept txn.
    // (If failure, then transition to Discovery phase.)
    let accepted_txns = {
      let mut current_epoch = self.current_epoch.write();
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
        *current_epoch = e;
      } else {
        self.phase = Discovery;
        return;
      }
      accepted_txns
    };
    // Send to leader AckMaster.
    let last_accepted_xid = match accepted_txns.last() {
      Some(ref txn) => txn.xid,
      None => TxnId::lower_bound(), // FIXME
    };
    self.send(&leader, AckMaster(self.accepted_epoch, last_accepted_xid));
    // Receive from leader CommitMaster.
    // (If self.accepted_epoch != e, then transition to Discovery phase.)
    let (e, xid) = match_until!(self.recv_from(&leader), CommitMaster(e, xid) => (e, xid));
    if e != self.accepted_epoch { // FIXME and check xid is recent.
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

impl Process<ProtocolMsg> for ReplicaProcess {
  fn send(&self, dest: &HostId, msg: ProtocolMsg) {
  }

  fn recv(&self) -> (HostId, ProtocolMsg) {
    self.helper.recv();
    (HostId(0), KeepAlive) // FIXME
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
