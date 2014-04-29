use config::{AhabConfig, HostId};
use internal::server::{ProcessMessage, Protocol};

pub type Epoch = u64;

pub enum TxnOp {
  Put(~str, ~[u8]),
  Delete(~str),
}

pub struct TxnId {
  epoch: Epoch,
  id: u64,
}

pub struct Txn {
  ops: Vec<TxnOp>,
}

pub enum ProtocolMessage {
  // Discovery phase.
  CurrentEpoch(Epoch),
  NewEpoch(Epoch),
  AckEpoch(HostId), // TODO +history.
  // Synchronization phase.
  NewMaster(Epoch), // TODO +history.
  AckMaster,
  CommitMaster,
  // Broadcast phase.
  Propose(Epoch, TxnId, Txn),
  Ack,
  Commit,
}

enum ProtocolPhase {
  Discovery,
  Synchronization,
  Broadcast,
}

pub struct MasterState {
  config: AhabConfig,
  ctrl_chan: Sender<ProcessMessage>,
  phase: ProtocolPhase,
}

impl MasterState {
  pub fn new(config: AhabConfig, ctrl_chan: Sender<ProcessMessage>) -> MasterState {
    MasterState{
      config: config,
      ctrl_chan: ctrl_chan,
      phase: Discovery,
    }
  }
}

pub fn master_process(state: &mut MasterState, port: Receiver<ProcessMessage>) {
  loop {
    match state.phase {
      Discovery => {
        // Receive from a quorum of followers CurrentEpoch(e).
        // Send to quorum NewEpoch(e') s.t. e' = max(e received from quorum) + 1.
        // Receive from quorum AckEpoch(m, diff).
        // Select a follower in quorum and its diff.
        // Transition to Synchronization phase.
      },
      Synchronization => {
        // Send to quorum NewMaster(e', diff).
        // Receive from quorum AckMaster, send to quorum CommitMaster, and
        // transition to Broadcast phase.
      },
      Broadcast => {
        // Propose to quorum 
      },
    }
  }
}

pub struct SlaveState {
  config: AhabConfig,
  ctrl_chan: Sender<ProcessMessage>,
  phase: ProtocolPhase,
  last_epoch: Option<Epoch>,
  last_master: Option<HostId>,
}

impl SlaveState {
  pub fn new(config: AhabConfig, ctrl_chan: Sender<ProcessMessage>) -> SlaveState {
    SlaveState{
      config: config,
      ctrl_chan: ctrl_chan,
      phase: Discovery,
      last_epoch: None,
      last_master: None,
    }
  }

  pub fn infer_master(&self) -> HostId {
    HostId(0)
  }
}

pub fn slave_process(state: &mut SlaveState, port: Receiver<ProcessMessage>) {
  loop {
    match state.phase {
      Discovery => {
        // Send to leader CurrentEpoch(state.last_epoch).
        // Receive from leader NewEpoch(e).
        // If state.last_epoch < e, then set state.last_epoch = e, send to
        // leader AckEpoch(state.last_master, diff), and transition to
        // Synchronization phase.
      },
      Synchronization => {
        // Receive from leader NewMaster(e, diff).
        // If state.last_epoch != e, then transition to Discovery phase.
        // Atomically:
        // 1. set state.last_epoch = e,
        // 2. for each txn in diff, accept txn.
        // Send to leader AckMaster.
        // Receive from leader CommitMaster.
        // For each txn in state.history, deliver txn.
        // Transition to Broadcast phase.
      },
      Broadcast => {
        // If state.infer_master() is itself, invole state.ready(e).
        // Forever: accept proposed txns from leader and append each txn to
        // state.history.
        // Upon receiving from leader Commit(e, txn), and when all txn' s.t.
        // txn' < txn are committed, commit txn.
      },
    }
  }
}
