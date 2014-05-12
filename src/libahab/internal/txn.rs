use internal::protocol::{Epoch};

use serialize::{Decodable, Decoder, Encodable, Encoder};
use std::str;

#[deriving(Clone, TotalEq, TotalOrd)]
#[deriving(Decodable, Encodable)]
pub struct TxnId {
  epoch: Epoch,
  index: u64,
}

impl TxnId {
  pub fn lower_bound() -> TxnId {
    TxnId{epoch: Epoch(0), index: 0}
  }
}

impl Eq for TxnId {
  fn eq(&self, other: &TxnId) -> bool {
    self.epoch == other.epoch && self.index == other.index
  }
}

impl Ord for TxnId {
  fn lt(&self, other: &TxnId) -> bool {
    self.epoch < other.epoch || (self.epoch == other.epoch && self.index < other.index)
  }
}

#[deriving(Clone)]
#[deriving(Decodable, Encodable)]
pub enum TxnOp {
  Put(TxnPutOp),
  Delete(TxnDeleteOp),
  CmpSwap(TxnCmpSwapOp),
  Expire(TxnExpireOp),
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
    unsafe {
      match s.emit_str(str::raw::from_utf8(value)) { //.to_base64(base64::STANDARD)) {
        Ok(()) => (),
        Err(e) => return Err(e),
      }
    }
    Ok(())
  }
}

impl<E, D: Decoder<E>> Decodable<D, E> for TxnPutOp {
  fn decode(d: &mut D) -> Result<TxnPutOp, E> {
    let key = match d.read_str() {
      Ok(key) => key,
      Err(e) => return Err(e),
    };
    let value = match d.read_str() {
      Ok(value) => value.into_bytes(), //value.from_base64().ok().unwrap(),
      Err(e) => return Err(e),
    };
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
pub struct TxnCmpSwapOp {
  key: ~str,
  //old_value: ~[u8],
  //new_value: ~[u8],
}

#[deriving(Clone)]
#[deriving(Decodable, Encodable)]
pub struct TxnExpireOp {
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
