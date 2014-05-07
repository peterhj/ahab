use serialize::{json, Decodable, Encodable};
use std::io::net::ip::{SocketAddr};
use std::io::net::tcp::{TcpListener, TcpStream};

pub enum RpcMsg {
}

pub struct RpcServer {
  addr: SocketAddr,
}
