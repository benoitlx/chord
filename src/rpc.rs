use std::net::SocketAddr;

use crate::id::Id;
use crate::node::NextHop;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Peer<const M: u32> {
    pub id: Id<M>,
    pub addr: SocketAddr,
}

#[derive(Clone, Debug)]
pub enum Request<const M: u32> {
    Ping,
    GetPredecessor,
    GetSuccessorList,
    Notify(Peer<M>),
    FindNextHop(Id<M>),
}

#[derive(Clone, Debug)]
pub enum Response<const M: u32> {
    Pong,
    Predecessor(Option<Peer<M>>),
    SuccessorList(Vec<Peer<M>>),
    Notified,
    NextHop(NextHop<M>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    Unreachable,
    UnexpectedResponse,
    TooManyHops,
    IdTaken,
}
