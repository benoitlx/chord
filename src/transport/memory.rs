use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::Transport;
use crate::id::Id;
use crate::rpc::{Error, Peer, Request, Response};

pub type Handler<const M: u32> = Arc<dyn Fn(Request<M>) -> Response<M> + Send + Sync>;

#[derive(Clone, Default)]
pub struct MemoryNetwork<const M: u32> {
    nodes: Arc<Mutex<HashMap<Id<M>, Handler<M>>>>,
}

impl<const M: u32> MemoryNetwork<M> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, peer: Peer<M>, handler: Handler<M>) {
        self.nodes.lock().unwrap().insert(peer.id, handler);
    }

    pub fn kill(&self, id: Id<M>) {
        self.nodes.lock().unwrap().remove(&id);
    }
}

impl<const M: u32> Transport<M> for MemoryNetwork<M> {
    async fn call(&self, to: &Peer<M>, req: Request<M>) -> Result<Response<M>, Error> {
        let handler = self.nodes.lock().unwrap().get(&to.id).cloned();

        match handler {
            Some(handler) => Ok(handler(req)),
            None => Err(Error::Unreachable),
        }
    }
}
