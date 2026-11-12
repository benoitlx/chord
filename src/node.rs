use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::id::Id;
use crate::rpc::{Error, Peer, Request, Response};
use crate::transport::Transport;

pub const SUCCESSOR_LIST_LEN: usize = 4;

pub const MAX_HOPS: u32 = 64;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum NextHop<const M: u32> {
    Found(Peer<M>),
    Forward(Peer<M>),
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Lookup<const M: u32> {
    pub owner: Peer<M>,
    pub hops: u32,
}

pub struct LocalView<const M: u32> {
    me: Peer<M>,
    predecessor: Option<Peer<M>>,
    successors: Vec<Peer<M>>,
    fingers: Vec<Option<Peer<M>>>,
}

impl<const M: u32> LocalView<M> {
    pub fn new(me: Peer<M>) -> Self {
        LocalView {
            me,
            predecessor: None,
            successors: vec![me],
            fingers: vec![None; M as usize],
        }
    }

    pub fn joined(me: Peer<M>, successor: Peer<M>) -> Self {
        LocalView {
            me,
            predecessor: None,
            successors: vec![successor],
            fingers: vec![None; M as usize],
        }
    }

    pub fn me(&self) -> Peer<M> {
        self.me
    }

    pub fn successor(&self) -> Peer<M> {
        self.successors[0]
    }

    pub fn predecessor(&self) -> Option<Peer<M>> {
        self.predecessor
    }

    pub fn successors(&self) -> &[Peer<M>] {
        &self.successors
    }

    pub fn owns(&self, key: Id<M>) -> bool {
        if let Some(pred) = self.predecessor {
            return key.in_open_closed(pred.id, self.me.id);
        } else {
            return self.successor().id == self.me.id;
        }
    }

    pub fn closest_preceding(&self, key: Id<M>) -> Peer<M> {
        let mut best = self.me;
        for finger in self.fingers.iter().flatten() {
            if finger.id.in_open(best.id, key) {
                best = *finger;
            }
        }

        for succ in &self.successors {
            if succ.id.in_open(best.id, key) {
                best = *succ;
            }
        }

        best
    }

    pub fn next_hop(&self, key: Id<M>) -> NextHop<M> {
        let successor = self.successor();
        if key.in_open_closed(self.me.id, successor.id) {
            return NextHop::Found(successor);
        }

        let next = self.closest_preceding(key);
        if next.id == self.me.id {
            NextHop::Forward(successor)
        } else {
            NextHop::Forward(next)
        }
    }

    pub fn notify(&mut self, candidate: Peer<M>) {
        if self.predecessor == None {
            self.predecessor = Some(candidate);
            return;
        }

        if let Some(pred) = self.predecessor {
            if candidate.id.in_open(pred.id, self.me.id) {
                self.predecessor = Some(candidate);
            }
        }
    }

    pub fn stabilize_with(&mut self, succ_pred: Option<Peer<M>>) {
        let Some(x) = succ_pred else {
            return;
        };

        if x.id.in_open(self.me.id, self.successor().id) {
            let me = self.me.id;
            self.successors.retain(|p| p.id != me);
            self.successors.insert(0, x);
            self.successors.truncate(SUCCESSOR_LIST_LEN);
        }
    }

    pub fn update_successors(&mut self, succ_list: &[Peer<M>]) {
        let successor = self.successor();
        let mut list = vec![successor];

        for p in succ_list {
            if list.len() >= SUCCESSOR_LIST_LEN || p.id == self.me.id || p.id == successor.id {
                break;
            }
            list.push(*p);
        }

        self.successors = list;
    }

    pub fn set_finger(&mut self, i: u32, peer: Peer<M>) {
        assert!(i < M, "i should be less than M");
        self.fingers[i as usize] = Some(peer);
    }

    pub fn forget(&mut self, id: Id<M>) {
        if id == self.me.id {
            return;
        }

        self.successors.retain(|p| p.id != id);
        if self.successors.is_empty() {
            self.successors.push(self.me);
        }

        for finger in &mut self.fingers {
            if finger.is_some_and(|p| p.id == id) {
                *finger = None;
            }
        }

        if self.predecessor.is_some_and(|p| p.id == id) {
            self.predecessor = None;
        }
    }
}

pub struct Node<T, const M: u32> {
    state: Mutex<LocalView<M>>,
    transport: T,
    next_finger: Mutex<u32>,
}

impl<T, const M: u32> Node<T, M>
where
    T: Transport<M>,
{
    pub fn new(me: Peer<M>, transport: T) -> Arc<Self> {
        Arc::new(Node {
            state: Mutex::new(LocalView::new(me)),
            transport,
            next_finger: Mutex::new(0),
        })
    }

    pub fn me(&self) -> Peer<M> {
        self.state.lock().unwrap().me()
    }

    pub fn successor(&self) -> Peer<M> {
        self.state.lock().unwrap().successor()
    }

    pub fn predecessor(&self) -> Option<Peer<M>> {
        self.state.lock().unwrap().predecessor()
    }

    pub fn successors(&self) -> Vec<Peer<M>> {
        self.state.lock().unwrap().successors().to_vec()
    }

    fn forget(&self, id: Id<M>) {
        self.state.lock().unwrap().forget(id);
    }

    pub fn handle(&self, req: Request<M>) -> Response<M> {
        let mut state = self.state.lock().unwrap();

        match req {
            Request::Ping => Response::Pong,
            Request::GetPredecessor => Response::Predecessor(state.predecessor()),
            Request::GetSuccessorList => Response::SuccessorList(state.successors().to_vec()),
            Request::Notify(candidate) => {
                state.notify(candidate);
                Response::Notified
            }
            Request::FindNextHop(key) => Response::NextHop(state.next_hop(key)),
        }
    }

    pub async fn join(&self, bootstrap: Peer<M>) -> Result<(), Error> {
        let me = self.me();
        let lookup = self.lookup_from(bootstrap, me.id).await?;

        if lookup.owner.id == me.id {
            return Err(Error::IdTaken);
        }

        *self.state.lock().unwrap() = LocalView::joined(me, lookup.owner);
        Ok(())
    }

    pub async fn find_successor(&self, key: Id<M>) -> Result<Lookup<M>, Error> {
        let mut attempts = 0;
        loop {
            let hop = self.state.lock().unwrap().next_hop(key);

            let result = match hop {
                NextHop::Found(owner) => return Ok(Lookup { owner, hops: 0 }),
                NextHop::Forward(next) => self.lookup_from(next, key).await,
            };

            attempts += 1;
            match result {
                Err(Error::Unreachable) if attempts <= SUCCESSOR_LIST_LEN => continue,
                other => return other,
            }
        }
    }

    pub async fn lookup_from(&self, start: Peer<M>, key: Id<M>) -> Result<Lookup<M>, Error> {
        let mut current = start;
        let mut hops = 0;

        loop {
            if hops >= MAX_HOPS {
                return Err(Error::TooManyHops);
            }

            let response = match self.transport.call(&current, Request::FindNextHop(key)).await {
                Ok(response) => response,
                Err(err) => {
                    self.forget(current.id);
                    return Err(err);
                }
            };
            hops += 1;

            match response {
                Response::NextHop(NextHop::Found(owner)) => return Ok(Lookup { owner, hops }),
                Response::NextHop(NextHop::Forward(next)) => current = next,
                _ => return Err(Error::UnexpectedResponse),
            }
        }
    }

    pub async fn stabilize(&self) {
        let (me, successor, my_pred) = {
            let state = self.state.lock().unwrap();
            (state.me(), state.successor(), state.predecessor())
        };

        let succ_pred = if successor.id == me.id {
            my_pred
        } else {
            match self.transport.call(&successor, Request::GetPredecessor).await {
                Ok(Response::Predecessor(p)) => p,
                Ok(_) => return,
                Err(_) => {
                    self.forget(successor.id);
                    return;
                }
            }
        };

        let successor = {
            let mut state = self.state.lock().unwrap();
            state.stabilize_with(succ_pred);
            state.successor()
        };
        if successor.id == me.id {
            return;
        }

        match self.transport.call(&successor, Request::GetSuccessorList).await {
            Ok(Response::SuccessorList(list)) => {
                self.state.lock().unwrap().update_successors(&list);
            }
            Ok(_) => return,
            Err(_) => {
                self.forget(successor.id);
                return;
            }
        }

        let _ = self.transport.call(&successor, Request::Notify(me)).await;
    }

    pub async fn fix_fingers(&self) {
        let i = {
            let mut next = self.next_finger.lock().unwrap();
            let i = *next;
            *next = (i + 1) % M;
            i
        };

        let target = self.me().id.finger_target(i);
        if let Ok(lookup) = self.find_successor(target).await {
            self.state.lock().unwrap().set_finger(i, lookup.owner);
        }
    }

    pub async fn check_predecessor(&self) {
        let Some(pred) = self.predecessor() else {
            return;
        };

        if self.transport.call(&pred, Request::Ping).await.is_err() {
            self.forget(pred.id);
        }
    }

    pub fn spawn_maintenance(self: Arc<Self>, period: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(period);
            loop {
                interval.tick().await;
                self.stabilize().await;
                self.fix_fingers().await;
                self.check_predecessor().await;
            }
        })
    }
}
