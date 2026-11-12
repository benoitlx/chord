pub mod memory;

use crate::rpc::{Error, Peer, Request, Response};

pub trait Transport<const M: u32>: Send + Sync + 'static {
    fn call(
        &self,
        to: &Peer<M>,
        req: Request<M>,
    ) -> impl Future<Output = Result<Response<M>, Error>> + Send;
}
