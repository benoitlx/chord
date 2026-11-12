#![allow(dead_code)]

#[path = "../src/id.rs"]
mod id;
#[path = "../src/node.rs"]
mod node;
#[path = "../src/rpc.rs"]
mod rpc;
#[path = "../src/transport/mod.rs"]
mod transport;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use id::Id;
use node::Node;
use rpc::{Error, Peer, Request, Response};
use transport::Transport;
use transport::memory::MemoryNetwork;

const M: u32 = 6;

#[derive(Clone)]
struct Traced {
    net: MemoryNetwork<M>,
    log: Arc<Mutex<Vec<u64>>>,
}

impl Transport<M> for Traced {
    async fn call(&self, to: &Peer<M>, req: Request<M>) -> Result<Response<M>, Error> {
        if matches!(req, Request::FindNextHop(_)) {
            self.log.lock().unwrap().push(to.id.value());
        }
        self.net.call(to, req).await
    }
}

type DemoNode = Node<Traced, M>;

fn peer(id: u64) -> Peer<M> {
    Peer { id: Id::new(id), addr: SocketAddr::from(([127, 0, 0, 1], 4000 + id as u16)) }
}

fn spawn(t: &Traced, id: u64) -> Arc<DemoNode> {
    let node = Node::new(peer(id), t.clone());
    let h = node.clone();
    t.net.register(node.me(), Arc::new(move |req| h.handle(req)));
    node
}

async fn stabilize_all(nodes: &[Arc<DemoNode>], rounds: usize) {
    for _ in 0..rounds {
        for n in nodes {
            n.stabilize().await;
            n.check_predecessor().await;
        }
    }
}

async fn fix_all(nodes: &[Arc<DemoNode>]) {
    for n in nodes {
        for _ in 0..M {
            n.fix_fingers().await;
        }
    }
}

fn print_ring(nodes: &[Arc<DemoNode>]) {
    let mut sorted: Vec<_> = nodes.iter().collect();
    sorted.sort_by_key(|n| n.me().id.value());
    println!("  node │ predecessor  │ successors");
    println!("  ─────┼──────────────┼──────────────────────");
    for n in sorted {
        let pred = n.predecessor().map(|p| format!("N{}", p.id.value())).unwrap_or("—".into());
        let succ: Vec<String> = n.successors().iter().map(|p| format!("N{}", p.id.value())).collect();
        println!("  N{:<3} │ {:<12} │ {}", n.me().id.value(), pred, succ.join(", "));
    }
}

async fn trace(t: &Traced, from: &Arc<DemoNode>, k: u64) {
    t.log.lock().unwrap().clear();
    let l = from.find_successor(Id::new(k)).await.unwrap();
    let path: Vec<String> = t.log.lock().unwrap().iter().map(|id| format!("N{id}")).collect();
    let chemin = if path.is_empty() {
        format!("N{} (local)", from.me().id.value())
    } else {
        format!("N{} → {}", from.me().id.value(), path.join(" → "))
    };
    println!(
        "  lookup(K{:<2}) from N{:<2} = N{:<2} in {} hop(s)   [{}]",
        k, from.me().id.value(), l.owner.id.value(), l.hops, chemin
    );
}

async fn total_hops(nodes: &[Arc<DemoNode>]) -> u32 {
    let mut tot = 0;
    for n in nodes {
        for k in 0..64 {
            tot += n.find_successor(Id::new(k)).await.unwrap().hops;
        }
    }
    tot
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let scene = std::env::args().nth(1).unwrap_or_default();
    let t = Traced { net: MemoryNetwork::new(), log: Arc::new(Mutex::new(vec![])) };
    let order = [32u64, 1, 56, 14, 42, 8, 51, 21, 48, 38];

    match scene.as_str() {
        "join" => {
            println!("$ chord-demo join   # M = 6, ids 0..63\n");
            let mut nodes = vec![spawn(&t, order[0])];
            println!("  [+] N{} creates the ring (alone)", order[0]);
            for &id in &order[1..] {
                let n = spawn(&t, id);
                n.join(peer(order[0])).await.unwrap();
                println!("  [+] N{:<2} joins via N{}  → initial successor N{}", id, order[0], n.successor().id.value());
                nodes.push(n);
                stabilize_all(&nodes, 1).await;
            }
            stabilize_all(&nodes, 3).await;
            println!("\n  After 3 stabilization rounds:\n");
            print_ring(&nodes);
            let other = Node::new(peer(42), t.clone());
            println!("\n  [!] A second N42 tries to join: {:?}", other.join(peer(32)).await);
        }
        "lookup" => {
            println!("$ chord-demo lookup   # the paper's ring\n");
            let mut nodes = vec![spawn(&t, order[0])];
            for &id in &order[1..] {
                let n = spawn(&t, id);
                n.join(peer(order[0])).await.unwrap();
                nodes.push(n);
                stabilize_all(&nodes, 1).await;
            }
            stabilize_all(&nodes, 3).await;
            let n8 = nodes.iter().find(|n| n.me().id.value() == 8).unwrap().clone();
            println!("  Without finger table (successor list only):");
            for k in [10, 24, 54, 3] { trace(&t, &n8, k).await; }
            let before = total_hops(&nodes).await;
            fix_all(&nodes).await;
            println!("\n  After fix_fingers (6 fingers per node):");
            for k in [10, 24, 54, 3] { trace(&t, &n8, k).await; }
            let after = total_hops(&nodes).await;
            println!("\n  N8 finger table:");
            for i in 0..M {
                let target = Id::<M>::new(8).finger_target(i).value();
                let owner = n8.find_successor(Id::new(target)).await.unwrap().owner.id.value();
                println!("    finger[{i}]  8 + 2^{i} = {:<2} → N{}", target, owner);
            }
            println!("\n  Total hops (10 nodes × 64 keys): {before} without fingers, {after} with");
        }
        "failure" => {
            println!("$ chord-demo failure\n");
            let mut nodes = vec![spawn(&t, order[0])];
            for &id in &order[1..] {
                let n = spawn(&t, id);
                n.join(peer(order[0])).await.unwrap();
                nodes.push(n);
                stabilize_all(&nodes, 1).await;
            }
            stabilize_all(&nodes, 3).await;
            fix_all(&nodes).await;
            let n8 = nodes.iter().find(|n| n.me().id.value() == 8).unwrap().clone();
            trace(&t, &n8, 18).await;
            println!("\n  [x] N14 and N21 crash at the same time");
            t.net.kill(Id::new(14));
            t.net.kill(Id::new(21));
            let surv: Vec<_> = nodes.into_iter().filter(|n| ![14, 21].contains(&n.me().id.value())).collect();
            println!("      N8 still sees successors {:?}",
                n8.successors().iter().map(|p| p.id.value()).collect::<Vec<_>>());
            stabilize_all(&surv, 3).await;
            fix_all(&surv).await;
            println!("\n  After repair (stabilize + check_predecessor + fix_fingers):\n");
            print_ring(&surv);
            println!();
            trace(&t, &n8, 18).await;
        }
        "auto" => {
            println!("$ chord-demo auto   # background maintenance (5 ms period)\n");
            let a = spawn(&t, 8);
            let b = spawn(&t, 42);
            let c = spawn(&t, 21);
            b.join(a.me()).await.unwrap();
            c.join(a.me()).await.unwrap();
            let all = [a.clone(), b.clone(), c.clone()];
            println!("  t = 0 ms (right after join):");
            print_ring(&all);
            let tasks: Vec<_> = all.iter().map(|n| n.clone().spawn_maintenance(Duration::from_millis(5))).collect();
            tokio::time::sleep(Duration::from_millis(200)).await;
            for task in &tasks { task.abort(); }
            println!("\n  t = 200 ms:");
            print_ring(&all);
        }
        _ => eprintln!("usage: chord-demo join|lookup|failure|auto"),
    }
}
