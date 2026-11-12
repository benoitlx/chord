use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::id::Id;
use crate::node::{LocalView, NextHop, Node};
use crate::rpc::{Error, Peer};
use crate::transport::memory::MemoryNetwork;

const PAPER_RING: [u64; 10] = [1, 8, 14, 21, 32, 38, 42, 48, 51, 56];

type TestNode = Node<MemoryNetwork<6>, 6>;

fn peer(id: u64) -> Peer<6> {
    Peer {
        id: Id::new(id),
        addr: SocketAddr::from(([127, 0, 0, 1], id as u16)),
    }
}

fn key(id: u64) -> Id<6> {
    Id::new(id)
}

fn expected_owner(ids: &[u64], key: u64) -> u64 {
    let mut sorted = ids.to_vec();
    sorted.sort();
    sorted.iter().copied().find(|&id| id >= key).unwrap_or(sorted[0])
}

fn n8_full() -> LocalView<6> {
    let mut view = LocalView::joined(peer(8), peer(14));
    view.update_successors(&[peer(21), peer(32), peer(38)]);
    for (i, f) in [14, 14, 14, 21, 32, 42].into_iter().enumerate() {
        view.set_finger(i as u32, peer(f));
    }
    view
}

#[test]
fn owns_exemples() {
    let mut n14 = LocalView::joined(peer(14), peer(21));
    n14.notify(peer(8));
    assert!(n14.owns(key(10)) && n14.owns(key(14)));
    assert!(!n14.owns(key(8)) && !n14.owns(key(15)));

    let mut n1 = LocalView::joined(peer(1), peer(8));
    n1.notify(peer(56));
    assert!(n1.owns(key(60)) && n1.owns(key(0)) && !n1.owns(key(2)));

    let alone = LocalView::new(peer(8));
    assert!((0..64).all(|k| alone.owns(key(k))));

    let just_joined = LocalView::joined(peer(21), peer(32));
    assert!((0..64).all(|k| !just_joined.owns(key(k))));
}

#[test]
fn closest_preceding_exemples() {
    let n8 = n8_full();
    assert_eq!(n8.closest_preceding(key(54)), peer(42));
    assert_eq!(n8.closest_preceding(key(30)), peer(21));
    assert_eq!(n8.closest_preceding(key(8)), peer(42));

    let just_joined = LocalView::joined(peer(21), peer(32));
    assert_eq!(just_joined.closest_preceding(key(40)), peer(32));
}

#[test]
fn next_hop_exemples() {
    let n8 = n8_full();
    assert_eq!(n8.next_hop(key(10)), NextHop::Found(peer(14)));
    assert_eq!(n8.next_hop(key(14)), NextHop::Found(peer(14)));
    assert_eq!(n8.next_hop(key(54)), NextHop::Forward(peer(42)));
    assert_eq!(n8.next_hop(key(8)), NextHop::Forward(peer(42)));

    let alone = LocalView::new(peer(8));
    assert_eq!(alone.next_hop(key(40)), NextHop::Found(peer(8)));
}

#[test]
fn notify_exemples() {
    let mut n14 = LocalView::new(peer(14));
    n14.notify(peer(1));
    assert_eq!(n14.predecessor(), Some(peer(1)));
    n14.notify(peer(8));
    assert_eq!(n14.predecessor(), Some(peer(8)));
    n14.notify(peer(1));
    assert_eq!(n14.predecessor(), Some(peer(8)));
}

#[test]
fn stabilize_with_exemples() {
    let mut n8 = LocalView::joined(peer(8), peer(21));
    n8.stabilize_with(None);
    assert_eq!(n8.successor(), peer(21));
    n8.stabilize_with(Some(peer(8)));
    assert_eq!(n8.successor(), peer(21));
    n8.stabilize_with(Some(peer(14)));
    assert_eq!(n8.successors(), &[peer(14), peer(21)]);
    n8.stabilize_with(Some(peer(1)));
    assert_eq!(n8.successor(), peer(14));

    let mut alone = LocalView::new(peer(8));
    alone.stabilize_with(Some(peer(42)));
    assert_eq!(alone.successors(), &[peer(42)]);
}

#[test]
fn update_successors_exemples() {
    let mut n8 = LocalView::joined(peer(8), peer(14));
    n8.update_successors(&[peer(21), peer(32), peer(38), peer(42)]);
    assert_eq!(n8.successors(), &[peer(14), peer(21), peer(32), peer(38)]);

    let mut n8 = LocalView::joined(peer(8), peer(14));
    n8.update_successors(&[peer(21), peer(8), peer(14), peer(21)]);
    assert_eq!(n8.successors(), &[peer(14), peer(21)]);

    let mut b = LocalView::joined(peer(8), peer(14));
    b.update_successors(&[peer(14)]);
    assert_eq!(b.successors(), &[peer(14)]);
}

#[test]
fn forget_exemples() {
    let mut n8 = n8_full();
    n8.notify(peer(1));
    n8.forget(Id::new(14));
    assert_eq!(n8.successors(), &[peer(21), peer(32), peer(38)]);
    assert_eq!(n8.next_hop(key(10)), NextHop::Found(peer(21)));

    n8.forget(Id::new(1));
    assert_eq!(n8.predecessor(), None);

    let mut lonely = LocalView::joined(peer(8), peer(14));
    lonely.forget(Id::new(14));
    assert_eq!(lonely.successors(), &[peer(8)]);
}

fn spawn_node(net: &MemoryNetwork<6>, id: u64) -> Arc<TestNode> {
    let node = Node::new(peer(id), net.clone());
    let handler_node = node.clone();
    net.register(node.me(), Arc::new(move |req| handler_node.handle(req)));
    node
}

async fn stabilize_all(nodes: &[Arc<TestNode>], rounds: usize) {
    for _ in 0..rounds {
        for node in nodes {
            node.stabilize().await;
            node.check_predecessor().await;
        }
    }
}

async fn fix_all_fingers(nodes: &[Arc<TestNode>]) {
    for node in nodes {
        for _ in 0..6 {
            node.fix_fingers().await;
        }
    }
}

async fn build_ring(net: &MemoryNetwork<6>, ids: &[u64]) -> Vec<Arc<TestNode>> {
    let mut nodes = vec![spawn_node(net, ids[0])];
    for &id in &ids[1..] {
        let node = spawn_node(net, id);
        node.join(peer(ids[0])).await.unwrap();
        nodes.push(node);
        stabilize_all(&nodes, 1).await;
    }
    nodes
}

fn assert_ring_is_correct(nodes: &[Arc<TestNode>]) {
    let mut ids: Vec<u64> = nodes.iter().map(|n| n.me().id.value()).collect();
    ids.sort();
    for node in nodes {
        let me = node.me().id.value();
        let pos = ids.iter().position(|&id| id == me).unwrap();
        let succ = ids[(pos + 1) % ids.len()];
        let pred = ids[(pos + ids.len() - 1) % ids.len()];
        assert_eq!(node.successor().id.value(), succ, "successeur de N{me}");
        assert_eq!(node.predecessor().map(|p| p.id.value()), Some(pred), "prédécesseur de N{me}");
    }
}

async fn assert_lookups_are_correct(nodes: &[Arc<TestNode>]) {
    let ids: Vec<u64> = nodes.iter().map(|n| n.me().id.value()).collect();
    for node in nodes {
        for k in 0..64 {
            let lookup = node.find_successor(key(k)).await.unwrap();
            assert_eq!(
                lookup.owner.id.value(),
                expected_owner(&ids, k),
                "N{} cherche la clé {k}",
                node.me().id.value()
            );
        }
    }
}

#[tokio::test]
async fn deux_noeuds() {
    let net = MemoryNetwork::new();
    let a = spawn_node(&net, 8);
    let b = spawn_node(&net, 42);

    b.join(a.me()).await.unwrap();
    assert_eq!(b.successor(), a.me());

    b.stabilize().await;
    assert_eq!(a.predecessor(), Some(b.me()));

    a.stabilize().await;
    assert_eq!(a.successor(), b.me());
    assert_eq!(b.predecessor(), Some(a.me()));
}

#[tokio::test]
async fn identifiant_deja_pris() {
    let net = MemoryNetwork::new();
    let a = spawn_node(&net, 8);
    let other = Node::new(peer(8), net.clone());
    assert_eq!(other.join(a.me()).await, Err(Error::IdTaken));
}

#[tokio::test]
async fn convergence() {
    let net = MemoryNetwork::new();
    let nodes = build_ring(&net, &[32, 1, 56, 14, 42, 8, 51, 21, 48, 38]).await;
    stabilize_all(&nodes, 3).await;

    assert_ring_is_correct(&nodes);
    assert_lookups_are_correct(&nodes).await;

    for node in &nodes {
        println!(
            "N{:<2} : prédécesseur N{:<2}  successeurs {:?}",
            node.me().id.value(),
            node.predecessor().unwrap().id.value(),
            node.successors().iter().map(|p| p.id.value()).collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn les_doigts_raccourcissent_les_recherches() {
    let net = MemoryNetwork::new();
    let nodes = build_ring(&net, &PAPER_RING).await;
    stabilize_all(&nodes, 3).await;
    let n8 = nodes.iter().find(|n| n.me().id.value() == 8).unwrap();

    let sans_doigts = total_hops(&nodes).await;
    fix_all_fingers(&nodes).await;
    let avec_doigts = total_hops(&nodes).await;

    println!("Toutes les recherches : {sans_doigts} sauts sans doigts, {avec_doigts} avec");
    assert!(avec_doigts < sans_doigts);

    let lookup = n8.find_successor(key(54)).await.unwrap();
    assert_eq!(lookup.owner, peer(56));
    assert_eq!(lookup.hops, 2);
    assert_lookups_are_correct(&nodes).await;
}

async fn total_hops(nodes: &[Arc<TestNode>]) -> u32 {
    let mut total = 0;
    for node in nodes {
        for k in 0..64 {
            total += node.find_successor(key(k)).await.unwrap().hops;
        }
    }
    total
}

#[tokio::test]
async fn pannes() {
    let net = MemoryNetwork::new();
    let nodes = build_ring(&net, &PAPER_RING).await;
    stabilize_all(&nodes, 3).await;
    fix_all_fingers(&nodes).await;

    net.kill(Id::new(14));
    net.kill(Id::new(21));
    let survivors: Vec<_> = nodes
        .into_iter()
        .filter(|n| ![14, 21].contains(&n.me().id.value()))
        .collect();

    stabilize_all(&survivors, 3).await;
    fix_all_fingers(&survivors).await;

    assert_ring_is_correct(&survivors);
    assert_lookups_are_correct(&survivors).await;
}

#[tokio::test]
async fn anneau_plein() {
    let net = MemoryNetwork::new();
    let ids: Vec<u64> = (0..64).collect();
    let nodes = build_ring(&net, &ids).await;
    stabilize_all(&nodes, 2).await;
    fix_all_fingers(&nodes).await;

    assert_ring_is_correct(&nodes);
    let n8 = &nodes[8];
    let lookup = n8.find_successor(key(54)).await.unwrap();
    assert_eq!(lookup.owner, peer(54));
    assert_eq!(lookup.hops, 4);
}

#[tokio::test]
async fn maintenance_automatique() {
    let net = MemoryNetwork::new();
    let a = spawn_node(&net, 8);
    let b = spawn_node(&net, 42);
    let c = spawn_node(&net, 21);
    b.join(a.me()).await.unwrap();
    c.join(a.me()).await.unwrap();

    let tasks: Vec<_> = [&a, &b, &c]
        .into_iter()
        .map(|n| n.clone().spawn_maintenance(Duration::from_millis(5)))
        .collect();
    tokio::time::sleep(Duration::from_millis(200)).await;
    for task in &tasks {
        task.abort();
    }

    assert_ring_is_correct(&[a, b, c]);
}
