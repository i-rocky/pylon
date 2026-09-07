//! Microbenchmark: the per-message cost of carrying `Box<ServerEvent>` vs a bare
//! `ServerEvent` through the per-connection mailbox channel. Isolates the Box
//! alloc/free overhead (send + drain) from co-location noise. Two event shapes:
//! a fat `ChannelEvent` (whose clone already heap-allocates its strings, so the
//! Box is marginal) and a tiny `Pong` (zero-alloc bare, so the Box is pure new cost).

use criterion::{criterion_group, criterion_main, Criterion};
use pylon::protocol::event::ServerEvent;
use serde_json::json;
use std::hint::black_box;
use tokio::sync::mpsc;

fn fat() -> ServerEvent {
    ServerEvent::ChannelEvent {
        channel: "presence-room-42".to_string(),
        event: "client-message".to_string(),
        data: json!({"msg": "hello world payload body"}),
        user_id: None,
    }
}

fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("mailbox_send_recv");

    let (tx, mut rx) = mpsc::channel::<ServerEvent>(1024);
    let t = fat();
    g.bench_function("plain_fat", |b| {
        b.iter(|| {
            tx.try_send(black_box(t.clone())).unwrap();
            black_box(rx.try_recv().unwrap());
        })
    });

    let (txb, mut rxb) = mpsc::channel::<Box<ServerEvent>>(1024);
    let tb = fat();
    g.bench_function("boxed_fat", |b| {
        b.iter(|| {
            txb.try_send(Box::new(black_box(tb.clone()))).unwrap();
            black_box(*rxb.try_recv().unwrap());
        })
    });

    let (txs, mut rxs) = mpsc::channel::<ServerEvent>(1024);
    g.bench_function("plain_small", |b| {
        b.iter(|| {
            txs.try_send(black_box(ServerEvent::Pong)).unwrap();
            black_box(rxs.try_recv().unwrap());
        })
    });

    let (txsb, mut rxsb) = mpsc::channel::<Box<ServerEvent>>(1024);
    g.bench_function("boxed_small", |b| {
        b.iter(|| {
            txsb.try_send(Box::new(black_box(ServerEvent::Pong)))
                .unwrap();
            black_box(*rxsb.try_recv().unwrap());
        })
    });

    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
