//! Baseline broadcast fan-out benchmark. SP8 builds the full load harness.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use pylon::channel::registry::Registry;
use pylon::connection::handle::ConnectionHandle;
use pylon::protocol::event::ServerEvent;
use pylon::protocol::socket_id::SocketId;
use tokio::sync::mpsc;

fn bench_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("broadcast_fanout");
    for n in [10usize, 100, 1000] {
        let reg = Registry::new();
        let mut receivers = Vec::new();
        for _ in 0..n {
            let (tx, rx) = mpsc::unbounded_channel::<ServerEvent>();
            receivers.push(rx);
            reg.subscribe(
                "app",
                "c",
                ConnectionHandle {
                    socket_id: SocketId::generate(),
                    mailbox: tx,
                },
            );
        }
        // Measures the broadcast plus a drain (drain keeps the unbounded mailboxes
        // from growing across iterations; broadcast is the dominant term).
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                reg.broadcast("app", "c", &ServerEvent::Pong, None);
                for rx in receivers.iter_mut() {
                    while rx.try_recv().is_ok() {}
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_fanout);
criterion_main!(benches);
