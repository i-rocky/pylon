# Benchmarks: Pylon vs soketi vs Laravel Reverb

2026-09-17

pylon held every connection step at both ramps up to 80,000 connections, at roughly a quarter of both soketi's and Reverb's bytes per connection at 10,000 (5,867.1 vs 23,201.4 vs 21,046.5 bytes per connection), and delivered the highest fan-out throughput within the 99%/100 ms budget at 79,629.7 deliveries/s. soketi (Aloware fork v2.0.0) is connect-rate bound, not connection-count bound: it seats 10,000 connections cleanly but its accept loop saturates a single core before 20,000 arrive at 500/s per feeder, only reaching 20,000 at a slowed 100/s ramp, and never seats 40,000 or 60,000 at either ramp. Reverb matches pylon's 80,000-connection ceiling at both ramps but at 3.6-5.7x the memory per connection and an S5 p99 of 10,428 µs against pylon's 4,440 µs.

## At a glance

### Table 7: Summary

| Metric | pylon | soketi | reverb | soketi-pm2 | Best | pylon/soketi | pylon/reverb |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Bytes/Connection at 10k (every server seated it, lower better) | 5867.1 | 23201.4 | 21046.5 | n/a | pylon | 0.25 | 0.28 |
| Bytes/Connection, largest step each server seated (lower better) | 3592.3 (S2-2, 500/s per feeder) | 19606.5 (S1b, 100/s per feeder) | 20740.2 (S2-2, 500/s per feeder) | n/a | pylon | 0.18 | 0.17 |
| Max subscribed reached, best of both ramps (higher better) | 80000 (S2-2, 500/s per feeder) | 20000 (S1b, 100/s per feeder) | 80000 (S2-2, 500/s per feeder) | n/a | pylon | 4.0 | 1.0 |
| Best deliveries/s within budget (S3, higher better) | 79629.7 (S3-4, rate=500) | 23204.4 (S3-3, rate=250) | 11000.7 (S3-4, rate=500) | none within budget; first step S3-1 (rate=50): 6740.4/s | pylon | 3.43 | 7.24 |
| S5 p99 latency µs (lower better) | 4440 | 5959 | 10428 | n/a | pylon | 0.75 | 0.43 |

## S1 — memory per connection (10k / 20k / 40k)

### Table 1: Memory per connection (S1a/S1b/S1c)

| Server | Step | Ramp | Requested | Subscribed | Idle Baseline RSS (KB) | Peak RSS (KB) | Bytes/Connection | Time to Subscribed (s) | Failure Mode |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| pylon | idle memory 10k | 500/s per feeder | 10000 | 10000 | 20700.0 | 77996 | 5867.1 | 10.2 | completed |
| pylon | idle memory 20k | 500/s per feeder | 20000 | 20000 | 20692.0 | 105960 | 4365.7 | 20.4 | completed |
| pylon | idle memory 40k | 500/s per feeder | 40000 | 40000 | 20692.0 | 168328 | 3779.5 | 40.6 | completed |
| pylon | idle memory 20k | 100/s per feeder | 20000 | 20000 | 20532.0 | 107724 | 4464.2 | 101.4 | completed |
| pylon | idle memory 40k | 100/s per feeder | 40000 | 40000 | 20520.0 | 169372 | 3810.6 | 202.6 | completed |
| soketi | idle memory 10k | 500/s per feeder | 10000 | 10000 | 116340.0 | 342916 | 23201.4 | 15.6 | completed |
| soketi | idle memory 20k | 500/s per feeder | no summary (server did not accept connections) | no summary (server did not accept connections) | 118764.0 | 374136 | no summary (server did not accept connections) | no summary (server did not accept connections) | connect-rate bound: CPU saturated at ~96.1% while ramping, seated ~13337 before the watchdog (estimated from RSS against the slow-ramp 20,000-connection figure of 19606.5 bytes/conn) |
| soketi | idle memory 40k | 500/s per feeder | no summary (server did not accept connections) | no summary (server did not accept connections) | 119236.0 | 481816 | no summary (server did not accept connections) | no summary (server did not accept connections) | connect-rate bound: CPU saturated at ~98.3% while ramping, seated ~18937 before the watchdog (estimated from RSS against the slow-ramp 20,000-connection figure of 19606.5 bytes/conn) |
| soketi | idle memory 20k | 100/s per feeder | 20000 | 20000 | 118156.0 | 501096 | 19606.5 | 105.9 | completed |
| soketi | idle memory 40k | 100/s per feeder | no summary (server did not accept connections) | no summary (server did not accept connections) | 118984.0 | 619700 | no summary (server did not accept connections) | no summary (server did not accept connections) | no loader summary (server did not accept connections) |
| reverb | idle memory 10k | 500/s per feeder | 10000 | 10000 | 58392.0 | 263924 | 21046.5 | 10.2 | completed |
| reverb | idle memory 20k | 500/s per feeder | 20000 | 20000 | 58448.0 | 467660 | 20951.7 | 20.4 | completed |
| reverb | idle memory 40k | 500/s per feeder | 40000 | 40000 | 58368.0 | 888352 | 21247.6 | 42.9 | completed |
| reverb | idle memory 20k | 100/s per feeder | 20000 | 20000 | 57944.0 | 466792 | 20933.0 | 102.2 | completed |
| reverb | idle memory 40k | 100/s per feeder | 40000 | 40000 | 57976.0 | 904544 | 21672.1 | 202.6 | completed |

Table 1/2's "Peak RSS" is the peak sampled value, standing in for a plateau median: `connect` holds connections only for its ~2 s RSS sample plus one broadcast before draining, so there is no multi-second plateau to median over. pylon and Reverb seat every requested count at the standard ramp; soketi seats only 10,000, needs the slow ramp for 20,000, and still fails 40,000 at either — connect-rate exhaustion, not memory or a crash: the sampler shows its single core pinned near 96-98% CPU while the loader reports no summary at all. Reverb's memory per connection is 3.59x pylon's at 10k, rising to 4.80x at 20k and 5.62x at 40k (4.69x/5.69x at the slow-ramp points) — a growing gap, not a fixed ratio.

## S2 — connection ceiling (60,000 then 80,000)

### Table 2: Connection ceiling (S2)

| Server | Step | Ramp | Requested | Subscribed | Subscribed/Requested | Peak RSS (KB) | Failure Mode |
| --- | --- | --- | --- | --- | --- | --- | --- |
| pylon | S2-1 (60000) | 500/s per feeder | 60000 | 60000 | 100.0% | 247028 | completed (fourth attempt; see Reruns) |
| pylon | S2-2 (80000) | 500/s per feeder | 80000 | 80000 | 100.0% | 301344 | completed |
| pylon | S2-1 (60000) | 100/s per feeder | 60000 | 60000 | 100.0% | 246452 | completed |
| pylon | S2-2 (80000) | 100/s per feeder | 80000 | 80000 | 100.0% | 308908 | completed |
| soketi | S2-1 (60000) | 500/s per feeder | no summary (server did not accept connections) | no summary (server did not accept connections) | no summary (server did not accept connections) | 419396 | connect-rate bound: CPU saturated at ~99.0% while ramping, seated ~15700 before the watchdog (estimated from RSS against the slow-ramp 20,000-connection figure of 19606.5 bytes/conn) |
| soketi | S2-2 (80000) | 500/s per feeder | not run | not run | not run | not run | not run |
| soketi | S2-1 (60000) | 100/s per feeder | no summary (server did not accept connections) | no summary (server did not accept connections) | no summary (server did not accept connections) | 482116 | no loader summary (server did not accept connections) |
| soketi | S2-2 (80000) | 100/s per feeder | not run | not run | not run | not run | not run |
| reverb | S2-1 (60000) | 500/s per feeder | 60000 | 60000 | 100.0% | 1291996 | completed |
| reverb | S2-2 (80000) | 500/s per feeder | 80000 | 80000 | 100.0% | 1678256 | completed |
| reverb | S2-1 (60000) | 100/s per feeder | 60000 | 60000 | 100.0% | 1291408 | completed |
| reverb | S2-2 (80000) | 100/s per feeder | 80000 | 80000 | 100.0% | 1690908 | completed |

pylon and Reverb both hold the full 80,000-connection step at either ramp, with no ceiling found and the free-memory abort (below 300 MB) never triggering. pylon's standard-ramp S2-1 (60,000) logs four outcomes before the kept success: two with no loader summary at all (the feeder port exhaustion described in the Method notes, before the range was widened), a third that actually seated 60,000 but was misread as a failure by a defective pass/fail check in the run script (a false negative), and the fourth is the kept "subscribed=60000 OK" result; S2-2 needed only one pass. soketi never seats 60,000 at any ramp, confirming the ceiling already visible in S1: its true ceiling sits between the 20,000 it reaches at the slow ramp and the 40,000 it fails at either ramp. Its 80,000 step is not run, not a measured failure.

## S3 — fan-out throughput, 10 subscribers per channel

### Table 3: Fan-out throughput (S3)

`Received (own)` counts only events published by the reporting process itself, isolated per channel; `Received (foreign)` is shown for information and is expected to be 0 here (`not recorded` for older loader builds). Received/Expected and Deliveries/s use `received` (own) only. Budget = received(own)/expected ≥ 99% and worst p99 ≤ 100,000 µs. Each reported percentile is the worst value of that percentile across loader processes, so p50 and p99 in a row may come from different processes.

| Server | Step | Offered (pub/s) | Sent | Received (own) | Received (foreign) | Received/Expected | Deliveries/s | Budget | p50 (µs) | p99 (µs) | p99.9 (µs) | Mean CPU % | mpstat usr/sys % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| pylon | S3-1 (rate=50) | 800 | 16016 | 160300 | 0 | 100.09% | 8015.0 | within budget | 1311 | 3338 | 6651 | 18.68 | 2.68/4.41 |
| pylon | S3-2 (rate=100) | 1600 | 32016 | 320370 | 0 | 100.07% | 16018.5 | within budget | 1036 | 2455 | 5091 | 29.07 | 4.33/6.92 |
| pylon | S3-3 (rate=250) | 4000 | 80016 | 800540 | 0 | 100.05% | 40027.0 | within budget | 886 | 3590 | 6901 | 53.17 | 7.47/11.21 |
| pylon | S3-4 (rate=500) | 8000 | 159186 | 1592594 | 0 | 100.05% | 79629.7 | within budget | 3067 | 10764 | 15745 | 72.82 | 9.5/14.34 |
| soketi | S3-1 (rate=50) | 800 | 16016 | 160300 | 0 | 100.09% | 8015.0 | within budget | 4055 | 6475 | 10592 | 36.2 | 8.04/5.09 |
| soketi | S3-2 (rate=100) | 1600 | 32016 | 320370 | 0 | 100.07% | 16018.5 | within budget | 3217 | 10330 | 15253 | 47.17 | 10.25/6.08 |
| soketi | S3-3 (rate=250) | 4000 | 46387 | 464088 | 0 | 100.05% | 23204.4 | within budget | 6565 | 11927 | 16244 | 53.78 | 12.11/7.72 |
| soketi | S3-4 (rate=500) | 8000 | 46177 | 462015 | 0 | 100.05% | 23100.8 | within budget | 6606 | 11706 | 14737 | 55.73 | 11.9/7.75 |
| reverb | S3-1 (rate=50) | 800 | 16016 | 160300 | 0 | 100.09% | 8015.0 | within budget | 5140 | 15777 | 119406 | 59.44 | 15.69/5.12 |
| reverb | S3-2 (rate=100) | 1600 | 20558 | 205720 | 0 | 100.07% | 10286.0 | within budget | 15286 | 16809 | 23298 | 65.93 | 18.91/6.36 |
| reverb | S3-3 (rate=250) | 4000 | 18784 | 187938 | 0 | 100.05% | 9396.9 | within budget | 16769 | 18464 | 20840 | 64.98 | 17.65/5.46 |
| reverb | S3-4 (rate=500) | 8000 | 21985 | 220014 | 0 | 100.07% | 11000.7 | within budget | 14327 | 15491 | 18235 | 65.21 | 18.75/6.42 |
| soketi-pm2 | S3-1 (rate=50) | 800 | 16016 | 134809 | 0 | 84.17% | 6740.4 | over budget | 2365 | 7360 | 18759 | 59.88 | 13.28/6.55 |
| soketi-pm2 | S3-2 (rate=100) | 1600 | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run |
| soketi-pm2 | S3-3 (rate=250) | 4000 | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run |
| soketi-pm2 | S3-4 (rate=500) | 8000 | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run |

All three single-process servers stay inside budget through all four steps: pylon's deliveries/s scale roughly linearly (8,015 to 79,630), while soketi's and Reverb's achieved sent totals plateau below the requested rate at steps 3-4 (soketi near 46k; Reverb 18-22k) — a throughput ceiling, not a dropped-message failure. soketi-pm2 fails the 99% bar on its first step at 84.17% received, well under single-process soketi's clean pass: the shortfall traces to the cluster adapter's UDP-broadcast IPC (`SOKETI_CLUSTER_CHECK_INTERVAL=500 ms`) not converging before messages are sent, even with the 20 s warm-up, so steps 2-4 were never attempted.

## S4 — hot-channel fan-out

### Table 4: Hot-channel fan-out (S4)

`Received (own)` counts only deliveries of a process's own publishes to its own subscribers; `Received (foreign)` counts deliveries of the other process's publishes to its own subscribers (`not recorded` for older loader builds, which makes Received/Expected and Deliveries/s unavailable for that row). Received/Expected = (received + received_foreign) summed over processes ÷ (sent summed × subscribed summed); Deliveries/s = (received + received_foreign) summed ÷ secs. Budget = that aggregate ratio ≥ 99% and worst p99 ≤ 100,000 µs (latency stays the process's own-event percentiles). Each reported percentile is the worst value of that percentile across loader processes, so p50 and p99 in a row may come from different processes.

| Server | Step | Offered (pub/s) | Sent | Received (own) | Received (foreign) | Received/Expected | Deliveries/s | Budget | p50 (µs) | p99 (µs) | p99.9 (µs) | Mean CPU % | mpstat usr/sys % |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| pylon | S4-1 (rate=5) | 40 | 808 | 404000 | 404000 | 100.0% | 40400.0 | within budget | 8044 | 19021 | 45744 | 20.15 | 1.86/6.77 |
| pylon | S4-2 (rate=10) | 80 | 1608 | 804000 | 804000 | 100.0% | 80400.0 | within budget | 8126 | 16474 | 20987 | 39.56 | 2.5/12.02 |
| pylon | S4-3 (rate=25) | 200 | 3313 | 1656500 | 1656500 | 100.0% | 165650.0 | over budget | 36208 | 170655 | 263585 | 59.94 | 3.56/15.74 |
| pylon | S4-4 (rate=50) | 400 | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run |
| soketi | S4-1 (rate=5) | 40 | 808 | 404000 | 404000 | 100.0% | 40400.0 | over budget | 70909 | 148242 | 169738 | 48.0 | 6.91/8.55 |
| soketi | S4-2 (rate=10) | 80 | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run |
| soketi | S4-3 (rate=25) | 200 | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run |
| soketi | S4-4 (rate=50) | 400 | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run |
| reverb | S4-1 (rate=5) | 40 | 808 | 404000 | 403997 | 100.0% | 40399.8 | over budget | 88735 | 126550 | 175767 | 53.69 | 11.16/8.05 |
| reverb | S4-2 (rate=10) | 80 | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run |
| reverb | S4-3 (rate=25) | 200 | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run |
| reverb | S4-4 (rate=50) | 400 | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run |
| soketi-pm2 | S4-1 (rate=5) | 40 | 808 | 332589 | 332127 | 82.27% | 33235.8 | over budget | 14376 | 31883 | 47382 | 48.33 | 7.42/8.44 |
| soketi-pm2 | S4-2 (rate=10) | 80 | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run |
| soketi-pm2 | S4-3 (rate=25) | 200 | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run |
| soketi-pm2 | S4-4 (rate=50) | 400 | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run | not run |

pylon is the only server to advance past the first hot-channel step: it passes S4-1/S4-2 within budget and stops at S4-3 on the 100 ms p99 cap (170,655 µs), while soketi and Reverb both stop after S4-1 on the same cap (148,242 µs and 126,550 µs) despite full delivery — pylon holds the budget twice as long on this single 1,000-connection channel. soketi-pm2's S4-1 fails outright on delivery (82.27%, under 99%) for the same cluster-convergence reason as S3 — a delivery failure, not a latency one.

## S5 — latency at fixed load

### Table 5: Latency at fixed load (S5)

Received/Expected is computed on `received` (own-process events only); `Received (foreign)` is the cross-process delivery count when the loader build records it (`not recorded` for older loader builds). Each reported percentile is the worst value of that percentile across loader processes, so p50 and p99 in a row may come from different processes.

| Server | p50 (µs) | p99 (µs) | p99.9 (µs) | max (µs) | Received (foreign) | Received/Expected | Mean CPU % |
| --- | --- | --- | --- | --- | --- | --- | --- |
| pylon | 1504 | 4440 | 8216 | 10846 | 0 | 100.07% | 13.79 |
| soketi | 2723 | 5959 | 10559 | 13246 | 0 | 100.07% | 25.39 |
| reverb | 6668 | 10428 | 110166 | 125763 | 0 | 100.07% | 47.96 |

At the fixed load of 10,000 connections across 1,000 channels publishing at 25/process, all three servers deliver 100% of expected events with zero cross-process contamination. pylon's p50 is 55% of soketi's and 23% of Reverb's; its p99 is 75% of soketi's and 43% of Reverb's (1,504/4,440 µs vs 2,723/5,959 µs vs 6,668/10,428 µs), with the lowest mean CPU (13.79% vs 25.39%/47.96%). Reverb's p99.9 (110,166 µs) is an order of magnitude above its own p50/p99 — a long tail. This is one fixed-load snapshot, not a latency trend; S3's per-step p99 is the source for how latency moves with throughput.

## S6 — graceful stop under load

### Table 6: Graceful stop (S6)

| Server | Stop Duration | Result | ExecMainStatus | Subscribed | Sent | Received |
| --- | --- | --- | --- | --- | --- | --- |
| pylon | 0m2.235s | success | 0 | 10000 | 24 | 240 |
| soketi | 0m3.632s | success | 0 | 10000 | 28 | 200 |
| reverb | 0m2.110s | success | 0 | 10000 | 20 | 200 |

S6 ran as `channels` at a 1/process publish rate rather than the `connect` scenario, because `connect` only samples RSS for ~2 s and fires one broadcast before draining, so a `systemctl stop` timed against a `secs`-long hold would land after the loader had already disconnected; `channels` at rate 1 keeps publishing so the mid-flight stop lands on a genuinely active connection. pylon's S6 was rerun with the fixed (`86d8a96`) loader; the discarded first pass is not used here. At the tested 10,000-connection load, all three stop cleanly within `TimeoutStopSec=20` (2.1-3.6 s, `ExecMainStatus=0`) — not representative of every load: at the 20,000-60,000-connection steps where it had seated an estimated 13,000-19,000 connections, soketi needed the full 20 s `SIGTERM` timeout and a `SIGKILL` in four runs (`real 0m20.297s` to `0m20.389s`, `Result=timeout`, `ExecMainStatus=9`), a pattern this table misses since S6 only ran at 10,000.

## Environment

| Component | Version / setting |
| --- | --- |
| pylon | v0.5.1, aarch64-unknown-linux-musl tarball, `PYLON_WORKERS=0` (auto, both cores) |
| soketi | Aloware fork `@aloware/soketi@2.0.0` (`aloware-soketi-2.0.0.tgz`), Node.js 24.21.0 / npm 11.19.0 (NodeSource `setup_24.x`), pm2 7.0.4 (PM2 row only) |
| Laravel Reverb | v1.11.1, Laravel framework 13.32.0, PHP 8.4.24 (NTS), Composer 2.10.3, ext-uv 0.3.0 (beta channel); cache, session and queue drivers file / file / sync |
| pylon-load | built from `pylon` master `4ae755ce1c07fd2df379383f7d748638ab9e83b8` in `rust:1.98.1-alpine3.24`; fixed for per-process channel isolation and own-event latency at commit `86d8a96` (see Method notes) |
| Payload | loader's default `{"seq":N,"t":"<nanos>"}` event body, 34-45 bytes; full published frame adds ~30-60 bytes for the channel-name wrapper |
| Kernel / hardware | `6.12.107+deb13-arm64`, 2 vCPU / 4 GB target (arm64) |
| Sysctls | `fs.nr_open=20000500`, `net.core.somaxconn=65535`, `net.ipv4.tcp_max_syn_backlog=65535` on target and feeders; feeders' `net.ipv4.ip_local_port_range` widened from the default `32768-60999` to `1024 65535` with `tcp_tw_reuse=1` for the run, restored after |
| Unit settings | every server unit: `LimitNOFILE=2000000` (pylon's shipped default, applied identically to soketi/soketi-pm2/reverb), `TimeoutStopSec=20`, `KillSignal=SIGTERM`, `Restart=no`; soketi-pm2 adds `SOKETI_ADAPTER_DRIVER=cluster` and `-i 2` (2 cluster instances). Heap/memory flags: soketi single-process `node --max-old-space-size=3072`, Reverb `php -d memory_limit=3G`, soketi-pm2 has no heap flag at all; none of these bound anything measured — soketi's highest observed peak was ~620 MB and Reverb's ~1.68 GB, both well under their configured ceilings |
| Ramp rates | standard 500 connections/s per feeder; slow-ramp addendum 100 connections/s per feeder for S1b/S1c/S2, added mid-run once soketi's single-core connect-rate ceiling was identified |
| Fairness settings | TLS off; one shared app id/key/secret (rotated once); client events off; no webhooks; debug off; metrics endpoints off; identical scenario order, payload, connection counts, channel shapes and durations per server |

## Method

One server at a time on a 2 vCPU / 4 GB arm64 host, load generated from two 4 vCPU /
8 GB hosts (the feeders) over a private network, one shared app, TLS off, per-second
RSS and CPU sampling on the server host.

| Scenario | Definition | Stop rule |
| --- | --- | --- |
| S1a/b/c | Connect 10k/20k/40k, hold for the sampler window, one post-ramp broadcast | none (idle-memory measurement) |
| S2 | Connect 60k then 80k | stop below 99% subscribed, on server exit, or target free memory < 300 MB |
| S3 | `channels` scenario, 10,000 conns / 1,000 channels / 8 procs per feeder, rate steps 50/100/250/500 per process | stop when received < 99% of sent x 10 recipients, or worst p99 > 100 ms |
| S4 | `fanout` scenario, 1,000 conns / 4 publishers per feeder on one hot channel, rate steps 5/10/25/50 per publisher | stop when aggregate (received+received_foreign) < 99% of sent x subscribed, or worst p99 > 100 ms |
| S5 | `channels` scenario, 10,000 conns / 1,000 channels / 8 procs per feeder, fixed rate 25/process, 60 s | none (latency snapshot at fixed load) |
| S6 | `channels` scenario, 10,000 conns held, mid-flight `systemctl stop` after ramp+10s | none (records stop duration and unit result) |

## Method notes

The feeders' default ephemeral port range (`32768-60999`) capped one feeder near
28,000 connections to a single address and port, so it was widened to `1024-65535`
with `tcp_tw_reuse=1` for the runs and restored afterwards.

The first pylon pass on S3-S6 was discarded: the loader named its channels identically
in every process, so the collisions inflated received counts 16x, and it stamped
latency with a process-local clock. The loader was fixed at commit `86d8a96` (PR #154)
— a per-process run id in channel names and payloads, receives classified as own or
foreign, latency measured only on own events — and pylon's S3-S6 numbers above come
from the fixed loader.

The S4 budget gate is the aggregate
`(received + received_foreign) ≥ 0.99 × (sent × subscribed)`, because own-only
counts are structurally ~50% in a symmetric two-process run.

Reruns: pylon's S2-1 at the standard ramp logged four attempts — two feeder port-range
failures, one gate false-negative on an actual success, and the kept success at 60,000
— and S2-2 one pass. pylon's S4-1 ran three times, the kept pass being the one after
the gate fix (p50 7,098/8,044 µs and p99 18,628/19,021 µs across its two processes).
soketi's S1b/S1c/S2 failures were kept as single passes because they are server
failures, not tooling. soketi-pm2 had no reruns.

## What this does not show

pylon runs as a single process using both cores (`PYLON_WORKERS=0`); soketi and Reverb run single-process on one core in every main-table row, and soketi-pm2 is soketi's own two-core variant, not comparable core-for-core. Reverb has no outbound webhook feature at all — a missing capability, not a disabled setting. Keepalive cadence was left at each shipped default and never aligned: soketi hardcodes a 120 s WS idle timeout, Reverb defaults `ping_interval=60s`/`activity_timeout=30s`, pylon's default `PYLON_ACTIVITY_TIMEOUT` is 120 s ([configuration reference](user-guide/configuration.md)). Payload limits (Reverb 10,000 bytes, soketi 100 KB) never bound anything, since every event is 34-45 bytes. TLS was off throughout; the load client is `pylon-load`, not a browser SDK; soketi here is the Aloware fork v2.0.0, not the stale upstream v1.6.1. Every p50/p99/p99.9 in Tables 3-5 is the worst value across the loader's own processes for that step, not a merged distribution, so a row's p50 and p99 can come from different processes. Table 7's "largest step each server seated" row compares different connection counts (80k for pylon/Reverb, 20k for soketi), not like-for-like; the "at 10k" row above it is. soketi's own published benchmark (500 idle connections, AWS t3.small, 6 ms internal latency) is the closest external reference for this hardware class but used a different scenario entirely — a sanity check only.

## Reproducing

The load generator is `pylon-load` in `load/` (`cargo run -p pylon-load -- --help`),
with the `connect`, `channels` and `fanout` scenarios used here. The raw outputs and
the analysis script are not part of this repository. Run each server alone on the
same host and keep the scenario order, payload, connection counts, channel shapes and
durations identical across servers.

!!! note "Trademark notice"
    "Pusher" is a trademark of its respective owner. Pylon is an independent, clean-room
    implementation and is not affiliated with or endorsed by Pusher.
