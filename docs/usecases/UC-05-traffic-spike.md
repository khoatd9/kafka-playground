# UC-05: Traffic spike, latency rises

| Severity | Alerts | Verified on lab |
|---|---|---|
| ⚠️ Warning | `KafkaBrokerSaturated` (only if broker threads are the bottleneck, see below) | 2026-11-08, 500k × 1 KB burst on `orders` |

## Detect

| Signal | Healthy | Spike |
|---|---|---|
| Grafana: Messages in / sec, Bytes in / out | baseline | jump (×15 in the drill) |
| Grafana: Request latency p99 (Produce) | ~10–20 ms | **hundreds of ms** |
| Grafana: Request handler idle / Network processor idle | high | drops *if* the brokers are the bottleneck |
| Grafana: Consumer lag | flat | jumps, then drains |
| Grafana: Log size | steady slope | steep climb |
| Producer side | normal | high send latency, `TimeoutException`, `buffer.memory` exhausted |

Rates in Grafana are averaged over 1 minute: a 90-second burst appears smaller and longer than it really is.

## What Kafka does automatically

Nothing adaptive. Brokers accept writes as fast as their threads, disk and network allow. The pressure
shows up first as latency, then as producer-side timeouts. Optional **quotas** can throttle a noisy client.

## Diagnose: find where the bottleneck is

| Observation | Bottleneck | Next step |
|---|---|---|
| Request handler / network idle low (< 0.3; < ~0.6 in this lab's combined mode) | Broker threads (CPU) | Raise `num.io.threads` / `num.network.threads`, or add brokers |
| Thread idle high, but broker Produce p99 high | **Disk or replication** (fsync, slow followers) | Check disk I/O and ISR shrinks; faster disks, more brokers or partitions |
| Broker latency fine, client latency huge | Producer client | Tune `batch.size`, `linger.ms`, `compression.type`, `buffer.memory`, add producer instances |
| One broker hot, others cool | Leadership or partition skew | Preferred leader election; rebalance partitions |
| Lag grows on consumers | Consumers can't keep up | [UC-02](UC-02-consumer-lag.md) |

## Remediate

- **Short burst:** usually nothing. Watch lag drain and latency recover.
- **Sustained growth:** scale (brokers, partitions, consumers), enable compression (`lz4`/`zstd`),
  tune batching.
- **One abusive client:** set a quota:
  `bin/kafka kafka-configs.sh --alter --entity-type clients --entity-name <client.id> --add-config producer_byte_rate=1048576`
- **Disk filling:** check retention on the busy topics before the disks fill.

## Verify

- [ ] Produce p99 back to baseline
- [ ] Consumer lag drained
- [ ] Thread idle back to normal
- [ ] Disk growth back to its normal slope; retention adequate

## When it gets worse

A full disk takes a broker down ([UC-01](UC-01-broker-down.md)), and several at once can mean
[UC-03](UC-03-under-min-isr.md). Watch *Log size by broker* during long spikes.

## Drill

```bash
bin/kafka kafka-producer-perf-test.sh --topic orders --num-records 500000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=kafka1:9092,kafka2:9092,kafka3:9092 acks=all
# Watch Grafana Throughput + Brokers rows while it runs (~90 s), then
make health
```

(This writes about 1.5 GB to disk including replicas. Repeat it only with retention set on `orders`.)

## Observed in this lab

| Moment | orders msg/s (1-min avg) | Broker Produce p99 | Request handler idle | orders-service lag |
|---|---|---|---|---|
| Before | 207 | ~13 ms | 1.97 / 2.0 | 612 |
| During (+25 s) | 3,079 | 346 ms | **1.97** (not saturated) | 4,057 |
| After (+30 s) | 1,984 (avg still decaying) | 645 ms | 1.97 | 862 |

Producer's own report: `500000 records sent, 5820 records/sec (5.55 MB/sec), 5508 ms avg latency,
31203 ms max latency, 19967 ms 95th`.

**What this shows:** the brokers' request threads stayed ~98% idle, so `KafkaBrokerSaturated` correctly
never fired. The real bottleneck was disk and replication inside Docker Desktop's VM (broker Produce p99
13 → 645 ms) plus the producer queuing records in its buffer (client latency up to 31 s). An alert on
thread idle alone misses this kind of overload; watch Produce p99 and client-side latency too.

## Lessons

1. Locate the bottleneck first: broker CPU, disk/replication, client, or skew. Each has a different fix.
2. Client latency can explode while brokers look idle. Monitor both ends.
3. Spikes eat disk: retention is your safety valve.
