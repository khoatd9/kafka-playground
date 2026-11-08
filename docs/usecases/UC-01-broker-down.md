# UC-01: A broker is down

| Severity | Alerts | Verified on lab |
|---|---|---|
| ⚠️ Warning (🔴 if a second broker goes down) | `KafkaBrokerDown`, `KafkaUnderReplicatedPartitions` | 2026-11-08, stopped `kafka2` |

Tiếng Việt: [UC-01-broker-down.vi.md](UC-01-broker-down.vi.md)

## Detect

| Signal | Healthy | Broker down |
|---|---|---|
| Grafana: Brokers online / `kafka_brokers` | 3 | **2** |
| Under-replicated partitions | 0 | every partition with a replica on that broker |
| Offline partitions | 0 | **0**: still serving |
| Under min ISR | 0 | **0** (with RF=3, min ISR=2) |
| Partition leaders per broker | balanced | the dead broker's leaders move to the others |

```bash
bin/kafka kafka-topics.sh --describe --under-replicated-partitions
# Partition: 0  Leader: 3  Replicas: 2,3,1  Isr: 3,1   ← broker 2 missing from ISR
```

Producer logs show a burst of `WARN … NOT_LEADER_OR_FOLLOWER … retrying` that stops on its own. That's
normal: the client wrote to the old leader, refreshed metadata and retried against the new one.

## What Kafka does automatically

1. The controller notices the broker is gone.
2. Partitions **led** by it get a new leader from the ISR (within seconds). Clients follow automatically.
3. Partitions where it was a **follower** just drop it from the ISR.
4. Kafka does **not** copy its replicas to another broker. It runs on 2 of 3 copies and waits for the broker
   to return (copying hundreds of GB on every short restart would be worse than waiting).

## Diagnose

```bash
docker compose ps kafka2                   # prod: systemctl status kafka / kubectl get pod
docker compose logs --tail 200 kafka2 | grep -iE 'error|exception|fatal|space|memory'
```

The usual causes: **full disk** (`No space left on device`), heap OOM (`OutOfMemoryError`), the host or
network down, or a planned restart.

## Remediate

| Case | Action |
|---|---|
| **A. Restart or transient failure, disk intact** (most common) | Start the broker. It fetches what it missed from the leaders and rejoins the ISR on its own |
| **B. Broker lost with its disk** | New machine with the **same `node.id`** and `cluster.id`, storage formatted with `kafka-storage.sh format`. It re-replicates everything from the leaders. Throttle with `leader.replication.throttled.rate` / `follower.replication.throttled.rate` |
| **C. Retire the broker for good** | Move its replicas with `kafka-reassign-partitions.sh --generate / --execute --throttle / --verify` (or Cruise Control). *Not drilled on this lab* |

In every case, finish by re-balancing leadership. Otherwise the returned broker sits idle while the
others carry its load. Kafka auto-rebalances every ~5 min (`leader.imbalance.check.interval.seconds`), or:

```bash
bin/kafka kafka-leader-election.sh --election-type preferred --all-topic-partitions
```

## Verify

- [ ] Brokers online = expected count
- [ ] Under-replicated = 0, Offline = 0, Under min ISR = 0
- [ ] Leaders balanced across brokers
- [ ] Throughput back to baseline, consumer lag flat
- [ ] `KafkaBrokerDown` and `KafkaUnderReplicatedPartitions` resolved
- [ ] Root cause written down (disk? OOM? maintenance?)

## When it gets worse

With RF=3, `min.insync.replicas=2` and `acks=all`:

| Live replicas of a partition | Reads | Writes | State |
|---|---|---|---|
| 3/3 | ✅ | ✅ | Healthy |
| 2/3 | ✅ | ✅ | Under-replicated ⚠️ (this runbook) |
| 1/3 | ✅ | ❌ `NOT_ENOUGH_REPLICAS` | Under min ISR 🔴 → [UC-03](UC-03-under-min-isr.md) |
| 0/3 | ❌ | ❌ | Offline 🔴 outage |

- In a 10-broker cluster with 2 brokers down, only partitions with replicas on *both* lose writes.
- The 3 KRaft controllers need a majority (2/3). This lab runs combined mode, so 2 nodes down = no
  metadata changes at all (no leader election, no ISR shrink).
- Prevent it with `broker.rack` (spread replicas across racks or AZs) and dedicated controllers in production.

## Drill

```bash
# Inject
docker compose stop kafka2
# Observe for ~1 min: Grafana cluster-health row, "Partition leaders per broker", http://localhost:9090/alerts
# Restore
docker compose start kafka2
bin/kafka kafka-leader-election.sh --election-type preferred --all-topic-partitions
make health
```

## Observed in this lab

| Moment | Brokers | URP | Offline | Under min ISR | Leaders (k1/k2/k3) | orders msg/s |
|---|---|---|---|---|---|---|
| Before | 3 | 0 | 0 | 0 | 20 / 20 / 19 | 200 |
| kafka2 down, +40 s | 2 | **62** | 0 | 0 | 30 / – / 32 | 193 |
| Started, +15 s (healthy) | 3 | 32 | 0 | 0 | 30 / 0 / 32 | – |
| Started, +25 s | 3 | **0** | 0 | 0 | 30 / **0** / 32 | 204 |
| After preferred election | 3 | 0 | 0 | 0 | **21 / 21 / 20** | 200 |

Alerts: `KafkaBrokerDown` and `KafkaUnderReplicatedPartitions` were pending at +40 s and firing at +70 s.

## Lessons

1. One broker down is **not an outage** with RF=3 and min ISR=2. That's why those are the defaults.
2. Page on **Offline** and **Under min ISR**. Under-replicated alone is a warning.
3. After recovery, **re-balance leaders**.
4. Kafka never re-replicates elsewhere on its own: replace the broker under the same ID, or reassign.
