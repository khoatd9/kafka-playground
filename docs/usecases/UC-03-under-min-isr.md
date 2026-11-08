# UC-03: Partitions under min ISR, writes rejected

| Severity | Alerts | Verified on lab |
|---|---|---|
| 🔴 Critical: producers with `acks=all` cannot write | `KafkaUnderMinIsr` | 2026-11-08, `payments` with `min.insync.replicas=3` + `kafka2` stopped |

## Detect

| Signal | Healthy | Problem |
|---|---|---|
| Grafana: Under min ISR | 0 | **> 0** |
| Grafana: Messages in / sec | normal | affected topics drop toward 0, others keep flowing |
| Producer logs | quiet | `NOT_ENOUGH_REPLICAS` (and `NOT_ENOUGH_REPLICAS_AFTER_APPEND`) |

```bash
bin/kafka kafka-topics.sh --describe --under-min-isr-partitions
bin/kafka kafka-configs.sh --describe --entity-type topics --entity-name payments   # min.insync.replicas override?
```

## What Kafka does automatically

It **rejects** `acks=all` writes to partitions whose ISR is smaller than `min.insync.replicas`, choosing
durability over availability. Reads still work. Producers retry until `delivery.timeout.ms`, then fail
the send. Producers using `acks=1` or `acks=0` keep writing, but those writes can be lost.

## Diagnose

ISR size < min ISR has two possible causes, and you have to tell which one it is:

1. **Replicas are missing.** Brokers are down ([UC-01](UC-01-broker-down.md)) or followers are lagging
   (slow disk or network). Check *Brokers online*, *Under-replicated*, and *ISR shrinks/expands*.
2. **The requirement is too high.** Someone set `min.insync.replicas` ≥ replication factor (as in the
   drill), so losing *any* replica blocks writes. Check the topic and broker configs.

## Remediate

| Cause | Fix |
|---|---|
| Broker(s) down | Bring them back ([UC-01](UC-01-broker-down.md)); ISR re-expands and writes resume on their own |
| Follower lagging | Fix the disk or network; check `replica.lag.time.max.ms` |
| min ISR ≥ RF | Set `min.insync.replicas` = RF − 1 (e.g. RF=3 → 2): `kafka-configs.sh --alter … --delete-config min.insync.replicas` or `--add-config min.insync.replicas=2` |
| Emergency, data loss accepted | Temporarily lower min ISR to 1. Document it and revert afterwards |

## Verify

- [ ] Under min ISR = 0
- [ ] Affected topics' throughput back to normal
- [ ] No new `NOT_ENOUGH_REPLICAS` in producer logs
- [ ] `min.insync.replicas` back to the intended value (RF − 1)
- [ ] `KafkaUnderMinIsr` resolved

## When it gets worse

ISR = 0 → the partition is **offline**: no reads either. Never enable `unclean.leader.election.enable`
without accepting data loss; it lets an out-of-sync replica become leader.

## Drill

```bash
# Inject
bin/kafka kafka-configs.sh --alter --entity-type topics --entity-name payments --add-config min.insync.replicas=3
docker compose stop kafka2
# Observe ~1 min: Grafana "Under min ISR", "Messages in / sec by topic"
docker compose logs --since 1m producer | grep -c NOT_ENOUGH_REPLICAS
# Restore
docker compose start kafka2
bin/kafka kafka-configs.sh --alter --entity-type topics --entity-name payments --delete-config min.insync.replicas
bin/kafka kafka-leader-election.sh --election-type preferred --all-topic-partitions
make health
```

## Observed in this lab

| Moment | Under min ISR | Under-replicated | payments msg/s | orders msg/s | Producer errors |
|---|---|---|---|---|---|
| Before | 0 | 0 | 50 | 200 | – |
| kafka2 down +45 s | **3** (all payments partitions) | 65 | **5.9** and falling | 182 | **154** `NOT_ENOUGH_REPLICAS` in 50 s |
| Restored | 0 | 0 | 54.5 (catch-up burst) | 205 | – |

`orders` (min ISR 2) kept flowing through the same outage. The setting is per-topic, and so is the impact.
`KafkaUnderMinIsr` was still pending at +45 s (`for: 30s` plus scrape and evaluation delay).

## Lessons

1. **RF=3 with min ISR=2** tolerates one replica loss. min ISR = RF tolerates none.
2. This is the alert that means "writes are failing". Page on it.
3. The fix is usually *restore replicas*, not *lower the bar*. Lowering min ISR trades away durability.
