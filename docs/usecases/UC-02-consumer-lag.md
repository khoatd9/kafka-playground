# UC-02: Consumer lag keeps growing

| Severity | Alerts | Verified on lab |
|---|---|---|
| ⚠️ Warning (🔴 if data is near retention, see below) | `KafkaConsumerLagGrowing` | 2026-11-08, stopped the `consumer` container |

## Detect

| Signal | Healthy | Problem |
|---|---|---|
| Grafana: Consumer lag by group / topic | flat line | **rising line** |
| Grafana: Consume rate vs produce rate | consumed ≈ produced | consumed < produced (0 if stopped) |
| Grafana: Total consumer lag (stat) | green | orange > 5k, red > 50k |

```bash
bin/kafka kafka-consumer-groups.sh --describe --group orders-service
bin/kafka kafka-consumer-groups.sh --describe --group orders-service --members --verbose
```

- `Consumer group 'orders-service' has no active members.` → nobody is consuming.
- Members present but LAG rising on all partitions → consumers are too slow.
- LAG rising on **one** partition only → a hot key, or one stuck consumer instance.

**The trend matters, not the number.** A steady lag of a few hundred is normal: it depends on when
offsets were last committed (the console consumer auto-commits every 5 s ≈ 1,000 messages at 200 msg/s).

## What Kafka does automatically

Nothing to fix the lag. Messages wait safely in the topic until **retention** deletes them. If a dead
consumer leaves its group, its partitions are rebalanced to the remaining members. Kafka can't add
consumers for you.

## Diagnose

1. **Is anyone consuming?** `--describe --members`. No members → the app is down: check its logs, its
   deployment, and crash loops.
2. **Members present but slow?** Compare *Consume rate* vs *produce rate* in Grafana. Usual causes: slow
   downstream calls (DB, HTTP) per message, a traffic spike ([UC-05](UC-05-traffic-spike.md)), or too few
   consumers.
3. **One partition only?** Look for a hot key, or a stuck member (frequent rebalances in the app logs:
   `max.poll.interval.ms` exceeded because processing a batch takes too long).
4. **How much time is left?** Lag ÷ produce rate = how far behind. If that approaches the topic's
   `retention.ms`, **unread data will be deleted**: escalate.

## Remediate

| Cause | Fix |
|---|---|
| Consumer down | Restart or fix the app. It resumes from its last committed offset; nothing is lost |
| Too slow, partitions > members | Add consumer instances (up to the partition count) |
| Too slow, members = partitions | Add partitions (changes key → partition mapping!) or make processing faster (batching, async I/O) |
| Rebalance storms | Raise `max.poll.interval.ms` or lower `max.poll.records` |
| Backlog is useless (only fresh data matters) | Skip ahead: [UC-04](UC-04-offset-reset.md) `--to-latest` |
| Near retention | Temporarily raise `retention.ms` on the topic to buy time |

## Verify

- [ ] Group has the expected number of active members
- [ ] Lag trending down, then flat at its normal level
- [ ] Consume rate ≥ produce rate while draining
- [ ] `KafkaConsumerLagGrowing` resolved

## When it gets worse

Lag approaching retention means data loss for that consumer. Many groups lagging at once points at the
cluster (see [UC-01](UC-01-broker-down.md), [UC-05](UC-05-traffic-spike.md)), not one app.

## Drill

```bash
# Inject
docker compose stop consumer
# Observe ~90 s: Grafana "Consumer lag", "Consume rate vs produce rate"; http://localhost:9090/alerts
bin/kafka kafka-consumer-groups.sh --describe --group orders-service
# Restore
docker compose start consumer
make health
```

## Observed in this lab

| Moment | orders-service lag | payments-service lag | Alert |
|---|---|---|---|
| Before | 135 | 31 | – |
| Stopped +30 s | 5,128 | 1,279 | – |
| Stopped +90 s | **17,139** | 4,282 | `KafkaConsumerLagGrowing` **firing** |
| Restarted +16 s | 21,138 | – | – |
| Restarted +21 s | **985** | – | resolving |

Lag grew at exactly the produce rate (~200/s for orders). The consumer drained 20k messages in under
5 seconds once restarted.

## Lessons

1. Alert on lag that is **high and still growing**, not on a fixed number.
2. Lag is safe until retention: know `lag ÷ produce rate` versus `retention.ms`.
3. Partitions cap parallelism: consumers beyond the partition count sit idle.
