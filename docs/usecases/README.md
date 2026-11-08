# Kafka incident runbooks

One file per incident type. Each was **drilled on this lab** and records the real numbers observed.
With Claude Code, say "run drill UC-02" or "an alert is firing" (the `kafka-incident` skill uses these files).

| ID | Situation | Severity | Alert |
|---|---|---|---|
| [UC-01](UC-01-broker-down.md) ([vi](UC-01-broker-down.vi.md)) | A broker is down | ⚠️ | `KafkaBrokerDown`, `KafkaUnderReplicatedPartitions` |
| [UC-02](UC-02-consumer-lag.md) | Consumer lag keeps growing | ⚠️ | `KafkaConsumerLagGrowing` |
| [UC-03](UC-03-under-min-isr.md) | Partitions under min ISR, writes rejected | 🔴 | `KafkaUnderMinIsr` |
| [UC-04](UC-04-offset-reset.md) | Replay or skip data by resetting offsets | planned op | – |
| [UC-05](UC-05-traffic-spike.md) | Traffic spike, latency rises | ⚠️ | `KafkaBrokerSaturated` (partial coverage) |

Not covered yet, good next ones to write: offline partitions / lost controller quorum (stop 2 brokers),
disk full, a consumer rebalance storm, a hot partition from a skewed key.

## Template for a new runbook

Keep these section names. The `kafka-incident` skill navigates by them.

```markdown
# UC-XX: <situation>

| Severity | Alerts | Verified on lab |
|---|---|---|

## Detect                       signals table (healthy vs problem) + CLI commands
## What Kafka does automatically
## Diagnose                     how to find the root cause
## Remediate                    fix per cause
## Verify                       checklist
## When it gets worse
## Drill                        Inject / Observe / Restore commands, ending with `make health`
## Observed in this lab         real numbers from running the drill
## Lessons
```
