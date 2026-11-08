# UC-04: Replay or skip data by resetting consumer offsets

| Severity | Alerts | Verified on lab |
|---|---|---|
| Planned operation (often the *fix* for a bug or a backlog) | – (lag alerts fire during a replay) | 2026-11-08, `orders-service` → earliest |

When to use it:
- **Replay:** a consumer bug corrupted downstream data; fix the code, then re-read from a point in time.
- **Skip:** the backlog is worthless (only fresh data matters); jump to the end.
- **New pipeline:** a new consumer should start from a specific time.

## Detect / plan

```bash
bin/kafka kafka-consumer-groups.sh --describe --group orders-service       # current offsets
bin/kafka kafka-topics.sh --describe --topic orders                        # partitions
bin/kafka kafka-configs.sh --describe --entity-type topics --entity-name orders --all | grep retention   # how far back data exists
```

**Ask before doing this on a real system.** A replay re-processes messages: downstream systems must be
**idempotent** (safe to see the same message twice), or you will create duplicates.

## What Kafka does automatically

Nothing. Offsets only change when the group commits them or you reset them. A reset is **refused while
the group has active members**:
`Error: Assignments can only be reset if the group 'orders-service' is inactive, but the current state is Stable.`

## Remediate (the procedure)

1. **Stop every consumer in the group.**
2. **Wait until the group is empty.** `--describe` must say `has no active members`. A consumer killed
   without leaving its group stays "Stable" until `session.timeout.ms` (45 s default). In this lab it
   took ~26 s even with clean shutdown.
3. **Dry-run.** The default is dry-run, so you see the new offsets without applying them:
   ```bash
   bin/kafka kafka-consumer-groups.sh --group orders-service --topic orders --reset-offsets --to-earliest --dry-run
   ```
4. **Execute** with the same command and `--execute`.
5. **Start the consumers** and watch the lag drain.

| Option | Effect |
|---|---|
| `--to-earliest` | Oldest data still retained |
| `--to-latest` | Skip the whole backlog |
| `--to-datetime 2026-11-08T09:00:00.000` | First offset at or after that time |
| `--shift-by -1000` | Back 1,000 messages per partition |
| `--to-offset 42` | Exact offset (usually with `--topic orders:0` for a single partition) |
| `--topic orders:0,1` | Only some partitions |
| `--all-topics` | Every topic the group reads |

## Verify

- [ ] Dry-run output matched the intent before executing
- [ ] Group active again with the expected members
- [ ] Lag drained back to its normal level
- [ ] Downstream checked for duplicates or gaps

## When it gets worse

- `--to-earliest` on a large topic can take hours and hammer downstream systems. Prefer
  `--to-datetime` with the narrowest window that fixes the problem.
- Data older than `retention.ms` is gone. A reset cannot bring it back.

## Drill

```bash
docker compose stop consumer
until bin/kafka kafka-consumer-groups.sh --describe --group orders-service 2>&1 | grep -q 'no active members'; do sleep 2; done
bin/kafka kafka-consumer-groups.sh --group orders-service --topic orders --reset-offsets --to-earliest --dry-run
bin/kafka kafka-consumer-groups.sh --group orders-service --topic orders --reset-offsets --to-earliest --execute
docker compose start consumer
# Watch Grafana "Consumer lag" spike to ~1M and drain
make health
```

## Observed in this lab

| Moment | orders-service lag |
|---|---|
| Stopped → group empty | 26 s |
| After reset to earliest | **1,129,921** (the whole topic) |
| Replay +24 s | 860,274 |
| Replay +45 s | 391,690 |
| Replay +56 s | **978** (caught up) |

The console consumer re-read 1.13 M messages in under a minute (~20k msg/s). A real consumer doing
work per message will be much slower: estimate `lag ÷ real processing rate` before you start.

In the first attempt the reset failed because the old consumer container was SIGKILLed instead of
leaving the group. The lab's consumer now forwards SIGTERM; see the `consumer` service in
`docker-compose.yml`.

## Lessons

1. Offsets belong to the **group**: resetting one group never affects other groups.
2. Stop → wait for empty → dry-run → execute → start. Never skip the dry-run.
3. Graceful shutdown matters: consumers that don't leave their group block operations for `session.timeout.ms`.
