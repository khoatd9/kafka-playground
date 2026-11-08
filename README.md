# kafka-playground

A local, self-contained Kafka cluster with a UI, metrics, dashboards, alerts and incident runbooks,
built for learning how to **operate, configure and monitor** Kafka, on your own or with an AI tutor.

## Quick start

Requirements: Docker Desktop (give it at least 4 GB RAM), `make`, `python3`, `curl`.

```bash
git clone https://github.com/khoatd9/kafka-playground.git
cd kafka-playground
make up        # first run pulls ~3 GB of images
make health    # all ✅ after ~1 minute
make open      # Kafka UI :8080, Grafana :3000 (admin/admin), Prometheus :9090
```

## Learn with Claude Code

The repo ships with a `CLAUDE.md` and three project skills in `.claude/skills/`. Open the folder in
[Claude Code](https://claude.com/claude-code) and just talk to it:

| Skill | Ask things like |
|---|---|
| `kafka-tutor` | "Teach me Kafka from step 1", "What's an ISR?", "Quiz me on consumer groups", "Tour Grafana with me" |
| `kafka-incident` | "Run drill UC-01", "An alert is firing, help", "Why is lag growing?" |
| `kafka-lab` | "Is the lab healthy?", "Create a topic with 6 partitions", "Set retention on orders to 1 hour" |

Incident runbooks with numbers measured on this lab are in [docs/usecases/](docs/usecases/README.md).
Or skip the AI and work through the steps below in order. Each step has things to *do* and questions
to *check your understanding*.

```
                ┌──────────── producer (200 msg/s → orders, 50 msg/s → payments)
                ▼
   ┌────────┐ ┌────────┐ ┌────────┐      consumer (groups: orders-service, payments-service)
   │ kafka1 │ │ kafka2 │ │ kafka3 │◄──── kafka-ui        http://localhost:8080
   └───┬────┘ └───┬────┘ └───┬────┘◄──── kafka-exporter  (lag, offsets)
       │ :7071 JMX exporter  │                │
       └──────────┬──────────┘                │
                  ▼                           ▼
             prometheus  http://localhost:9090  (scrapes every 10s, evaluates alerts)
                  ▼
              grafana    http://localhost:3000  (admin / admin)
```

| Piece | What it is | Why it's here |
|---|---|---|
| `kafka1..3` | Apache Kafka 4.1, KRaft mode (no ZooKeeper). Each node is broker **and** controller | Smallest cluster that can survive one node failing |
| JMX exporter | Java agent inside each broker, serves metrics on `:7071` | Broker internals: throughput, latency, replication |
| `kafka-exporter` | Separate process acting as a Kafka client | Consumer lag and offsets (the client's view) |
| `kafka-ui` | Kafbat UI | Browse topics, messages, groups, configs |
| Prometheus | Time-series DB | Stores metrics, runs alert rules |
| Grafana | Dashboards | The "Kafka Overview" dashboard |
| `producer` / `consumer` | Kafka's perf-test and console tools | Steady fake traffic so graphs move |
| `tools` | Empty Kafka container | Where `bin/kafka` runs CLI commands |

---

## Step 0: Start, stop, reset

```bash
make up        # start everything (same as: ./scripts/setup.sh && docker compose up -d)
make status    # all "Up"; brokers "(healthy)"; init-topics "Exited (0)" is expected
make health    # health checklist read from Prometheus
docker compose stop   # pause, keeping data
make down      # remove containers. Kafka data lives in them, so this RESETS the cluster
```

The lab uses about 2.5 GB of RAM. Every CLI example below uses `bin/kafka`, which runs a Kafka tool
inside the `tools` container and adds `--bootstrap-server` for you.

> **Gotcha you'll meet in real life:** don't run CLI tools with `docker exec kafka1 ...`. The broker
> sets `KAFKA_OPTS` to load the JMX agent, so every CLI tool started there also tries to bind port
> 7071 and prints `Prometheus JMX Exporter exiting`. The broker healthcheck in
> `docker-compose.yml` clears `KAFKA_OPTS=` for exactly this reason.

---

## Step 1: Core concepts, seen in the UI

Open **http://localhost:8080** (Kafka UI).

1. **Brokers** page: 3 brokers, each holding about 59 partition *replicas* and about 20 *leaders*.
2. **Topics → orders**: 6 partitions, replication factor 3.
   * Open the **Partitions** tab. Each partition has one **leader** (which handles reads and writes)
     and two **followers** (copies). Leaders are spread across brokers.
   * **ISR (in-sync replicas)** = the replicas that are caught up. Normally all 3.
3. **Topics → orders → Messages**: live records. Note each one's *partition*, *offset* and *key*.
4. **Consumers**: `orders-service` reads `orders`. For each partition you see the **committed
   offset**, the **end offset**, and the **lag** (end minus committed).

Vocabulary to own after this step:

| Term | Meaning |
|---|---|
| Topic | Named stream of records, split into partitions |
| Partition | Ordered, append-only log. Ordering is only guaranteed *within* a partition |
| Offset | A record's position in a partition |
| Replica / RF | Copies of a partition on different brokers (RF=3 → 3 copies) |
| Leader / follower | The leader serves clients; followers copy from the leader |
| ISR | Replicas that are caught up with the leader |
| Consumer group | Consumers sharing work: each partition goes to exactly one member |
| Lag | How far a group is behind the newest record |

> **Check:** Kafka UI says "Active Controller: 3", but run
> `bin/kafka kafka-metadata-quorum.sh describe --status` and the `LeaderId` may differ.
> Why? In KRaft, the controller ID that brokers return to clients is just some live broker, not the
> real quorum leader. **Trust `kafka-metadata-quorum.sh` and the `activecontrollercount` metric.**

---

## Step 2: Hands-on with the CLI

```bash
# Topics
bin/kafka kafka-topics.sh --list
bin/kafka kafka-topics.sh --describe --topic orders
bin/kafka kafka-topics.sh --create --topic demo --partitions 3 --replication-factor 3
```

In `--describe` output, `Leader`, `Replicas` and `Isr` are broker IDs. **Healthy means Isr = Replicas.**

```bash
# Produce keyed messages (key:value). Same key → same partition → ordering preserved per key
printf 'alice:login\nbob:login\ncarol:login\nalice:click\nbob:click\nalice:logout\n' \
  | bin/kafka kafka-console-producer.sh --topic demo --property parse.key=true --property key.separator=:

# Read them back, printing partition and key
bin/kafka kafka-console-consumer.sh --topic demo --from-beginning --max-messages 6 \
  --property print.key=true --property print.partition=true --property print.offset=true

# Interactive producer: type lines, Ctrl-D to quit
bin/kafka kafka-console-producer.sh --topic demo
```

> **Check:** are all of `alice`'s events in one partition, in order? Are different keys in different
> partitions? (With 3 partitions, two keys can still share one by chance.)

```bash
# Consumer groups
bin/kafka kafka-consumer-groups.sh --list
bin/kafka kafka-consumer-groups.sh --describe --group orders-service
bin/kafka kafka-consumer-groups.sh --describe --group orders-service --members --verbose
```

**Scale a consumer group.** Open two terminals and run this in each:

```bash
bin/kafka kafka-console-consumer.sh --topic demo --group demo-readers
```

Then, in a third terminal, run `--describe --group demo-readers --members --verbose`. The 3
partitions are split between the 2 members. Kill one: its partitions move to the other member
(a **rebalance**). A 4th member on a 3-partition topic sits idle, because **partitions cap
parallelism**.

---

## Step 3: Configure Kafka

Kafka config lives at three levels: **broker** (cluster defaults), **topic** (overrides), and
**client** (producer/consumer settings). Topic configs and many broker configs are *dynamic*:
you change them with `kafka-configs.sh` and no restart is needed.

### 3a. Read the effective configuration

```bash
bin/kafka kafka-configs.sh --describe --entity-type topics --entity-name orders --all | head -40
bin/kafka kafka-configs.sh --describe --entity-type brokers --entity-name 1 --all | grep -E 'min.insync|replication.factor|retention|num.partitions'
```

This lab sets these broker defaults in `docker-compose.yml` (`KAFKA_FOO_BAR` → `foo.bar`):

| Setting | Lab value | Why it matters |
|---|---|---|
| `default.replication.factor` | 3 | New topics get 3 copies |
| `min.insync.replicas` | 2 | With `acks=all`, a write needs 2 replicas to confirm. RF=3 + minISR=2 survives 1 broker failure without data loss |
| `auto.create.topics.enable` | false | Typos don't silently create topics (recommended in prod) |
| `num.partitions` | 3 | Default partition count |

### 3b. Topic-level overrides

```bash
# Retention: keep orders for 10 minutes only (default is 7 days)
bin/kafka kafka-configs.sh --alter --entity-type topics --entity-name orders --add-config retention.ms=600000
# Watch "Log size by topic" in Grafana level off. Segments are deleted only once closed
# (segment.ms / segment.bytes), so it's not instant
bin/kafka kafka-configs.sh --alter --entity-type topics --entity-name orders --add-config segment.ms=60000

# Undo
bin/kafka kafka-configs.sh --alter --entity-type topics --entity-name orders --delete-config retention.ms,segment.ms
```

```bash
# Log compaction: keep only the LATEST value per key (used for state, changelogs, configs)
bin/kafka kafka-topics.sh --create --topic user-profile --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact --config segment.ms=10000 --config min.cleanable.dirty.ratio=0.01
printf 'u1:v1\nu2:v1\nu1:v2\nu1:v3\nu2:v2\n' | bin/kafka kafka-console-producer.sh --topic user-profile --property parse.key=true --property key.separator=:
# Wait about a minute, produce one more record (to roll the segment), then read from the beginning.
# The older u1/u2 values disappear
```

### 3c. Add partitions (one-way!)

```bash
bin/kafka kafka-topics.sh --alter --topic payments --partitions 6
```

You can never *reduce* partitions, and adding them changes which partition a key maps to.
That breaks per-key ordering for keys that move. Plan partition counts up front.

### 3d. Producer settings that matter

| Setting | Trade-off |
|---|---|
| `acks=0 / 1 / all` | Speed vs durability. `all` + `min.insync.replicas=2` is the safe default |
| `linger.ms`, `batch.size` | Higher = bigger batches, better throughput, slightly more latency |
| `compression.type` | `lz4`/`zstd` cut network and disk use a lot |
| `enable.idempotence` | No duplicates on retry (default `true` in modern clients) |

Try it with the perf tool and compare throughput and latency:

```bash
bin/kafka kafka-producer-perf-test.sh --topic demo --num-records 200000 --record-size 500 --throughput -1 \
  --producer-props bootstrap.servers=kafka1:9092 acks=1 linger.ms=0
bin/kafka kafka-producer-perf-test.sh --topic demo --num-records 200000 --record-size 500 --throughput -1 \
  --producer-props bootstrap.servers=kafka1:9092 acks=all linger.ms=20 compression.type=lz4
```

---

## Step 4: Monitor Kafka

### 4a. The dashboard

Open **http://localhost:3000** (admin / admin, then "Skip" the password change). The **Kafka
Overview** dashboard is the home page. Hover the ⓘ on any panel for what "healthy" looks like.

Read it top-down; that is the order you check things during an incident:

1. **Cluster health** (the stat row). These must be `3 / 1 / 0 / 0 / 0`. Anything else, act now.
   * Brokers online · Active controllers (exactly 1) · Under-replicated · Offline · Under min ISR
2. **Throughput**: is traffic normal? A sudden drop to 0 usually means producers are broken.
3. **Consumers**: is lag flat (OK) or climbing (a consumer is stuck or too slow)?
4. **Brokers**: latency, thread idle %, leader balance, heap. This is where you find *why*.
5. **Storage**: disk growth vs retention.

### 4b. Where the metrics come from

There are two complementary sources, and you need both:

* **Broker JMX** (`kafka_server_*`, `kafka_controller_*`, `kafka_network_*`, `jvm_*`):
  what the broker itself sees. The rules in `config/jmx-exporter/kafka-broker.yml` turn JMX
  MBeans into Prometheus names, for example
  `kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=orders` →
  `kafka_server_brokertopicmetrics_messagesin_total{topic="orders"}`.
  The agent reloads that file automatically when you edit it.
* **kafka-exporter** (`kafka_consumergroup_lag`, `kafka_topic_partition_current_offset`):
  computed from the outside like a client would. **Brokers don't export consumer lag**, which is
  why this exists.

### 4c. Learn PromQL in Prometheus

Open **http://localhost:9090**, paste these queries, and switch to the *Graph* tab:

```promql
# Messages/sec per topic. rate() turns an ever-increasing counter into a per-second rate
sum by (topic)(rate(kafka_server_brokertopicmetrics_messagesin_total{topic!=""}[1m]))

# Lag per group and topic
sum by (consumergroup, topic)(kafka_consumergroup_lag)

# p99 produce latency per broker
kafka_network_requestmetrics_totaltimems{request="Produce", quantile="0.99"}

# Is leadership balanced?
kafka_server_replicamanager_leadercount
```

Then browse **Status → Targets** (all should be UP) and **Alerts** (all green for now).

### 4d. Alerts

`config/prometheus/alerts.yml` defines: BrokerDown, NoActiveController, OfflinePartitions,
UnderReplicatedPartitions, UnderMinIsr, ConsumerLagGrowing, BrokerSaturated. Read each rule and
predict which drill below will fire it. (To get notified in Slack or email, you would add
Alertmanager. That's a good follow-up exercise.)

---

## Step 5: Incident drills (break it, watch it, fix it)

> Written-up runbooks with real numbers from this lab live in [docs/usecases/](docs/usecases/README.md),
> starting with [UC-01: broker down](docs/usecases/UC-01-broker-down.md).

Keep Grafana and http://localhost:9090/alerts open side by side. Run one drill at a time and
restore the cluster before starting the next.

### Drill 1: Stuck consumer → growing lag

```bash
docker compose stop consumer
```

**Watch:** "Consumer lag" climbs about 200/s for orders. "Consume rate" drops to 0.
`KafkaConsumerLagGrowing` goes pending, then firing.
`bin/kafka kafka-consumer-groups.sh --describe --group orders-service` reports *no active members*.

```bash
docker compose start consumer      # lag drains back to ~0
```

### Drill 2: Broker failure → under-replication and failover

```bash
docker compose stop kafka3
```

**Watch:**
* Brokers online **2**. Under-replicated partitions jump to about 60. That's every partition,
  because with RF=3 on 3 brokers each partition has a replica on kafka3. Offline stays **0**.
* "Partition leaders per broker": kafka3's leaders move to kafka1 and kafka2 (**failover**).
* Throughput continues. Producers use `acks=all` and minISR=2, and 2 replicas are still alive.
* `bin/kafka kafka-topics.sh --describe --under-replicated-partitions` lists them. Note `Isr`.

```bash
docker compose start kafka3
```

**Watch:** ISR expands, and under-replicated partitions return to 0. **But** kafka3 now leads 0
partitions. Leadership comes back via automatic preferred-leader rebalancing (every ~5 min), or
force it now:

```bash
bin/kafka kafka-leader-election.sh --election-type preferred --all-topic-partitions
```

### Drill 3: Below min ISR → writes rejected

```bash
# Require 3 in-sync replicas for payments, then lose one broker
bin/kafka kafka-configs.sh --alter --entity-type topics --entity-name payments --add-config min.insync.replicas=3
docker compose stop kafka2
docker compose logs --tail 20 producer      # NOT_ENOUGH_REPLICAS errors for payments
```

**Watch:** "Under min ISR" turns red and `KafkaUnderMinIsr` fires. payments throughput drops to 0
while orders keeps flowing. This is Kafka **choosing durability over availability**.

```bash
docker compose start kafka2
bin/kafka kafka-configs.sh --alter --entity-type topics --entity-name payments --delete-config min.insync.replicas
```

> Why not just stop 2 brokers? The 3 controllers need a majority (2 of 3) to make any metadata
> change. With 2 nodes down, the cluster can't even shrink the ISR, so writes just time out. That's
> a good thing to know, but a confusing first drill.

### Drill 4: Replay data by resetting offsets

```bash
docker compose stop consumer       # a group must have no active members to reset
until bin/kafka kafka-consumer-groups.sh --describe --group orders-service 2>&1 | grep -q 'no active members'; do sleep 2; done
bin/kafka kafka-consumer-groups.sh --group orders-service --topic orders --reset-offsets --to-earliest --dry-run
bin/kafka kafka-consumer-groups.sh --group orders-service --topic orders --reset-offsets --to-earliest --execute
docker compose start consumer      # lag spikes, then the group re-reads everything
```

Other options: `--to-datetime 2026-11-08T09:00:00.000`, `--shift-by -1000`, `--to-latest`
(skip the backlog).

### Drill 5: Traffic spike

```bash
bin/kafka kafka-producer-perf-test.sh --topic orders --num-records 500000 --record-size 1000 --throughput -1 \
  --producer-props bootstrap.servers=kafka1:9092,kafka2:9092,kafka3:9092 acks=all
```

**Watch:** Messages in/sec and bytes in spike, Produce p99 latency rises, consumer lag briefly jumps,
and log size climbs. Do the request/network thread idle % drop? On this lab they don't: the bottleneck
is disk and replication, not broker CPU. See [UC-05](docs/usecases/UC-05-traffic-spike.md).

---

## Step 6: From the lab to a real cluster

What changes in production, and what to review in an existing setup:

* **Dedicated controllers.** Run 3 controller-only nodes, separate from brokers
  (`process.roles=controller`). Then request-handler idle maxes at 1.0. In this lab it maxes at
  ~2.0 because combined mode counts two thread pools.
* **Security.** Use TLS + SASL (SCRAM or OAuth) listeners and ACLs. Add
  `KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL` etc. to Kafka UI, and SASL flags to kafka-exporter.
* **Persistent storage.** Mount volumes for `log.dirs`. This lab keeps data in the container.
* **Kafka UI access.** Enable auth (`AUTH_TYPE`), and consider `KAFKA_CLUSTERS_0_READONLY=true`.
* **Alert routing.** Use Alertmanager → Slack/PagerDuty, with longer `for:` windows.
* **Review checklist for an existing cluster:** RF ≥ 3 and `min.insync.replicas` = 2 on important
  topics. Producers use `acks=all`. `auto.create.topics.enable=false`. Retention matches the
  business need and disk size. Partition counts are sized for consumer parallelism. Leader and disk
  balance across brokers. Consumer lag alerts exist per critical group.

---

## Files

```
Makefile                                   make up / down / reset / status / health / logs / dashboard / open
docker-compose.yml                         the whole lab
bin/kafka                                  run any Kafka CLI tool against the lab
scripts/setup.sh                           downloads + checksums the JMX exporter agent (run by make up)
scripts/health.py                          health checklist from Prometheus (--json for tools/AI)
scripts/gen_dashboard.py                   source of the Grafana dashboard (make dashboard)
config/jmx-exporter/kafka-broker.yml       JMX → Prometheus metric rules (hot-reloaded)
config/prometheus/prometheus.yml           scrape targets
config/prometheus/alerts.yml               alert rules
config/grafana/                            datasource, dashboard provisioning, generated dashboard JSON
docs/usecases/                             incident runbooks (UC-01…), verified on this lab
CLAUDE.md, .claude/skills/                 AI tutor setup: kafka-tutor, kafka-incident, kafka-lab
```

Change the dashboard either way: edit `scripts/gen_dashboard.py`, then run `make dashboard`. Or edit
it in the Grafana UI and use *Export → JSON* to save it back into `config/grafana/dashboards/`.
