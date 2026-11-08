---
name: kafka-lab
description: Operate the local Kafka lab. Use to start, stop, reset or check the lab's status or health, run any Kafka CLI command (topics, consumer groups, configs, producing or consuming messages), or change broker, topic, Prometheus or Grafana configuration.
---

# Kafka lab operations

## Services

| Service | Address | Notes |
|---|---|---|
| kafka1..3 | `kafka1:9092` inside Docker, `localhost:19092/29092/39092` from the host | KRaft combined mode, RF=3, min ISR=2 |
| Kafka UI | http://localhost:8080 | Browse topics, messages, groups, configs |
| Grafana | http://localhost:3000 (admin/admin) | "Kafka Overview" dashboard |
| Prometheus | http://localhost:9090 | Queries, `/alerts`, `/targets` |
| producer | container | 200 msg/s → `orders`, 50 msg/s → `payments`, `acks=all` |
| consumer | container | Groups `orders-service`, `payments-service` |
| kafka-exporter | `:9308` | Consumer lag and offsets |
| tools | container | Where `bin/kafka` runs CLI tools |

## Lifecycle

`make up` · `make down` · `make reset` · `make status` · `make health` · `make logs s=<service>` · `make open`

A first `make up` downloads about 3 GB of images. The brokers need ~30 s to become `healthy`.
`init-topics` showing `Exited (0)` is correct.

## Health check

`make health` (or `python3 scripts/health.py --json` for structured output) checks brokers online,
scrape targets, the active controller, under-replicated / offline / under-min-ISR partitions, leader
balance and firing alerts. Exit code 0 means healthy. Run it before and after any change you make.

## Kafka CLI

Always go through `bin/kafka <tool>.sh [args]`. It adds `--bootstrap-server` and allocates a TTY only
when stdin is a terminal, so piping works:

```bash
bin/kafka kafka-topics.sh --describe --topic orders
printf 'k1:v1\n' | bin/kafka kafka-console-producer.sh --topic demo --property parse.key=true --property key.separator=:
bin/kafka kafka-console-consumer.sh --topic demo --from-beginning --max-messages 5 --property print.key=true --property print.partition=true
bin/kafka kafka-consumer-groups.sh --describe --group orders-service
bin/kafka kafka-configs.sh --alter --entity-type topics --entity-name orders --add-config retention.ms=3600000
bin/kafka kafka-metadata-quorum.sh describe --status
```

Files a tool reads (`--reassignment-json-file` etc.) must exist inside the `tools` container. Pipe them
in with `docker compose exec -T tools sh -c 'cat > /tmp/f.json' < f.json`.

## Changing configuration

| What | Where | Apply with |
|---|---|---|
| Broker defaults | `x-kafka-env` in `docker-compose.yml` (`KAFKA_FOO_BAR` → `foo.bar`) | `docker compose up -d` (rolling recreate) |
| Topic / dynamic broker config | `bin/kafka kafka-configs.sh --alter …` | Immediate |
| Demo topics | `init-topics` service in `docker-compose.yml` | `docker compose up -d` |
| JMX → Prometheus metric names | `config/jmx-exporter/kafka-broker.yml` | Hot-reloaded by the agent |
| Scrape targets | `config/prometheus/prometheus.yml` | `docker compose restart prometheus` |
| Alert rules | `config/prometheus/alerts.yml` | `docker kill -s HUP prometheus` |
| Dashboard | `scripts/gen_dashboard.py` | `make dashboard` |

After a metric or dashboard change, confirm the metric returns data with a Prometheus query
(`curl -s localhost:9090/api/v1/query --data-urlencode 'query=<expr>'`) and look at the panel in the
browser before calling it done.
