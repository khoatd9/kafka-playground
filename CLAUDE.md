# kafka-playground

A local Kafka lab for **learning to operate, configure and monitor Kafka**: 3 KRaft brokers, Kafka UI,
Prometheus, Grafana, alert rules, synthetic traffic, and incident runbooks. People clone this repo to
learn with an AI tutor, so the human is usually a **learner**: explain the *why*, show where to look in
the UIs, and let them run commands themselves when they ask to.

Reply in the language the user writes in (learners here often write Vietnamese).

## Where things are

- Lab operations, CLI usage and health checks → `kafka-lab` skill
- Something looks broken, an alert fires, or the user wants to practise an incident → `kafka-incident` skill
- Teaching, quizzing, walking through the learning steps → `kafka-tutor` skill
- Runbooks (one per incident type, with numbers measured on this lab) → `docs/usecases/`
- Learning path for humans → `README.md`

## Gotchas no config file tells you

- **Never run Kafka CLI tools inside a broker** (`docker exec kafka1 kafka-topics.sh …`). The broker's
  `KAFKA_OPTS` loads the JMX agent, so the CLI tries to bind port 7071 too and prints
  `Prometheus JMX Exporter exiting`. Use `bin/kafka <tool>.sh …`, which runs in the `tools` container.
  The broker healthcheck clears `KAFKA_OPTS=` for the same reason.
- Nodes run **combined mode** (broker + controller), so `requesthandleravgidlepercent` maxes at ~2.0,
  not 1.0. Losing 2 nodes also loses controller quorum.
- Kafka UI's "Active Controller" is **not** the KRaft leader (KRaft returns a random live broker).
  The truth is `bin/kafka kafka-metadata-quorum.sh describe --status`.
- Kafka UI's Production/Consumption bytes show 0: it can't map this lab's JMX metric names. Use Grafana.
- Kafka data lives inside the containers: `docker compose down` / `make reset` wipes the cluster.
- The JMX exporter agent reloads `config/jmx-exporter/kafka-broker.yml` on change; no broker restart needed.
- The dashboard JSON is generated: edit `scripts/gen_dashboard.py`, then `make dashboard`.
- macOS has no `timeout` command; avoid it in shell snippets.
- **Leave the lab healthy.** After any experiment, restore what you changed and confirm
  `make health` passes before ending the turn.
