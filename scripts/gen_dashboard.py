"""Generates config/grafana/dashboards/kafka-overview.json.

Edit this file, then:  python3 scripts/gen_dashboard.py && docker restart grafana
(Or edit the dashboard in the Grafana UI and export the JSON - both work.)
"""
import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "prometheus"}
panels, y, pid = [], 0, 0


def nid():
    global pid
    pid += 1
    return pid


def row(title):
    global y
    panels.append({"type": "row", "title": title, "id": nid(), "collapsed": False,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})
    y += 1


def stat(x, w, title, expr, desc, thresholds, unit="short"):
    panels.append({
        "type": "stat", "id": nid(), "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": 4},
        "targets": [{"refId": "A", "expr": expr, "instant": True, "datasource": DS}],
        "options": {"colorMode": "background", "graphMode": "none",
                    "reduceOptions": {"calcs": ["lastNotNull"]}},
        "fieldConfig": {"defaults": {"unit": unit,
                                     "thresholds": {"mode": "absolute", "steps": thresholds}},
                        "overrides": []},
    })


def ts(x, w, title, targets, desc, unit="short", h=8, stack=False):
    panels.append({
        "type": "timeseries", "id": nid(), "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": l, "datasource": DS}
                    for i, (e, l) in enumerate(targets)],
        "options": {"legend": {"displayMode": "table", "placement": "bottom",
                               "calcs": ["lastNotNull", "max"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
        "fieldConfig": {"defaults": {"unit": unit, "custom": {
            "fillOpacity": 15 if stack else 5, "lineWidth": 1,
            "stacking": {"mode": "normal" if stack else "none"}}}, "overrides": []},
    })


def G(c):
    return {"color": c, "value": None}


def T(c, v):
    return {"color": c, "value": v}


# ---------------------------------------------------------------- Cluster health
row("Cluster health  (all green = healthy)")
stat(0, 4, "Brokers online", "kafka_brokers",
     "Brokers visible in cluster metadata. Expect 3. Fewer = a broker is down.",
     [G("red"), T("orange", 2), T("green", 3)])
stat(4, 4, "Active controllers", "sum(kafka_controller_kafkacontroller_activecontrollercount)",
     "Exactly 1 KRaft controller must be active. 0 = no metadata leader; >1 = split brain.",
     [G("red"), T("green", 1), T("red", 2)])
stat(8, 4, "Under-replicated partitions", "sum(kafka_server_replicamanager_underreplicatedpartitions)",
     "Partitions where a replica has fallen out of the ISR. Should be 0. "
     "Non-zero = broker down, slow disk or network.",
     [G("green"), T("red", 1)])
stat(12, 4, "Offline partitions", "max(kafka_controller_kafkacontroller_offlinepartitionscount)",
     "Partitions with NO leader: unreadable and unwritable. Must be 0. Any value is an outage.",
     [G("green"), T("red", 1)])
stat(16, 4, "Under min ISR", "sum(kafka_cluster_partition_underminisr)",
     "Partitions with fewer in-sync replicas than min.insync.replicas (2). "
     "acks=all producers get NotEnoughReplicas errors on these.",
     [G("green"), T("red", 1)])
stat(20, 4, "Total consumer lag", "sum(kafka_consumergroup_lag)",
     "Messages produced but not yet consumed, across all groups. Watch the trend, not the number.",
     [G("green"), T("orange", 5000), T("red", 50000)])
y += 4

# ---------------------------------------------------------------- Throughput
row("Throughput")
ts(0, 12, "Messages in / sec by topic",
   [('sum by (topic)(rate(kafka_server_brokertopicmetrics_messagesin_total{topic=~"$topic"}[1m]))', "{{topic}}")],
   "Producer write rate per topic (counted on partition leaders).", stack=True)
ts(12, 12, "Bytes in / out per broker",
   [('sum by (broker)(rate(kafka_server_brokertopicmetrics_bytesin_total{topic=""}[1m]))', "in {{broker}}"),
    ('-sum by (broker)(rate(kafka_server_brokertopicmetrics_bytesout_total{topic=""}[1m]))', "out {{broker}}")],
   "Above zero: bytes produced in. Below zero: bytes fetched by consumers. "
   "Uneven brokers = skewed partition leadership.", "Bps")
y += 8

# ---------------------------------------------------------------- Consumers
row("Consumers")
ts(0, 12, "Consumer lag by group / topic",
   [('sum by (consumergroup, topic)(kafka_consumergroup_lag{topic=~"$topic"})', "{{consumergroup}} → {{topic}}")],
   "Lag that keeps growing = consumer slower than producer, or stopped. Flat small lag is normal.")
ts(12, 12, "Consume rate vs produce rate",
   [('sum by (consumergroup, topic)(rate(kafka_consumergroup_current_offset{topic=~"$topic"}[1m]))', "consumed {{consumergroup}} → {{topic}}"),
    ('sum by (topic)(rate(kafka_topic_partition_current_offset{topic=~"$topic"}[1m]))', "produced {{topic}}")],
   "When consumed < produced for a sustained period, lag grows.")
y += 8

# ---------------------------------------------------------------- Brokers
row("Brokers")
ts(0, 8, "Request latency p99",
   [('max by (request)(kafka_network_requestmetrics_totaltimems{request=~"Produce|FetchConsumer|FetchFollower", quantile="0.99"})', "{{request}}")],
   "Time a broker spends on a request. Fetch requests long-poll, so ~500ms (fetch.max.wait.ms) "
   "is normal for them; Produce should stay low.", "ms")
ts(8, 8, "Request handler idle (max 2.0 in this lab)",
   [("kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent", "{{broker}}")],
   "Idle ratio of the I/O threads. On a dedicated broker max is 1.0 and below 0.3 = saturated. "
   "This lab runs each node as broker+controller (combined mode), so both thread pools add into "
   "one metric: max is ~2.0, worry below ~0.6.")
ts(16, 8, "Network processor idle",
   [("kafka_network_socketserver_networkprocessoravgidlepercent", "{{broker}}")],
   "Fraction of time network threads are idle. Below 0.3 the broker is saturated.", "percentunit")
y += 8
ts(0, 8, "Partition leaders per broker",
   [("kafka_server_replicamanager_leadercount", "{{broker}}")],
   "Should be roughly equal. After a broker restart, leadership moves away and comes back "
   "via preferred-leader election.")
ts(8, 8, "ISR shrinks / expands per min",
   [("sum by (broker)(rate(kafka_server_replicamanager_isrshrinks_total[5m]))*60", "shrink {{broker}}"),
    ("sum by (broker)(rate(kafka_server_replicamanager_isrexpands_total[5m]))*60", "expand {{broker}}")],
   "Replicas dropping out of / rejoining the in-sync set. Spikes during restarts are expected; "
   "constant flapping is not.")
ts(16, 8, "JVM heap used",
   [('sum by (broker)(jvm_memory_used_bytes{area="heap"})', "{{broker}}"),
    ('max(jvm_memory_max_bytes{area="heap"})', "max")],
   "Sawtooth = healthy GC. Heap pinned near max = memory pressure.", "bytes")
y += 8

# ---------------------------------------------------------------- Storage
row("Storage")
ts(0, 12, "Log size by topic (all replicas)",
   [('sum by (topic)(kafka_log_log_size{topic=~"$topic"})', "{{topic}}")],
   "Bytes on disk including every replica (RF=3 ⇒ 3× the logical size). "
   "Drops when retention deletes old segments.", "bytes", stack=True)
ts(12, 12, "Log size by broker",
   [("sum by (broker)(kafka_log_log_size)", "{{broker}}")],
   "Disk usage per broker. Large imbalance = uneven partition placement.", "bytes")
y += 8

TOPIC_Q = 'label_values(kafka_topic_partitions{topic!~"__.*"}, topic)'
dash = {
    "uid": "kafka-overview", "title": "Kafka Overview", "tags": ["kafka"], "timezone": "browser",
    "refresh": "10s", "time": {"from": "now-30m", "to": "now"}, "schemaVersion": 39, "editable": True,
    "templating": {"list": [{
        "name": "topic", "label": "Topic", "type": "query", "datasource": DS,
        "query": {"query": TOPIC_Q, "refId": "q"}, "definition": TOPIC_Q,
        "includeAll": True, "multi": True, "allValue": "[^_].*",
        "current": {"text": "All", "value": "$__all"}, "refresh": 2}]},
    "panels": panels,
}
out = Path(__file__).resolve().parent.parent / "config/grafana/dashboards/kafka-overview.json"
out.write_text(json.dumps(dash, indent=2))
print(f"wrote {out} ({len(panels)} panels)")
