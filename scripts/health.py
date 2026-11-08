#!/usr/bin/env python3
"""Kafka lab health snapshot, read from Prometheus.

Usage:
  python3 scripts/health.py            # human-readable checklist, exit 1 if anything is unhealthy
  python3 scripts/health.py --json     # machine-readable (used by Claude skills / drills)
  PROM_URL=http://host:9090 python3 scripts/health.py
"""
import json
import os
import sys
import urllib.parse
import urllib.request

PROM = os.environ.get("PROM_URL", "http://localhost:9090")
EXPECTED_BROKERS = int(os.environ.get("EXPECTED_BROKERS", "3"))


def query(expr):
    url = f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.load(r)["data"]["result"]


def scalar(expr):
    res = query(expr)
    return (lambda v: int(v) if v == int(v) else round(v, 1))(float(res[0]["value"][1])) if res else None


def by_label(expr, label):
    return {x["metric"].get(label, "?"): round(float(x["value"][1]), 1) for x in query(expr)}


def snapshot():
    return {
        "brokers_online": scalar("kafka_brokers"),
        "scrape_targets_down": by_label('up == 0', "instance"),
        "active_controllers": scalar("sum(kafka_controller_kafkacontroller_activecontrollercount)"),
        "under_replicated": scalar("sum(kafka_server_replicamanager_underreplicatedpartitions)"),
        "offline_partitions": scalar("max(kafka_controller_kafkacontroller_offlinepartitionscount)"),
        "under_min_isr": scalar("sum(kafka_cluster_partition_underminisr)"),
        "leaders_per_broker": by_label("kafka_server_replicamanager_leadercount", "broker"),
        "msgs_in_per_sec_by_topic": by_label(
            'sum by (topic)(rate(kafka_server_brokertopicmetrics_messagesin_total{topic!~"|__.*"}[1m]))', "topic"),
        "lag_by_group": by_label("sum by (consumergroup)(kafka_consumergroup_lag)", "consumergroup"),
        "produce_p99_ms": scalar('max(kafka_network_requestmetrics_totaltimems{request="Produce",quantile="0.99"})'),
        "network_idle_min": scalar("min(kafka_network_socketserver_networkprocessoravgidlepercent)"),
        "firing_alerts": sorted({a["labels"]["alertname"] for a in
                                 json.load(urllib.request.urlopen(f"{PROM}/api/v1/alerts", timeout=5))["data"]["alerts"]
                                 if a["state"] == "firing"}),
    }


def checks(s):
    leaders = list(s["leaders_per_broker"].values())
    return [
        ("Brokers online", s["brokers_online"] == EXPECTED_BROKERS, f'{s["brokers_online"]} (expect {EXPECTED_BROKERS})'),
        ("Scrape targets up", not s["scrape_targets_down"], ", ".join(s["scrape_targets_down"]) or "all up"),
        ("Exactly 1 active controller", s["active_controllers"] == 1, s["active_controllers"]),
        ("Under-replicated = 0", s["under_replicated"] == 0, s["under_replicated"]),
        ("Offline partitions = 0", s["offline_partitions"] == 0, s["offline_partitions"]),
        ("Under min ISR = 0", s["under_min_isr"] == 0, s["under_min_isr"]),
        ("Leaders balanced (max-min <= 3)", bool(leaders) and max(leaders) - min(leaders) <= 3, s["leaders_per_broker"]),
        ("No firing alerts", not s["firing_alerts"], ", ".join(s["firing_alerts"]) or "none"),
    ]


def main():
    try:
        s = snapshot()
    except OSError as e:
        print(f"Cannot reach Prometheus at {PROM}: {e}\nIs the lab running? Try: make up", file=sys.stderr)
        sys.exit(2)
    results = checks(s)
    if "--json" in sys.argv:
        s["healthy"] = all(ok for _, ok, _ in results)
        print(json.dumps(s, indent=2))
    else:
        for name, ok, detail in results:
            print(f'{"✅" if ok else "❌"} {name:34} {detail}')
        print(f'\nThroughput msg/s: {s["msgs_in_per_sec_by_topic"]}')
        print(f'Consumer lag:     {s["lag_by_group"]}')
        print(f'Produce p99 ms:   {s["produce_p99_ms"]}   network idle min: {s["network_idle_min"]}')
    sys.exit(0 if all(ok for _, ok, _ in results) else 1)


if __name__ == "__main__":
    main()
