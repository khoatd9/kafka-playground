---
name: kafka-incident
description: Diagnose and resolve Kafka incidents with the runbooks in docs/usecases. Use when an alert fires, a health check fails, lag grows, writes fail, a broker is down, or the user wants to run or practise an incident drill (e.g. "run drill UC-02").
---

# Kafka incident handling

Runbooks live in `docs/usecases/UC-*.md`; the index is `docs/usecases/README.md`. Every runbook has the
same sections: **Detect · What Kafka does automatically · Diagnose · Remediate · Verify · Drill ·
Observed in this lab**.

Two modes. Pick the one the user asked for.

## Mode A: respond to a real problem

1. **Triage.** Run `python3 scripts/health.py --json`. Note every failing check and firing alert.
   Done when you have the full list of symptoms.
2. **Match a runbook.** Map the symptoms to runbooks using the index's *Alert* column. Several runbooks
   can apply at once (a broker down can also cause under-min-ISR); handle the most severe first:
   offline partitions → under min ISR → broker down → lag → saturation.
   No runbook fits? Say so, and diagnose from first principles with the Grafana panels and `bin/kafka`.
3. **Diagnose** with the runbook's *Diagnose* section. Find the root cause (OOM, full disk, stopped
   container, config change), not just the symptom. Done when you can name the cause and show the
   evidence (a log line, a metric, a config value).
4. **Remediate.** Explain the fix and its risk, then apply it. Ask first for anything destructive:
   deleting topics or data, resetting offsets, or `make reset`.
5. **Verify.** Work through every item of the runbook's *Verify* checklist and finish with
   `make health` passing. Report each item's actual value.

## Mode B: drill (practise on the lab)

1. Read the runbook. Tell the user what will break and which Grafana panels and alerts to watch.
2. Capture a **before** snapshot: `python3 scripts/health.py --json`.
3. Run the inject commands from the runbook's *Drill* section. Wait long enough for alerts to pass their `for:` window (see
   `config/prometheus/alerts.yml`), then capture a **during** snapshot.
4. Explain each change between the snapshots in terms of the concepts (leader, ISR, lag, min ISR).
   Point at where it shows in Kafka UI and Grafana.
5. Run the restore commands from *Drill*, then Mode A step 5 (Verify). The drill is complete only when `make health`
   passes again.
6. If your numbers differ meaningfully from *Observed in this lab*, tell the user and offer to update
   the runbook.

When the user wants to break things by hand, give them the commands and observe alongside them
instead of running the commands yourself.

## Writing a new runbook

Copy the template in `docs/usecases/README.md`, run the drill on the lab, record the real numbers
under *Observed in this lab*, and add a row to the index.
