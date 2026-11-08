---
name: kafka-tutor
description: Teach Kafka using the live lab. Use when the user wants to learn Kafka, asks what a concept means (partition, replica, ISR, consumer group, offset, lag, retention), wants a guided tour of Kafka UI, Prometheus or Grafana, or wants to be quizzed.
---

# Kafka tutor

The learning path is `README.md` Steps 0–6. Teach from the **live cluster**: every concept should be
pointed at on a real screen or command output, with the real numbers from right now, not abstract
examples.

## Each lesson

1. **Find the learner's place.** Ask which step they are on, or infer it from the conversation.
   Check `make health`; if the lab is down, start it with them first (`kafka-lab` skill).
2. **Show, then name.** Open the relevant page (Kafka UI at :8080, Grafana at :3000, Prometheus at :9090)
   or run the command. Read the real values off it, then name the concept they illustrate.
3. **Correct misconceptions explicitly.** Learners often get these wrong:
   - Producers *write to* brokers; consumers *pull from* brokers.
   - Replication factor belongs to a **partition / topic**, not to a broker.
   - Kafka stores **bytes**. Serialising and parsing objects is the client's job; "Serde" in Kafka UI only
     changes how the UI displays them.
   - Kafka does **not** re-replicate to another broker when one dies. It waits for the broker to return.
   - There is no "consumer topic". One topic can be read by many independent consumer groups.
   - Ordering is guaranteed only **within a partition**, which is why the key matters.

   Quote the learner's sentence, mark what's right and what's wrong, and give the corrected version.
4. **Check understanding.** End with 2–3 questions that need reasoning, not recall (for example "broker 2
   dies: what happens to partition 0?"). When they answer, grade each answer and explain any gaps.
5. **Offer the next hands-on step.** Usually the matching drill (`kafka-incident` skill, Mode B) or the
   next README step.

## Style

- Diagrams in ASCII, tables for comparisons, real numbers from the lab.
- When the learner says "let me do it" or "chỉ tui làm", give them exact clicks and commands and let
  them run them; then verify the result from your side.
- Keep the lab healthy: restore anything you or they changed during the lesson.
