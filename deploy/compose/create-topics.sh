#!/usr/bin/env bash
# Creates every Kafka topic the platform uses. Safe to run more than once.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP_SERVER:-kafka:9092}"
PARTITIONS="${TOPIC_PARTITIONS:-3}"
REPLICATION="${TOPIC_REPLICATION:-1}"
KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh

base_topics=(
  inventory.commands
  inventory.events
  payment.commands
  payment.events
  order.events
)

topics=()
for t in "${base_topics[@]}"; do
  topics+=("$t" "$t.dlq")
done

for t in "${topics[@]}"; do
  "$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$t" --partitions "$PARTITIONS" --replication-factor "$REPLICATION"
done

# Fail loudly if any topic is missing afterwards.
existing="$("$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP" --list)"
for t in "${topics[@]}"; do
  if ! grep -qx "$t" <<<"$existing"; then
    echo "topic $t was not created" >&2
    exit 1
  fi
done
echo "all ${#topics[@]} topics present"
