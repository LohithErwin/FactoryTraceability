#!/usr/bin/env bash
# Starts a single-node Kafka in KRaft mode (no Zookeeper). Usage: KAFKA_HOME=/path/to/kafka ./scripts/start_kafka.sh
set -e
: "${KAFKA_HOME:?Set KAFKA_HOME to the extracted Kafka directory}"
CFG="$KAFKA_HOME/config/kraft/server.properties"
[ -f "$CFG" ] || CFG="$KAFKA_HOME/config/server.properties"   # Kafka 4.x layout
LOGDIR="${KAFKA_LOG_DIR:-/tmp/kraft-combined-logs}"             # matches default log.dirs
if [ ! -f "$LOGDIR/meta.properties" ]; then
  ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
  "$KAFKA_HOME/bin/kafka-storage.sh" format -t "$ID" -c "$CFG"
fi
exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$CFG"
