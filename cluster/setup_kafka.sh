#!/usr/bin/env bash
# Run on the master. Single KRaft broker (no separate ZooKeeper needed for Kafka).
set -euo pipefail; source "$(dirname "$0")/env.sh"
fetch "https://archive.apache.org/dist/kafka/$KAFKA_VER/kafka_$KAFKA_SCALA-$KAFKA_VER.tgz" "$INSTALL_DIR"
CFG=$KAFKA_HOME/config/kraft/server.properties
sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://$MASTER_HOST:9092|; s|^controller.quorum.voters=.*|controller.quorum.voters=1@$MASTER_HOST:9093|; s|^listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://$MASTER_HOST:9093|; s|^log.dirs=.*|log.dirs=$INSTALL_DIR/data/kafka-logs|" "$CFG"
if [ ! -f "$INSTALL_DIR/data/kafka-logs/meta.properties" ]; then
  kafka-storage.sh format -t "$(kafka-storage.sh random-uuid)" -c "$CFG"
fi
echo "Kafka $KAFKA_VER configured (KRaft)"
