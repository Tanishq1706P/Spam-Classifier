#!/usr/bin/env bash
# Start the whole stack from the master, then create HDFS dirs, topics and HBase tables.
set -euo pipefail; source "$(dirname "$0")/env.sh"; cd "$(dirname "$0")/.."
start-dfs.sh && start-yarn.sh
hdfs dfsadmin -safemode wait
kafka-server-start.sh -daemon "$KAFKA_HOME/config/kraft/server.properties"; sleep 8
start-hbase.sh; hbase-daemon.sh start thrift
hdfs dfs -mkdir -p /data/raw /data/clean /models
for t in emails-in results; do
  kafka-topics.sh --create --if-not-exists --topic $t --partitions 3 --bootstrap-server $MASTER_HOST:9092
done
echo "create 'predictions','cf'
create 'model_runs','info','metrics'
create 'phase_log','log'
create 'datasets','meta'" | hbase shell -n 2>&1 | grep -v "already exists" || true
./cluster/health_check.sh
