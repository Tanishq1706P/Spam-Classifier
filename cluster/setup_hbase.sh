#!/usr/bin/env bash
# Usage: ./setup_hbase.sh   (run on EVERY node so region servers have the binaries)
set -euo pipefail; source "$(dirname "$0")/env.sh"
fetch "https://archive.apache.org/dist/hbase/$HBASE_VER/hbase-$HBASE_VER-hadoop3-bin.tar.gz" "$INSTALL_DIR"
[ -d "$HBASE_HOME" ] || mv "$INSTALL_DIR/hbase-$HBASE_VER-hadoop3" "$HBASE_HOME"
echo "export JAVA_HOME=$JAVA_HOME
export HBASE_MANAGES_ZK=true" >> "$HBASE_HOME/conf/hbase-env.sh"
cat > "$HBASE_HOME/conf/hbase-site.xml" <<X
<configuration>
 <property><name>hbase.cluster.distributed</name><value>true</value></property>
 <property><name>hbase.rootdir</name><value>hdfs://$MASTER_HOST:9000/hbase</value></property>
 <property><name>hbase.zookeeper.quorum</name><value>$MASTER_HOST</value></property>
 <property><name>hbase.unsafe.stream.capability.enforce</name><value>false</value></property>
</configuration>
X
printf '%s\n' $WORKER_HOSTS > "$HBASE_HOME/conf/regionservers"
echo "HBase $HBASE_VER configured"
