#!/usr/bin/env bash
# Usage: ./setup_hadoop.sh master|worker      (run on EVERY node; see docs/setup.md)
set -euo pipefail; source "$(dirname "$0")/env.sh"
ROLE=${1:?usage: setup_hadoop.sh master|worker}
sudo apt-get update -y && sudo apt-get install -y openjdk-11-jdk ssh pdsh curl
fetch "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VER/hadoop-$HADOOP_VER.tar.gz" "$INSTALL_DIR"
echo "export JAVA_HOME=$JAVA_HOME" >> "$HADOOP_CONF_DIR/hadoop-env.sh"
cat > "$HADOOP_CONF_DIR/core-site.xml" <<X
<configuration><property><name>fs.defaultFS</name><value>hdfs://$MASTER_HOST:9000</value></property></configuration>
X
cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<X
<configuration>
 <property><name>dfs.replication</name><value>2</value></property>
 <property><name>dfs.namenode.name.dir</name><value>file://$INSTALL_DIR/data/nn</value></property>
 <property><name>dfs.datanode.data.dir</name><value>file://$INSTALL_DIR/data/dn</value></property>
</configuration>
X
cat > "$HADOOP_CONF_DIR/mapred-site.xml" <<X
<configuration><property><name>mapreduce.framework.name</name><value>yarn</value></property></configuration>
X
cat > "$HADOOP_CONF_DIR/yarn-site.xml" <<X
<configuration>
 <property><name>yarn.resourcemanager.hostname</name><value>$MASTER_HOST</value></property>
 <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
 <property><name>yarn.nodemanager.resource.memory-mb</name><value>3072</value></property>
 <property><name>yarn.scheduler.maximum-allocation-mb</name><value>3072</value></property>
 <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
</configuration>
X
printf '%s\n' $WORKER_HOSTS > "$HADOOP_CONF_DIR/workers"
mkdir -p "$INSTALL_DIR/data/nn" "$INSTALL_DIR/data/dn"
grep -q "cluster/env.sh" ~/.bashrc || echo "source $(cd "$(dirname "$0")" && pwd)/env.sh" >> ~/.bashrc
if [ "$ROLE" = master ] && [ ! -f "$INSTALL_DIR/data/nn/current/VERSION" ]; then hdfs namenode -format -force; fi
echo "Hadoop $HADOOP_VER configured as $ROLE"
