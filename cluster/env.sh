#!/usr/bin/env bash
# Shared settings for all cluster scripts. Edit hostnames here (must match /etc/hosts on every node).
export MASTER_HOST=${MASTER_HOST:-master}
export WORKER_HOSTS=${WORKER_HOSTS:-"worker1 worker2"}      # add more workers here to scale out
export INSTALL_DIR=${INSTALL_DIR:-$HOME/bigdata}
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export HADOOP_VER=3.3.6 SPARK_VER=3.5.3 KAFKA_VER=3.6.1 HBASE_VER=2.5.8 KAFKA_SCALA=2.13
export HADOOP_HOME=$INSTALL_DIR/hadoop-$HADOOP_VER
export SPARK_HOME=$INSTALL_DIR/spark-$SPARK_VER-bin-hadoop3
export KAFKA_HOME=$INSTALL_DIR/kafka_$KAFKA_SCALA-$KAFKA_VER
export HBASE_HOME=$INSTALL_DIR/hbase-$HBASE_VER
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$KAFKA_HOME/bin:$HBASE_HOME/bin
ALL_HOSTS="$MASTER_HOST $WORKER_HOSTS"
fetch() {  # fetch <url> <dest-dir>   (skips if already extracted)
  local url=$1 dest=$2 f; f=$(basename "$url")
  mkdir -p "$dest"; [ -d "$dest/${f%.tar.gz}" ] || [ -d "$dest/${f%.tgz}" ] && return 0
  curl -fL "$url" -o "/tmp/$f" && tar -xzf "/tmp/$f" -C "$dest"
}
