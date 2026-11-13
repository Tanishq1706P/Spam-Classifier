#!/usr/bin/env bash
# Run on the master (Spark runs on YARN, so workers only need Hadoop).
set -euo pipefail; source "$(dirname "$0")/env.sh"
fetch "https://archive.apache.org/dist/spark/spark-$SPARK_VER/spark-$SPARK_VER-bin-hadoop3.tgz" "$INSTALL_DIR"
cat > "$SPARK_HOME/conf/spark-env.sh" <<X
export HADOOP_CONF_DIR=$HADOOP_CONF_DIR
export JAVA_HOME=$JAVA_HOME
X
cat > "$SPARK_HOME/conf/spark-defaults.conf" <<X
spark.master                     yarn
spark.submit.deployMode          client
spark.executor.memory            1g
spark.executor.instances         2
spark.driver.memory              1g
spark.yarn.am.memory             512m
X
pip3 install --user -r "$(dirname "$0")/../requirements.txt"
echo "Spark $SPARK_VER configured (YARN mode)"
