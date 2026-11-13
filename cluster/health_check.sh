#!/usr/bin/env bash
# Verifies every daemon on every node. Exit code 0 = healthy; otherwise prints SPM-CLU-1xx code.
source "$(dirname "$0")/env.sh"; rc=0
fail() { echo "FAIL $1 $2"; rc=1; }
chk() { # chk <host> <process-name> <code>
  ssh -o ConnectTimeout=5 "$1" "jps" 2>/dev/null | grep -qw "$2" && echo "OK   $1 $2" || fail "$3" "$1 missing $2"; }
chk $MASTER_HOST NameNode SPM-CLU-101
chk $MASTER_HOST ResourceManager SPM-CLU-103
chk $MASTER_HOST HMaster SPM-CLU-105
ssh $MASTER_HOST "jps" 2>/dev/null | grep -qw Kafka && echo "OK   $MASTER_HOST Kafka" || fail SPM-CLU-104 "Kafka broker down"
for w in $WORKER_HOSTS; do chk $w DataNode SPM-CLU-102; chk $w NodeManager SPM-CLU-103; chk $w HRegionServer SPM-CLU-105; done
echo "--- hdfs dfsadmin -report (summary)"; hdfs dfsadmin -report 2>/dev/null | grep -E "Live datanodes|Configured Capacity" || fail SPM-CLU-101 "HDFS unreachable"
echo "--- yarn node -list";               yarn node -list 2>/dev/null | grep -E "Total Nodes|RUNNING" || fail SPM-CLU-103 "YARN unreachable"
exit $rc
