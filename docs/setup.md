# Cluster setup (1 master + 2 workers, Ubuntu 22.04)

## 0. Every PC
1. Static IPs; same LAN. Edit `config/cluster.yaml` and `/etc/hosts` on **all** nodes:
   ```
   192.168.1.10 master
   192.168.1.11 worker1
   192.168.1.12 worker2
   ```
2. Same Linux user on all nodes. On the master: `ssh-keygen -t ed25519 && ssh-copy-id master && ssh-copy-id worker1 && ssh-copy-id worker2`.
3. `git clone <your-repo> ~/spam-classifier` on **every** node (scripts expect the same path).
4. Open firewall between nodes (or `sudo ufw disable` on an isolated lab LAN).

## 1. Install (order matters)
| Where | Command |
|---|---|
| all nodes | `./cluster/setup_hadoop.sh master` (master) / `./cluster/setup_hadoop.sh worker` (workers) |
| all nodes | `./cluster/setup_hbase.sh` |
| master | `./cluster/setup_spark.sh` then `./cluster/setup_kafka.sh` |

Re-open the shell (`source ~/.bashrc`).

## 2. Start and verify
```
./cluster/start_all.sh          # starts everything, creates /data/raw, /models, topics, HBase tables, runs health check
./cluster/health_check.sh       # exit 0 = all daemons present on every node
hdfs dfs -put data/sample/sms.csv /data/raw/
```
Web UIs: NameNode `master:9870`, YARN `master:8088`, HBase `master:16010`.

## 3. Run the system (master)
```
spark-submit --master yarn src/ml/trainer.py --algo logreg                     # P1-P6 batch training
spark-submit --master yarn --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
             src/streaming/stream_predictor.py                                 # P7 real-time scoring
uvicorn src.api.main:app --host 0.0.0.0 --port 8000                            # API + UI at http://master:8000
python -m src.ingestion.kafka_producer --rate 5 --limit 100                    # replay emails into Kafka
```
Single-PC development: `SPAM_SPARK_MASTER='local[*]' SPAM_ALLOW_MEMORY_STORE=1 uvicorn src.api.main:app`.

## Troubleshooting
Read the error code in the UI banner or `logs/spam.log`; see `docs/errors.md`.
Common: DataNode missing -> clusterID mismatch after re-format (delete `~/bigdata/data/dn` on workers); Spark on YARN containers killed -> lower `spark.executor.memory`.
