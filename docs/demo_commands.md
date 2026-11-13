# Basic usage commands, explained (use these in the demonstration)

## Hadoop (HDFS + YARN)
| Command | What it does / what to say |
|---|---|
| `start-dfs.sh` / `start-yarn.sh` | Starts NameNode+DataNodes / ResourceManager+NodeManagers across the nodes listed in `workers` (uses SSH). |
| `jps` | Lists running Java daemons on this node; master shows NameNode, ResourceManager; workers show DataNode, NodeManager. |
| `hdfs dfsadmin -report` | Cluster capacity and **Live datanodes: 2** proves both workers joined. |
| `hdfs dfs -mkdir -p /data/raw` | Creates a directory in the distributed file system. |
| `hdfs dfs -put data/sample/sms.csv /data/raw/` | Copies a local file into HDFS; blocks are replicated (factor 2). |
| `hdfs dfs -ls /data/raw` · `hdfs fsck /data/raw -files -blocks -locations` | Lists files · shows which DataNodes hold each block. |
| `yarn node -list` | Shows NodeManagers in RUNNING state (the compute capacity Spark can use). |

## Kafka (KRaft mode)
| Command | Meaning |
|---|---|
| `kafka-server-start.sh -daemon config/kraft/server.properties` | Starts the broker in the background. |
| `kafka-topics.sh --create --topic emails-in --partitions 3 --bootstrap-server master:9092` | Creates a topic; partitions allow parallel consumers. |
| `kafka-topics.sh --list --bootstrap-server master:9092` | Lists topics. |
| `kafka-console-producer.sh --topic emails-in --bootstrap-server master:9092` | Type messages (JSON `{"id":"1","text":"..."}`) to publish. |
| `kafka-console-consumer.sh --topic results --from-beginning --bootstrap-server master:9092` | Reads classified results published by Spark. |

## HBase
| Command (inside `hbase shell`) | Meaning |
|---|---|
| `status` | Shows live region servers (should be 2). |
| `create 'predictions','cf'` | Table with one column family. |
| `put 'predictions','row1','cf:label','spam'` · `get 'predictions','row1'` | Write / read one row by key. |
| `scan 'predictions',{LIMIT=>5}` | Browse the newest classified emails (row key = reverse timestamp). |
| `list` · `describe 'predictions'` | List tables · show schema. |

## Spark
| Command | Meaning |
|---|---|
| `spark-submit --master yarn src/ml/trainer.py --algo logreg` | Runs the training job; YARN launches executors on the workers (watch `master:8088`). |
| `spark-submit --master yarn --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 src/streaming/stream_predictor.py` | Starts the streaming classifier reading Kafka. |
| `pyspark --master yarn` | Interactive shell for demonstrating DataFrames on the cluster. |
| `SPAM_SPARK_MASTER='local[*]' python src/ml/trainer.py` | Same job on one PC (development). |

## End-to-end demo script (≈5 min)
1. `./cluster/health_check.sh` (all OK) → show `hdfs dfsadmin -report`, `yarn node -list`.
2. UI at `http://master:8000`: top bar shows master/worker1/worker2 green.
3. Train tab → Start training; phase bar P1…P6 advances; Metrics tab shows confusion matrix.
4. `python -m src.ingestion.kafka_producer --rate 5 --limit 50`; Live stream tab fills; `hbase shell` → `scan 'predictions',{LIMIT=>3}`.
5. Predict tab: paste a spam message → label + probability.
6. Break something on purpose: `kafka-server-stop.sh`, click "Send to Kafka stream" → banner shows `SPM-ING-204` with cause and fix.
