# Architecture
```
Browser UI ──HTTP──> FastAPI (master:8000) ──> Spark (YARN) ──> HDFS (data, models)
                          │                          │
                          └──> Kafka emails-in ──> Structured Streaming ──> HBase (predictions)
                                                          └──> Kafka results
```
| Node | Daemons |
|---|---|
| master | NameNode, ResourceManager, Spark driver, HMaster, ZooKeeper (HBase-managed), Kafka (KRaft), HBase Thrift, FastAPI |
| worker1 / worker2 | DataNode, NodeManager (Spark executors), HRegionServer |

Phases: P1 INGEST, P2 CLEAN, P3 FEATURES, P4 TRAIN, P5 EVALUATE, P6 SAVE_MODEL, P7 STREAM. Every phase writes START/OK/FAIL, duration and an error code to the console, `logs/spam.log` and HBase `phase_log`.

HBase tables: `predictions` (row key `reverse_ts#email_id`), `model_runs`, `phase_log` (`run_id#ts`), `datasets`.
Not implemented from the plan: ROC curve chart, `NODE_STATUS` table (node health is probed live over TCP instead).
