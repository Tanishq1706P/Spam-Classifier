# Error codes

Format: `SPM-<MODULE>-<NNN>`. The code identifies the failing file and phase.

| Code | Meaning | Fix |
|---|---|---|
| `SPM-API-901` | Request validation failed | Check JSON body and text length |
| `SPM-API-902` | Job conflict | Wait for the running job to finish |
| `SPM-API-903` | Operation timed out | Retry; check cluster load |
| `SPM-CFG-001` | Config file missing | Check config/settings.yaml or SPAM_CONFIG |
| `SPM-CFG-002` | Invalid config key | Compare key with config/settings.yaml |
| `SPM-CLU-101` | HDFS NameNode down | Run start-dfs.sh on master; check jps |
| `SPM-CLU-102` | DataNode missing | Check worker logs; verify workers file |
| `SPM-CLU-103` | YARN unreachable | Run start-yarn.sh; check ResourceManager on master:8088 |
| `SPM-CLU-104` | Kafka broker down | Start kafka-server-start.sh with KRaft config |
| `SPM-CLU-105` | HBase master down | Run start-hbase.sh; check ZooKeeper |
| `SPM-EVL-601` | Metric below threshold | Tune hyperparameters or add data |
| `SPM-EVL-602` | Test set empty | Check train/test split ratio |
| `SPM-FEA-401` | Tokenizer failed | Check text column type |
| `SPM-FEA-402` | Empty vocabulary | Dataset may contain only stop words |
| `SPM-ING-201` | Input file not found | Check path and permissions |
| `SPM-ING-202` | Bad dataset schema | CSV needs columns: label,text |
| `SPM-ING-203` | HDFS write failed | Check hdfs dfsadmin -report and free space |
| `SPM-ING-204` | Kafka send failed | Verify broker address and topic |
| `SPM-MOD-701` | Model save failed | Check HDFS /models permissions |
| `SPM-MOD-702` | Model not found | Train a model first (POST /api/v1/train) |
| `SPM-PRE-301` | Dataset empty after cleaning | Inspect raw data for empty/null text |
| `SPM-PRE-302` | Encoding error | Re-save file as UTF-8 |
| `SPM-STR-801` | Kafka offset error | Reset checkpoint dir or offsets |
| `SPM-STR-802` | Model load failed | Verify model path in registry |
| `SPM-STR-803` | HBase write failed | Check HBase Thrift server on port 9090 |
| `SPM-TRN-501` | Executor out of memory | Increase spark.executor.memory or lower numFeatures |
| `SPM-TRN-502` | Severe class imbalance | Enable balance_classes or add data |
| `SPM-TRN-503` | Training did not converge | Increase maxIter or regParam |
| `SPM-UI-A01` | API unreachable (browser could not reach the API) | Start uvicorn on the master, check port 8000 |
| `SPM-UI-A02` | Malformed response from the API | Check `logs/spam.log` |
