# Testing
```
pip install -r requirements.txt pytest-cov
python -m pytest -q --cov=src        # unit + API integration, local Spark, in-memory HBase
python tests/e2e/smoke.py http://master:8000   # live cluster acceptance check
```
Covered: cleaner edge cases (empty, HTML-only, non-English, long, missing label, duplicates), error codes, phase tracking,
metrics thresholds, model registry versions, API validation and error schema, Kafka-down error, failed-phase visibility.
Needs the real cluster (not covered by local tests): `stream_predictor.start_stream`, real HDFS/HBase/Kafka I/O, fault tests
(kill a worker mid-job; confirm `SPM-CLU-102` and recovery). Performance (1 vs 2 workers) must be measured on your cluster with the Spark UI.
