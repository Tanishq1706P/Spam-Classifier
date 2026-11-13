# API reference
Base path `/api/v1` (also available as `/api`). Interactive OpenAPI docs: `http://master:8000/docs`.

| Endpoint | Purpose | Response |
|---|---|---|
| `POST /train {dataset?, algo, tune}` | Start a training job (one at a time) | `{run_id, phase}` |
| `GET /train/result` | Job state / result / error | `{running, run_id, result}` |
| `POST /predict {text}` | Classify one email, store in HBase | `{label, prob, run_id}` |
| `POST /send {text}` | Push an email onto Kafka `emails-in` | `{id, queued}` |
| `GET /status` | Phase, last error, node health | `{phase, status, error_code, cause, hint, nodes}` |
| `GET /metrics?run_id=` | Accuracy, precision, recall, F1, AUC, FPR, confusion | object |
| `GET /predictions?limit=` | Recent predictions (HBase scan) | `[{id,label,prob,run_id,text}]` |
| `GET /logs?limit=&code=` | Phase log, filterable by error code | list |
| `GET /errors` | Error catalogue | object |

Errors always use: `{"error_code","phase","message","cause","hint"}`.
