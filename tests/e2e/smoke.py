"""Live end-to-end check against the RUNNING cluster + API (run on the master after start_all.sh):
    python tests/e2e/smoke.py http://master:8000
Verifies acceptance criteria 1, 3, 4, 5 from the plan. Exit code 0 = pass."""
import json, sys, time, urllib.request

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000").rstrip("/") + "/api/v1"


def call(path, body=None):
    req = urllib.request.Request(BASE + path, data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json"})
    try:
        return json.load(urllib.request.urlopen(req, timeout=60))
    except urllib.error.HTTPError as e:
        return json.load(e)


def check(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name, detail); return ok


res = []
nodes = call("/status")["nodes"]
res.append(check("3 nodes, all services up", len(nodes) >= 3 and all(n["up"] for n in nodes), [n["node"] for n in nodes if not n["up"]]))
call("/train", {"algo": "logreg"})
for _ in range(120):
    time.sleep(5)
    j = call("/train/result")
    if not j["running"]:
        break
res.append(check("model trained via YARN", bool(j.get("result")) and "error" not in j["result"], j.get("result", {}).get("metrics", {}).get("accuracy")))
sent = call("/send", {"text": "WINNER! Claim your free prize now, call 09061701461"})
found = None
t0 = time.time()
while time.time() - t0 < 30 and not found:
    time.sleep(1)
    found = next((p for p in call("/predictions?limit=20") if p["id"] == sent.get("id")), None)
res.append(check("Kafka -> Spark -> HBase round trip < 30 s", bool(found), f"{round(time.time()-t0,1)}s label={found and found['label']}"))
bad = call("/train", {"dataset": "/does/not/exist.csv"}); time.sleep(10)
st = call("/status")
res.append(check("error code with cause shown in /status", st.get("error_code", "").startswith("SPM-") and bool(st.get("cause")), st.get("error_code")))
sys.exit(0 if all(res) else 1)
