const $ = s => document.querySelector(s);
const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const PHASE_LABELS = { INGEST: "Ingest", CLEAN: "Clean", FEATURES: "Features", TRAIN: "Train", EVALUATE: "Evaluate", SAVE_MODEL: "Save model", STREAM: "Stream" };
let tab = "dashboard", phases = Object.keys(PHASE_LABELS);

// ---- tabs ----
document.querySelectorAll(".tabs button").forEach(b => b.onclick = () => {
  tab = b.dataset.tab;
  document.querySelectorAll(".tabs button").forEach(x => x.setAttribute("aria-selected", x === b));
  document.querySelectorAll(".panel").forEach(p => p.hidden = p.id !== "tab-" + tab);
  refreshTab();
});

// ---- phase banner / stepper / nodes ----
function showError(e) {
  const b = $("#banner"); b.className = "banner fail";
  b.innerHTML = `<span class="code">${esc(e.error_code)}</span> ${esc(e.message)}
    <dl><dt>Phase</dt><dd>${esc(e.phase || "–")}</dd><dt>Cause</dt><dd>${esc(e.cause)}</dd><dt>Fix</dt><dd>${esc(e.hint)}</dd></dl>`;
}

function renderStatus(s) {
  phases = s.phases || phases;
  const idx = phases.indexOf(s.phase);
  $("#stepper").innerHTML = phases.map((p, i) => {
    let cls = "";
    if (s.phase && i < idx) cls = "done";
    if (i === idx) cls = s.status === "FAIL" ? "fail" : s.status === "OK" ? "done" : "active";
    return `<li class="${cls}"><b>P${i + 1}</b>${PHASE_LABELS[p] || p}</li>`;
  }).join("");
  const b = $("#banner");
  if (s.status === "FAIL") showError({ ...s, phase: s.phase });
  else if (s.status === "START") { b.className = "banner run"; b.textContent = `Running: ${PHASE_LABELS[s.phase] || s.phase}…`; }
  else if (s.status === "OK") { b.className = "banner ok"; b.textContent = `${PHASE_LABELS[s.phase] || s.phase} finished.`; }
  else { b.className = "banner idle"; b.textContent = "Idle. Start a training run to begin."; }
  $("#nodes").innerHTML = s.nodes.map(n => {
    const cls = n.up ? "up" : n.partial ? "partial" : "down";
    const down = Object.entries(n.services).filter(([, v]) => !v).map(([k]) => k).join(", ");
    return `<li class="node ${cls}" title="${esc(down ? "Not reachable: " + down : "All services reachable")}">${esc(n.node)}</li>`;
  }).join("");
}

async function pollStatus() {
  try { renderStatus(await call("/status")); }
  catch (e) { showError(e); }
}

// ---- dashboard / stream ----
async function loadPredictions(limit) { return call("/predictions?limit=" + limit); }

async function refreshDashboard() {
  const rows = await loadPredictions(200), spam = rows.filter(r => r.label === "spam").length;
  $("#s-total").textContent = rows.length; $("#s-spam").textContent = spam;
  $("#s-ham").textContent = rows.length - spam;
  $("#s-ratio").textContent = rows.length ? Math.round(100 * spam / rows.length) + "%" : "–";
  $("#dash-empty").hidden = rows.length > 0;
  drawHistogram($("#c-dist"), rows.map(r => r.prob));
  try { const m = await call("/metrics"); if (m.version) $("#model").textContent = `Model v${m.version} · ${m.algo} · accuracy ${(m.accuracy * 100).toFixed(1)}%`; } catch (_) {}
}

async function refreshStream() {
  const rows = await loadPredictions(50);
  $("#stream-empty").hidden = rows.length > 0;
  $("#s-table tbody").innerHTML = rows.map(r =>
    `<tr><td class="${esc(r.label)}">${esc(r.label)}</td><td>${(r.prob * 100).toFixed(1)}%</td><td class="clip" title="${esc(r.text)}">${esc(r.text)}</td><td>${esc(r.run_id)}</td></tr>`).join("");
}

async function refreshMetrics() {
  const m = await call("/metrics");
  const has = m && m.accuracy !== undefined;
  $("#m-empty").hidden = has; $("#m-body").hidden = !has;
  if (!has) return;
  const pct = v => (v * 100).toFixed(1) + "%";
  $("#m-stats").innerHTML = [["Accuracy", pct(m.accuracy)], ["Precision", pct(m.precision)], ["Recall", pct(m.recall)],
    ["F1 (spam)", m.f1.toFixed(3)], ["AUC", m.auc.toFixed(3)], ["False-positive rate", pct(m.fpr)]]
    .map(([k, v]) => `<div><span>${v}</span><label>${k}</label></div>`).join("");
  drawConfusion($("#c-cm"), m.confusion);
}

async function refreshLogs() {
  const code = $("#l-code").value.trim();
  const rows = await call("/logs?limit=200" + (code ? "&code=" + encodeURIComponent(code) : ""));
  $("#l-table tbody").innerHTML = rows.reverse().map(r =>
    `<tr><td>${esc(r.ts)}</td><td>${esc(r.phase)}</td><td>${esc(r.status)}</td><td class="mono">${esc(r.error_code || "")}</td><td>${esc(r.message)}</td><td>${esc(r.cause)}</td></tr>`).join("");
}
$("#l-refresh").onclick = () => refreshLogs().catch(showError);

function refreshTab() {
  const f = { dashboard: refreshDashboard, stream: refreshStream, metrics: refreshMetrics, logs: refreshLogs }[tab];
  if (f) f().catch(showError);
}

// ---- actions ----
$("#t-go").onclick = async () => {
  const btn = $("#t-go"); btn.disabled = true;
  try {
    const r = await call("/train", { method: "POST", body: { dataset: $("#t-data").value.trim() || null, algo: $("#t-algo").value, tune: $("#t-tune").checked } });
    const out = $("#t-out"); out.hidden = false; out.textContent = `Started ${r.run_id}. Watch the phase bar above.`;
    const poll = setInterval(async () => {
      const j = await call("/train/result").catch(() => null);
      if (j && !j.running) { clearInterval(poll); btn.disabled = false; out.textContent = JSON.stringify(j.result, null, 2); }
    }, 2000);
  } catch (e) { showError(e); btn.disabled = false; }
};

$("#p-go").onclick = async () => {
  const text = $("#p-text").value.trim(), box = $("#p-result"), btn = $("#p-go");
  if (!text) { box.hidden = false; box.className = "result"; box.textContent = "Paste an email first."; return; }
  btn.disabled = true;
  try {
    const r = await call("/predict", { method: "POST", body: { text } });
    box.hidden = false; box.className = "result " + r.label;
    box.innerHTML = `<strong>${r.label === "spam" ? "Spam" : "Not spam"}</strong> · ${(r.prob * 100).toFixed(1)}% spam probability · model ${esc(r.run_id)}<div class="bar"><i style="width:${r.prob * 100}%"></i></div>`;
  } catch (e) { showError(e); } finally { btn.disabled = false; }
};

$("#p-send").onclick = async () => {
  const text = $("#p-text").value.trim(); if (!text) return;
  try { const r = await call("/send", { method: "POST", body: { text } }); $("#p-result").hidden = false; $("#p-result").className = "result"; $("#p-result").textContent = `Queued as ${r.id}. It will appear under Live stream.`; }
  catch (e) { showError(e); }
};

pollStatus(); refreshTab();
setInterval(pollStatus, 2000);
setInterval(() => { if (tab === "stream" || tab === "dashboard") refreshTab(); }, 2000);
