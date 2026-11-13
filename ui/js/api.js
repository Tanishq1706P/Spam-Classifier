// All server access goes through here. Client-side failures use SPM-UI-Axx codes (see docs/errors.md).
const API = "/api/v1";
async function call(path, opts = {}) {
  let res;
  try {
    res = await fetch(API + path, {
      method: opts.method || "GET",
      headers: opts.body ? { "Content-Type": "application/json" } : {},
      body: opts.body ? JSON.stringify(opts.body) : undefined,
    });
  } catch (e) {
    throw { error_code: "SPM-UI-A01", message: "API unreachable", cause: String(e), hint: "Is uvicorn running on the master (port 8000)?", phase: "" };
  }
  let data;
  try { data = await res.json(); }
  catch (e) { throw { error_code: "SPM-UI-A02", message: "Malformed response", cause: `HTTP ${res.status}`, hint: "Check the API log in logs/spam.log", phase: "" }; }
  if (!res.ok) throw data;
  return data;
}
