// Browser tracking SDK: buffered, batched, retry-safe (event ids fixed at track time).
(function () {
  const C = window.ANALYTICS_CONFIG, uid = () => crypto.randomUUID().replace(/-/g, "");
  const store = (s, k) => { try { return s[k] || (s[k] = uid()); } catch { return uid(); } };
  const anon = store(localStorage, "an_anon"), sess = store(sessionStorage, "an_sess");
  let q = [], timer;
  async function flush() {
    if (!q.length) return;
    const batch = q.splice(0, 100);
    try {
      const r = await fetch(C.endpoint + "/events", { method: "POST", keepalive: true,
        headers: { "Content-Type": "application/json", "X-API-Key": C.writeKey }, body: JSON.stringify({ events: batch }) });
      if (r.status === 429 || r.status >= 500) q = batch.concat(q); // transient: retry later (idempotent)
    } catch { q = batch.concat(q); }
    q = q.slice(-500);
  }
  window.Analytics = {
    anonymousId: anon, sessionId: sess,
    track(name, properties = {}) {
      q.push({ event_id: uid(), event_name: name, project_id: C.projectId, source: "browser", anonymous_id: anon, session_id: sess,
        timestamp: new Date().toISOString(), schema_version: 1, properties: { page_path: location.pathname, ...properties } });
      clearTimeout(timer); timer = setTimeout(flush, 1000);
    }, flush,
  };
  addEventListener("visibilitychange", () => document.visibilityState === "hidden" && flush());
  setInterval(flush, 5000);
})();
