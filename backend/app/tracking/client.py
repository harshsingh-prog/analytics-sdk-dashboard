"""Async server-side tracking client. track() is buffered; flush() retries transient failures
(network, 429, 5xx) with backoff+jitter. Event ids are fixed at track() time so retries are idempotent."""
import asyncio, logging, random, uuid
from datetime import datetime, timezone
import httpx

log = logging.getLogger("analytics.client")

class DeliveryError(Exception): pass

class AnalyticsClient:
    def __init__(self, endpoint, api_key, project_id, *, timeout=5.0, retries=3, transport=None, max_queue=1000):
        self.endpoint, self.project_id, self.retries, self.max_queue = endpoint, project_id, retries, max_queue
        self._http = httpx.AsyncClient(timeout=timeout, headers={"X-API-Key": api_key}, transport=transport)  # key never logged
        self.queue: list[dict] = []

    def track(self, name, properties=None, *, anonymous_id, user_id=None, session_id=None, event_id=None, timestamp=None):
        if len(self.queue) >= self.max_queue:
            log.warning("queue full; dropping oldest event"); self.queue.pop(0)
        ev = {"event_id": event_id or uuid.uuid4().hex, "event_name": name, "project_id": self.project_id, "source": "server",
              "anonymous_id": anonymous_id, "user_id": user_id, "session_id": session_id, "schema_version": 1,
              "timestamp": (timestamp or datetime.now(timezone.utc)).isoformat(), "properties": properties or {}}
        self.queue.append(ev); return ev["event_id"]

    async def send(self, events: list[dict]) -> dict:
        """Deliver a batch. Raises DeliveryError (retryable flag) instead of swallowing failures."""
        last = None
        for attempt in range(self.retries + 1):
            try:
                r = await self._http.post(self.endpoint, json={"events": events})
                if r.status_code < 300:
                    body = r.json()
                    if body.get("rejected"): raise DeliveryError(f"permanent: events rejected {body['rejected']}")
                    return body
                if r.status_code not in (429, 500, 502, 503, 504): raise DeliveryError(f"permanent: HTTP {r.status_code}")
                last = f"HTTP {r.status_code}"
            except httpx.TransportError as e: last = type(e).__name__
            log.warning("transient delivery failure (%s), attempt %d", last, attempt + 1)
            await asyncio.sleep(min(10, 0.2 * 2 ** attempt) * random.uniform(0.5, 1.5) if attempt < self.retries else 0)
        raise DeliveryError(f"transient: gave up after {self.retries + 1} attempts ({last})")

    async def flush(self):
        batch, self.queue = self.queue[:100], self.queue[100:]
        if not batch: return None
        try: return await self.send(batch)
        except DeliveryError: self.queue = batch + self.queue; raise  # keep for next flush

    async def aclose(self): await self._http.aclose()
