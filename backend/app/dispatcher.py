"""Outbox dispatcher. Run: python -m app.dispatcher  (loops) — safe to run multiple copies (SKIP LOCKED)."""
import asyncio, json, logging, os, pathlib
from datetime import timedelta
from sqlalchemy import select
from .db import Outbox, SessionLocal, now
from .tracking.client import AnalyticsClient, DeliveryError

log = logging.getLogger("analytics.outbox")

async def dispatch_once(client: AnalyticsClient, batch=50) -> dict:
    sent = failed = 0
    with SessionLocal() as db:
        rows = db.scalars(select(Outbox).where(Outbox.delivered_at.is_(None), Outbox.next_retry_at <= now())
                          .order_by(Outbox.id).limit(batch).with_for_update(skip_locked=True)).all()
        for row in rows:
            try:
                await client.send([row.payload]); row.delivered_at = now(); sent += 1
            except DeliveryError as e:
                row.attempts += 1; row.last_error = str(e)[:300]; failed += 1
                row.next_retry_at = now() + timedelta(seconds=min(3600, 5 * 2 ** row.attempts))  # backoff; stays pending, never dropped
                log.error("outbox %s delivery failed (attempt %d): %s", row.id, row.attempts, e)
        db.commit()
    return {"sent": sent, "failed": failed}

async def main():
    kf = pathlib.Path(__file__).resolve().parents[1] / ".keys.json"
    key = os.environ.get("SERVER_ANALYTICS_WRITE_KEY") or json.loads(kf.read_text())["server"]
    client = AnalyticsClient(os.environ.get("ANALYTICS_INGESTION_URL", "http://localhost:8000/api/v1/events"), key, os.environ.get("PROJECT_ID", "restaurant-demo"))
    try:
        while True: await dispatch_once(client); await asyncio.sleep(2)
    finally: await client.aclose()

if __name__ == "__main__": asyncio.run(main())
