import asyncio, uuid
from datetime import datetime, timedelta, timezone
import httpx, pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker

TEST_URL = "postgresql+psycopg://analytics:analytics@localhost:5432/analytics_test"
import os; os.environ["DATABASE_URL"] = TEST_URL  # dedicated DB; never the dev one
from app import db as dbm
from app.main import app
from app.security import create_project
from app.tracking.client import AnalyticsClient, DeliveryError
from app.dispatcher import dispatch_once

@pytest.fixture(autouse=True)
def fresh():
    dbm.Base.metadata.drop_all(dbm.engine); dbm.Base.metadata.create_all(dbm.engine)
    with dbm.SessionLocal() as s:
        global K, K2; K = create_project(s, "restaurant-demo", "R"); K2 = create_project(s, "other", "O")
    yield

c = TestClient(app)
H = lambda k: {"X-API-Key": k}
T0 = datetime.now(timezone.utc).replace(microsecond=0)
def ev(name="page_view", anon="anon-1", path="/", at=None, source="browser", **kw):
    return {"event_id": uuid.uuid4().hex, "event_name": name, "project_id": "restaurant-demo", "source": source, "anonymous_id": anon,
            "session_id": kw.get("session", "s1"), "timestamp": (at or T0).isoformat(), "properties": {"page_path": path}, "schema_version": 1}
def post(evs, key=None): return c.post("/api/v1/events", json={"events": evs}, headers=H(key or K["write"]))
def q(path, key=None, days=1, **p):
    return c.get(path, params={"start": (T0 - timedelta(days=days)).isoformat(), "end": (T0 + timedelta(hours=1)).isoformat(), **p}, headers=H(key or K["read"]))

def test_health(): assert c.get("/health").json() == {"status": "ok"}

def test_ingest_batch_dedupe_and_partial():
    e = ev(); bad = {**ev(), "event_name": "1 bad name"}
    r = post([e, bad]).json(); assert (r["accepted"], len(r["rejected"]), r["persisted"]) == (1, 1, True)
    r = post([e]).json(); assert (r["accepted"], r["duplicates"]) == (0, 1)
    assert q("/api/v1/analytics/overview").json()["events"] == 1  # retry did not inflate

def test_auth_and_isolation():
    assert c.post("/api/v1/events", json={"events": [ev()]}).status_code == 401
    assert post([ev()], key="nope").status_code == 401
    assert post([ev(source="server")]).json()["rejected"]            # browser key can't claim server source
    assert post([ev()], key=K["server"]).json()["accepted"] == 1
    r = post([ev()], key=K2["write"]).json(); assert r["rejected"] and r["accepted"] == 0   # project mismatch
    assert q("/api/v1/analytics/overview", key=K["write"]).status_code == 403              # write key can't read
    assert q("/api/v1/analytics/overview", key=K2["read"]).json()["events"] == 0           # other project sees nothing

def test_limits_and_validation():
    assert post([ev() for _ in range(101)]).status_code == 413
    assert post([{**ev(), "timestamp": "2026-01-01T00:00:00"}]).json()["rejected"]           # naive timestamp
    assert post([{**ev(), "properties": {"user_id": "x"}}]).json()["rejected"]               # reserved field

def test_analytics_aggregates_and_range():
    post([ev(anon="a", path="/"), ev(anon="a", path="/"), ev(anon="b", path="/menu"), ev("add_to_cart", anon="b"),
          ev(anon="c", at=T0 - timedelta(days=5))])
    o = q("/api/v1/analytics/overview").json()
    assert (o["page_views"], o["unique_visitors"], o["events"]) == (3, 2, 4)                  # old event excluded
    p = q("/api/v1/analytics/pages").json()["pages"]; assert p[0] == {"path": "/", "views": 2, "visitors": 1}
    assert q("/api/v1/analytics/events").json()["series"]
    assert {b["event_name"]: b["count"] for b in q("/api/v1/analytics/event-breakdown").json()["breakdown"]}["page_view"] == 3
    r = q("/api/v1/analytics/recent-events", limit=2, offset=1).json(); assert r["total"] == 4 and len(r["events"]) == 2
    assert q("/api/v1/analytics/overview", days=10).json()["events"] == 5
    assert q("/api/v1/analytics/overview", days=400).status_code == 422
    assert c.get("/api/v1/analytics/overview", params={"start": T0.isoformat(), "end": (T0 - timedelta(1)).isoformat()}, headers=H(K["read"])).status_code == 422

def test_empty_dataset():
    o = q("/api/v1/analytics/overview").json(); assert o["events"] == 0 and o["unique_visitors"] == 0

ORDER = {"items": [{"item_id": "burger-001", "qty": 2}], "anonymous_id": "anon-1", "session_id": "s1", "customer_name": "Asha"}
def order(body=ORDER, key="idem-key-0001", k=None): return c.post("/api/v1/orders", json=body, headers={**H(k or K["write"]), "Idempotency-Key": key})

def test_order_server_side_pricing_and_idempotency():
    r = order({**ORDER, "total": 1}); assert r.status_code == 201 and r.json()["total"] == 2 * 24900   # client total ignored
    r2 = order(); assert r2.status_code == 200 and r2.json()["order_id"] == r.json()["order_id"]
    with dbm.SessionLocal() as s: assert s.scalar(select(func.count()).select_from(dbm.Order)) == 1
    assert order({**ORDER, "items": [{"item_id": "fake", "qty": 1}]}, key="idem-key-0002").status_code == 422

def test_order_commits_without_analytics_then_outbox_delivers_via_ingestion():
    assert order().status_code == 201                         # analytics endpoint never contacted
    with dbm.SessionLocal() as s: assert s.scalar(select(func.count()).select_from(dbm.Outbox).where(dbm.Outbox.delivered_at.is_(None))) == 1
    down = AnalyticsClient("http://x/api/v1/events", K["server"], "restaurant-demo", retries=1,
                           transport=httpx.MockTransport(lambda r: httpx.Response(503)))
    asyncio.run(dispatch_once(down))
    with dbm.SessionLocal() as s:
        ob = s.scalar(select(dbm.Outbox)); assert ob.attempts == 1 and ob.delivered_at is None  # retained, retry scheduled
        ob.next_retry_at = datetime.now(timezone.utc); s.commit()
    class ASGI(httpx.AsyncBaseTransport):  # deliver into the real app/DB
        async def handle_async_request(self, req):
            r = c.post("/api/v1/events", content=req.content, headers=dict(req.headers)); return httpx.Response(r.status_code, json=r.json())
    up = AnalyticsClient("http://x/api/v1/events", K["server"], "restaurant-demo", transport=ASGI())
    assert asyncio.run(dispatch_once(up)) == {"sent": 1, "failed": 0}
    asyncio.run(dispatch_once(up))  # nothing pending => no duplicate
    o = q("/api/v1/analytics/overview").json(); assert o["orders_created"] == 1 and o["events"] == 1
    assert q("/api/v1/analytics/recent-events").json()["events"][0]["event_name"] == "order_created"

def test_client_permanent_failure_not_retried_and_kept():
    calls = []
    cl = AnalyticsClient("http://x", "k", "p", retries=3, transport=httpx.MockTransport(lambda r: (calls.append(1), httpx.Response(401))[1]))
    cl.track("x", anonymous_id="a")
    with pytest.raises(DeliveryError): asyncio.run(cl.flush())
    assert len(calls) == 1 and len(cl.queue) == 1
