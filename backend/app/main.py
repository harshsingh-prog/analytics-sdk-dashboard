import logging, os, time, uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import pathlib
from pydantic import TypeAdapter, ValidationError
from sqlalchemy import func, select, text, distinct
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import Session
from .db import Event, Order, Outbox, get_db, now
from .schemas import EventIn, OrderIn
from .security import principal

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
log = logging.getLogger("analytics.api")
MAX_BATCH, MAX_RANGE = 100, timedelta(days=92)
RATE_LIMIT = int(os.environ.get("RATE_LIMIT_PER_MIN", "600"))
MENU = {"burger-001": ("Classic Burger", 24900), "pizza-001": ("Margherita Pizza", 32900), "pasta-001": ("Alfredo Pasta", 28900), "dessert-001": ("Tiramisu", 17900)}  # paise

app = FastAPI(title="Analytics Platform", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=os.environ.get("CORS_ORIGINS", "http://localhost:5173,http://localhost:5174").split(","),
                   allow_methods=["GET", "POST"], allow_headers=["X-API-Key", "Content-Type", "Idempotency-Key"])
_hits: dict = defaultdict(int)

@app.middleware("http")
async def request_id(request: Request, call_next):
    rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
    resp = await call_next(request); resp.headers["X-Request-ID"] = rid; return resp

def err(status, code, msg, **extra): return JSONResponse(status_code=status, content={"error": {"code": code, "message": msg, **extra}})

@app.exception_handler(HTTPException)
async def http_exc(_, e):
    d = e.detail if isinstance(e.detail, dict) else {"code": "error", "message": str(e.detail)}
    return JSONResponse(status_code=e.status_code, content={"error": d})

@app.exception_handler(OperationalError)
async def db_down(_, e):
    log.error("database unavailable: %s", type(e).__name__)
    return err(503, "db_unavailable", "storage temporarily unavailable", retryable=True)

@app.get("/health")
def health(db: Session = Depends(get_db)):
    try: db.execute(text("SELECT 1")); return {"status": "ok"}
    except Exception: return JSONResponse(status_code=503, content={"status": "degraded"})

def rate_limit(pid: str):
    k = (pid, int(time.time() // 60)); _hits[k] += 1
    if len(_hits) > 5000: _hits.clear()
    if _hits[k] > RATE_LIMIT: raise HTTPException(429, {"code": "rate_limited", "message": "slow down", "retryable": True})

# ---------------- Ingestion ----------------
_ev = TypeAdapter(EventIn)

@app.post("/api/v1/events")
async def ingest(request: Request, p=Depends(principal), db: Session = Depends(get_db)):
    project, kind = p
    if kind == "read": raise HTTPException(403, {"code": "forbidden", "message": "read key cannot ingest"})
    rate_limit(project.id)
    if int(request.headers.get("content-length") or 0) > 512_000: raise HTTPException(413, {"code": "too_large", "message": "payload too large"})
    body = await request.json()
    raw = body.get("events") if isinstance(body, dict) and "events" in body else [body]
    if not isinstance(raw, list) or not 1 <= len(raw) <= MAX_BATCH:
        raise HTTPException(413 if isinstance(raw, list) and raw else 422, {"code": "bad_batch", "message": f"1..{MAX_BATCH} events required"})
    rows, results = [], []
    for i, r in enumerate(raw):  # per-event outcomes; valid events are kept, none silently lost
        try:
            e = _ev.validate_python(r)
            if e.project_id != project.id: raise ValueError("project_id does not match API key")
            if e.source == "server" and kind != "server": raise ValueError("server source requires the server key")
            rows.append(dict(e.model_dump(exclude={"timestamp"}), occurred_at=e.timestamp, ingested_at=now()))
            results.append({"index": i, "event_id": e.event_id, "status": "pending"})
        except (ValidationError, ValueError) as ex:
            msg = "; ".join(f"{'.'.join(map(str, x['loc']))}: {x['msg']}" for x in ex.errors()) if isinstance(ex, ValidationError) else str(ex)
            results.append({"index": i, "event_id": r.get("event_id") if isinstance(r, dict) else None, "status": "rejected", "error": msg[:300]})
    inserted = set()
    if rows:
        stmt = insert(Event).values(rows).on_conflict_do_nothing(constraint="uq_event_project_eventid").returning(Event.event_id)
        inserted = set(db.scalars(stmt)); db.commit()  # DB errors propagate => 503, never fake success
    for r in results:
        if r["status"] == "pending": r["status"] = "accepted" if r["event_id"] in inserted else "duplicate"
    c = lambda s: sum(r["status"] == s for r in results)
    return JSONResponse(status_code=200, content={"persisted": True, "accepted": c("accepted"), "duplicates": c("duplicate"),
        "rejected": [r for r in results if r["status"] == "rejected"], "results": results})

# ---------------- Analytics ----------------
def read_ctx(start: datetime = Query(...), end: datetime = Query(...), p=Depends(principal)):
    project, kind = p
    if kind != "read": raise HTTPException(403, {"code": "forbidden", "message": "dashboard (read) key required"})
    if start.tzinfo is None or end.tzinfo is None: raise HTTPException(422, {"code": "bad_range", "message": "timezone required"})
    start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    if end <= start or end - start > MAX_RANGE: raise HTTPException(422, {"code": "bad_range", "message": "end must be > start; max 92 days"})
    return project.id, start, end

def scope(pid, start, end, col=Event.occurred_at):  # half-open [start, end)
    return (Event.project_id == pid, col >= start, col < end)

@app.get("/api/v1/analytics/overview")
def overview(ctx=Depends(read_ctx), db: Session = Depends(get_db)):
    pid, s, e = ctx; w = scope(pid, s, e)
    row = db.execute(select(func.count().label("events"), func.count().filter(Event.event_name == "page_view").label("pv"),
        func.count(distinct(Event.anonymous_id)).label("uv"), func.count(distinct(Event.session_id)).label("sessions")).where(*w)).one()
    orders = db.scalar(select(func.count()).select_from(Order).where(Order.project_id == pid, Order.created_at >= s, Order.created_at < e))
    revenue = db.scalar(select(func.coalesce(func.sum(Order.total_amount), 0)).where(Order.project_id == pid, Order.created_at >= s, Order.created_at < e))
    # unique visitor = distinct anonymous_id (user_id is an optional label, not the identity key)
    return {"page_views": row.pv, "unique_visitors": row.uv, "events": row.events, "sessions": row.sessions, "orders_created": orders, "revenue_minor": revenue, "currency": "INR"}

@app.get("/api/v1/analytics/pages")
def pages(limit: int = Query(10, ge=1, le=50), ctx=Depends(read_ctx), db: Session = Depends(get_db)):
    pid, s, e = ctx; path = Event.properties["page_path"].astext
    q = select(path.label("path"), func.count().label("views"), func.count(distinct(Event.anonymous_id)).label("visitors")) \
        .where(*scope(pid, s, e), Event.event_name == "page_view", path.is_not(None)).group_by(path).order_by(func.count().desc()).limit(limit)
    return {"pages": [dict(r._mapping) for r in db.execute(q)]}

@app.get("/api/v1/analytics/events")
def trend(interval: str = Query("day", pattern="^(hour|day)$"), ctx=Depends(read_ctx), db: Session = Depends(get_db)):
    pid, s, e = ctx; b = func.date_trunc(interval, Event.occurred_at, "UTC")
    q = select(b.label("bucket"), Event.event_name, func.count().label("count"), func.count(distinct(Event.anonymous_id)).label("visitors")) \
        .where(*scope(pid, s, e)).group_by(b, Event.event_name).order_by(b)
    return {"interval": interval, "series": [{"bucket": r.bucket.isoformat(), "event_name": r.event_name, "count": r.count, "visitors": r.visitors} for r in db.execute(q)]}

@app.get("/api/v1/analytics/event-breakdown")
def breakdown(ctx=Depends(read_ctx), db: Session = Depends(get_db)):
    pid, s, e = ctx
    q = select(Event.event_name, func.count().label("count")).where(*scope(pid, s, e)).group_by(Event.event_name).order_by(func.count().desc())
    return {"breakdown": [dict(r._mapping) for r in db.execute(q)]}

@app.get("/api/v1/analytics/recent-events")
def recent(limit: int = Query(25, ge=1, le=100), offset: int = Query(0, ge=0), ctx=Depends(read_ctx), db: Session = Depends(get_db)):
    pid, s, e = ctx; w = scope(pid, s, e)
    total = db.scalar(select(func.count()).select_from(Event).where(*w))
    rows = db.scalars(select(Event).where(*w).order_by(Event.occurred_at.desc(), Event.id.desc()).limit(limit).offset(offset))
    safe = lambda pr: {k: v for k, v in pr.items() if isinstance(v, (str, int, float, bool)) and not str(k).startswith("_")}
    return {"total": total, "limit": limit, "offset": offset, "events": [{"event_id": x.event_id, "event_name": x.event_name, "source": x.source,
            "occurred_at": x.occurred_at.isoformat(), "anonymous_id": x.anonymous_id[:8] + "…", "properties": safe(x.properties)} for x in rows]}

# ---------------- Orders (authoritative) ----------------
@app.post("/api/v1/orders", status_code=201)
def create_order(body: OrderIn, p=Depends(principal), idempotency_key: str = Header(..., min_length=8, max_length=100), db: Session = Depends(get_db)):
    project, kind = p
    if kind == "read": raise HTTPException(403, {"code": "forbidden", "message": "read key cannot create orders"})
    existing = db.scalar(select(Order).where(Order.project_id == project.id, Order.idempotency_key == idempotency_key))
    fmt = lambda o: {"order_id": o.id, "status": o.status, "total": o.total_amount, "currency": o.currency, "item_count": o.item_count, "items": o.items}
    if existing: return JSONResponse(status_code=200, content={**fmt(existing), "idempotent_replay": True})
    lines = []
    for it in body.items:
        if it.item_id not in MENU: raise HTTPException(422, {"code": "invalid_item", "message": f"unknown item {it.item_id}"})
        name, price = MENU[it.item_id]; lines.append({"item_id": it.item_id, "name": name, "qty": it.qty, "unit_price": price})
    total = sum(l["qty"] * l["unit_price"] for l in lines)  # never trust client totals
    order = Order(project_id=project.id, idempotency_key=idempotency_key, total_amount=total, item_count=sum(l["qty"] for l in lines),
                  items=lines, anonymous_id=body.anonymous_id, session_id=body.session_id)
    db.add(order); db.flush()
    db.add(Outbox(project_id=project.id, payload={"event_id": f"order-{order.id}", "event_name": "order_created", "project_id": project.id,
        "source": "server", "anonymous_id": body.anonymous_id or f"order:{order.id}", "session_id": body.session_id, "schema_version": 1,
        "timestamp": now().isoformat(), "properties": {"order_id": order.id, "total": total, "currency": "INR", "item_count": order.item_count}}))
    try: db.commit()  # order + outbox row commit atomically; analytics availability is irrelevant here
    except IntegrityError:  # concurrent duplicate submit
        db.rollback(); order = db.scalar(select(Order).where(Order.project_id == project.id, Order.idempotency_key == idempotency_key))
        return JSONResponse(status_code=200, content={**fmt(order), "idempotent_replay": True})
    return fmt(order)

@app.get("/api/v1/analytics/recent-orders")
def recent_orders(limit: int = Query(10, ge=1, le=50), ctx=Depends(read_ctx), db: Session = Depends(get_db)):
    pid, s, e = ctx
    rows = db.scalars(select(Order).where(Order.project_id == pid, Order.created_at >= s, Order.created_at < e).order_by(Order.created_at.desc()).limit(limit))
    return {"orders": [{"order_id": o.id, "total": o.total_amount, "currency": o.currency, "item_count": o.item_count, "status": o.status, "created_at": o.created_at.isoformat()} for o in rows]}

# ---------------- Frontend (same origin => no CORS needed) ----------------
_fe = pathlib.Path(__file__).resolve().parents[2] / "frontend"
if _fe.exists(): app.mount("/", StaticFiles(directory=_fe, html=True), name="frontend")
