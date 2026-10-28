# Analytics Platform — FastAPI + PostgreSQL + vanilla-JS frontend

**Quick start:** `pip install -r backend/requirements.txt`, set `DATABASE_URL`, then `./run.sh` (seeds, starts dispatcher + API).
Storefront: http://localhost:8000/ · Dashboard: http://localhost:8000/dashboard.html (paste the `rk_` read key printed by seed).
Frontend is served by FastAPI (same origin, no CORS). Tests: `cd backend && pytest`.

The earlier Node/SQLite prototype in `../analytics` is a separate, simpler implementation and does not follow this stack.

## Run (native)
```
cd backend && python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
export DATABASE_URL=postgresql+psycopg://analytics:analytics@localhost:5432/analytics
python -m app.seed                 # creates tables (dev only) + demo project; prints 3 keys once
uvicorn app.main:app --reload      # docs: http://localhost:8000/docs
ANALYTICS_INGESTION_URL=http://localhost:8000/api/v1/events SERVER_ANALYTICS_WRITE_KEY=<server key> python -m app.dispatcher
pytest                             # needs a Postgres DB `analytics_test` (user analytics/analytics); drops/recreates its tables
```

## Design
- **Keys (hashed SHA-256 in `projects`)**: `write` (public, browser; cannot claim `source=server`), `server` (private), `read` (dashboard). Each is bound to one project; payload `project_id` must match.
- **Ingestion** `POST /api/v1/events`: single event or `{events:[…≤100]}`. Per-event outcomes (`accepted|duplicate|rejected`); valid events persist even if others are rejected. Idempotent via `UNIQUE(project_id,event_id)` + `ON CONFLICT DO NOTHING`. DB failure → 503, never fake success.
- **Unique visitor** = distinct `anonymous_id` in range. Ranges are UTC, `[start, end)`, max 92 days.
- **Orders** `POST /api/v1/orders` (+`Idempotency-Key`): server-side pricing from menu, order + outbox row in one transaction. `python -m app.dispatcher` delivers outbox events via `AnalyticsClient` with retry state (attempts, next_retry_at, delivered_at); `SKIP LOCKED` allows multiple workers.
- Analytics: `/api/v1/analytics/{overview,pages,events,event-breakdown,recent-events,recent-orders}`.

## Limitations
In-memory rate limiter (per process); tables created via `create_all` instead of Alembic; allowed-origins column is stored but CORS uses `CORS_ORIGINS` env; no key rotation; no retention job; amounts in paise.
