import os, uuid
from datetime import datetime, timezone
from sqlalchemy import (create_engine, String, Integer, DateTime, ForeignKey, UniqueConstraint, Index, text)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+psycopg://analytics:analytics@localhost:5432/analytics")
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(engine, expire_on_commit=False)
now = lambda: datetime.now(timezone.utc)

class Base(DeclarativeBase): pass

class Project(Base):
    __tablename__ = "projects"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    write_key_hash: Mapped[str] = mapped_column(String(64), unique=True)   # public browser key
    server_key_hash: Mapped[str] = mapped_column(String(64), unique=True)  # private server key
    read_key_hash: Mapped[str] = mapped_column(String(64), unique=True)    # dashboard key
    allowed_origins: Mapped[list] = mapped_column(JSONB, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now, onupdate=now)

class Event(Base):
    __tablename__ = "events"
    id: Mapped[int] = mapped_column(primary_key=True)
    event_id: Mapped[str] = mapped_column(String(64))
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"))
    event_name: Mapped[str] = mapped_column(String(100))
    source: Mapped[str] = mapped_column(String(10))
    anonymous_id: Mapped[str] = mapped_column(String(64))
    user_id: Mapped[str | None] = mapped_column(String(100))
    session_id: Mapped[str | None] = mapped_column(String(64))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    properties: Mapped[dict] = mapped_column(JSONB, default=dict)
    schema_version: Mapped[int] = mapped_column(Integer, default=1)
    __table_args__ = (
        UniqueConstraint("project_id", "event_id", name="uq_event_project_eventid"),  # idempotency
        Index("ix_ev_proj_time", "project_id", "occurred_at"),
        Index("ix_ev_proj_name_time", "project_id", "event_name", "occurred_at"),
        Index("ix_ev_proj_anon", "project_id", "anonymous_id"),
        Index("ix_ev_proj_session", "project_id", "session_id"),
        Index("ix_ev_pageviews", "project_id", text("(properties->>'page_path')"), postgresql_where=text("event_name = 'page_view'")),
    )

class Order(Base):
    __tablename__ = "orders"
    id: Mapped[str] = mapped_column(String(40), primary_key=True, default=lambda: "ord_" + uuid.uuid4().hex[:12])
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"))
    idempotency_key: Mapped[str] = mapped_column(String(100))
    status: Mapped[str] = mapped_column(String(20), default="confirmed")
    total_amount: Mapped[int] = mapped_column(Integer)        # minor units (paise)
    currency: Mapped[str] = mapped_column(String(3), default="INR")
    item_count: Mapped[int] = mapped_column(Integer)
    items: Mapped[list] = mapped_column(JSONB)                # [{item_id,name,qty,unit_price}]
    anonymous_id: Mapped[str | None] = mapped_column(String(64))
    session_id: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    __table_args__ = (UniqueConstraint("project_id", "idempotency_key", name="uq_order_idem"),
                      Index("ix_orders_proj_time", "project_id", "created_at"))

class Outbox(Base):  # transactional outbox: written in the same tx as the order
    __tablename__ = "outbox"
    id: Mapped[int] = mapped_column(primary_key=True)
    project_id: Mapped[str] = mapped_column(ForeignKey("projects.id"))
    payload: Mapped[dict] = mapped_column(JSONB)  # full event incl. stable event_id => idempotent redelivery
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_retry_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(String(300))
    __table_args__ = (Index("ix_outbox_pending", "next_retry_at", postgresql_where=text("delivered_at IS NULL")),)

def get_db():
    with SessionLocal() as s:
        yield s
