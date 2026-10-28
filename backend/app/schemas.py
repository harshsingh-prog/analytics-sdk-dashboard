import re
from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field, field_validator

RESERVED = {"event_id", "project_id", "anonymous_id", "user_id", "session_id", "source", "timestamp"}

class EventIn(BaseModel):
    event_id: str = Field(min_length=1, max_length=64)
    event_name: str = Field(pattern=r"^[a-z][a-z0-9_.]{0,99}$")
    project_id: str = Field(min_length=1, max_length=64)
    source: Literal["browser", "server"]
    anonymous_id: str = Field(min_length=1, max_length=64)
    user_id: str | None = Field(None, max_length=100)
    session_id: str | None = Field(None, max_length=64)
    timestamp: datetime
    properties: dict = Field(default_factory=dict)
    schema_version: int = Field(1, ge=1, le=1)

    @field_validator("timestamp")
    @classmethod
    def _tz(cls, v):
        if v.tzinfo is None: raise ValueError("timezone required")
        return v

    @field_validator("properties")
    @classmethod
    def _props(cls, v):
        bad = RESERVED & set(v)
        if bad: raise ValueError(f"reserved property names: {sorted(bad)}")
        if len(str(v)) > 8000: raise ValueError("properties too large")
        return v

class OrderItemIn(BaseModel):
    item_id: str = Field(max_length=64)
    qty: int = Field(ge=1, le=50)

class OrderIn(BaseModel):  # client-sent totals/prices are ignored (extra fields dropped)
    items: list[OrderItemIn] = Field(min_length=1, max_length=50)
    anonymous_id: str | None = Field(None, max_length=64)
    session_id: str | None = Field(None, max_length=64)
