import hashlib, secrets
from fastapi import Depends, Header, HTTPException
from sqlalchemy import or_, select
from sqlalchemy.orm import Session
from .db import Project, get_db

h = lambda k: hashlib.sha256(k.encode()).hexdigest()

def create_project(db: Session, pid: str, name: str, origins=None) -> dict:
    keys = {"write": "pk_" + secrets.token_urlsafe(24), "server": "sk_" + secrets.token_urlsafe(24), "read": "rk_" + secrets.token_urlsafe(24)}
    db.add(Project(id=pid, name=name, write_key_hash=h(keys["write"]), server_key_hash=h(keys["server"]),
                   read_key_hash=h(keys["read"]), allowed_origins=origins or []))
    db.commit(); return keys

def principal(x_api_key: str | None = Header(None), db: Session = Depends(get_db)):
    if not x_api_key: raise HTTPException(401, {"code": "unauthorized", "message": "X-API-Key required"})
    k = h(x_api_key)
    p = db.scalar(select(Project).where(or_(Project.write_key_hash == k, Project.server_key_hash == k, Project.read_key_hash == k)))
    if not p: raise HTTPException(401, {"code": "unauthorized", "message": "invalid API key"})
    return p, ("write" if p.write_key_hash == k else "server" if p.server_key_hash == k else "read")
