"""python -m app.seed — creates tables + demo project; writes frontend/config.js and .keys.json."""
import json, pathlib
from sqlalchemy import select
from .db import Base, Project, SessionLocal, engine
from .security import create_project

Base.metadata.create_all(engine)
root = pathlib.Path(__file__).resolve().parents[2]
with SessionLocal() as s:
    if s.scalar(select(Project).where(Project.id == "restaurant-demo")):
        print("restaurant-demo already exists (keys are hashed and cannot be shown again). Drop the DB to reseed."); raise SystemExit
    k = create_project(s, "restaurant-demo", "Restaurant Demo")
(root / "backend/.keys.json").write_text(json.dumps(k))
(root / "frontend/config.js").write_text(f'window.ANALYTICS_CONFIG={{endpoint:"/api/v1",projectId:"restaurant-demo",writeKey:"{k["write"]}"}};\n')
print("Keys (shown once):"); [print(f"  {n:7}{v}") for n, v in k.items()]
print("Dashboard read key -> paste at http://localhost:8000/dashboard.html")
