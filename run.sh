#!/usr/bin/env bash
# usage: DATABASE_URL=... ./run.sh   (after: pip install -r backend/requirements.txt)
cd "$(dirname "$0")/backend" && python -m app.seed
python -m app.dispatcher & uvicorn app.main:app --port 8000; kill %1
