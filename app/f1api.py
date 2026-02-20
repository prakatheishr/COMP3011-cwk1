from fastapi import FastAPI
from app.db import DB_PATH

app = FastAPI(
    title="F1 Stats API",
    version="0.1.0",
    description="FastAPI + SQLite API for F1 historical data (Ergast-style dataset).",
)

@app.get("/health")
def health():
    return {"status": "ok", "db_file": str(DB_PATH)}
