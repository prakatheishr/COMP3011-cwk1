from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

DB_PATH = Path("f1.sqlite3")

def get_engine() -> Engine:
    # check_same_thread=False allows FastAPI to use SQLite across requests/threads
    return create_engine(
        f"sqlite:///{DB_PATH}",
        connect_args={"check_same_thread": False},
        future=True,
    )
