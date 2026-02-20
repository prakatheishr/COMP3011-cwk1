from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Database file in project root (same level as requirements.txt)
DB_PATH = Path("f1.sqlite3")

def get_engine() -> Engine:
    return create_engine(
        f"sqlite:///{DB_PATH}",
        connect_args={"check_same_thread": False},
        future=True,
    )
