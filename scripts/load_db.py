from pathlib import Path
import pandas as pd
from sqlalchemy import text
from app.db import get_engine

DATA_DIR = Path("data")

MVP_FILES = {
    "circuits": "circuits.csv",
    "constructors": "constructors.csv",
    "drivers": "drivers.csv",
    "races": "races.csv",
    "status": "status.csv",
    "results": "results.csv",
}

DDL = """
PRAGMA foreign_keys = ON;

DROP TABLE IF EXISTS results;
DROP TABLE IF EXISTS races;
DROP TABLE IF EXISTS drivers;
DROP TABLE IF EXISTS constructors;
DROP TABLE IF EXISTS circuits;
DROP TABLE IF EXISTS status;

CREATE TABLE circuits (
  circuitId INTEGER PRIMARY KEY,
  circuitRef TEXT,
  name TEXT,
  location TEXT,
  country TEXT,
  lat REAL,
  lng REAL,
  alt INTEGER,
  url TEXT
);

CREATE TABLE constructors (
  constructorId INTEGER PRIMARY KEY,
  constructorRef TEXT,
  name TEXT,
  nationality TEXT,
  url TEXT
);

CREATE TABLE drivers (
  driverId INTEGER PRIMARY KEY,
  driverRef TEXT,
  number TEXT,
  code TEXT,
  forename TEXT,
  surname TEXT,
  dob TEXT,
  nationality TEXT,
  url TEXT
);

CREATE TABLE races (
  raceId INTEGER PRIMARY KEY,
  year INTEGER NOT NULL,
  round INTEGER NOT NULL,
  circuitId INTEGER NOT NULL,
  name TEXT,
  date TEXT,
  time TEXT,
  url TEXT,
  FOREIGN KEY (circuitId) REFERENCES circuits(circuitId)
);

CREATE TABLE status (
  statusId INTEGER PRIMARY KEY,
  status TEXT
);

CREATE TABLE results (
  resultId INTEGER PRIMARY KEY,
  raceId INTEGER NOT NULL,
  driverId INTEGER NOT NULL,
  constructorId INTEGER NOT NULL,
  number TEXT,
  grid INTEGER,
  position TEXT,
  positionOrder INTEGER,
  points REAL,
  laps INTEGER,
  time TEXT,
  milliseconds INTEGER,
  fastestLap TEXT,
  fastestLapTime TEXT,
  fastestLapSpeed TEXT,
  statusId INTEGER NOT NULL,
  FOREIGN KEY (raceId) REFERENCES races(raceId),
  FOREIGN KEY (driverId) REFERENCES drivers(driverId),
  FOREIGN KEY (constructorId) REFERENCES constructors(constructorId),
  FOREIGN KEY (statusId) REFERENCES status(statusId)
);

CREATE INDEX idx_races_year ON races(year);
CREATE INDEX idx_results_raceId ON results(raceId);
CREATE INDEX idx_results_driverId ON results(driverId);
CREATE INDEX idx_results_constructorId ON results(constructorId);
"""

def must_exist(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing: {path}. Put CSVs in ./data/")

def exec_ddl(conn, ddl: str) -> None:
    for stmt in ddl.split(";"):
        s = stmt.strip()
        if s:
            conn.execute(text(s))

def main():
    engine = get_engine()

    # Check files exist
    for fname in MVP_FILES.values():
        must_exist(DATA_DIR / fname)

    # Create schema
    with engine.begin() as conn:
        exec_ddl(conn, DDL)

    # Load tables in dependency order
    load_order = ["circuits", "constructors", "drivers", "races", "status", "results"]

    for table in load_order:
        csv_path = DATA_DIR / MVP_FILES[table]
        df = pd.read_csv(csv_path)

        # keep only columns that exist in table (safer than assuming)
        with engine.connect() as conn:
            cols = [r[1] for r in conn.execute(text(f"PRAGMA table_info({table})")).fetchall()]
        df = df[[c for c in df.columns if c in cols]]

        df.to_sql(table, engine, if_exists="append", index=False)
        print(f"Loaded {table}: {len(df):,} rows")

    # Sanity check
    with engine.connect() as conn:
        min_year, max_year = conn.execute(text("SELECT MIN(year), MAX(year) FROM races")).fetchone()
        print(f"Years in races: {min_year}–{max_year}")

if __name__ == "__main__":
    main()
