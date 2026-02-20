from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from fastapi import HTTPException
from app.db import DB_PATH
from app.db_session import get_db

app = FastAPI(
    title="F1 Stats API",
    version="0.1.1",
    description="FastAPI + SQLite API for F1 historical data (Ergast-style dataset).",
)

@app.get("/health")
def health():
    return {"status": "ok", "db_file": str(DB_PATH)}

@app.get("/drivers")
def list_drivers(
    db: Session = Depends(get_db),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    rows = db.execute(
        text("""
            SELECT driverId, forename, surname, nationality, dob
            FROM drivers
            ORDER BY surname, forename
            LIMIT :limit OFFSET :offset
        """),
        {"limit": limit, "offset": offset},
    ).mappings().all()

    return {
        "count": len(rows),
        "results": [dict(r) for r in rows],
    }



@app.get("/drivers/{driverId}")
def get_driver(driverId: int, db: Session = Depends(get_db)):
    row = db.execute(
        text("""
            SELECT driverId, driverRef, number, code, forename, surname, dob, nationality, url
            FROM drivers
            WHERE driverId = :driverId
        """),
        {"driverId": driverId},
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Driver not found")

    return dict(row)

@app.get("/races")
def list_races(
    db: Session = Depends(get_db),
    year: int | None = Query(None, ge=1950),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    if year is None:
        # default: most recent year in DB
        year = db.execute(text("SELECT MAX(year) FROM races")).scalar_one()

    rows = db.execute(
        text("""
            SELECT raceId, year, round, name, date, time, circuitId
            FROM races
            WHERE year = :year
            ORDER BY round
            LIMIT :limit OFFSET :offset
        """),
        {"year": year, "limit": limit, "offset": offset},
    ).mappings().all()

    return {"year": year, "count": len(rows), "results": [dict(r) for r in rows]}

@app.get("/races/{raceId}/results")
def race_results(raceId: int, db: Session = Depends(get_db)):
    # confirm race exists + fetch race metadata
    race = db.execute(
        text("""
            SELECT raceId, year, round, name, date
            FROM races
            WHERE raceId = :raceId
        """),
        {"raceId": raceId},
    ).mappings().first()

    if not race:
        raise HTTPException(status_code=404, detail="Race not found")

    rows = db.execute(
        text("""
            SELECT
                r.resultId,
                r.positionOrder,
                r.points,
                r.grid,
                r.laps,
                r.time,
                r.milliseconds,
                d.driverId,
                d.forename || ' ' || d.surname AS driverName,
                c.constructorId,
                c.name AS constructorName,
                s.status
            FROM results r
            JOIN drivers d ON d.driverId = r.driverId
            JOIN constructors c ON c.constructorId = r.constructorId
            JOIN status s ON s.statusId = r.statusId
            WHERE r.raceId = :raceId
            ORDER BY r.positionOrder ASC
        """),
        {"raceId": raceId},
    ).mappings().all()

    return {"race": dict(race), "count": len(rows), "results": [dict(x) for x in rows]}
