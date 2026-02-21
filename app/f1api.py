from fastapi import FastAPI, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from fastapi import HTTPException
from app.db import DB_PATH
from app.db_session import get_db

app = FastAPI(
    title="F1 Stats API",
    version="1.3.0",
    description="FastAPI + SQLite API for F1 historical data (Ergast-style dataset).",
)

def get_latest_year(db: Session) -> int:
    return db.execute(text("SELECT MAX(year) FROM races")).scalar_one()

def validate_year(db: Session, year: int) -> None:
    min_year, max_year = db.execute(text("SELECT MIN(year), MAX(year) FROM races")).fetchone()
    if year < min_year or year > max_year:
        raise HTTPException(
            status_code=400,
            detail=f"year must be between {min_year} and {max_year}"
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
        "limit": limit,
        "offset": offset,
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
        year = get_latest_year(db)

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

    return {
        "year": year,
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "results": [dict(r) for r in rows],
    }


@app.get("/races/{raceId}/results")
def race_results(
    raceId: int,
    db: Session = Depends(get_db),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
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
            LIMIT :limit OFFSET :offset
        """),
        {"raceId": raceId, "limit": limit, "offset": offset},
    ).mappings().all()

    return {
        "race": dict(race),
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "results": [dict(x) for x in rows],
    }


@app.get("/races/{raceId}")
def get_race(raceId: int, db: Session = Depends(get_db)):
    row = db.execute(
        text("""
            SELECT
                ra.raceId,
                ra.year,
                ra.round,
                ra.name AS raceName,
                ra.date,
                ra.time,
                ra.url,
                c.circuitId,
                c.name AS circuitName,
                c.location,
                c.country
            FROM races ra
            JOIN circuits c ON c.circuitId = ra.circuitId
            WHERE ra.raceId = :raceId
        """),
        {"raceId": raceId},
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Race not found")

    return dict(row)


@app.get("/seasons/{year}/driver-standings")
def driver_standings(
    year: int,
    db: Session = Depends(get_db),
    limit: int = Query(30, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    validate_year(db, year)

    rows = db.execute(
        text("""
            SELECT
                d.driverId,
                d.forename || ' ' || d.surname AS driverName,
                d.nationality,
                SUM(res.points) AS points,
                SUM(CASE WHEN res.positionOrder = 1 THEN 1 ELSE 0 END) AS wins,
                COUNT(*) AS starts
            FROM results res
            JOIN races ra ON ra.raceId = res.raceId
            JOIN drivers d ON d.driverId = res.driverId
            WHERE ra.year = :year
            GROUP BY d.driverId, driverName, d.nationality
            ORDER BY points DESC, wins DESC
            LIMIT :limit OFFSET :offset
        """),
        {"year": year, "limit": limit, "offset": offset},
    ).mappings().all()

    results = []
    for i, r in enumerate(rows, start=1 + offset):
        item = dict(r)
        item["rank"] = i
        results.append(item)

    return {
        "year": year,
        "count": len(results),
        "limit": limit,
        "offset": offset,
        "results": results,
    }


@app.get("/seasons/{year}/constructor-standings")
def constructor_standings(
    year: int,
    db: Session = Depends(get_db),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    validate_year(db, year)

    rows = db.execute(
        text("""
            SELECT
                c.constructorId,
                c.name AS constructorName,
                c.nationality,
                SUM(res.points) AS points,
                SUM(CASE WHEN res.positionOrder = 1 THEN 1 ELSE 0 END) AS wins,
                COUNT(*) AS starts
            FROM results res
            JOIN races ra ON ra.raceId = res.raceId
            JOIN constructors c ON c.constructorId = res.constructorId
            WHERE ra.year = :year
            GROUP BY c.constructorId, constructorName, c.nationality
            ORDER BY points DESC, wins DESC
            LIMIT :limit OFFSET :offset
        """),
        {"year": year, "limit": limit, "offset": offset},
    ).mappings().all()

    results = []
    for i, r in enumerate(rows, start=1 + offset):
        item = dict(r)
        item["rank"] = i
        results.append(item)

    return {
        "year": year,
        "count": len(results),
        "limit": limit,
        "offset": offset,
        "results": results,
    }


@app.get("/constructors")
def list_constructors(
    db: Session = Depends(get_db),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    rows = db.execute(
        text("""
            SELECT constructorId, name, nationality
            FROM constructors
            ORDER BY name
            LIMIT :limit OFFSET :offset
        """),
        {"limit": limit, "offset": offset},
    ).mappings().all()

    return {
        "count": len(rows),
        "limit": limit,
        "offset": offset,
        "results": [dict(r) for r in rows],
    }


@app.get("/constructors/{constructorId}")
def get_constructor(constructorId: int, db: Session = Depends(get_db)):
    row = db.execute(
        text("""
            SELECT constructorId, constructorRef, name, nationality, url
            FROM constructors
            WHERE constructorId = :constructorId
        """),
        {"constructorId": constructorId},
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Constructor not found")

    return dict(row)


@app.get("/drivers/{driverId}/seasons/{year}")
def driver_season_summary(
    driverId: int,
    year: int,
    db: Session = Depends(get_db),
    include_results: bool = Query(False),
):
    # validate driver exists
    driver = db.execute(
        text("""
            SELECT driverId, forename || ' ' || surname AS driverName
            FROM drivers
            WHERE driverId = :driverId
        """),
        {"driverId": driverId},
    ).mappings().first()

    if not driver:
        raise HTTPException(status_code=404, detail="Driver not found")

    # validate year exists in races
    min_year, max_year = db.execute(text("SELECT MIN(year), MAX(year) FROM races")).fetchone()
    if year < min_year or year > max_year:
        raise HTTPException(status_code=400, detail=f"year must be between {min_year} and {max_year}")

    # aggregate season stats
    summary = db.execute(
        text("""
            SELECT
                SUM(res.points) AS points,
                SUM(CASE WHEN res.positionOrder = 1 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN res.positionOrder BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS podiums,
                COUNT(*) AS starts
            FROM results res
            JOIN races ra ON ra.raceId = res.raceId
            WHERE ra.year = :year AND res.driverId = :driverId
        """),
        {"year": year, "driverId": driverId},
    ).mappings().first()

    points = summary["points"] if summary["points"] is not None else 0
    wins = summary["wins"] if summary["wins"] is not None else 0
    podiums = summary["podiums"] if summary["podiums"] is not None else 0
    starts = summary["starts"] if summary["starts"] is not None else 0

    payload = {
        "driver": dict(driver),
        "year": year,
        "points": float(points),
        "wins": int(wins),
        "podiums": int(podiums),
        "starts": int(starts),
    }

    if include_results:
        results = db.execute(
            text("""
                SELECT
                    ra.raceId,
                    ra.round,
                    ra.name AS raceName,
                    res.positionOrder,
                    res.points,
                    c.name AS constructorName,
                    s.status
                FROM results res
                JOIN races ra ON ra.raceId = res.raceId
                JOIN constructors c ON c.constructorId = res.constructorId
                JOIN status s ON s.statusId = res.statusId
                WHERE ra.year = :year AND res.driverId = :driverId
                ORDER BY ra.round
            """),
            {"year": year, "driverId": driverId},
        ).mappings().all()

        payload["results"] = [dict(r) for r in results]

    return payload
