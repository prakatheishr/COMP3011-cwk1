from __future__ import annotations

from sqlalchemy.orm import Session
from sqlalchemy import text


def _fetch_one(db: Session, sql: str, params: dict) -> dict | None:
    row = db.execute(text(sql), params).mappings().first()
    return dict(row) if row else None


def _fetch_all(db: Session, sql: str, params: dict) -> list[dict]:
    rows = db.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]


def build_race_fact_pack(db: Session, raceId: int) -> dict:
    """
    Deterministic "fact pack" for a race.
    This returns structured facts suitable for later narrative generation.
    """
    race = _fetch_one(
        db,
        """
        SELECT
            ra.raceId,
            ra.year,
            ra.round,
            ra.name AS raceName,
            ra.date,
            ra.time,
            c.circuitId,
            c.name AS circuitName,
            c.location,
            c.country
        FROM races ra
        JOIN circuits c ON c.circuitId = ra.circuitId
        WHERE ra.raceId = :raceId
        """,
        {"raceId": raceId},
    )
    if not race:
        return {}

    # Top 10 finishers
    top10 = _fetch_all(
        db,
        """
        SELECT
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
        LIMIT 10
        """,
        {"raceId": raceId},
    )

    podium = [x for x in top10 if x["positionOrder"] in (1, 2, 3)]

    # DNF list (status != Finished) — keep it short for narrative usefulness
    dnfs = _fetch_all(
        db,
        """
        SELECT
            d.driverId,
            d.forename || ' ' || d.surname AS driverName,
            c.name AS constructorName,
            s.status
        FROM results r
        JOIN drivers d ON d.driverId = r.driverId
        JOIN constructors c ON c.constructorId = r.constructorId
        JOIN status s ON s.statusId = r.statusId
        WHERE r.raceId = :raceId
          AND s.status != 'Finished'
        ORDER BY d.surname, d.forename
        """,
        {"raceId": raceId},
    )

    dnf_count = len(dnfs)

    # Fastest lap (if dataset has it populated for this race)
    fastest = _fetch_one(
        db,
        """
        SELECT
            d.forename || ' ' || d.surname AS driverName,
            c.name AS constructorName,
            r.fastestLap AS fastestLapNumber,
            r.fastestLapTime,
            r.fastestLapSpeed
        FROM results r
        JOIN drivers d ON d.driverId = r.driverId
        JOIN constructors c ON c.constructorId = r.constructorId
        WHERE r.raceId = :raceId
          AND r.fastestLapTime IS NOT NULL
          AND r.fastestLapTime != '\\N'
        ORDER BY r.fastestLapTime ASC
        LIMIT 1
        """,
        {"raceId": raceId},
    )

    return {
        "type": "race_fact_pack",
        "race": race,
        "podium": podium,
        "top10": top10,
        "dnf_count": dnf_count,
        "dnfs": dnfs[:10],  # cap for readability
        "fastest_lap": fastest,
    }

