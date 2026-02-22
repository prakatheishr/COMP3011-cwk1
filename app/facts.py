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


def build_season_fact_pack(db: Session, year: int) -> dict:
    """
    Deterministic "fact pack" for a season.
    Focuses on standings and season-level dominance metrics.
    """
    # Number of races
    race_count_row = _fetch_one(
        db,
        "SELECT COUNT(*) AS raceCount FROM races WHERE year = :year",
        {"year": year},
    )
    if not race_count_row or race_count_row["raceCount"] == 0:
        return {}

    race_count = int(race_count_row["raceCount"])

    # Top 3 drivers by points
    top_drivers = _fetch_all(
        db,
        """
        SELECT
            d.driverId,
            d.forename || ' ' || d.surname AS driverName,
            d.nationality,
            SUM(res.points) AS points,
            SUM(CASE WHEN res.positionOrder = 1 THEN 1 ELSE 0 END) AS wins
        FROM results res
        JOIN races ra ON ra.raceId = res.raceId
        JOIN drivers d ON d.driverId = res.driverId
        WHERE ra.year = :year
        GROUP BY d.driverId, driverName, d.nationality
        ORDER BY points DESC, wins DESC
        LIMIT 3
        """,
        {"year": year},
    )

    # Top 3 constructors by points
    top_constructors = _fetch_all(
        db,
        """
        SELECT
            c.constructorId,
            c.name AS constructorName,
            c.nationality,
            SUM(res.points) AS points,
            SUM(CASE WHEN res.positionOrder = 1 THEN 1 ELSE 0 END) AS wins
        FROM results res
        JOIN races ra ON ra.raceId = res.raceId
        JOIN constructors c ON c.constructorId = res.constructorId
        WHERE ra.year = :year
        GROUP BY c.constructorId, constructorName, c.nationality
        ORDER BY points DESC, wins DESC
        LIMIT 3
        """,
        {"year": year},
    )

    # Champion points gap (1st - 2nd)
    top2_driver_points = _fetch_all(
        db,
        """
        SELECT
            d.driverId,
            d.forename || ' ' || d.surname AS driverName,
            SUM(res.points) AS points
        FROM results res
        JOIN races ra ON ra.raceId = res.raceId
        JOIN drivers d ON d.driverId = res.driverId
        WHERE ra.year = :year
        GROUP BY d.driverId, driverName
        ORDER BY points DESC
        LIMIT 2
        """,
        {"year": year},
    )
    champion_gap = None
    if len(top2_driver_points) == 2:
        champion_gap = float(top2_driver_points[0]["points"]) - float(top2_driver_points[1]["points"])

    # Constructor win share (wins / race_count) for top constructor
    top_constructor_wins = _fetch_one(
        db,
        """
        SELECT
            c.constructorId,
            c.name AS constructorName,
            COUNT(*) AS wins
        FROM results res
        JOIN races ra ON ra.raceId = res.raceId
        JOIN constructors c ON c.constructorId = res.constructorId
        WHERE ra.year = :year AND res.positionOrder = 1
        GROUP BY c.constructorId, constructorName
        ORDER BY wins DESC
        LIMIT 1
        """,
        {"year": year},
    )
    constructor_win_share = None
    if top_constructor_wins:
        constructor_win_share = float(top_constructor_wins["wins"]) / float(race_count)

    return {
        "type": "season_fact_pack",
        "year": year,
        "race_count": race_count,
        "top_drivers": top_drivers,
        "top_constructors": top_constructors,
        "champion_points_gap": champion_gap,
        "top_constructor_win_share": constructor_win_share,
    }


def build_season_comparison_fact_pack(db: Session, year: int, compare_to: int) -> dict:
    """
    Deterministic comparison fact pack for two seasons.
    Produces both fact packs + a small set of computed deltas.
    """
    a = build_season_fact_pack(db, year)
    b = build_season_fact_pack(db, compare_to)

    if not a or not b:
        return {}

    # Champion names (driver at rank 1 in top_drivers)
    a_champ = a["top_drivers"][0]["driverName"] if a["top_drivers"] else None
    b_champ = b["top_drivers"][0]["driverName"] if b["top_drivers"] else None

    champ_changed = (a_champ is not None and b_champ is not None and a_champ != b_champ)

    # Gap delta (if both present)
    gap_delta = None
    if a["champion_points_gap"] is not None and b["champion_points_gap"] is not None:
        gap_delta = float(a["champion_points_gap"]) - float(b["champion_points_gap"])

    # Win share delta (if both present)
    win_share_delta = None
    if a["top_constructor_win_share"] is not None and b["top_constructor_win_share"] is not None:
        win_share_delta = float(a["top_constructor_win_share"]) - float(b["top_constructor_win_share"])

    return {
        "type": "season_comparison_fact_pack",
        "year": year,
        "compare_to": compare_to,
        "season": a,
        "comparison": b,
        "deltas": {
            "champion_changed": champ_changed,
            "champion_year": a_champ,
            "champion_compare_to": b_champ,
            "champion_gap_delta": gap_delta,
            "top_constructor_win_share_delta": win_share_delta,
        },
    }
