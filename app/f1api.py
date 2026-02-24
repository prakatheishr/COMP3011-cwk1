from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db import DB_PATH, ensure_notes_table, ensure_notes_ai_columns
from app.db_session import get_db, SessionLocal
from app.facts import (
    build_race_fact_pack,
    build_season_fact_pack,
    build_season_comparison_fact_pack,
)
from app.insights import (
    render_race_insight,
    render_season_insight,
    render_season_comparison_insight,
)
from app.llm import generate_llm_insight

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    db = SessionLocal()
    try:
        ensure_notes_table(db)
        ensure_notes_ai_columns(db)
    finally:
        db.close()

    yield

    # Shutdown logic (if needed in future)

app = FastAPI(
    title="F1 Stats API",
    version="5.0.0",
    description="FastAPI + SQLite API for F1 historical data (Ergast-style dataset).",
    lifespan=lifespan,
)

from pydantic import BaseModel, Field
from typing import Literal, Optional

class NoteCreate(BaseModel):
    entity_type: Literal["race", "driver", "season"]
    entity_id: int = Field(..., ge=1)
    title: str = Field(..., min_length=1, max_length=120)
    content: str = Field(..., min_length=1, max_length=5000)

class NoteUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=120)
    content: Optional[str] = Field(None, min_length=1, max_length=5000)


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

@app.get("/debug/races/{raceId}/facts")
def debug_race_facts(raceId: int, db: Session = Depends(get_db)):
    facts = build_race_fact_pack(db, raceId)
    if not facts:
        raise HTTPException(status_code=404, detail="Race not found")
    return facts


@app.get("/debug/seasons/{year}/facts")
def debug_season_facts(year: int, db: Session = Depends(get_db)):
    validate_year(db, year)
    facts = build_season_fact_pack(db, year)
    if not facts:
        raise HTTPException(status_code=404, detail="Season not found")
    return facts


@app.get("/debug/seasons/{year}/compare/{compare_to}")
def debug_season_compare(year: int, compare_to: int, db: Session = Depends(get_db)):
    validate_year(db, year)
    validate_year(db, compare_to)
    facts = build_season_comparison_fact_pack(db, year, compare_to)
    if not facts:
        raise HTTPException(status_code=404, detail="Comparison not available")
    return facts


@app.get("/races/{raceId}/insights")
def race_insights(
    raceId: int,
    mode: str = Query("recap", pattern="^(recap|impact)$"),
    fmt: str = Query("plain", alias="format", pattern="^(plain|radio)$"),
    generator: str = Query("template", pattern="^(template|llm)$"),
    db: Session = Depends(get_db),
):
    facts = build_race_fact_pack(db, raceId)
    if not facts:
        raise HTTPException(status_code=404, detail="Race not found")

    if generator == "llm":
        insight = generate_llm_insight(facts, mode=mode, fmt=fmt)
    else:
        insight = render_race_insight(facts, mode=mode, fmt=fmt)

    return {
        "raceId": raceId,
        "mode": mode,
        "format": fmt,
        "generator": generator,
        "facts": facts,
        "insight": insight,
    }


@app.get("/seasons/{year}/insights")
def season_insights(
    year: int,
    compare_to: int | None = Query(None),
    mode: str = Query("recap", pattern="^(recap|impact)$"),
    fmt: str = Query("plain", alias="format", pattern="^(plain|radio)$"),
    generator: str = Query("template", pattern="^(template|llm)$"),
    db: Session = Depends(get_db),
):
    validate_year(db, year)

    # Comparison mode
    if compare_to is not None:
        validate_year(db, compare_to)
        facts = build_season_comparison_fact_pack(db, year, compare_to)
        if not facts:
            raise HTTPException(status_code=404, detail="Comparison not available")

        if generator == "llm":
            insight = generate_llm_insight(facts, mode=mode, fmt=fmt)
        else:
            insight = render_season_comparison_insight(facts, mode=mode, fmt=fmt)

        return {
            "year": year,
            "compare_to": compare_to,
            "mode": mode,
            "format": fmt,
            "generator": generator,
            "facts": facts,
            "insight": insight,
        }

    # Single season mode
    facts = build_season_fact_pack(db, year)
    if not facts:
        raise HTTPException(status_code=404, detail="Season not found")

    if generator == "llm":
        insight = generate_llm_insight(facts, mode=mode, fmt=fmt)
    else:
        insight = render_season_insight(facts, mode=mode, fmt=fmt)

    return {
        "year": year,
        "mode": mode,
        "format": fmt,
        "generator": generator,
        "facts": facts,
        "insight": insight,
    }


@app.post("/notes", status_code=201)
def create_note(payload: NoteCreate, db: Session = Depends(get_db)):
    row = db.execute(
        text("""
            INSERT INTO notes (entity_type, entity_id, title, content)
            VALUES (:entity_type, :entity_id, :title, :content)
            RETURNING id, entity_type, entity_id, title, content, created_at, updated_at
        """),
        payload.model_dump(),
    ).mappings().first()
    db.commit()
    return dict(row)

@app.get("/notes")
def list_notes(
    db: Session = Depends(get_db),
    entity_type: str | None = Query(None, pattern="^(race|driver|season)$"),
    entity_id: int | None = Query(None, ge=1),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    where = []
    params = {"limit": limit, "offset": offset}
    if entity_type is not None:
        where.append("entity_type = :entity_type")
        params["entity_type"] = entity_type
    if entity_id is not None:
        where.append("entity_id = :entity_id")
        params["entity_id"] = entity_id

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    rows = db.execute(
        text(f"""
            SELECT id, entity_type, entity_id, title, content, created_at, updated_at
            FROM notes
            {where_sql}
            ORDER BY updated_at DESC
            LIMIT :limit OFFSET :offset
        """),
        params,
    ).mappings().all()

    return {"count": len(rows), "limit": limit, "offset": offset, "results": [dict(r) for r in rows]}

@app.get("/notes/{note_id}")
def get_note(note_id: int, db: Session = Depends(get_db)):
    row = db.execute(
        text("""
            SELECT id, entity_type, entity_id, title, content, created_at, updated_at
            FROM notes
            WHERE id = :id
        """),
        {"id": note_id},
    ).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Note not found")
    return dict(row)

@app.put("/notes/{note_id}")
def update_note(note_id: int, payload: NoteUpdate, db: Session = Depends(get_db)):
    existing = db.execute(
        text("SELECT id FROM notes WHERE id = :id"),
        {"id": note_id},
    ).mappings().first()
    if not existing:
        raise HTTPException(status_code=404, detail="Note not found")

    data = payload.model_dump(exclude_unset=True)
    if not data:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    set_parts = []
    params = {"id": note_id}
    if "title" in data:
        set_parts.append("title = :title")
        params["title"] = data["title"]
    if "content" in data:
        set_parts.append("content = :content")
        params["content"] = data["content"]

    set_parts.append("updated_at = datetime('now')")

    row = db.execute(
        text(f"""
            UPDATE notes
            SET {", ".join(set_parts)}
            WHERE id = :id
            RETURNING id, entity_type, entity_id, title, content, created_at, updated_at
        """),
        params,
    ).mappings().first()
    db.commit()
    return dict(row)

@app.delete("/notes/{note_id}", status_code=204)
def delete_note(note_id: int, db: Session = Depends(get_db)):
    result = db.execute(
        text("DELETE FROM notes WHERE id = :id"),
        {"id": note_id},
    )
    db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Note not found")
    return



@app.post("/notes/{note_id}/ai/tldr")
def note_tldr(
    note_id: int,
    generator: str = Query("llm", pattern="^(template|llm)$"),
    db: Session = Depends(get_db),
):
    note = db.execute(
        text("""
            SELECT id, entity_type, entity_id, title, content, ai_summary
            FROM notes
            WHERE id = :id
        """),
        {"id": note_id},
    ).mappings().first()

    if not note:
        raise HTTPException(status_code=404, detail="Note not found")

    content = (note["content"] or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="Note content is empty")

    if generator == "template":
        # Deterministic fallback: first ~240 chars
        summary = content[:240].strip()
        if len(content) > 240:
            summary += "..."
    else:
        # LLM summary prompt (kept simple + grounded)
        facts = {
            "type": "user_note",
            "title": note["title"],
            "content": content,
        }
        # Reuse your llm function, but treat as recap/plain
        summary = generate_llm_insight(
            facts=facts,
            mode="recap",
            fmt="plain",
        )
        # Optional: enforce shortness
        summary = summary.strip()

    db.execute(
        text("""
            UPDATE notes
            SET ai_summary = :ai_summary,
                updated_at = datetime('now')
            WHERE id = :id
        """),
        {"id": note_id, "ai_summary": summary},
    )
    db.commit()

    updated = db.execute(
        text("""
            SELECT id, entity_type, entity_id, title, content, ai_summary, created_at, updated_at
            FROM notes
            WHERE id = :id
        """),
        {"id": note_id},
    ).mappings().first()

    return {"generator": generator, "note": dict(updated)}