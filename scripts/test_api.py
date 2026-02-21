import sys
from pathlib import Path

# Ensure project root is in Python path
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

from fastapi.testclient import TestClient
from app.f1api import app

client = TestClient(app)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_drivers_list():
    r = client.get("/drivers?limit=5")
    assert r.status_code == 200
    data = r.json()
    assert "results" in data
    assert len(data["results"]) <= 5


def test_races_latest_year():
    r = client.get("/races?limit=1")
    assert r.status_code == 200
    data = r.json()
    assert "year" in data
    assert "results" in data


def test_race_not_found():
    r = client.get("/races/999999999")
    assert r.status_code == 404


def test_driver_standings_valid_year():
    races = client.get("/races?limit=1").json()
    year = races["year"]
    r = client.get(f"/seasons/{year}/driver-standings?limit=5")
    assert r.status_code == 200
    assert "results" in r.json()
