import { useState } from "react";
import { Card } from "../components/Card.jsx";
import { Input } from "../components/Input.jsx";
import { Button } from "../components/Button.jsx";
import { api } from "../api/client.js";

export default function Standings() {
  const [year, setYear] = useState("2024");
  const [type, setType] = useState("drivers"); // "drivers" | "constructors"
  const [data, setData] = useState(null);
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(false);

  async function load() {
    setErr("");
    setData(null);

    const y = String(year).trim();
    if (!/^\d{4}$/.test(y)) {
      setErr("Please enter a 4-digit year (e.g., 2024).");
      return;
    }

    setLoading(true);
    try {
      const res =
        type === "drivers"
          ? await api.driverStandings(y, 200, 0)
          : await api.constructorStandings(y, 200, 0);

      setData(res);
    } catch (e) {
      setErr(String(e.message || e));
    } finally {
      setLoading(false);
    }
  }

  const results = data?.results ?? [];

  return (
    <Card
      title="Championship Standings"
      right={
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <div style={{ width: 120 }}>
            <Input
              placeholder="Year"
              value={year}
              onChange={(e) => setYear(e.target.value)}
            />
          </div>

          <select
            value={type}
            onChange={(e) => setType(e.target.value)}
            style={{
              padding: "8px",
              borderRadius: "10px",
              border: "1px solid var(--line)",
              background: "rgba(255,255,255,0.03)",
              color: "var(--text)",
            }}
          >
            <option value="drivers">Drivers</option>
            <option value="constructors">Constructors</option>
          </select>

          <Button onClick={load} disabled={!String(year).trim() || loading}>
            {loading ? "Loading..." : "Load"}
          </Button>
        </div>
      }
    >
      {err && <div style={{ color: "salmon" }}>{err}</div>}

      {!data && !err && (
        <div>Enter a season year to view final championship standings.</div>
      )}

      {data && results.length === 0 && (
        <div style={{ color: "var(--muted)" }}>
          No standings returned for {year}. Check the year exists in the dataset.
        </div>
      )}

      {data && results.length > 0 && (
        <div style={{ display: "grid", gap: 8 }}>
          {results.map((r) => (
            <div
              key={type === "drivers" ? r.driverId : r.constructorId}
              style={{
                display: "flex",
                justifyContent: "space-between",
                gap: 10,
                padding: 10,
                border: "1px solid var(--line)",
                borderRadius: 14,
                background: "rgba(255,255,255,0.02)",
              }}
            >
              <div>
                <b>{r.rank}.</b>{" "}
                {type === "drivers" ? r.driverName : r.constructorName}
                <span style={{ color: "var(--muted)" }}>
                  {" "}
                  ({r.nationality})
                </span>
              </div>

              <div style={{ color: "var(--muted)" }}>
                {r.points} pts • {r.wins} wins
              </div>
            </div>
          ))}
        </div>
      )}
    </Card>
  );
}