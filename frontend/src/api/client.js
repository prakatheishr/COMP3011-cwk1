const BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";

async function request(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  if (!res.ok) {
    let detail = "";
    try {
      const data = await res.json();
      detail = data?.detail ? String(data.detail) : JSON.stringify(data);
    } catch {
      detail = await res.text();
    }
    throw new Error(`${res.status} ${res.statusText}: ${detail}`);
  }

  // 204 no content
  if (res.status === 204) return null;
  return res.json();
}

async function driverStandings(year){
  const res = await fetch(`/seasons/${year}/driver-standings?limit=200`);
  if(!res.ok) throw new Error("Failed to fetch driver standings");
  return res.json();
}

async function constructorStandings(year){
  const res = await fetch(`/seasons/${year}/constructor-standings?limit=200`);
  if(!res.ok) throw new Error("Failed to fetch constructor standings");
  return res.json();
}



export const api = {
  health: () => request("/health"),

  drivers: (limit = 50, offset = 0) => request(`/drivers?limit=${limit}&offset=${offset}`),

  races: (year, limit = 50, offset = 0) => {
    const y = year ? `year=${encodeURIComponent(year)}&` : "";
    return request(`/races?${y}limit=${limit}&offset=${offset}`);
  },

  race: (raceId) => request(`/races/${raceId}`),
  raceResults: (raceId) => request(`/races/${raceId}/results`),

  raceInsights: ({ raceId, mode, format, generator }) =>
    request(
      `/races/${raceId}/insights?mode=${mode}&format=${format}&generator=${generator}`
    ),

  seasonInsights: ({ year, compareTo, mode, format, generator }) => {
    const c = compareTo ? `&compare_to=${compareTo}` : "";
    return request(
      `/seasons/${year}/insights?mode=${mode}&format=${format}&generator=${generator}${c}`
    );
  },
  driverStandings,
  constructorStandings,

  notesList: ({ entityType, entityId, limit = 50, offset = 0 }) => {
    const et = entityType ? `entity_type=${encodeURIComponent(entityType)}&` : "";
    const ei = entityId ? `entity_id=${encodeURIComponent(entityId)}&` : "";
    return request(`/notes?${et}${ei}limit=${limit}&offset=${offset}`);
  },

  noteGet: (id) => request(`/notes/${id}`),
  noteCreate: (payload) => request(`/notes`, { method: "POST", body: JSON.stringify(payload) }),
  noteUpdate: (id, payload) =>
    request(`/notes/${id}`, { method: "PUT", body: JSON.stringify(payload) }),
  noteDelete: (id) => request(`/notes/${id}`, { method: "DELETE" }),

  noteTldr: (id, generator = "llm") =>
    request(`/notes/${id}/ai/tldr?generator=${generator}`, { method: "POST" }),

  // If you added these endpoints:
  saveRaceInsightAsNote: (raceId, payload) =>
    request(`/races/${raceId}/notes`, { method: "POST", body: JSON.stringify(payload) }),

  saveSeasonInsightAsNote: (year, payload) =>
    request(`/seasons/${year}/notes`, { method: "POST", body: JSON.stringify(payload) }),
};