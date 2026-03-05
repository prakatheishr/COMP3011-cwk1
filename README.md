# F1 Insight API (COMP3011 Coursework 1)

FastAPI REST API for historical Formula 1 analytics with deterministic insights, optional local LLM generation, and a demonstration frontend client.

Includes:
- FastAPI REST API
- deterministic analytics pipeline ("fact packs")
- optional local LLM insights (Ollama)
- React frontend client (apiV8)

This project demonstrates **clean API engineering, layered architecture, deterministic analytics pipelines, and controlled Generative AI integration.**

---

# Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Setup](#setup)
  - [Backend Setup](#backend-setup)
  - [Database](#database)
  - [Optional: Local LLM (Ollama)](#optional-local-llm-ollama)
  - [Optional: Frontend (apiV8)](#optional-frontend-apiv8)
- [API Usage](#api-usage)
- [Version History (apiV1 → apiV8)](#version-history-apiv1--apiv8)
- [Testing / Quick Demo Calls](#testing--quick-demo-calls)

---

# Project Overview

Most sports APIs expose structured tables such as standings, race results, and lap times, but provide little explanation of **why results mattered within the broader championship context**.

This project builds a **Formula 1 analytics API** that augments traditional sports data with a **narrative insight layer**.

The API exposes core F1 entities:

- drivers  
- constructors  
- races  
- race results  
- championship standings  

On top of this data, the system generates contextual insights using two analytical modes:

**Recap mode**  
Explains what happened during a race or season.

**Impact mode**  
Explains why the outcome mattered within the championship.

Narratives can be produced in two presentation formats:

- **Plain** — concise analytical summary  
- **Radio** — fan-friendly commentary style  

To maintain reliability, all insights are generated from deterministic **fact packs**, which are structured statistical summaries computed from the database. Generative AI is optional and operates only on these structured summaries, ensuring that outputs remain grounded in verified statistics.

---

# Architecture

The system follows a **layered architecture** separating data storage, analytical computation, narrative rendering, and API access.

## 1. Database Layer (SQLite)

Stores historical Formula 1 data derived from the Ergast Motor Racing dataset, including:

- drivers  
- constructors  
- races  
- results  
- circuits  

These tables are treated as **read-only reference data**.

A separate writable table is implemented for:

- **notes** (user-generated commentary)

---

## 2. Fact Pack Analytics Layer

Analytical queries aggregate race and season statistics into deterministic JSON summaries called **fact packs**.

Examples include:

- race winner and podium  
- championship margin changes  
- season standings  
- constructor performance  

Fact packs act as the **intermediate analytical representation** between raw database queries and narrative generation.

---

## 3. Insight Layer

Narrative insights are generated from fact packs using two generators.

**Template generator**

- deterministic  
- reproducible  
- used as baseline output  

**LLM generator**

- optional  
- uses a local model via **Ollama**  
- produces richer narrative descriptions  

Both generators operate on the same fact pack input.

---

## 4. API Layer (FastAPI)

FastAPI provides:

- REST endpoints  
- request validation  
- structured JSON responses  
- automatic OpenAPI documentation  

---

## 5. Frontend Client (apiV8)

A lightweight **React + Vite frontend** provides a graphical interface for interacting with the API.

The UI allows users to:

- browse drivers  
- view race results  
- retrieve season standings  
- generate narrative insights  
- create and manage notes  

The frontend acts as a **demonstration client** for the API and is not required for the backend to run.

---

# API Documentation

Full API documentation is included in the repository.

**PDF Documentation**

```
docs/COMP3011 API Documentation.pdf
```

**Interactive Swagger UI**

```
http://127.0.0.1:8000/docs
```

---

# Quick Start

Run the API with minimal setup:

```bash
git clone https://github.com/prakatheishr/COMP3011-cwk1.git
cd COMP3011-cwk1

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt

uvicorn app.f1api:app --reload
```

API will start at:

```
http://127.0.0.1:8000
```

Interactive documentation:

```
http://127.0.0.1:8000/docs
```

---

# Setup

## Backend Setup

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate

python -m pip install -U pip
python -m pip install -r requirements.txt
```

Run the API:

```bash
uvicorn app.f1api:app --reload
```

---

## Database

The API uses a local **SQLite database** containing the historical Formula 1 dataset.

On startup, the application automatically ensures that the writable tables exist:

- `notes`
- `notes_ai_columns`

No manual database migration is required.

---

# Optional: Local LLM (Ollama)

The API works fully **without a language model**.

Ollama is only required if the **LLM insight generator** is used.

Install Ollama:

```
https://ollama.com
```

Pull the model used for insight generation:

```bash
ollama pull llama3
```

Run the Ollama server:

```bash
ollama serve
```

Once running, insights can be generated with:

```
generator=llm
```

If Ollama is not running, the API will still function using **template-based insights**.

---

# Optional: Frontend

The repository includes an optional **React frontend client** located in the `frontend` directory.

The frontend is designed to demonstrate how the API can be consumed by a user interface.

Install dependencies:

```bash
cd frontend
npm install
```

Run the frontend:

```bash
npm run dev
```

The UI will be available at:

```
http://localhost:5173
```

The frontend communicates with the API via:

```
http://127.0.0.1:8000
```

---

# API Usage

Example endpoints:

```
GET /drivers
GET /races
GET /races/{raceId}/results
GET /seasons/{year}/driver-standings
GET /races/{raceId}/insights
```

Notes CRUD:

```
POST /notes
GET /notes
PUT /notes/{id}
DELETE /notes/{id}
```

Insight parameters:

```
mode = recap | impact
format = plain | radio
generator = template | llm
```

---

# Version History (apiV1 → apiV8)

| Version | Description |
|------|------|
| apiV1 | Basic FastAPI setup and initial endpoints |
| apiV2 | Drivers and races endpoints |
| apiV3 | Race results queries and improved SQL joins |
| apiV4 | Championship standings calculations |
| apiV5 | Fact pack analytics layer introduced |
| apiV6 | Narrative insight generation (template renderer) |
| apiV7 | Generative AI integration using local LLM |
| apiV8 | React frontend interface for interacting with the API |

---

# Testing / Quick Demo Calls

Health check:

```bash
curl http://127.0.0.1:8000/health
```

Drivers:

```bash
curl "http://127.0.0.1:8000/drivers?limit=5"
```

Race results:

```bash
curl "http://127.0.0.1:8000/races/1100/results"
```

Template insight:

```bash
curl "http://127.0.0.1:8000/races/1100/insights?mode=recap&format=radio&generator=template"
```

LLM insight:

```bash
curl "http://127.0.0.1:8000/races/1100/insights?mode=recap&format=radio&generator=llm"
```

Create note:

```bash
curl -X POST http://127.0.0.1:8000/notes \
-H "Content-Type: application/json" \
-d '{"entity_type":"race","entity_id":1100,"title":"My note","content":"Great race"}'
```

---