# F1 Insight API (COMP3011 Coursework 1)

A FastAPI + SQLite REST API for historical Formula 1 data (Ergast-style Kaggle dataset) with:
- deterministic analytics ("fact packs")
- narrative insight generation (template + local LLM)
- a full CRUD model (`notes`) for user-generated content
- optional F1-themed frontend (apiV8)

This project is designed to demonstrate clean API engineering, layered architecture, and controlled Generative AI integration.

---

## Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Setup](#setup)
  - [Backend Setup](#backend-setup)
  - [Database](#database)
  - [Optional: Local LLM (Ollama)](#optional-local-llm-ollama)
  - [Optional: Frontend (apiV8)](#optional-frontend-apiv8)
- [API Usage](#api-usage)
- [Version History (apiV1 → apiV8)](#version-history-apiv1--apiv8)
- [Testing / Quick Demo Calls](#testing--quick-demo-calls)

---

## Project Overview

Most sports APIs expose structured tables (standings, results, lap times) but provide limited explanation of why a result mattered. This API exposes core F1 entities (drivers, races, constructors, results) and adds a narrative layer that reframes data into:

- **Recap** mode: "What happened?"
- **Impact** mode: "Why it mattered?"
- **Plain** format: concise technical summary
- **Radio** format: fan-friendly commentary style

To keep outputs grounded and auditable, narrative generation is built on deterministic **fact packs** (structured analytics outputs), and GenAI (LLM) is optional and controlled.

---

## Architecture

Layered design:

1. **Database layer (SQLite)**
   - Read-only historical F1 dataset tables (drivers/races/results/constructors etc.)
   - Separate writable model for user content (`notes`)

2. **Fact pack analytics layer**
   - Aggregates race/season statistics into deterministic JSON structures

3. **Insight layer**
   - Renders narrative insight from fact packs using:
     - deterministic templates (reproducible baseline)
     - optional local LLM (Ollama) using structured prompting

4. **API layer (FastAPI)**
   - REST endpoints + validation + error handling

5. **Frontend**
   - F1-themed UI consuming the API

---

## Setup

### Backend Setup

From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate

python -m pip install -U pip
python -m pip install -r requirements.txt