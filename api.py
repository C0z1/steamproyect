"""
api.py
======
Backend FastAPI que sirve el historial de precios desde DuckDB.

Uso:
  pip install fastapi uvicorn duckdb
  uvicorn api:app --reload

Endpoints:
  GET /games              → lista todos los juegos con estadísticas
  GET /games/{appid}      → estadísticas de un juego
  GET /games/{appid}/history  → historial de precios
  GET /search?q=...       → buscar juegos por nombre (requiere steamspy cache)
"""

import os
from contextlib import asynccontextmanager
from typing import Optional

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

DB_PATH = os.getenv("STEAM_DB", "steam.db")
_con: duckdb.DuckDBPyConnection = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _con
    _con = duckdb.connect(DB_PATH, read_only=True)
    yield
    _con.close()


app = FastAPI(title="Steam Price History API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def db():
    return _con


# ── GET /games ──────────────────────────────────────────────
@app.get("/games")
def list_games(limit: int = Query(100, le=500), offset: int = 0):
    rows = db().execute(f"""
        SELECT appid, total_records, first_seen, last_seen,
               min_price, max_price, avg_price, max_discount
        FROM game_stats
        ORDER BY total_records DESC
        LIMIT {limit} OFFSET {offset}
    """).fetchdf()
    return rows.to_dict(orient="records")


# ── GET /games/{appid} ──────────────────────────────────────
@app.get("/games/{appid}")
def game_stats(appid: int):
    row = db().execute(f"""
        SELECT * FROM game_stats WHERE appid = {appid}
    """).fetchdf()
    if row.empty:
        raise HTTPException(404, f"appid {appid} no encontrado")
    return row.iloc[0].to_dict()


# ── GET /games/{appid}/history ──────────────────────────────
@app.get("/games/{appid}/history")
def price_history(
    appid: int,
    since: Optional[str] = None,
    until: Optional[str] = None,
    year: Optional[int] = None,
):
    filters = [f"appid = {appid}"]
    if year:
        filters.append(f"year = {year}")
    if since:
        filters.append(f"timestamp >= '{since}'")
    if until:
        filters.append(f"timestamp <= '{until}'")

    where = " AND ".join(filters)
    rows = db().execute(f"""
        SELECT timestamp, price_usd, regular_usd, cut_pct, shop_name
        FROM price_history
        WHERE {where}
        ORDER BY timestamp
    """).fetchdf()

    if rows.empty:
        raise HTTPException(404, f"Sin historial para appid {appid}")

    # Convertir timestamps a string ISO para JSON
    rows["timestamp"] = rows["timestamp"].astype(str)
    return {
        "appid": appid,
        "count": len(rows),
        "history": rows.to_dict(orient="records"),
    }


# ── GET /summary ─────────────────────────────────────────────
@app.get("/summary")
def summary():
    row = db().execute("""
        SELECT
            COUNT(DISTINCT appid)    AS total_games,
            COUNT(*)                 AS total_records,
            MIN(year)                AS year_from,
            MAX(year)                AS year_to,
            ROUND(AVG(price_usd),2)  AS global_avg_price,
            MIN(price_usd)           AS global_min_price,
            MAX(price_usd)           AS global_max_price
        FROM price_history
    """).fetchdf()
    return row.iloc[0].to_dict()


# ── GET /years ────────────────────────────────────────────────
@app.get("/years")
def available_years():
    rows = db().execute("""
        SELECT year, COUNT(DISTINCT appid) AS games, COUNT(*) AS records
        FROM price_history
        GROUP BY year ORDER BY year
    """).fetchdf()
    return rows.to_dict(orient="records")


# ── GET /top-discounts ────────────────────────────────────────
@app.get("/top-discounts")
def top_discounts(limit: int = 10):
    rows = db().execute(f"""
        SELECT appid, max_discount, min_price, avg_price
        FROM game_stats
        ORDER BY max_discount DESC
        LIMIT {limit}
    """).fetchdf()
    return rows.to_dict(orient="records")
