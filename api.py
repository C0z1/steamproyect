"""
api.py
======
Backend FastAPI - SteamPulse Price Analytics
Lee historial de precios desde archivos Parquet particionados.

Estructura esperada:
  histograms/
    year=2022/id=730/data.parquet
    year=2023/id=730/data.parquet

Uso:
  pip install fastapi uvicorn duckdb numpy scikit-learn
  uvicorn api:app --reload
"""

import os
import datetime
from contextlib import asynccontextmanager
from typing import Optional

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# ── CONFIG ───────────────────────────────────────────────────
PARQUET_GLOB = os.getenv("PARQUET_GLOB", "histograms/**/*.parquet")

_con: duckdb.DuckDBPyConnection = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _con
    _con = duckdb.connect(
        config={
            "memory_limit": "128MB",
            "threads": 1,
        }
    )

    # Vista principal sobre todos los parquet particionados
    _con.execute(f"""
        CREATE OR REPLACE VIEW price_history AS
        SELECT
            timestamp,
            price_usd,
            regular_usd,
            cut_pct,
            shop_id,
            shop_name,
            appid,
            year
        FROM read_parquet('{PARQUET_GLOB}', hive_partitioning=true)
    """)

    # Vista de estadisticas agregadas por juego
    _con.execute("""
        CREATE OR REPLACE VIEW game_stats AS
        SELECT
            appid,
            COUNT(*)                        AS total_records,
            MIN(timestamp)::VARCHAR         AS first_seen,
            MAX(timestamp)::VARCHAR         AS last_seen,
            MIN(price_usd)                  AS min_price,
            MAX(price_usd)                  AS max_price,
            ROUND(AVG(price_usd), 2)        AS avg_price,
            MAX(cut_pct)                    AS max_discount
        FROM price_history
        GROUP BY appid
    """)

    yield
    _con.close()


app = FastAPI(title="SteamPulse API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def db():
    return _con


# ── GET / ────────────────────────────────────────────────────
@app.get("/")
def root():
    import glob
    files = glob.glob("histograms/**/*.parquet", recursive=True)
    return {"status": "ok", "parquets_found": len(files), "cwd": os.getcwd()}


# ── GET /health ──────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok"}


# ── GET /summary ─────────────────────────────────────────────
@app.get("/summary")
def summary():
    row = db().execute("""
        SELECT
            COUNT(DISTINCT appid)       AS total_games,
            COUNT(*)                    AS total_records,
            MIN(year)                   AS year_from,
            MAX(year)                   AS year_to,
            ROUND(AVG(price_usd), 2)    AS global_avg_price,
            MIN(price_usd)              AS global_min_price,
            MAX(price_usd)              AS global_max_price
        FROM price_history
    """).fetchdf()
    return row.iloc[0].to_dict()


# ── GET /years ────────────────────────────────────────────────
@app.get("/years")
def available_years():
    rows = db().execute("""
        SELECT
            year,
            COUNT(DISTINCT appid)   AS games,
            COUNT(*)                AS records
        FROM price_history
        GROUP BY year
        ORDER BY year
    """).fetchdf()
    return rows.to_dict(orient="records")


# ── GET /games ───────────────────────────────────────────────
@app.get("/games")
def list_games(limit: int = Query(50, le=200), offset: int = 0):
    rows = db().execute(f"""
        SELECT
            appid,
            total_records,
            first_seen,
            last_seen,
            min_price,
            max_price,
            avg_price,
            max_discount
        FROM game_stats
        ORDER BY total_records DESC
        LIMIT {limit} OFFSET {offset}
    """).fetchdf()
    return rows.to_dict(orient="records")


# ── GET /games/{appid} ───────────────────────────────────────
@app.get("/games/{appid}")
def game_detail(appid: int):
    row = db().execute(f"""
        SELECT * FROM game_stats WHERE appid = {appid}
    """).fetchdf()
    if row.empty:
        raise HTTPException(404, f"appid {appid} no encontrado")
    return row.iloc[0].to_dict()


# ── GET /games/{appid}/history ───────────────────────────────
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
        SELECT
            timestamp::VARCHAR  AS timestamp,
            price_usd,
            regular_usd,
            cut_pct,
            shop_name
        FROM price_history
        WHERE {where}
        ORDER BY timestamp
    """).fetchdf()

    if rows.empty:
        raise HTTPException(404, f"Sin historial para appid {appid}")

    return {
        "appid": appid,
        "count": len(rows),
        "history": rows.to_dict(orient="records"),
    }


# ── GET /top-discounts ───────────────────────────────────────
@app.get("/top-discounts")
def top_discounts(limit: int = Query(10, le=50)):
    rows = db().execute(f"""
        SELECT
            appid,
            max_discount,
            min_price,
            avg_price
        FROM game_stats
        WHERE max_discount > 0
        ORDER BY max_discount DESC
        LIMIT {limit}
    """).fetchdf()
    return rows.to_dict(orient="records")


# ── GET /search ──────────────────────────────────────────────
@app.get("/search")
def search_games(q: str = Query(..., min_length=1), limit: int = 10):
    """Buscar juegos por appid"""
    rows = db().execute(f"""
        SELECT
            appid,
            total_records,
            avg_price,
            max_discount
        FROM game_stats
        WHERE CAST(appid AS VARCHAR) LIKE '%{q}%'
        ORDER BY total_records DESC
        LIMIT {limit}
    """).fetchdf()
    return rows.to_dict(orient="records")


# ── GET /games/{appid}/predict ───────────────────────────────
@app.get("/games/{appid}/predict")
def predict_price(appid: int, days: int = Query(30, le=180)):
    """
    Prediccion de precio futuro usando regresion lineal simple
    sobre el historial de precios del juego.
    """
    try:
        import numpy as np
        from sklearn.linear_model import LinearRegression
    except ImportError:
        raise HTTPException(500, "numpy/scikit-learn no instalado en el servidor")

    rows = db().execute(f"""
        SELECT
            epoch(timestamp)    AS ts_epoch,
            price_usd
        FROM price_history
        WHERE appid = {appid}
          AND price_usd IS NOT NULL
          AND price_usd > 0
        ORDER BY timestamp
    """).fetchdf()

    if len(rows) < 5:
        raise HTTPException(400, "Historial insuficiente para predecir (minimo 5 puntos)")

    X = rows["ts_epoch"].values.reshape(-1, 1)
    y = rows["price_usd"].values

    model = LinearRegression()
    model.fit(X, y)

    last_ts = X[-1][0]
    day_seconds = 86400
    future_epochs = [last_ts + day_seconds * i for i in range(1, days + 1)]
    predictions = model.predict(np.array(future_epochs).reshape(-1, 1))
    predictions = [max(0.0, round(float(p), 2)) for p in predictions]

    future_dates = [
        datetime.datetime.fromtimestamp(e).strftime("%Y-%m-%d")
        for e in future_epochs
    ]

    return {
        "appid": appid,
        "days": days,
        "r2_score": round(float(model.score(X, y)), 4),
        "trend": "down" if model.coef_[0] < 0 else "up",
        "current_price": round(float(y[-1]), 2),
        "predicted_price_end": predictions[-1],
        "predictions": [
            {"date": d, "price_usd": p}
            for d, p in zip(future_dates, predictions)
        ],
    }