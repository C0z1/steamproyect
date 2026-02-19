"""
build_db.py
===========
Convierte los parquets de histograms/ a una base de datos DuckDB.

Uso:
  pip install duckdb pandas pyarrow
  python build_db.py --parquet-dir histograms --db steam.db
"""

import argparse
import logging
import os
import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("build_db")


def build(parquet_dir: str, db_path: str):
    con = duckdb.connect(db_path)

    logger.info("Creando tabla price_history desde %s/**/*.parquet ...", parquet_dir)

    con.execute(f"""
        CREATE OR REPLACE TABLE price_history AS
        SELECT *
        FROM read_parquet('{parquet_dir}/**/data.parquet', hive_partitioning=true)
        ORDER BY appid, timestamp
    """)

    total = con.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
    games = con.execute("SELECT COUNT(DISTINCT appid) FROM price_history").fetchone()[0]
    years = con.execute("SELECT MIN(year), MAX(year) FROM price_history").fetchone()

    logger.info("Tabla creada: %d registros, %d juegos, años %s-%s", total, games, years[0], years[1])

    # Índice para búsquedas rápidas por appid
    con.execute("CREATE INDEX IF NOT EXISTS idx_appid ON price_history(appid)")

    # Vista de metadatos por juego
    con.execute("""
        CREATE OR REPLACE VIEW game_stats AS
        SELECT
            appid,
            COUNT(*)                          AS total_records,
            MIN(timestamp)                    AS first_seen,
            MAX(timestamp)                    AS last_seen,
            MIN(price_usd)                    AS min_price,
            MAX(price_usd)                    AS max_price,
            ROUND(AVG(price_usd), 2)          AS avg_price,
            MAX(cut_pct)                      AS max_discount
        FROM price_history
        GROUP BY appid
    """)

    con.close()
    size_mb = os.path.getsize(db_path) / 1024 / 1024
    logger.info("Base de datos guardada en %s (%.1f MB)", db_path, size_mb)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet-dir", default="histograms")
    parser.add_argument("--db",          default="steam.db")
    args = parser.parse_args()
    build(args.parquet_dir, args.db)
