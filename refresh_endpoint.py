# ── GET /refresh ─────────────────────────────────────────────
@app.get("/refresh")
async def refresh_data(
    itad_key: str = Query(..., description="ITAD API key"),
    top_n: int = Query(50, le=200),
):
    """
    Llama a IsThereAnyDeal API y recarga los datos en memoria.
    Los datos duran hasta que Render reinicia el servidor.
    """
    import httpx
    import pandas as pd

    ITAD_BASE = "https://api.isthereanydeal.com"
    results = {"loaded": 0, "errors": 0, "games": []}

    async with httpx.AsyncClient(timeout=30) as client:

        # 1. Obtener lista de juegos populares de Steam via SteamSpy
        try:
            spy = await client.get(
                "https://steamspy.com/api.php",
                params={"request": "top100forever"},
            )
            spy_data = spy.json()
            appids = list(spy_data.keys())[:top_n]
        except Exception as e:
            raise HTTPException(500, f"SteamSpy error: {e}")

        rows_all = []

        for appid_str in appids:
            appid = int(appid_str)
            try:
                # 2. Buscar game_id en ITAD por Steam appid
                lookup = await client.get(
                    f"{ITAD_BASE}/games/lookup/v1",
                    params={"key": itad_key, "appid": appid},
                )
                if lookup.status_code != 200:
                    results["errors"] += 1
                    continue

                ldata = lookup.json()
                game_id = ldata.get("game", {}).get("id")
                if not game_id:
                    results["errors"] += 1
                    continue

                # 3. Obtener historial de precios
                hist = await client.get(
                    f"{ITAD_BASE}/games/history/v2",
                    params={
                        "key": itad_key,
                        "id": game_id,
                        "country": "US",
                        "since": "2020-01-01T00:00:00Z",
                    },
                )
                if hist.status_code != 200:
                    results["errors"] += 1
                    continue

                hdata = hist.json()
                prices = hdata.get("prices", [])
                if not prices:
                    results["errors"] += 1
                    continue

                for entry in prices:
                    for shop_entry in entry.get("cut", [{}]):
                        rows_all.append({
                            "timestamp": pd.Timestamp(entry.get("timestamp", "")),
                            "price_usd": entry.get("price", {}).get("amount", 0),
                            "regular_usd": entry.get("regular", {}).get("amount", 0),
                            "cut_pct": entry.get("cut", 0),
                            "shop_id": entry.get("shop", {}).get("id", ""),
                            "shop_name": entry.get("shop", {}).get("name", "Steam"),
                            "appid": appid,
                            "year": pd.Timestamp(entry.get("timestamp", "")).year,
                        })

                results["loaded"] += 1
                results["games"].append(appid)

            except Exception:
                results["errors"] += 1
                continue

    if not rows_all:
        raise HTTPException(500, "No se pudieron cargar datos de ITAD")

    # 4. Cargar en DuckDB en memoria
    import pandas as pd
    df = pd.DataFrame(rows_all)

    db().execute("DROP VIEW IF EXISTS price_history")
    db().execute("DROP VIEW IF EXISTS game_stats")
    db().register("_refresh_df", df)
    db().execute("CREATE OR REPLACE VIEW price_history AS SELECT * FROM _refresh_df")
    db().execute("""
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

    results["total_records"] = len(df)
    return results
