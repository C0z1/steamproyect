"""
steam_price_history.py
======================
Recolecta el historial de precios de los top-N juegos de Steam usando:
  - SteamSpy  -> lista de appids mas resenados
  - Steam API -> detalles del juego (nombre, tipo)
  - ITAD API  -> historial real de precios por juego

Genera parquets particionados por anio y appid:
  histograms/
    year=2023/
      id=10/
        data.parquet
    year=2024/
      ...

Uso:
  pip install requests pandas pyarrow tqdm
  python steam_price_history.py --itad-key TU_API_KEY
"""

import argparse
import logging
import os
import time
from datetime import datetime

import pandas as pd
import requests
from requests.adapters import HTTPAdapter

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(x, **kwargs):
        return x

try:
    from urllib3.util import Retry
    HAS_RETRY = True
except ImportError:
    HAS_RETRY = False

# ===============================
# CONFIG
# ===============================
TOP_N_MOST_RATED  = 300
HISTORY_SINCE     = "2022-01-01T00:00:00Z"
STEAM_SHOP_ID     = 61
COUNTRY_CODE      = "US"
REQUEST_DELAY     = 1.2
OUTPUT_DIR        = "histograms"

STEAMSPY_URL      = "https://steamspy.com/api.php?request=all"
STEAM_DETAILS_URL = "https://store.steampowered.com/api/appdetails"
ITAD_LOOKUP_URL   = "https://api.isthereanydeal.com/games/lookup/v1"
ITAD_HISTORY_URL  = "https://api.isthereanydeal.com/games/history/v2"

HEADERS = {"User-Agent": "SteamPriceHistoryCollector/1.0 (academic project)"}

logger = logging.getLogger("steam_price_history")
SESSION = None


# ===============================
# SESSION
# ===============================
def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    if HAS_RETRY:
        retry = Retry(
            total=4,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
    return s


# ===============================
# PASO 1 - Lista de juegos (SteamSpy)
# ===============================
def get_top_apps(n: int) -> list:
    logger.info("Obteniendo top %d juegos de SteamSpy...", n)
    r = SESSION.get(STEAMSPY_URL, timeout=60)
    r.raise_for_status()
    data = r.json()

    apps = []
    for appid_str, v in data.items():
        try:
            pos = int(v.get("positive") or 0)
            neg = int(v.get("negative") or 0)
        except (ValueError, TypeError):
            pos = neg = 0
        apps.append({
            "appid": int(appid_str),
            "name": v.get("name", ""),
            "total_reviews": pos + neg,
        })

    apps.sort(key=lambda x: x["total_reviews"], reverse=True)
    logger.info("SteamSpy devolvio %d apps; limitando a %d.", len(apps), n)
    return apps[:n]


# ===============================
# PASO 2 - Verificar que sea un juego
# ===============================
def is_game(appid: int) -> bool:
    try:
        r = SESSION.get(
            STEAM_DETAILS_URL,
            params={"appids": appid, "cc": COUNTRY_CODE.lower(), "l": "en"},
            timeout=20,
        )
        if r.status_code != 200:
            return False
        payload = r.json().get(str(appid), {})
        if not payload.get("success"):
            return False
        return payload.get("data", {}).get("type") == "game"
    except Exception:
        return False


# ===============================
# PASO 3 - Obtener ITAD game ID
# ===============================
def get_itad_id(appid: int, api_key: str):
    try:
        r = SESSION.get(
            ITAD_LOOKUP_URL,
            params={"appid": appid, "key": api_key},   # <-- key como query param
            timeout=15,
        )
        if r.status_code != 200:
            logger.debug("ITAD lookup devolvio %d para appid=%d", r.status_code, appid)
            return None
        data = r.json()
        if data.get("found"):
            return data["game"]["id"]
    except Exception as exc:
        logger.warning("Error en lookup para appid=%d: %s", appid, exc)
    return None


# ===============================
# PASO 4 - Historial de precios (ITAD)
# ===============================
def get_price_history(itad_id: str, api_key: str, since: str) -> list:
    try:
        params = {
            "id": itad_id,
            "country": COUNTRY_CODE,
            "since": since,
            "key": api_key,                            # <-- key como query param
        }
        if STEAM_SHOP_ID is not None:
            params["shops"] = STEAM_SHOP_ID

        r = SESSION.get(
            ITAD_HISTORY_URL,
            params=params,
            timeout=20,
        )
        if r.status_code != 200:
            logger.debug("ITAD history devolvio %d para id=%s", r.status_code, itad_id)
            return []
        raw = r.json()
    except Exception as exc:
        logger.warning("Error obteniendo historial para %s: %s", itad_id, exc)
        return []

    records = []
    for entry in raw:
        try:
            ts = datetime.fromisoformat(entry["timestamp"].replace("Z", "+00:00"))
            deal        = entry.get("deal", {}) or {}
            price_obj   = deal.get("price", {}) or {}
            regular_obj = deal.get("regular", {}) or {}
            shop        = entry.get("shop", {}) or {}

            records.append({
                "timestamp":   ts,
                "price_usd":   price_obj.get("amount"),
                "regular_usd": regular_obj.get("amount"),
                "cut_pct":     deal.get("cut"),
                "shop_id":     shop.get("id"),
                "shop_name":   shop.get("name"),
            })
        except Exception:
            continue

    return records


# ===============================
# PASO 5 - Escribir parquet particionado
# ===============================
def write_parquet(appid: int, records: list, output_dir: str) -> int:
    if not records:
        return 0

    df = pd.DataFrame(records)
    df["appid"] = appid
    df["year"]  = df["timestamp"].dt.year

    written = 0
    for year, group in df.groupby("year"):
        path = os.path.join(output_dir, f"year={year}", f"id={appid}")
        os.makedirs(path, exist_ok=True)
        out_file = os.path.join(path, "data.parquet")

        if os.path.exists(out_file):
            existing = pd.read_parquet(out_file)
            group = pd.concat([existing, group], ignore_index=True)
            group = group.drop_duplicates(subset=["timestamp", "shop_id"])

        group.to_parquet(out_file, index=False, engine="pyarrow")
        written += 1

    return written


# ===============================
# MAIN
# ===============================
def main():
    global SESSION, COUNTRY_CODE, REQUEST_DELAY, STEAM_SHOP_ID

    parser = argparse.ArgumentParser(description="Steam price history - parquet particionado")
    parser.add_argument("--itad-key",        required=True,  help="API key de IsThereAnyDeal")
    parser.add_argument("--top-n",           type=int,   default=TOP_N_MOST_RATED)
    parser.add_argument("--since",           default=HISTORY_SINCE,
                        help="Fecha ISO 8601 desde cuando traer historial (ej: 2022-01-01T00:00:00Z)")
    parser.add_argument("--country",         default=COUNTRY_CODE)
    parser.add_argument("--output-dir",      default=OUTPUT_DIR)
    parser.add_argument("--delay",           type=float, default=REQUEST_DELAY)
    parser.add_argument("--all-shops",       action="store_true",
                        help="Incluir todos los shops, no solo Steam")
    parser.add_argument("--skip-game-check", action="store_true",
                        help="Omitir verificacion de tipo game en Steam API (mas rapido)")
    parser.add_argument("--log-level",       default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    COUNTRY_CODE  = args.country
    REQUEST_DELAY = args.delay
    if args.all_shops:
        STEAM_SHOP_ID = None

    SESSION = make_session()
    os.makedirs(args.output_dir, exist_ok=True)

    apps = get_top_apps(args.top_n)

    stats = {"procesados": 0, "sin_itad_id": 0, "sin_historial": 0,
             "parquets_escritos": 0, "errores": 0}

    for app in tqdm(apps, desc="Procesando juegos"):
        appid = app["appid"]
        name  = app.get("name", f"appid_{appid}")

        try:
            if not args.skip_game_check:
                if not is_game(appid):
                    logger.debug("Saltando appid=%d (%s): no es juego", appid, name)
                    time.sleep(args.delay * 0.5)
                    continue
                time.sleep(args.delay * 0.5)

            itad_id = get_itad_id(appid, args.itad_key)
            if not itad_id:
                logger.info("Sin ITAD ID para appid=%d (%s)", appid, name)
                stats["sin_itad_id"] += 1
                time.sleep(args.delay)
                continue

            records = get_price_history(itad_id, args.itad_key, args.since)
            if not records:
                logger.info("Sin historial para appid=%d (%s)", appid, name)
                stats["sin_historial"] += 1
                time.sleep(args.delay)
                continue

            logger.info("appid=%d (%s): %d registros de precio", appid, name, len(records))

            written = write_parquet(appid, records, args.output_dir)
            stats["parquets_escritos"] += written
            stats["procesados"] += 1

        except Exception:
            logger.exception("Error procesando appid=%d (%s)", appid, name)
            stats["errores"] += 1

        time.sleep(args.delay)

    logger.info("=" * 50)
    logger.info("RESUMEN FINAL")
    logger.info("  Juegos con datos escritos : %d", stats["procesados"])
    logger.info("  Sin ITAD ID               : %d", stats["sin_itad_id"])
    logger.info("  Sin historial de precios  : %d", stats["sin_historial"])
    logger.info("  Archivos parquet escritos : %d", stats["parquets_escritos"])
    logger.info("  Errores                   : %d", stats["errores"])
    logger.info("Directorio de salida        : %s", os.path.abspath(args.output_dir))
    logger.info("=" * 50)

    print("\nEstructura generada:")
    for root, dirs, files in os.walk(args.output_dir):
        depth = root.replace(args.output_dir, "").count(os.sep)
        indent = "  " * depth
        print(f"{indent}{os.path.basename(root)}/")
        for f in files:
            print(f"{indent}  {f}")


if __name__ == "__main__":
    main()
