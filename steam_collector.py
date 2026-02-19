import argparse
import logging
import requests
import csv
import time
import os
import tempfile
import shutil

# tqdm is optional; fall back to identity iterator if missing
try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, **kwargs):
        return x

# ===============================
# CONFIG
# ===============================
OUTPUT_FILE = "steam_games_dataset.csv"
METADATA_FILE = "steam_metadata.csv"
REVIEWS_FILE = "steam_reviews.csv"
PRICING_FILE = "steam_pricing.csv"
GENRES_FILE = "steam_genres.csv"
ML_FILE = "steam_ml_dataset.csv"
COUNTRY_CODE = "us"
LANG = "en"
REQUEST_DELAY = 1.2        # seconds (do not go lower)
MAX_APPS = 100             # 🔴 set to None for full run
TOP_N_MOST_RATED = 300    # set to None to disable (collects top-N by review count)

# ===============================
# HEADERS
# ===============================
HEADERS = {
    "User-Agent": "SteamDataCollector/1.0 (academic project)"
}


def get_session(retries: int = 3, backoff_factor: float = 0.5, status_forcelist=(500, 502, 503, 504)):
    """Create a requests Session with retry/backoff behavior."""
    from requests.adapters import HTTPAdapter
    try:
        from urllib3.util import Retry
    except Exception:
        # Fallback: simple session without advanced Retry if urllib3 missing
        s = requests.Session()
        s.headers.update(HEADERS)
        return s

    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=frozenset(["GET", "POST"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HEADERS)
    return s

# module-level session (will be set in main)
SESSION = None

# logger
logger = logging.getLogger("steam_collector")


def _atomic_write_full(path, header, rows):
    d = os.path.dirname(path) or '.'
    fd, tmp = tempfile.mkstemp(prefix='.tmp_', dir=d)
    os.close(fd)
    try:
        with open(tmp, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass


def append_rows_safe(path, rows, header):
    """Append rows to CSV safely.

    - If file doesn't exist: write header + rows atomically.
    - If file exists: validate header matches; if not, move old file to .bak and create new with header+rows.
    - If header matches: append rows (fast path).
    """
    if not rows:
        return

    if os.path.exists(path):
        try:
            with open(path, 'r', newline='', encoding='utf-8') as f:
                existing = next(csv.reader(f), None)
        except Exception:
            existing = None

        if existing != header:
            # backup old file and write new atomically
            try:
                shutil.move(path, path + '.bak')
            except Exception:
                pass
            _atomic_write_full(path, header, rows)
            return

        # header matches — append
        with open(path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
    else:
        _atomic_write_full(path, header, rows)

# ===============================
# API ENDPOINTS (FIXED)
# ===============================
APP_LIST_URL = "https://api.steampowered.com/ISteamApps/GetAppList/v0002/"
STEAMSPY_URL = "https://steamspy.com/api.php?request=all"
DETAILS_URL = "https://store.steampowered.com/api/appdetails"
REVIEWS_URL = "https://store.steampowered.com/appreviews/"

# ===============================
# HELPERS
# ===============================
def get_all_appids():
    logger.info("Fetching Steam App list...")
    sess = SESSION or requests
    try:
        r = sess.get(APP_LIST_URL, timeout=30)
        r.raise_for_status()
        apps = r.json().get("applist", {}).get("apps", [])
        if apps:
            return apps[:MAX_APPS] if MAX_APPS else apps
    except Exception:
        logger.warning("Primary API failed; attempting SteamSpy fallback...")

    # Fallback: use SteamSpy's full app list (returns {appid: {...}})
    try:
        r2 = requests.get(STEAMSPY_URL, headers=HEADERS, timeout=30)
        r2.raise_for_status()
        data = r2.json()
        apps = [{"appid": int(k), "name": v.get("name")} for k, v in data.items()]
        apps.sort(key=lambda x: x["appid"])  # deterministic ordering
        return apps[:MAX_APPS] if MAX_APPS else apps
    except Exception as e:
        raise RuntimeError(f"Unable to fetch app list from Steam APIs: {e}")


def get_most_rated_appids(n):
    """Return list of apps (dicts with 'appid' and 'name') of the top-n apps
    sorted by total user reviews (positive + negative) using SteamSpy data.
    """
    if not n or n <= 0:
        return []

    logger.info(f"Fetching top {n} most-rated apps via SteamSpy...")
    sess = SESSION or requests
    r = sess.get(STEAMSPY_URL, timeout=60)
    r.raise_for_status()
    data = r.json()

    apps = []
    for k, v in data.items():
        try:
            pos = int(v.get("positive") or 0)
            neg = int(v.get("negative") or 0)
        except Exception:
            pos = 0
            neg = 0
        total = pos + neg
        apps.append({"appid": int(k), "name": v.get("name"), "total_reviews": total})

    apps.sort(key=lambda x: x["total_reviews"], reverse=True)
    return apps[:n]

def get_app_details(appid):
    params = {
        "appids": appid,
        "cc": COUNTRY_CODE,
        "l": LANG
    }

    sess = SESSION or requests
    r = sess.get(
        DETAILS_URL,
        params=params,
        timeout=30
    )

    if r.status_code != 200:
        return None

    payload = r.json().get(str(appid))
    if not payload or not payload.get("success"):
        return None

    return payload["data"]

def get_review_data(appid):
    sess = SESSION or requests
    r = sess.get(
        f"{REVIEWS_URL}{appid}",
        params={"json": 1},
        timeout=30
    )

    if r.status_code != 200:
        return {}

    return r.json().get("query_summary", {})

def extract_price(data):
    if data.get("is_free"):
        return 0.0, 0, True

    price = data.get("price_overview")
    if not price:
        return None, None, False

    return price["final"] / 100, price["discount_percent"], False

# ===============================
# MAIN
# ===============================
def main():
    global SESSION, MAX_APPS, TOP_N_MOST_RATED, REQUEST_DELAY, COUNTRY_CODE, LANG, OUTPUT_FILE, METADATA_FILE, REVIEWS_FILE, PRICING_FILE, GENRES_FILE, ML_FILE

    parser = argparse.ArgumentParser(description="Steam data collector")
    parser.add_argument("--max-apps", type=int, default=MAX_APPS)
    parser.add_argument("--top-n-most-rated", type=int, default=TOP_N_MOST_RATED)
    parser.add_argument("--request-delay", type=float, default=REQUEST_DELAY)
    parser.add_argument("--country-code", default=COUNTRY_CODE)
    parser.add_argument("--lang", default=LANG)
    parser.add_argument("--output-file", default=OUTPUT_FILE)
    parser.add_argument("--metadata-file", default=METADATA_FILE)
    parser.add_argument("--reviews-file", default=REVIEWS_FILE)
    parser.add_argument("--pricing-file", default=PRICING_FILE)
    parser.add_argument("--genres-file", default=GENRES_FILE)
    parser.add_argument("--ml-file", default=ML_FILE)
    args = parser.parse_args()

    # apply overrides
    MAX_APPS = args.max_apps
    TOP_N_MOST_RATED = args.top_n_most_rated
    REQUEST_DELAY = args.request_delay
    COUNTRY_CODE = args.country_code
    LANG = args.lang
    OUTPUT_FILE = args.output_file
    METADATA_FILE = args.metadata_file
    REVIEWS_FILE = args.reviews_file
    PRICING_FILE = args.pricing_file
    GENRES_FILE = args.genres_file
    ML_FILE = args.ml_file

    # configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # create resilient session
    SESSION = get_session()

    # If TOP_N_MOST_RATED is set, use SteamSpy to pick the top N by review counts.
    if TOP_N_MOST_RATED:
        apps = get_most_rated_appids(TOP_N_MOST_RATED)
    else:
        apps = get_all_appids()

    # Ensure CSVs exist with correct headers (atomic)
    append_rows_safe(METADATA_FILE, [], ["appId", "nombre", "release_date", "metacritic_score", "genres", "platforms", "is_free"])
    append_rows_safe(REVIEWS_FILE, [], ["appId", "total_reviews", "positive_ratio", "review_score"])
    append_rows_safe(PRICING_FILE, [], ["appId", "price", "discount_percent", "is_free"])
    append_rows_safe(GENRES_FILE, [], ["appId", "genre"])
    append_rows_safe(ML_FILE, [], ["appId", "metacritic_score", "total_reviews", "positive_ratio", "price", "discount_percent", "is_free", "num_genres", "platforms_count"])

    for app in tqdm(apps):
        appid = app["appid"]

        try:
            details = get_app_details(appid)
            if not details:
                continue

            # Only video games
            if details.get("type") != "game":
                continue

            reviews = get_review_data(appid) or {}

            price, discount, is_free = extract_price(details)

            release_info = details.get("release_date", {})
            release_date = None
            if not release_info.get("coming_soon"):
                release_date = release_info.get("date")

            # Compute review totals and positive ratio safely
            total_reviews = None
            positive = None
            if reviews:
                total_reviews = reviews.get("total_reviews") or reviews.get("review_count")
                positive = reviews.get("total_positive") or reviews.get("total_positive_reviews")

            positive_ratio = None
            try:
                if total_reviews and positive:
                    positive_ratio = float(positive) / float(total_reviews)
            except Exception:
                positive_ratio = None

            # Genres and platforms
            genres_list = [g.get("description") for g in details.get("genres", []) if g.get("description")]
            platforms = [p for p, v in details.get("platforms", {}).items() if v]

            # Prepare rows
            meta_row = [
                appid,
                details.get("name"),
                release_date,
                details.get("metacritic", {}).get("score"),
                ";".join(genres_list),
                ";".join(platforms),
                is_free
            ]

            rev_row = [
                appid,
                total_reviews,
                positive_ratio,
                reviews.get("review_score")
            ]

            pr_row = [
                appid,
                price,
                discount,
                is_free
            ]

            gen_rows = [[appid, g] for g in genres_list]

            num_genres = len(genres_list)
            platforms_count = len(platforms)
            ml_row = [
                appid,
                details.get("metacritic", {}).get("score"),
                total_reviews,
                positive_ratio,
                price,
                discount,
                int(bool(is_free)),
                num_genres,
                platforms_count
            ]

            # Append safely (atomic when creating or header mismatch)
            append_rows_safe(METADATA_FILE, [meta_row], ["appId", "nombre", "release_date", "metacritic_score", "genres", "platforms", "is_free"])
            append_rows_safe(REVIEWS_FILE, [rev_row], ["appId", "total_reviews", "positive_ratio", "review_score"])
            append_rows_safe(PRICING_FILE, [pr_row], ["appId", "price", "discount_percent", "is_free"])
            if gen_rows:
                append_rows_safe(GENRES_FILE, gen_rows, ["appId", "genre"])
            append_rows_safe(ML_FILE, [ml_row], ["appId", "metacritic_score", "total_reviews", "positive_ratio", "price", "discount_percent", "is_free", "num_genres", "platforms_count"])

            time.sleep(REQUEST_DELAY)

        except Exception:
            logger.exception("Error with appid %s", appid)

    logger.info("Dataset saved to %s (files: %s, %s, %s, %s, %s)", OUTPUT_FILE, METADATA_FILE, REVIEWS_FILE, PRICING_FILE, GENRES_FILE, ML_FILE)

# ===============================
if __name__ == "__main__":
    main()
