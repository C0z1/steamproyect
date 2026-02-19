import argparse
import csv
import datetime
import json
import re
import time
from collections import defaultdict

import pandas as pd
import requests
import os


START_DATE = datetime.date(2024, 1, 1)
OUT_CSV = "monthly_prices_since_2024.csv"


def append_rows_to_csv(path, rows, header=False):
    if not rows:
        return
    keys = rows[0].keys()
    mode = 'a' if header is False and os.path.exists(path) else 'w'
    with open(path, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(keys))
        if mode == 'w':
            writer.writeheader()
        writer.writerows(rows)



def month_range(start, end):
    cur = datetime.date(start.year, start.month, 1)
    while cur <= end:
        yield cur.strftime("%Y-%m")
        # next month
        if cur.month == 12:
            cur = datetime.date(cur.year + 1, 1, 1)
        else:
            cur = datetime.date(cur.year, cur.month + 1, 1)


def fetch_steamdb(appid):
    url = f"https://steamdb.info/api/GetPriceHistory/?appid={appid}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def fetch_store_pricehistory_html(appid):
    url = f"https://store.steampowered.com/pricehistory/?appid={appid}"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None


def parse_store_html_for_prices(html):
    # Best-effort: try to find arrays of [timestamp_ms,price]
    # Look for JSON-like arrays inside script tags
    patterns = [r"\[\s*\[\d{10,13},.*?\]\s*(?:,\s*\[\d{10,13},.*?\]\s*)+\]",
                r"rgPriceHistory\s*=\s*(\[.*?\])"]
    for pat in patterns:
        m = re.search(pat, html, flags=re.S)
        if m:
            text = m.group(0)
            # try to extract numbers
            try:
                arr = json.loads(text)
                return arr
            except Exception:
                # try to coerce single quotes to double
                try:
                    text2 = text.replace("'", '"')
                    arr = json.loads(text2)
                    return arr
                except Exception:
                    continue
    return None


def aggregate_monthly(appid, points):
    # points expected as list of [timestamp_ms, price]
    if not points:
        return {}
    daily = defaultdict(list)
    for p in points:
        try:
            ts = int(p[0])
            # some sources give seconds, some ms
            if ts > 10 ** 12:
                ts = ts // 1000
            date = datetime.date.fromtimestamp(ts)
            if date < START_DATE:
                continue
            daily[date].append(float(p[1]))
        except Exception:
            continue

    monthly = defaultdict(list)
    for d, vals in daily.items():
        month = d.strftime("%Y-%m")
        monthly[month].extend(vals)

    # ensure months from START_DATE to now exist (fill with None)
    result = {}
    today = datetime.date.today()
    for m in month_range(START_DATE, today):
        vals = monthly.get(m, [])
        if vals:
            result[m] = {"avg_price": sum(vals) / len(vals), "count": len(vals)}
        else:
            result[m] = {"avg_price": None, "count": 0}
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=0, help='Max number of appIds to process (0 = all)')
    parser.add_argument('--start', type=int, default=0, help='Start index (0-based) into appId list')
    parser.add_argument('--out', default=OUT_CSV, help='Output CSV path')
    args = parser.parse_args()

    games = pd.read_csv("steam_games_dataset.csv", dtype={"appId": str})
    appids = games['appId'].dropna().unique().tolist()

    total = len(appids)
    start = max(0, args.start)
    end = total if args.limit <= 0 else min(total, start + args.limit)

    import os
    # ensure output exists if starting from 0
    if start == 0 and os.path.exists(args.out):
        os.remove(args.out)

    for idx in range(start, end):
        appid = appids[idx]
        print(f"[{idx+1}/{total}] processing appId={appid}")
        data = fetch_steamdb(appid)
        points = None
        if data:
            points = data
        else:
            html = fetch_store_pricehistory_html(appid)
            if html:
                arr = parse_store_html_for_prices(html)
                if arr:
                    points = arr

        monthly = aggregate_monthly(appid, points)
        rows = []
        for month, info in monthly.items():
            rows.append({
                "appId": appid,
                "month": month,
                "avg_price_usd": info['avg_price'],
                "sample_count": info['count'],
            })

        # append rows incrementally
        append_rows_to_csv(args.out, rows, header=(not os.path.exists(args.out)))

        time.sleep(1)  # be polite / rate limit

    print("Completed subset run. Wrote/updated", args.out)


if __name__ == '__main__':
    main()
