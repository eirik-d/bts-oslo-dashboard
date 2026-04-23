#!/usr/bin/env python3
"""
BTS Oslo Dashboard refresh script.
Queries Snowflake for engagement data by byområde/property type/package,
then generates the dashboard HTML on the Desktop.

Requirements: pip install openpyxl (in a venv at /tmp/pyenv)
Snowflake CLI (snow) must be installed and configured.
"""

import json
import subprocess
import sys
import os
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
SQL_FILE = PROJECT_DIR / "sql" / "engagement_by_byomrade.sql"
PRICE_SQL_FILE = PROJECT_DIR / "sql" / "price_by_byomrade.sql"
TEMPLATE_FILE = PROJECT_DIR / "templates" / "dashboard_template.html"
OUTPUT_FILE = Path.home() / "Desktop" / "bts_oslo_dashboard.html"

DISPLAY_BYOMRADER_EXCLUDE = {"Ukjent"}
UI_TYPES = ["FLAT", "DETACHED", "TERRACED", "SEMIDETACHED"]
METRICS = ["views", "fav", "viewing", "prospect", "msg"]
DAYS = list(range(8))


def run_snowflake_query(sql_file, label="engagement"):
    print(f"Querying Snowflake ({label})...")
    result = subprocess.run(
        ["snow", "sql", "-f", str(sql_file), "--warehouse", "NMP_BI_WH", "--format", "json"],
        capture_output=True, text=True, timeout=300
    )
    if result.returncode != 0:
        print(f"Snowflake error: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    data = json.loads(result.stdout)
    print(f"  Got {len(data)} rows from Snowflake ({label})")
    return data


def build_raw_index(data, exclude_blink=False):
    raw = {}
    for r in data:
        has_blink = r["HAS_BLINK"] in (True, "true", "True", 1, "1")
        if exclude_blink and has_blink:
            continue
        key = (r["BYOMRADE"], r["PROPERTY_TYPE"], r["PAKKE"], r["DAY_NUM"])
        entry = {
            "views": float(r["AVG_VIEWS"]),
            "fav": float(r["AVG_FAV"]),
            "viewing": float(r["AVG_VIEWING"]),
            "prospect": float(r["AVG_PROSPECT"]),
            "msg": float(r["AVG_MSG"]),
            "n": int(r["N_ADS"]),
        }
        if key in raw:
            existing = raw[key]
            total_n = existing["n"] + entry["n"]
            for m in METRICS:
                existing[m] = (existing[m] * existing["n"] + entry[m] * entry["n"]) / total_n
            existing["n"] = total_n
        else:
            raw[key] = entry
    return raw


def aggregate(raw, filters_byo, filters_type, all_types):
    result = {}
    for pakke in ["Stor", "Medium"]:
        daily = []
        for day in DAYS:
            total_n = 0
            sums = {m: 0.0 for m in METRICS}
            for byo in filters_byo:
                for typ in filters_type:
                    key = (byo, typ, pakke, day)
                    if key in raw:
                        r = raw[key]
                        for m in METRICS:
                            sums[m] += r[m] * r["n"]
                        total_n += r["n"]
            if total_n > 0:
                daily.append({m: round(sums[m] / total_n, 2) for m in METRICS})
                daily[-1]["n"] = total_n
            else:
                daily.append(None)
        result[pakke] = daily
    n_stor = max((d["n"] for d in result["Stor"] if d), default=0)
    n_med = max((d["n"] for d in result["Medium"] if d), default=0)
    return result, n_stor, n_med


def make_entry(raw, filters_byo, filters_type, all_types):
    agg, nS, nM = aggregate(raw, filters_byo, filters_type, all_types)
    if nS == 0 and nM == 0:
        return None

    def arr(pakke, metric):
        return [agg[pakke][d][metric] if agg[pakke][d] else 0 for d in DAYS]

    s = {m: arr("Stor", m) for m in METRICS}
    m = {m: arr("Medium", m) for m in METRICS}
    for d in [s, m]:
        d["favorites"] = d.pop("fav")
        d["messages"] = d.pop("msg")
    return {"nStor": nS, "nMedium": nM, "stor": s, "medium": m}


def build_dashboard_data(raw, data):
    all_byomrader = sorted(set(r["BYOMRADE"] for r in data))
    all_types = sorted(set(r["PROPERTY_TYPE"] for r in data))
    display_byomrader = [b for b in all_byomrader if b not in DISPLAY_BYOMRADER_EXCLUDE]

    D = {}

    e = make_entry(raw, display_byomrader, all_types, all_types)
    if e:
        D["ALL|ALL"] = e

    for t in UI_TYPES:
        e = make_entry(raw, display_byomrader, [t], all_types)
        if e:
            D[f"{t}|ALL"] = e

    for byo in display_byomrader:
        e = make_entry(raw, [byo], all_types, all_types)
        if e:
            D[f"ALL|{byo}"] = e

    for t in UI_TYPES:
        for byo in display_byomrader:
            e = make_entry(raw, [byo], [t], all_types)
            if e:
                D[f"{t}|{byo}"] = e

    print(f"  Built {len(D)} data entries")
    return D, display_byomrader


PRICE_METRICS = ["avg_price_diff_pct", "median_price_diff_pct", "avg_days_to_sale",
                  "median_days_to_sale", "avg_price_changes", "pct_over", "pct_at", "pct_under",
                  "avg_asking_price", "avg_sales_price"]


def build_price_index(price_data, exclude_blink=False):
    raw = {}
    for r in price_data:
        has_blink = r["HAS_BLINK"] in (True, "true", "True", 1, "1")
        if exclude_blink and has_blink:
            continue
        key = (r["BYOMRADE"], r["PROPERTY_TYPE"], r["PAKKE"])
        n = int(r["N_ADS"])
        entry = {
            "n": n,
            "sum_price_diff": float(r["AVG_PRICE_DIFF_PCT"]) * n,
            "sum_days": float(r["AVG_DAYS_TO_SALE"]) * n,
            "sum_price_adj": float(r["AVG_PRICE_ADJ_PCT"]) * n,
            "sum_fornying": float(r["AVG_FORNYING"]) * n,
            "sum_superfornying": float(r["AVG_SUPERFORNYING"]) * n,
            "ads_with_fornying": int(r["ADS_WITH_FORNYING"]),
            "ads_with_superfornying": int(r["ADS_WITH_SUPERFORNYING"]),
            "sold_over": int(r["SOLD_OVER"]),
            "sold_at": int(r["SOLD_AT"]),
            "sold_under": int(r["SOLD_UNDER"]),
        }
        if key in raw:
            e = raw[key]
            e["n"] += n
            for k in ["sum_price_diff", "sum_days", "sum_price_adj", "sum_fornying", "sum_superfornying"]:
                e[k] += entry[k]
            for k in ["ads_with_fornying", "ads_with_superfornying", "sold_over", "sold_at", "sold_under"]:
                e[k] += entry[k]
        else:
            raw[key] = entry
    return raw


def aggregate_price(raw, filters_byo, filters_type):
    result = {}
    for pakke in ["Stor", "Medium"]:
        total_n = 0
        sums = {"sum_price_diff": 0, "sum_days": 0, "sum_price_adj": 0,
                "sum_fornying": 0, "sum_superfornying": 0,
                "ads_with_fornying": 0, "ads_with_superfornying": 0,
                "sold_over": 0, "sold_at": 0, "sold_under": 0}
        for byo in filters_byo:
            for typ in filters_type:
                key = (byo, typ, pakke)
                if key in raw:
                    r = raw[key]
                    total_n += r["n"]
                    for k in sums:
                        sums[k] += r[k]
        if total_n > 0:
            result[pakke] = {
                "n": total_n,
                "avgPriceDiffPct": round(sums["sum_price_diff"] / total_n, 2),
                "avgDaysToSale": round(sums["sum_days"] / total_n, 1),
                "avgPriceAdjPct": round(sums["sum_price_adj"] / total_n, 2),
                "avgFornying": round(sums["sum_fornying"] / total_n, 2),
                "avgSuperfornying": round(sums["sum_superfornying"] / total_n, 2),
                "pctFornying": round(sums["ads_with_fornying"] * 100 / total_n, 1),
                "pctSuperfornying": round(sums["ads_with_superfornying"] * 100 / total_n, 1),
                "pctOver": round(sums["sold_over"] * 100 / total_n, 1),
                "pctAt": round(sums["sold_at"] * 100 / total_n, 1),
                "pctUnder": round(sums["sold_under"] * 100 / total_n, 1),
            }
        else:
            result[pakke] = None
    return result


def build_price_dashboard_data(raw, price_data):
    all_byomrader = sorted(set(r["BYOMRADE"] for r in price_data))
    all_types = sorted(set(r["PROPERTY_TYPE"] for r in price_data))
    display_byomrader = [b for b in all_byomrader if b not in DISPLAY_BYOMRADER_EXCLUDE]

    P = {}

    entry = aggregate_price(raw, display_byomrader, all_types)
    if entry.get("Stor") or entry.get("Medium"):
        P["ALL|ALL"] = entry

    for t in UI_TYPES:
        entry = aggregate_price(raw, display_byomrader, [t])
        if entry.get("Stor") or entry.get("Medium"):
            P[f"{t}|ALL"] = entry

    for byo in display_byomrader:
        entry = aggregate_price(raw, [byo], all_types)
        if entry.get("Stor") or entry.get("Medium"):
            P[f"ALL|{byo}"] = entry

    for t in UI_TYPES:
        for byo in display_byomrader:
            entry = aggregate_price(raw, [byo], [t])
            if entry.get("Stor") or entry.get("Medium"):
                P[f"{t}|{byo}"] = entry

    print(f"  Built {len(P)} price entries")
    return P


def generate_html(data_json, byomrader):
    template = TEMPLATE_FILE.read_text(encoding="utf-8")
    options_html = "\n".join(f'      <option value="{b}">{b}</option>' for b in byomrader)
    html = template.replace("{{DATA_JSON}}", data_json)
    html = html.replace("{{BYOMRADE_OPTIONS}}", options_html)
    return html


def main():
    data = run_snowflake_query(SQL_FILE, "engagement")
    price_data = run_snowflake_query(PRICE_SQL_FILE, "price")

    raw_all = build_raw_index(data, exclude_blink=False)
    D_all, byomrader = build_dashboard_data(raw_all, data)

    raw_no_blink = build_raw_index(data, exclude_blink=True)
    D_no_blink, _ = build_dashboard_data(raw_no_blink, data)

    price_raw_all = build_price_index(price_data, exclude_blink=False)
    P_all = build_price_dashboard_data(price_raw_all, price_data)

    price_raw_no_blink = build_price_index(price_data, exclude_blink=True)
    P_no_blink = build_price_dashboard_data(price_raw_no_blink, price_data)

    combined = {
        "all": D_all, "noBlink": D_no_blink,
        "price": {"all": P_all, "noBlink": P_no_blink},
    }
    data_json = json.dumps(combined, separators=(",", ":"))

    print("Generating HTML...")
    html = generate_html(data_json, byomrader)
    OUTPUT_FILE.write_text(html, encoding="utf-8")
    print(f"  Dashboard written to {OUTPUT_FILE} ({len(html):,} bytes)")
    print("Done!")


if __name__ == "__main__":
    main()
