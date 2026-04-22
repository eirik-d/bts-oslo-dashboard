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
TEMPLATE_FILE = PROJECT_DIR / "templates" / "dashboard_template.html"
OUTPUT_FILE = Path.home() / "Desktop" / "bts_oslo_dashboard.html"

DISPLAY_BYOMRADER_EXCLUDE = {"Ukjent"}
UI_TYPES = ["FLAT", "DETACHED", "TERRACED", "SEMIDETACHED"]
METRICS = ["views", "fav", "viewing", "prospect", "msg"]
DAYS = list(range(8))


def run_snowflake_query():
    print("Querying Snowflake...")
    result = subprocess.run(
        ["snow", "sql", "-f", str(SQL_FILE), "--warehouse", "NMP_BI_WH", "--format", "json"],
        capture_output=True, text=True, timeout=300
    )
    if result.returncode != 0:
        print(f"Snowflake error: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    data = json.loads(result.stdout)
    print(f"  Got {len(data)} rows from Snowflake")
    return data


def build_raw_index(data):
    raw = {}
    for r in data:
        key = (r["BYOMRADE"], r["PROPERTY_TYPE"], r["PAKKE"], r["DAY_NUM"])
        raw[key] = {
            "views": float(r["AVG_VIEWS"]),
            "fav": float(r["AVG_FAV"]),
            "viewing": float(r["AVG_VIEWING"]),
            "prospect": float(r["AVG_PROSPECT"]),
            "msg": float(r["AVG_MSG"]),
            "n": int(r["N_ADS"]),
        }
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


def generate_html(data_json, byomrader):
    template = TEMPLATE_FILE.read_text(encoding="utf-8")
    options_html = "\n".join(f'      <option value="{b}">{b}</option>' for b in byomrader)
    html = template.replace("{{DATA_JSON}}", data_json)
    html = html.replace("{{BYOMRADE_OPTIONS}}", options_html)
    return html


def main():
    data = run_snowflake_query()
    raw = build_raw_index(data)
    D, byomrader = build_dashboard_data(raw, data)
    data_json = json.dumps(D, separators=(",", ":"))

    print("Generating HTML...")
    html = generate_html(data_json, byomrader)
    OUTPUT_FILE.write_text(html, encoding="utf-8")
    print(f"  Dashboard written to {OUTPUT_FILE} ({len(html):,} bytes)")
    print("Done!")


if __name__ == "__main__":
    main()
