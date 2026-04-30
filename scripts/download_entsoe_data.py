from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlencode, urlparse
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from py_backend.config import config

BASE_URL = "https://web-api.tp.entsoe.eu/api"
GREECE_DOMAIN = "10YGR-HTSO-----Y"


def compact_period(date_string: str) -> tuple[str, str]:
    start = date_string.replace("-", "") + "0000"
    next_day = (datetime.fromisoformat(date_string) + timedelta(days=1)).date().isoformat()
    end = next_day.replace("-", "") + "0000"
    return start, end


def fetch_xml(params: dict[str, str]) -> str:
    token = os.environ.get("ENTSOE_TOKEN") or config.entsoe_token
    if not token:
        raise SystemExit(
            "ENTSOE_TOKEN is not set. Register/login at https://transparency.entsoe.eu/ "
            "and create an API security token, then put it in .env."
        )
    url = f"{BASE_URL}?{urlencode({'securityToken': token, **params})}"
    with urlopen(url, timeout=60) as response:
        return response.read().decode("utf-8", errors="replace")


def extract_positions(xml: str) -> list[dict]:
    rows: list[dict] = []
    for period in re.findall(r"<Period>([\s\S]*?)</Period>", xml):
        start_match = re.search(r"<start>(.*?)</start>", period)
        resolution_match = re.search(r"<resolution>(.*?)</resolution>", period)
        if not start_match:
            continue
        start = datetime.fromisoformat(start_match.group(1).replace("Z", "+00:00")).astimezone(timezone.utc)
        step = 15 if resolution_match and resolution_match.group(1) == "PT15M" else 60
        for point in re.findall(r"<Point>([\s\S]*?)</Point>", period):
            position_match = re.search(r"<position>(.*?)</position>", point)
            if not position_match:
                continue
            position = int(position_match.group(1))
            price_match = re.search(r"<price\.amount>(.*?)</price\.amount>", point)
            quantity_match = re.search(r"<quantity>(.*?)</quantity>", point)
            ts = start + timedelta(minutes=(position - 1) * step)
            rows.append(
                {
                    "timestamp": ts.isoformat().replace("+00:00", "Z"),
                    "price": float(price_match.group(1)) if price_match else None,
                    "quantity": float(quantity_match.group(1)) if quantity_match else None,
                }
            )
    return rows


def write_prices(date: str, output: Path, domain: str) -> None:
    start, end = compact_period(date)
    xml = fetch_xml(
        {
            "documentType": "A44",
            "in_Domain": domain,
            "out_Domain": domain,
            "periodStart": start,
            "periodEnd": end,
        }
    )
    rows = [{"timestamp": row["timestamp"], "price": row["price"]} for row in extract_positions(xml) if row["price"] is not None]
    append_csv(output, ["timestamp", "price"], rows)
    print(f"wrote {len(rows)} price rows -> {output}")


def write_load(date: str, output: Path, domain: str) -> None:
    forecast = fetch_load_series(date, domain, "A01")
    actual = fetch_load_series(date, domain, "A16")
    actual_by_ts = {row["timestamp"]: row["load"] for row in actual}
    rows = [
        {
            "timestamp": row["timestamp"],
            "load_forecast": row["load"],
            "load_actual": actual_by_ts.get(row["timestamp"], ""),
        }
        for row in forecast
    ]
    append_csv(output, ["timestamp", "load_forecast", "load_actual"], rows)
    print(f"wrote {len(rows)} load rows -> {output}")


def fetch_load_series(date: str, domain: str, process_type: str) -> list[dict]:
    start, end = compact_period(date)
    xml = fetch_xml(
        {
            "documentType": "A65",
            "processType": process_type,
            "outBiddingZone_Domain": domain,
            "periodStart": start,
            "periodEnd": end,
        }
    )
    return [{"timestamp": row["timestamp"], "load": row["quantity"]} for row in extract_positions(xml) if row["quantity"] is not None]


def append_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, dict] = {}
    if path.exists():
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                existing[row["timestamp"]] = row
    for row in rows:
        existing[row["timestamp"]] = {name: row.get(name, "") for name in fieldnames}
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(existing[key] for key in sorted(existing))


def inspect_ui_url(url: str) -> None:
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    state_raw = qs.get("appState", [""])[0]
    if not state_raw:
        print("No appState found.")
        return
    state = json.loads(unquote(state_raw))
    print(json.dumps(state, indent=2))
    print()
    print("This UI URL is for congestion income / flow-based day-ahead.")
    print("For the BESS model, first download: prices (A44) and load forecast/actual (A65).")
    print(f"Default Greece domain used by this script: {GREECE_DOMAIN}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download useful ENTSO-E data into data/manual CSV files.")
    parser.add_argument("--date", help="Date as YYYY-MM-DD")
    parser.add_argument("--domain", default=GREECE_DOMAIN, help="ENTSO-E domain code")
    parser.add_argument("--dataset", choices=["prices", "load", "all"], default="all")
    parser.add_argument("--inspect-url", help="Decode an ENTSO-E UI URL appState and explain it")
    args = parser.parse_args()

    if args.inspect_url:
        inspect_ui_url(args.inspect_url)
        return
    if not args.date:
        raise SystemExit("--date is required unless --inspect-url is used")

    manual_dir = config.manual_data_dir
    if args.dataset in ("prices", "all"):
        write_prices(args.date, manual_dir / "prices.csv", args.domain)
    if args.dataset in ("load", "all"):
        write_load(args.date, manual_dir / "load.csv", args.domain)


if __name__ == "__main__":
    main()
