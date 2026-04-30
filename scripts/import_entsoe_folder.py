from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from py_backend.config import config

GREECE = "BZN|GR"
RES_TYPES = {"Solar", "Wind Onshore", "Wind Offshore"}


def parse_mtu_start(raw: str) -> datetime:
    start = raw.split(" - ", 1)[0].strip()
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(start, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Unsupported MTU timestamp: {raw}")


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def num(raw: str | None) -> float | None:
    if raw is None:
        return None
    raw = raw.strip()
    if not raw or raw.lower() in {"n/e", "-", "na", "nan"}:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def expand_hour(dt: datetime) -> list[str]:
    return [iso(dt + timedelta(minutes=15 * i)) for i in range(4)]


def read_csvs(folder: Path, pattern: str):
    for path in sorted(folder.glob(pattern)):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                yield path, row


def import_prices(folder: Path) -> dict[str, dict]:
    rows: dict[str, dict] = {}
    for _, row in read_csvs(folder, "GUI_ENERGY_PRICES_*.csv"):
        if row.get("Area") != GREECE:
            continue
        price = num(row.get("Day-ahead Price (EUR/MWh)"))
        if price is None:
            continue
        ts = iso(parse_mtu_start(row["MTU (UTC)"]))
        rows[ts] = {"timestamp": ts, "price": price}
    return rows


def import_load(folder: Path) -> dict[str, dict]:
    rows: dict[str, dict] = {}
    for _, row in read_csvs(folder, "GUI_TOTAL_LOAD_DAYAHEAD_*.csv"):
        if row.get("Area") != GREECE:
            continue
        forecast = num(row.get("Day-ahead Total Load Forecast (MW)"))
        actual = num(row.get("Actual Total Load (MW)"))
        if forecast is None:
            continue
        ts = iso(parse_mtu_start(row["MTU (UTC)"]))
        rows[ts] = {"timestamp": ts, "load_forecast": forecast, "load_actual": actual if actual is not None else ""}
    return rows


def import_res(folder: Path) -> dict[str, dict]:
    # ENTSO-E generation export here is actual generation by type and hourly.
    # We use it as res_actual and leave res_forecast blank if no forecast file exists.
    sums: dict[str, float] = defaultdict(float)
    for _, row in read_csvs(folder, "AGGREGATED_GENERATION_PER_TYPE_GENERATION_*.csv"):
        if row.get("Area") != GREECE or row.get("Production Type") not in RES_TYPES:
            continue
        value = num(row.get("Generation (MW)"))
        if value is None:
            continue
        start = parse_mtu_start(row["MTU (UTC)"])
        for ts in expand_hour(start):
            sums[ts] += value
    return {ts: {"timestamp": ts, "res_forecast": "", "res_actual": value} for ts, value in sums.items()}


def import_cross_border(folder: Path) -> dict[str, dict]:
    hourly: dict[str, dict[str, float]] = defaultdict(lambda: {"imports_mw": 0.0, "exports_mw": 0.0})
    for _, row in read_csvs(folder, "GUI_NET_CROSS_BORDER_PHYSICAL_FLOWS_*.csv"):
        out_area = row.get("Out Area")
        in_area = row.get("In Area")
        value = num(row.get("Physical Flow (MW)"))
        if value is None:
            continue
        start = parse_mtu_start(row["MTU"])
        for ts in expand_hour(start):
            if in_area == GREECE and out_area != GREECE:
                hourly[ts]["imports_mw"] += value
            elif out_area == GREECE and in_area != GREECE:
                hourly[ts]["exports_mw"] += value
    max_import = max((row["imports_mw"] for row in hourly.values()), default=0.0)
    max_export = max((row["exports_mw"] for row in hourly.values()), default=0.0)
    ntc_import_proxy = round(max(max_import * 1.15, 1.0), 3)
    ntc_export_proxy = round(max(max_export * 1.15, 1.0), 3)
    return {
        ts: {
            "timestamp": ts,
            "imports_mw": row["imports_mw"],
            "exports_mw": row["exports_mw"],
            "ntc_import_mw": ntc_import_proxy,
            "ntc_export_mw": ntc_export_proxy,
        }
        for ts, row in hourly.items()
    }


def import_grid_outages(folder: Path) -> dict[str, dict]:
    rows: dict[str, dict] = {}
    for _, row in read_csvs(folder, "GUI_AGGREGATED_UNAVAILABILITY_OF_CONSUMPTION_UNITS_*.csv"):
        if row.get("Area") != GREECE:
            continue
        planned = num(row.get("Planned Unavailability (MW)")) or 0.0
        actual = num(row.get("Actual Unavailability (MW)")) or 0.0
        total = num(row.get("Total Unavailability (MW)")) or max(planned, actual)
        start = parse_mtu_start(row["MTU (UTC)"])
        for ts in expand_hour(start):
            rows[ts] = {
                "timestamp": ts,
                "planned_unavailability_mw": planned,
                "actual_unavailability_mw": actual,
                "total_unavailability_mw": total,
            }
    return rows


def write_csv(path: Path, fieldnames: list[str], rows: dict[str, dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for ts in sorted(rows):
            writer.writerow({field: rows[ts].get(field, "") for field in fieldnames})
    print(f"wrote {len(rows)} rows -> {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize ENTSO-E GUI CSV exports into app manual CSVs.")
    parser.add_argument("--input", default="data/manual/entsoe")
    parser.add_argument("--output", default=str(config.manual_data_dir))
    args = parser.parse_args()

    folder = Path(args.input)
    out = Path(args.output)
    if not folder.exists():
        raise SystemExit(f"Input folder not found: {folder}")

    write_csv(out / "prices.csv", ["timestamp", "price"], import_prices(folder))
    write_csv(out / "load.csv", ["timestamp", "load_forecast", "load_actual"], import_load(folder))
    write_csv(out / "res.csv", ["timestamp", "res_forecast", "res_actual"], import_res(folder))
    write_csv(
        out / "cross_border.csv",
        ["timestamp", "imports_mw", "exports_mw", "ntc_import_mw", "ntc_export_mw"],
        import_cross_border(folder),
    )
    write_csv(
        out / "grid_outages.csv",
        ["timestamp", "planned_unavailability_mw", "actual_unavailability_mw", "total_unavailability_mw"],
        import_grid_outages(folder),
    )


if __name__ == "__main__":
    main()
