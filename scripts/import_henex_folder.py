from __future__ import annotations

import argparse
import csv
import io
import re
import zipfile
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree as ET


NS = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
EXCEL_EPOCH = datetime(1899, 12, 30, tzinfo=timezone.utc)


def _cell_ref_to_col(ref: str) -> int:
    letters = "".join(ch for ch in ref if ch.isalpha()).upper()
    col = 0
    for ch in letters:
        col = col * 26 + (ord(ch) - ord("A") + 1)
    return col - 1


def _shared_strings(xlsx: zipfile.ZipFile) -> list[str]:
    try:
        raw = xlsx.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ET.fromstring(raw)
    values = []
    for item in root.findall("x:si", NS):
        text_parts = [node.text or "" for node in item.findall(".//x:t", NS)]
        values.append("".join(text_parts))
    return values


def _sheet_rows(xlsx_bytes: bytes) -> list[list[str]]:
    with zipfile.ZipFile(io.BytesIO(xlsx_bytes)) as xlsx:
        strings = _shared_strings(xlsx)
        workbook = ET.fromstring(xlsx.read("xl/workbook.xml"))
        sheet = workbook.find("x:sheets/x:sheet", NS)
        if sheet is None:
            return []
        sheet_xml = "xl/worksheets/sheet1.xml"
        root = ET.fromstring(xlsx.read(sheet_xml))
        rows: list[list[str]] = []
        for row in root.findall(".//x:sheetData/x:row", NS):
            values: list[str] = []
            for cell in row.findall("x:c", NS):
                ref = cell.attrib.get("r", "")
                col = _cell_ref_to_col(ref)
                while len(values) <= col:
                    values.append("")
                values[col] = _cell_value(cell, strings)
            rows.append(values)
        return rows

def _cell_value(cell: ET.Element, strings: list[str]) -> str:
    value = cell.find("x:v", NS)
    if value is None or value.text is None:
        return ""
    text = value.text.strip()
    if cell.attrib.get("t") == "s":
        try:
            return strings[int(text)]
        except (ValueError, IndexError):
            return text
    return text


def _num(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _date_from_value(value: str, fallback_name: str) -> date | None:
    number = _num(value)
    if number is not None:
        return (EXCEL_EPOCH + timedelta(days=number)).date()
    match = re.search(r"(20\d{6})", fallback_name)
    if match:
        return datetime.strptime(match.group(1), "%Y%m%d").date()
    return None


def _timestamp(day: date, hour_1_based: int, quarter: int = 0) -> str:
    dt = datetime(day.year, day.month, day.day, tzinfo=timezone.utc) + timedelta(hours=hour_1_based - 1, minutes=quarter * 15)
    return dt.isoformat().replace("+00:00", "Z")


def _zip_xlsx_entries(folder: Path):
    for zip_path in sorted(folder.rglob("*.zip")):
        with zipfile.ZipFile(zip_path) as archive:
            for entry in archive.infolist():
                if entry.filename.lower().endswith(".xlsx"):
                    yield str(zip_path.relative_to(folder)), entry.filename, archive.read(entry)


def import_premarket(folder: Path) -> list[dict]:
    rows: list[dict] = []
    for zip_name, entry_name, data in _zip_xlsx_entries(folder):
        if "premarketsummary" not in entry_name.lower() and "premarketsummary" not in zip_name.lower():
            continue
        sheet = _sheet_rows(data)
        if len(sheet) < 8:
            continue
        delivery_day = _date_from_value(sheet[1][0] if sheet[1] else "", entry_name)
        if delivery_day is None:
            continue
        by_label = {str(row[0]).strip().lower(): row for row in sheet if row}
        demand = by_label.get("demand") or by_label.get("total buy nominations")
        supply = by_label.get("greece mainland") or by_label.get("total sell nominations")
        res = by_label.get("renewables")
        gas = by_label.get("gas")
        hydro = by_label.get("hydro")
        lignite = by_label.get("lignite")
        pump = by_label.get("pump")
        imports = by_label.get("total imports")
        exports = by_label.get("total exports")
        for hour in range(1, 25):
            col = hour
            rows.append(
                {
                    "timestamp": _timestamp(delivery_day, hour),
                    "total_demand_mwh": _value_at(demand, col),
                    "total_supply_mwh": _value_at(supply, col),
                    "res_mwh": _value_at(res, col),
                    "gas_mwh": _value_at(gas, col),
                    "hydro_mwh": _value_at(hydro, col),
                    "lignite_mwh": _value_at(lignite, col),
                    "pump_mwh": _value_at(pump, col),
                    "imports_mwh": _value_at(imports, col),
                    "exports_mwh": _value_at(exports, col),
                    "source_file": f"{zip_name}:{entry_name}",
                    "source": "henex-premarket",
                }
            )
    return rows


def _value_at(row: list[str] | None, index: int) -> float | None:
    if row is None or index >= len(row):
        return None
    return _num(row[index])


def import_posnoms(folder: Path) -> list[dict]:
    rows: list[dict] = []
    for zip_name, entry_name, data in _zip_xlsx_entries(folder):
        if "posnom" not in entry_name.lower() and "posnom" not in zip_name.lower():
            continue
        sheet = _sheet_rows(data)
        if not sheet:
            continue
        headers = [str(item).strip().lower() for item in sheet[0]]
        for raw in sheet[1:]:
            row = {headers[idx]: raw[idx] if idx < len(raw) else "" for idx in range(len(headers))}
            timestamp = row.get("delivery_mtu", "").replace(" ", "T") + "Z"
            rows.append(
                {
                    "timestamp": timestamp,
                    "target": row.get("target", ""),
                    "side": row.get("side_descr", ""),
                    "delivery_day": row.get("dday", ""),
                    "asset": row.get("asset_descr", ""),
                    "classification": row.get("classification", ""),
                    "duration_min": row.get("delivery_duration", ""),
                    "sort": row.get("sort", ""),
                    "total_orders_mwh": row.get("total_orders", ""),
                    "publication_time": row.get("pub_time", ""),
                    "version": row.get("ver", ""),
                    "source_file": f"{zip_name}:{entry_name}",
                    "source": "henex-posnoms",
                }
            )
    return rows


def import_dam_curve_prices(folder: Path) -> list[dict]:
    grouped: dict[str, dict[str, list[tuple[float, float]]]] = defaultdict(lambda: {"buy": [], "sell": []})
    source_files: dict[str, str] = {}
    for zip_name, entry_name, data in _zip_xlsx_entries(folder):
        lower_entry = entry_name.lower()
        if "/dam/" not in lower_entry.replace("\\", "/") or "_el-dam_aggrcurves" not in lower_entry:
            continue
        sheet = _sheet_rows(data)
        if not sheet:
            continue
        headers = [str(item).strip().lower() for item in sheet[0]]
        for raw in sheet[1:]:
            row = {headers[idx]: raw[idx] if idx < len(raw) else "" for idx in range(len(headers))}
            if str(row.get("target", "")).strip().upper() != "DAM":
                continue
            side = str(row.get("side_descr", "")).strip().lower()
            if side not in ("buy", "sell"):
                continue
            timestamp = _normalize_timestamp(row.get("delivery_mtu", ""))
            quantity = _num(row.get("quantity"))
            price = _num(row.get("unitprice"))
            if not timestamp or quantity is None or price is None:
                continue
            grouped[timestamp][side].append((price, quantity))
            source_files[timestamp] = f"{zip_name}:{entry_name}"

    rows = []
    for timestamp in sorted(grouped):
        price, quantity = _clearing_price(grouped[timestamp]["buy"], grouped[timestamp]["sell"])
        if price is None:
            continue
        rows.append(
            {
                "timestamp": timestamp,
                "price": round(price, 4),
                "clearing_quantity_mwh": "" if quantity is None else round(quantity, 4),
                "source_file": source_files.get(timestamp, ""),
                "source": "henex-dam-aggrcurve",
            }
        )
    return rows


def import_dam_results(folder: Path) -> list[dict]:
    by_ts: dict[str, dict] = {}
    for zip_name, entry_name, data in _zip_xlsx_entries(folder):
        lower_entry = entry_name.lower().replace("\\", "/")
        lower_zip = zip_name.lower()
        if "_el-dam_results" not in lower_entry:
            continue
        if "prelim" in lower_entry or "prelim" in lower_zip:
            continue
        sheet = _sheet_rows(data)
        if not sheet:
            continue
        headers = [str(item).strip().lower() for item in sheet[0]]
        for raw in sheet[1:]:
            row = {headers[idx]: raw[idx] if idx < len(raw) else "" for idx in range(len(headers))}
            if str(row.get("target", "")).strip().upper() != "DAM":
                continue
            timestamp = _normalize_timestamp(row.get("delivery_mtu", ""))
            mcp = _num(row.get("mcp"))
            if not timestamp or mcp is None:
                continue
            total_trades = _num(row.get("total_trades"))
            duration_min = _num(row.get("delivery_duration"))
            current = by_ts.get(timestamp)
            if current is None or (total_trades or 0.0) > (current.get("total_trades_mwh") or 0.0):
                by_ts[timestamp] = {
                    "timestamp": timestamp,
                    "price": round(mcp, 4),
                    "total_trades_mwh": "" if total_trades is None else round(total_trades, 4),
                    "duration_min": "" if duration_min is None else round(duration_min, 4),
                    "source_file": f"{zip_name}:{entry_name}",
                    "source": "henex-dam-results",
                }
    return [by_ts[key] for key in sorted(by_ts)]


def _normalize_timestamp(value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""
    text = text.replace("/", "-").replace(" ", "T")
    if not text.endswith("Z"):
        text += "Z"
    return text


def _clearing_price(buy_points: list[tuple[float, float]], sell_points: list[tuple[float, float]]) -> tuple[float | None, float | None]:
    if not buy_points or not sell_points:
        return None, None
    buy_by_price = _max_quantity_by_price(buy_points)
    sell_by_price = _max_quantity_by_price(sell_points)
    prices = sorted(set(buy_by_price) | set(sell_by_price))
    buy_qty = None
    sell_qty = None
    closest: tuple[float, float, float] | None = None
    for price in prices:
        if price in buy_by_price:
            buy_qty = buy_by_price[price]
        if price in sell_by_price:
            sell_qty = sell_by_price[price]
        if buy_qty is None or sell_qty is None:
            continue
        diff = sell_qty - buy_qty
        if closest is None or abs(diff) < abs(closest[2]):
            closest = (price, min(buy_qty, sell_qty), diff)
        if diff >= 0:
            return price, min(buy_qty, sell_qty)
    if closest is not None:
        return closest[0], closest[1]
    return None, None


def _max_quantity_by_price(points: list[tuple[float, float]]) -> dict[float, float]:
    by_price: dict[float, float] = {}
    for price, quantity in points:
        by_price[price] = max(quantity, by_price.get(price, quantity))
    return by_price


def _expand_quarter_hour(rows: list[dict]) -> dict[str, dict]:
    expanded: dict[str, dict] = {}
    for row in rows:
        hour_dt = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
        for quarter in range(4):
            ts = (hour_dt + timedelta(minutes=quarter * 15)).isoformat().replace("+00:00", "Z")
            expanded[ts] = row
    return expanded


def canonical_rows(premarket: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    by_ts = _expand_quarter_hour(premarket)
    max_imports = max((_num(row.get("imports_mwh")) or 0 for row in premarket), default=0)
    max_exports = max((_num(row.get("exports_mwh")) or 0 for row in premarket), default=0)
    load_rows = []
    res_rows = []
    border_rows = []
    for ts in sorted(by_ts):
        row = by_ts[ts]
        demand = _num(row.get("total_demand_mwh"))
        res = _num(row.get("res_mwh"))
        imports = _num(row.get("imports_mwh")) or 0.0
        exports = _num(row.get("exports_mwh")) or 0.0
        if demand is not None:
            load_rows.append({"timestamp": ts, "load_forecast": round(demand, 3), "load_actual": "", "source": "henex-premarket"})
        if res is not None:
            res_rows.append({"timestamp": ts, "res_forecast": round(res, 3), "res_actual": "", "source": "henex-premarket"})
        border_rows.append(
            {
                "timestamp": ts,
                "imports_mw": round(imports, 3),
                "exports_mw": round(exports, 3),
                "ntc_import_mw": round(max_imports, 3),
                "ntc_export_mw": round(max_exports, 3),
                "source": "henex-premarket",
            }
        )
    return load_rows, res_rows, border_rows


def canonical_prices(curve_prices: list[dict]) -> list[dict]:
    rows = []
    for row in curve_prices:
        hour_dt = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
        for quarter in range(4):
            ts = (hour_dt + timedelta(minutes=quarter * 15)).isoformat().replace("+00:00", "Z")
            rows.append({"timestamp": ts, "price": row["price"], "source": row["source"]})
    return rows


def canonical_result_prices(result_prices: list[dict]) -> list[dict]:
    rows = []
    for row in result_prices:
        hour_dt = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
        duration = _num(row.get("duration_min")) or 60.0
        steps = 4 if duration >= 60 else 1
        for quarter in range(steps):
            ts = (hour_dt + timedelta(minutes=quarter * 15)).isoformat().replace("+00:00", "Z")
            rows.append({"timestamp": ts, "price": row["price"], "source": row["source"]})
    return rows


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _read_existing(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _merge_by_timestamp(path: Path, fieldnames: list[str], new_rows: list[dict]) -> None:
    merged = {row.get("timestamp", ""): row for row in _read_existing(path) if row.get("timestamp")}
    for row in new_rows:
        if row.get("timestamp"):
            merged[row["timestamp"]] = row
    _write_csv(path, fieldnames, [merged[key] for key in sorted(merged)])


def main() -> None:
    parser = argparse.ArgumentParser(description="Import HEnEx DAM PreMarketSummary and POSNOM ZIPs.")
    parser.add_argument("--input", default="data/manual/henex")
    parser.add_argument("--output", default="data/manual")
    parser.add_argument(
        "--merge-canonical",
        action="store_true",
        help=(
            "Also merge PreMarket nomination values into load/res/cross_border. "
            "Use only for analysis; these are not final Greek system load or DAM prices."
        ),
    )
    parser.add_argument("--only-prices", action="store_true", help="Only extract derived DAM aggregate-curve prices.")
    parser.add_argument("--only-results", action="store_true", help="Only extract official DAM Results MCP prices and update prices.csv.")
    parser.add_argument(
        "--merge-prices",
        action="store_true",
        help="Merge derived DAM aggregate-curve prices into prices.csv. Disabled by default because these are inferred, not official MCP rows.",
    )
    parser.add_argument("--skip-posnoms", action="store_true", help="Skip POSNOM extraction.")
    parser.add_argument("--skip-premarket", action="store_true", help="Skip PreMarketSummary extraction.")
    args = parser.parse_args()

    source = Path(args.input)
    output = Path(args.output)
    extracted = output / "henex" / "extracted"

    premarket = [] if (args.only_prices or args.only_results or args.skip_premarket) else import_premarket(source)
    posnoms = [] if (args.only_prices or args.only_results or args.skip_posnoms) else import_posnoms(source)
    curve_prices = [] if args.only_results else import_dam_curve_prices(source)
    result_prices = import_dam_results(source)
    if not args.only_prices and not args.only_results and not args.skip_premarket:
        _write_csv(
            extracted / "henex_premarket.csv",
            [
                "timestamp",
                "total_demand_mwh",
                "total_supply_mwh",
                "res_mwh",
                "gas_mwh",
                "hydro_mwh",
                "lignite_mwh",
                "pump_mwh",
                "imports_mwh",
                "exports_mwh",
                "source_file",
                "source",
            ],
            premarket,
        )
    if not args.only_prices and not args.only_results and not args.skip_posnoms:
        _write_csv(
            extracted / "henex_posnoms.csv",
            [
                "timestamp",
                "target",
                "side",
                "delivery_day",
                "asset",
                "classification",
                "duration_min",
                "sort",
                "total_orders_mwh",
                "publication_time",
                "version",
                "source_file",
                "source",
            ],
            posnoms,
        )
    _write_csv(
        extracted / "henex_dam_curve_prices.csv",
        ["timestamp", "price", "clearing_quantity_mwh", "source_file", "source"],
        curve_prices,
    )
    _write_csv(
        extracted / "henex_dam_results_prices.csv",
        ["timestamp", "price", "total_trades_mwh", "duration_min", "source_file", "source"],
        result_prices,
    )

    if curve_prices and args.merge_prices:
        _merge_by_timestamp(output / "prices.csv", ["timestamp", "price", "source"], canonical_prices(curve_prices))
    if result_prices:
        _merge_by_timestamp(output / "prices.csv", ["timestamp", "price", "source"], canonical_result_prices(result_prices))

    if args.merge_canonical:
        load_rows, res_rows, border_rows = canonical_rows(premarket)
        _merge_by_timestamp(output / "load.csv", ["timestamp", "load_forecast", "load_actual", "source"], load_rows)
        _merge_by_timestamp(output / "res.csv", ["timestamp", "res_forecast", "res_actual", "source"], res_rows)
        _merge_by_timestamp(
            output / "cross_border.csv",
            ["timestamp", "imports_mw", "exports_mw", "ntc_import_mw", "ntc_export_mw", "source"],
            border_rows,
        )

    print(f"premarket_rows={len(premarket)}")
    print(f"posnom_rows={len(posnoms)}")
    print(f"dam_curve_price_rows={len(curve_prices)}")
    print(f"dam_result_price_rows={len(result_prices)}")
    if curve_prices and args.merge_prices:
        print("merged=prices.csv")
    if result_prices:
        print("merged=prices.csv")
    if args.merge_canonical:
        print("merged=load.csv,res.csv,cross_border.csv")


if __name__ == "__main__":
    main()
