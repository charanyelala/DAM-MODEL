from __future__ import annotations

import io
import json
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen

# ADMIE/IPTO (Greek TSO) file download API — no authentication required
# Docs: https://www.admie.gr/en/market/market-statistics/file-download-api
_BASE = "https://www.admie.gr/getOperationMarketFile"
_NS = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}

# Known ADMIE file categories
_CAT_LOAD_FORECAST = "ISP1DayAheadLoadForecast"
_CAT_SCADA = "ISP1SystemRealizationSCADA"


def _metadata(date_string: str, category: str) -> list[dict]:
    url = f"{_BASE}?dateStart={date_string}&dateEnd={date_string}&FileCategory={category}"
    with urlopen(url, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _download(url: str) -> bytes:
    with urlopen(url, timeout=60) as resp:
        return resp.read()


def _shared_strings(z: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in z.namelist():
        return []
    tree = ET.fromstring(z.read("xl/sharedStrings.xml"))
    result = []
    for si in tree.findall(".//x:si", _NS):
        t = si.find(".//x:t", _NS)
        result.append((t.text or "") if t is not None else "")
    return result


def _sheet_xml(z: zipfile.ZipFile) -> bytes | None:
    for name in z.namelist():
        if name.startswith("xl/worksheets/sheet") and name.endswith(".xml"):
            return z.read(name)
    return None


def _excel_serial_to_ts(serial: float) -> str:
    """Convert Excel serial date (Athens local = UTC+2 or UTC+3) to UTC ISO string."""
    dt_local = datetime(1899, 12, 30) + timedelta(days=float(serial))
    # Assume EET/EEST offset of +2h for winter, +3h for summer (Apr-Oct)
    offset_h = 3 if 4 <= dt_local.month <= 10 else 2
    dt_utc = dt_local.replace(tzinfo=timezone.utc) - timedelta(hours=offset_h)
    return dt_utc.isoformat().replace("+00:00", "Z")


def _parse_xlsx(data: bytes) -> list[dict]:
    """Minimal stdlib xlsx parser — handles single-sheet files with numeric/string cells."""
    try:
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            strings = _shared_strings(z)
            sheet_bytes = _sheet_xml(z)
            if not sheet_bytes:
                return []
            tree = ET.fromstring(sheet_bytes)
    except (zipfile.BadZipFile, ET.ParseError):
        return []

    headers: list[str] = []
    rows: list[dict] = []
    for row_el in tree.findall(".//x:row", _NS):
        cells_raw: list[str | float | None] = []
        for cell in row_el.findall("x:c", _NS):
            ctype = cell.get("t", "")
            v_el = cell.find("x:v", _NS)
            raw = v_el.text if v_el is not None else None
            if raw is None:
                cells_raw.append(None)
            elif ctype == "s":
                idx = int(raw)
                cells_raw.append(strings[idx] if idx < len(strings) else "")
            else:
                try:
                    cells_raw.append(float(raw))
                except ValueError:
                    cells_raw.append(raw)
        if not headers:
            headers = [str(v).strip() for v in cells_raw]
        elif any(v is not None for v in cells_raw):
            rows.append(dict(zip(headers, cells_raw)))
    return rows


def _to_ts(value: str | float | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, float):
        return _excel_serial_to_ts(value)
    raw = str(value).strip()
    # Try common ISO formats
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            dt_local = datetime.strptime(raw, fmt)
            offset_h = 3 if 4 <= dt_local.month <= 10 else 2
            dt_utc = dt_local.replace(tzinfo=timezone.utc) - timedelta(hours=offset_h)
            return dt_utc.isoformat().replace("+00:00", "Z")
        except ValueError:
            continue
    return None


def _load_col(headers: list[str]) -> str | None:
    for candidate in ("LoadForecast", "Load_Forecast", "Load Forecast", "LoadMW", "Load (MW)", "MW"):
        if candidate in headers:
            return candidate
    # fuzzy: first numeric-named column that isn't the date
    for h in headers:
        if any(kw in h.lower() for kw in ("load", "forecast", "mw")):
            return h
    return None


def _res_col(headers: list[str], res_type: str) -> str | None:
    for h in headers:
        if res_type.lower() in h.lower():
            return h
    return None


def _ts_col(headers: list[str]) -> str | None:
    for candidate in ("DateTime", "Date Time", "Timestamp", "Time", "Date"):
        if candidate in headers:
            return candidate
    return headers[0] if headers else None


def _fetch_and_parse(date_string: str, category: str) -> list[dict]:
    meta = _metadata(date_string, category)
    if not meta:
        return []
    file_url = meta[0].get("file_path") or meta[0].get("filePath") or meta[0].get("url") or ""
    if not file_url:
        return []
    data = _download(file_url)
    # Some ADMIE files are direct ZIPs; others are directly XLSX (which are also ZIPs)
    return _parse_xlsx(data)


def fetch_load_forecast(date_string: str) -> list[dict]:
    """Return 15-min load forecast from ADMIE for the given date."""
    raw_rows = _fetch_and_parse(date_string, _CAT_LOAD_FORECAST)
    if not raw_rows:
        return []
    headers = list(raw_rows[0].keys()) if raw_rows else []
    ts_col = _ts_col(headers)
    load_col = _load_col(headers)
    if not ts_col or not load_col:
        return []
    result = []
    for row in raw_rows:
        ts = _to_ts(row.get(ts_col))
        load_val = row.get(load_col)
        if ts and load_val is not None:
            try:
                result.append({
                    "timestamp": ts,
                    "loadForecast": float(load_val),
                    "source": "admie-ipto",
                })
            except (TypeError, ValueError):
                pass
    return result


def fetch_res_generation(date_string: str) -> list[dict]:
    """Return actual wind + solar generation from ADMIE SCADA data."""
    raw_rows = _fetch_and_parse(date_string, _CAT_SCADA)
    if not raw_rows:
        return []
    headers = list(raw_rows[0].keys()) if raw_rows else []
    ts_col = _ts_col(headers)
    wind_col = _res_col(headers, "wind")
    solar_col = _res_col(headers, "solar") or _res_col(headers, "pv")
    if not ts_col:
        return []
    result = []
    for row in raw_rows:
        ts = _to_ts(row.get(ts_col))
        if not ts:
            continue
        wind = row.get(wind_col) if wind_col else None
        solar = row.get(solar_col) if solar_col else None
        try:
            result.append({
                "timestamp": ts,
                "windActualMw": float(wind) if wind is not None else None,
                "solarActualMw": float(solar) if solar is not None else None,
                "resActual": (float(wind or 0) + float(solar or 0)) if (wind or solar) else None,
                "source": "admie-scada",
            })
        except (TypeError, ValueError):
            pass
    return result
