from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.request import urlopen

from py_backend.config import config

# EU Joint Research Centre PVGIS v5.3 — free, no auth required
# https://joint-research-centre.ec.europa.eu/pvgis-photovoltaic-geographical-information-system
_BASE = "https://re.jrc.ec.europa.eu/api/v5_3/seriescalc"
_SARAH3_LAST_YEAR = 2023  # PVGIS-SARAH3 database covers 2005–2023


def _pvgis_ref_year(target_date: str) -> int:
    year = datetime.fromisoformat(target_date).year
    return min(year - 1, _SARAH3_LAST_YEAR)


def _fetch_node(lat: float, lon: float, year: int) -> list[dict]:
    """Fetch annual hourly irradiance from PVGIS for one location.

    Uses angle=0 (horizontal panel) so G(i) = global horizontal irradiance G(h).
    PVGIS timestamps are in local solar time: UTC = solar_time - lon/15h.
    """
    params = {
        "lat": lat,
        "lon": lon,
        "startyear": year,
        "endyear": year,
        "outputformat": "json",
        "angle": 0,
        "aspect": 0,
        "raddatabase": "PVGIS-SARAH3",
    }
    with urlopen(f"{_BASE}?{urlencode(params)}", timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    hourly = data.get("outputs", {}).get("hourly", [])

    # Longitude correction: solar time → UTC
    lon_offset_h = lon / 15.0  # hours ahead of UTC (positive = east)
    rows = []
    for row in hourly:
        raw_time = row.get("time", "")
        if len(raw_time) < 13:
            continue
        # PVGIS format: "YYYYMMDD:HHMM" where MM ≈ 0–59 (solar minute offset)
        try:
            solar_dt = datetime.strptime(raw_time, "%Y%m%d:%H%M")
        except ValueError:
            continue
        # Convert solar time → UTC, then round to nearest whole hour
        utc_dt = solar_dt - timedelta(hours=lon_offset_h)
        # Round to nearest hour so timestamps align with our interval grid
        utc_dt = utc_dt.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        ts = utc_dt.isoformat().replace("+00:00", "Z")

        ghi = float(row.get("G(i)", 0) or 0)   # horizontal plane → G(i) = G(h)
        sun_elev = float(row.get("H_sun", 0) or 0)
        temp = float(row.get("T2m", 0) or 0)
        wind = float(row.get("WS10m", 0) or 0)
        rows.append({
            "ts": ts,
            "ghi": ghi,
            "sunElev": sun_elev,
            "temp": temp,
            "wind": wind,
        })
    return rows


def _avg_day(node_rows_list: list[list[dict]], target_date: str) -> list[dict]:
    """Average across grid nodes and filter to target date (±1 day window for timezone edge)."""
    # Collect by rounded UTC hour across all nodes
    by_hour: dict[str, dict[str, list[float]]] = {}
    for rows in node_rows_list:
        for r in rows:
            ts = r["ts"]
            # Keep rows within target date ± 1 day
            if not (ts[:10] == target_date or ts[:10] == _adjacent_date(target_date, -1)):
                continue
            slot = by_hour.setdefault(ts, {"ghi": [], "sunElev": [], "temp": [], "wind": []})
            slot["ghi"].append(r["ghi"])
            slot["sunElev"].append(r["sunElev"])
            slot["temp"].append(r["temp"])
            slot["wind"].append(r["wind"])

    def avg(vals: list[float]) -> float:
        return sum(vals) / max(1, len(vals))

    result = []
    for ts, slot in sorted(by_hour.items()):
        if not ts.startswith(target_date):
            continue
        result.append({
            "timestamp": ts,
            "ghiRefWm2": round(avg(slot["ghi"]), 1),
            "sunElevDeg": round(avg(slot["sunElev"]), 1),
            "source": "pvgis-sarah3",
        })
    return result


def _adjacent_date(date_string: str, days: int) -> str:
    dt = datetime.fromisoformat(date_string) + timedelta(days=days)
    return dt.date().isoformat()


def fetch_solar_reference(date_string: str) -> list[dict]:
    """Fetch PVGIS climatological solar irradiance for the same calendar day last year.

    Returns one row per UTC hour on date_string with:
      - ghiRefWm2: global horizontal irradiance (W/m²) — clear-sky reference
      - sunElevDeg: solar elevation angle (degrees)

    Used in features.py to compute solarAnomalyRatio:
      actual_forecast / clear_sky_reference → how much cloudier than normal
    """
    ref_year = _pvgis_ref_year(date_string)
    month = datetime.fromisoformat(date_string).month
    day = datetime.fromisoformat(date_string).day

    # Reference date: same calendar day in ref_year
    try:
        ref_date = datetime(ref_year, month, day).date().isoformat()
    except ValueError:
        # Feb 29 in non-leap year → use Mar 1
        ref_date = datetime(ref_year, 3, 1).date().isoformat()

    node_rows_list: list[list[dict]] = []
    for _name, lat, lon in config.weather_nodes:
        try:
            node_rows_list.append(_fetch_node(lat, lon, ref_year))
        except Exception:
            pass

    if not node_rows_list:
        return []

    # Map ref_date rows to target_date timestamps (shift year)
    ref_rows = _avg_day(node_rows_list, ref_date)

    # Re-stamp from ref_date to date_string (same hour-of-day)
    result = []
    for row in ref_rows:
        ts = row["timestamp"]
        restamped_ts = date_string + ts[10:]  # keep time portion, replace date
        result.append({**row, "timestamp": restamped_ts, "referenceYear": ref_year})
    return result
