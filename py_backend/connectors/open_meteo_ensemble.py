from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import urlopen

from py_backend.config import config

# ECMWF IFS 0.25° ensemble — 51 members, 4 runs/day, free with no key
# Falls back to GFS ensemble if ECMWF is unavailable
_ENSEMBLE_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
_ENSEMBLE_MODELS = ["ecmwf_ifs025", "gfs025"]

_VARS = [
    "temperature_2m",
    "cloud_cover",
    "shortwave_radiation",
    "wind_speed_10m",
    "wind_direction_10m",
    "precipitation",
    "relative_humidity_2m",
]


def _stats(values: list[float]) -> dict:
    if not values:
        return {"mean": 0.0, "std": 0.0, "p10": 0.0, "p90": 0.0, "members": 0}
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / max(1, len(values))
    std = math.sqrt(variance)
    sv = sorted(values)
    n = len(sv)
    return {
        "mean": round(mean, 3),
        "std": round(std, 3),
        "p10": round(sv[max(0, int(n * 0.10))], 3),
        "p90": round(sv[min(n - 1, int(n * 0.90))], 3),
        "members": n,
    }


def _try_model(date_string: str, model: str) -> list[dict]:
    params = {
        "latitude": ",".join(str(n[1]) for n in config.weather_nodes),
        "longitude": ",".join(str(n[2]) for n in config.weather_nodes),
        "hourly": ",".join(_VARS),
        "models": model,
        "timezone": "UTC",
        "start_date": date_string,
        "end_date": date_string,
    }
    with urlopen(f"{_ENSEMBLE_URL}?{urlencode(params)}", timeout=60) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    locations = payload if isinstance(payload, list) else [payload]
    by_hour: dict[str, dict[str, list[float]]] = {}

    for loc in locations:
        hourly = loc.get("hourly", {})
        times = hourly.get("time", [])
        # Discover all member keys dynamically
        member_keys: dict[str, list[str]] = {v: [] for v in _VARS}
        for key in hourly:
            for var in _VARS:
                if key.startswith(var + "_member") or key == var:
                    member_keys[var].append(key)

        for idx, raw_time in enumerate(times):
            ts = (
                datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
                .astimezone(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
            slot = by_hour.setdefault(ts, {v: [] for v in _VARS})
            for var in _VARS:
                for key in member_keys[var]:
                    vals = hourly.get(key, [])
                    if idx < len(vals) and vals[idx] is not None:
                        try:
                            slot[var].append(float(vals[idx]))
                        except (TypeError, ValueError):
                            pass

    result = []
    for ts, slot in sorted(by_hour.items()):
        temp = _stats(slot["temperature_2m"])
        cloud = _stats(slot["cloud_cover"])
        solar = _stats(slot["shortwave_radiation"])
        wind = _stats(slot["wind_speed_10m"])
        precip = _stats(slot["precipitation"])
        humid = _stats(slot["relative_humidity_2m"])
        result.append({
            "timestamp": ts,
            # Mean values (drop-in replacement for standard weather rows)
            "temperature": temp["mean"],
            "humidity": humid["mean"],
            "cloudCover": cloud["mean"],
            "solarRadiation": solar["mean"],
            "windSpeed": wind["mean"],
            "precipitation": precip["mean"],
            # Uncertainty bands — new fields used by updated features + forecast models
            "temperatureStd": temp["std"],
            "cloudCoverP10": cloud["p10"],
            "cloudCoverP90": cloud["p90"],
            "solarRadiationP10": solar["p10"],
            "solarRadiationP90": solar["p90"],
            "windSpeedP10": wind["p10"],
            "windSpeedP90": wind["p90"],
            "ensembleMembers": temp["members"] or solar["members"] or wind["members"],
            "source": f"open-meteo-ensemble-{model}",
        })
    return result


def fetch_ensemble_weather(date_string: str) -> list[dict]:
    """Try ECMWF IFS ensemble, fall back to GFS ensemble."""
    last_exc: Exception | None = None
    for model in _ENSEMBLE_MODELS:
        try:
            rows = _try_model(date_string, model)
            if rows:
                return rows
        except Exception as exc:
            last_exc = exc
    if last_exc:
        raise last_exc
    return []
