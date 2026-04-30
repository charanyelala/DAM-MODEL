from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import urlopen

from py_backend.config import config


DWD_URL = "https://api.open-meteo.com/v1/dwd-icon"

_VARS_15 = [
    "temperature_2m",
    "relative_humidity_2m",
    "cloud_cover",
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "wind_speed_10m",
    "wind_direction_10m",
    "precipitation",
]


def fetch_dwd_icon_weather(date_string: str) -> list[dict]:
    """Fetch free DWD ICON weather through Open-Meteo at 15-minute granularity.

    Open-Meteo needs no API key for non-commercial use. For Greece, this endpoint
    gives a good free operational model feed. Some 15-minute variables may be
    interpolated from hourly model output, but they still align with the 96 DAM
    market time units better than hourly-only weather.
    """
    params = {
        "latitude": ",".join(str(node[1]) for node in config.weather_nodes),
        "longitude": ",".join(str(node[2]) for node in config.weather_nodes),
        "minutely_15": ",".join(_VARS_15),
        "timezone": "UTC",
        "start_date": date_string,
        "end_date": date_string,
        "cell_selection": "land",
    }
    with urlopen(f"{DWD_URL}?{urlencode(params)}", timeout=45) as response:
        payload = json.loads(response.read().decode("utf-8"))

    locations = payload if isinstance(payload, list) else [payload]
    by_slot: dict[str, dict[str, list[float]]] = {}
    for location in locations:
        minute_rows = location.get("minutely_15", {})
        times = minute_rows.get("time", [])
        for idx, raw_time in enumerate(times):
            ts = (
                datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
                .astimezone(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
            row = by_slot.setdefault(
                ts,
                {
                    "temperature": [],
                    "humidity": [],
                    "cloudCover": [],
                    "solarRadiation": [],
                    "directRadiation": [],
                    "diffuseRadiation": [],
                    "windSpeed": [],
                    "windDirection": [],
                    "precipitation": [],
                },
            )
            _append(row["temperature"], minute_rows, "temperature_2m", idx)
            _append(row["humidity"], minute_rows, "relative_humidity_2m", idx)
            _append(row["cloudCover"], minute_rows, "cloud_cover", idx)
            _append(row["solarRadiation"], minute_rows, "shortwave_radiation", idx)
            _append(row["directRadiation"], minute_rows, "direct_radiation", idx)
            _append(row["diffuseRadiation"], minute_rows, "diffuse_radiation", idx)
            _append(row["windSpeed"], minute_rows, "wind_speed_10m", idx)
            _append(row["windDirection"], minute_rows, "wind_direction_10m", idx)
            _append(row["precipitation"], minute_rows, "precipitation", idx)

    return [
        {
            "timestamp": ts,
            **{key: _avg(values) for key, values in values_by_key.items()},
            "source": "open-meteo-dwd-icon-15m",
        }
        for ts, values_by_key in sorted(by_slot.items())
        if ts[:10] == date_string
    ]


def _append(target: list[float], rows: dict, key: str, idx: int) -> None:
    values = rows.get(key, [])
    if idx >= len(values) or values[idx] is None:
        return
    try:
        target.append(float(values[idx]))
    except (TypeError, ValueError):
        pass


def _avg(values: list[float]) -> float:
    return round(sum(values) / max(1, len(values)), 3)
