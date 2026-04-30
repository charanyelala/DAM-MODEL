from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import urlopen

from py_backend.config import config

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_weather(date_string: str) -> list[dict]:
    params = {
        "latitude": ",".join(str(node[1]) for node in config.weather_nodes),
        "longitude": ",".join(str(node[2]) for node in config.weather_nodes),
        "hourly": "temperature_2m,relative_humidity_2m,cloud_cover,shortwave_radiation,wind_speed_10m,wind_direction_10m,precipitation",
        "timezone": "UTC",
        "start_date": date_string,
        "end_date": date_string,
    }
    with urlopen(f"{FORECAST_URL}?{urlencode(params)}", timeout=30) as response:
        payload = json.loads(response.read().decode("utf-8"))

    locations = payload if isinstance(payload, list) else [payload]
    by_hour: dict[str, dict[str, list[float]]] = {}
    for location in locations:
        hourly = location.get("hourly", {})
        times = hourly.get("time", [])
        for idx, raw_time in enumerate(times):
            ts = datetime.fromisoformat(raw_time.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            row = by_hour.setdefault(
                ts,
                {
                    "temperature": [],
                    "humidity": [],
                    "cloudCover": [],
                    "solarRadiation": [],
                    "windSpeed": [],
                    "windDirection": [],
                    "precipitation": [],
                },
            )
            row["temperature"].append(float(hourly.get("temperature_2m", [0])[idx] or 0))
            row["humidity"].append(float(hourly.get("relative_humidity_2m", [0])[idx] or 0))
            row["cloudCover"].append(float(hourly.get("cloud_cover", [0])[idx] or 0))
            row["solarRadiation"].append(float(hourly.get("shortwave_radiation", [0])[idx] or 0))
            row["windSpeed"].append(float(hourly.get("wind_speed_10m", [0])[idx] or 0))
            row["windDirection"].append(float(hourly.get("wind_direction_10m", [0])[idx] or 0))
            row["precipitation"].append(float(hourly.get("precipitation", [0])[idx] or 0))

    def avg(values: list[float]) -> float:
        return sum(values) / max(1, len(values))

    return [
        {
            "timestamp": ts,
            **{key: avg(values) for key, values in values_by_key.items()},
            "source": "open-meteo-live",
        }
        for ts, values_by_key in sorted(by_hour.items())
    ]
