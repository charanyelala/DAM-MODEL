from __future__ import annotations

import math
import os

from py_backend.config import BatteryConfig
from py_backend.utils.time import intervals_for_date


LFP_BASELINE = [
    {"year": 1, "sohPct": 98.00, "cycles": 300, "rtePct": 88.6, "avgTempC": 24.5, "capLossPct": 2.00},
    {"year": 2, "sohPct": 96.04, "cycles": 600, "rtePct": 88.2, "avgTempC": 24.6, "capLossPct": 3.96},
    {"year": 3, "sohPct": 94.12, "cycles": 900, "rtePct": 87.8, "avgTempC": 24.8, "capLossPct": 5.88},
    {"year": 4, "sohPct": 92.24, "cycles": 1200, "rtePct": 87.4, "avgTempC": 25.0, "capLossPct": 7.76},
    {"year": 5, "sohPct": 90.39, "cycles": 1500, "rtePct": 87.0, "avgTempC": 25.2, "capLossPct": 9.61},
    {"year": 6, "sohPct": 88.58, "cycles": 1800, "rtePct": 86.6, "avgTempC": 25.4, "capLossPct": 11.42},
    {"year": 7, "sohPct": 86.81, "cycles": 2100, "rtePct": 86.2, "avgTempC": 25.7, "capLossPct": 13.19},
    {"year": 8, "sohPct": 85.08, "cycles": 2400, "rtePct": 85.8, "avgTempC": 26.0, "capLossPct": 14.92},
    {"year": 9, "sohPct": 83.37, "cycles": 2700, "rtePct": 85.4, "avgTempC": 26.3, "capLossPct": 16.63},
    {"year": 10, "sohPct": 81.71, "cycles": 3000, "rtePct": 85.0, "avgTempC": 26.6, "capLossPct": 18.29},
    {"year": 11, "sohPct": 80.07, "cycles": 3300, "rtePct": 84.6, "avgTempC": 27.0, "capLossPct": 19.93},
    {"year": 12, "sohPct": 78.47, "cycles": 3600, "rtePct": 84.2, "avgTempC": 27.3, "capLossPct": 21.53},
    {"year": 13, "sohPct": 76.90, "cycles": 3900, "rtePct": 83.8, "avgTempC": 27.7, "capLossPct": 23.10},
    {"year": 14, "sohPct": 75.36, "cycles": 4200, "rtePct": 83.4, "avgTempC": 28.1, "capLossPct": 24.64},
    {"year": 15, "sohPct": 73.86, "cycles": 4500, "rtePct": 83.0, "avgTempC": 28.5, "capLossPct": 26.14},
]


def generate_battery_twin(date_string: str, weather: list[dict], battery: BatteryConfig) -> list[dict]:
    """Generate a weather-aware battery health digital twin.

    This is a synthetic replica for planning when no BMS/EMS telemetry is
    uploaded. It uses public LFP-style stationary-storage assumptions and
    local weather forecasts to estimate cell temperature, available capacity,
    thermal derating, and calendar-aging pressure for every 15-minute MTU.

    It must be replaced by real SCADA/BMS data before production trading.
    """
    weather_map = {row["timestamp"]: row for row in weather}
    baseline = _baseline_for_year(_operation_year())
    soh_pct = baseline["sohPct"]
    avg_ambient = _avg([float(row.get("temperature", 20.0) or 20.0) for row in weather]) if weather else 20.0
    rows = []
    for slot in intervals_for_date(date_string):
        weather_row = _nearest_weather(weather_map, slot["timestamp"])
        ambient = float(weather_row.get("temperature", 20.0) or 20.0)
        solar = float(weather_row.get("solarRadiation", 0.0) or 0.0)
        wind = float(weather_row.get("windSpeed", 2.0) or 2.0)
        hour = slot["hour"] + slot["minute"] / 60
        soc_shape = 0.5 + 0.18 * math.sin(((hour - 6) / 24) * 2 * math.pi)
        solar_heat = min(4.0, solar / 250.0)
        ventilation_cooling = min(2.5, max(0.0, wind - 2.0) * 0.35)
        cell_temp = ambient + battery.thermal_management_delta_c + solar_heat - ventilation_cooling
        climate_temp_delta = cell_temp - baseline["avgTempC"]
        temp_factor = 2 ** ((cell_temp - battery.degradation_reference_temp_c) / 10)
        climate_soh_penalty = max(0.0, avg_ambient - baseline["avgTempC"]) * 0.12
        live_temp_penalty = max(0.0, climate_temp_delta) * 0.015
        calendar_fade_pct = (
            battery.calendar_fade_pct_per_year
            / 365
            / 96
            * max(0.35, temp_factor)
            * (1 + max(0, soc_shape - 0.65) * 0.8)
        )
        effective_soh = max(60.0, soh_pct - climate_soh_penalty - live_temp_penalty)
        effective_rte = max(78.0, baseline["rtePct"] - max(0.0, climate_temp_delta) * 0.08)
        temp_derating = 1.0
        if cell_temp >= 45:
            temp_derating = 0.72
        elif cell_temp >= 40:
            temp_derating = 0.86
        elif cell_temp >= 35:
            temp_derating = 0.94
        elif cell_temp <= -5:
            temp_derating = 0.80
        elif cell_temp <= 0:
            temp_derating = 0.90

        available_capacity = battery.capacity_mwh * (effective_soh / 100.0)
        rows.append(
            {
                "timestamp": slot["timestamp"],
                "sohPct": round(effective_soh, 4),
                "availableCapacityMwh": round(available_capacity, 3),
                "cellTempC": round(cell_temp, 2),
                "dailyThroughputMwh": 0.0,
                "cumulativeCycles": baseline["cycles"],
                "thermalDeratingFactor": round(temp_derating, 3),
                "capacityHealthFactor": round(effective_soh / 100.0, 5),
                "calendarFadePct": round(calendar_fade_pct, 8),
                "roundTripEfficiencyPct": round(effective_rte, 3),
                "baselineYear": baseline["year"],
                "baselineSohPct": baseline["sohPct"],
                "baselineAnnualCapacityLossPct": baseline["capLossPct"],
                "baselineAvgOpTempC": baseline["avgTempC"],
                "climateTempDeltaC": round(climate_temp_delta, 3),
                "operatingSocWindow": "20-80%",
                "predictionConfidence": 0.62,
                "source": "battery-twin-weather-lfp",
            }
        )
        soh_pct = max(60.0, soh_pct - calendar_fade_pct)
    return rows


def _nearest_weather(weather_map: dict[str, dict], timestamp: str) -> dict:
    if timestamp in weather_map:
        return weather_map[timestamp]
    hour_ts = timestamp[:14] + "00:00Z"
    return weather_map.get(hour_ts, {})


def _operation_year() -> int:
    try:
        return min(15, max(1, int(os.environ.get("BATTERY_TWIN_OPERATION_YEAR", "1"))))
    except ValueError:
        return 1


def _baseline_for_year(year: int) -> dict:
    return next((row for row in LFP_BASELINE if row["year"] == year), LFP_BASELINE[0])


def _avg(values: list[float]) -> float:
    return sum(values) / max(1, len(values))
