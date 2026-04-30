from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def load_env() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        os.environ.setdefault(key.strip(), _clean_env_value(value))


def _clean_env_value(value: str) -> str:
    cleaned = value.strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in ("'", '"'):
        return cleaned[1:-1].strip()
    return cleaned


def env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


@dataclass(frozen=True)
class BatteryConfig:
    capacity_mwh: float
    max_charge_mw: float
    max_discharge_mw: float
    # One-way efficiencies: round-trip = charge_efficiency * discharge_efficiency
    charge_efficiency: float
    discharge_efficiency: float
    min_soc_fraction: float
    max_soc_fraction: float
    initial_soc_fraction: float
    degradation_eur_per_mwh: float
    # Maximum energy charged per day relative to capacity (0 = no limit)
    max_daily_cycles: float
    replacement_cost_eur_per_mwh: float
    cycle_life_to_80pct: float
    calendar_fade_pct_per_year: float
    degradation_reference_temp_c: float
    thermal_management_delta_c: float


@dataclass(frozen=True)
class Config:
    port: int
    data_dir: Path
    manual_data_dir: Path
    entsoe_token: str
    entsoe_domain: str
    weather_nodes: list[tuple[str, float, float]]
    battery: BatteryConfig
    auto_run_enabled: bool
    auto_run_hours: list[int]


def parse_run_hours(raw: str) -> list[int]:
    hours = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            hours.append(int(part))
    return hours or [6, 13]


def parse_weather_nodes(raw: str) -> list[tuple[str, float, float]]:
    nodes: list[tuple[str, float, float]] = []
    for item in raw.split(","):
        parts = item.split(":")
        if len(parts) != 3:
            continue
        name, lat, lon = parts
        try:
            nodes.append((name, float(lat), float(lon)))
        except ValueError:
            continue
    return nodes


load_env()

DEFAULT_NODES = (
    "athens:37.9838:23.7275,"
    "thessaloniki:40.6401:22.9444,"
    "west_macedonia:40.3000:21.7900,"
    "peloponnese:37.5079:22.3735,"
    "central_greece:38.6044:22.7152"
)

config = Config(
    port=env_int("PORT", 8080),
    data_dir=Path(os.environ.get("DATA_DIR", "./data/store")).resolve(),
    manual_data_dir=Path(os.environ.get("MANUAL_DATA_DIR", "./data/manual")).resolve(),
    entsoe_token=os.environ.get("ENTSOE_TOKEN", ""),
    entsoe_domain=os.environ.get("ENTSOE_DOMAIN", "10YGR-HTSO-----Y"),
    weather_nodes=parse_weather_nodes(os.environ.get("WEATHER_NODES", DEFAULT_NODES)),
    auto_run_enabled=os.environ.get("AUTO_RUN_ENABLED", "true").lower() not in ("false", "0", "no"),
    auto_run_hours=parse_run_hours(os.environ.get("AUTO_RUN_HOURS", "6,13")),
    battery=BatteryConfig(
        capacity_mwh=env_float("BATTERY_CAPACITY_MWH", 4),
        max_charge_mw=env_float("BATTERY_MAX_CHARGE_MW", 1),
        max_discharge_mw=env_float("BATTERY_MAX_DISCHARGE_MW", 1),
        charge_efficiency=env_float("BATTERY_CHARGE_EFFICIENCY", env_float("BATTERY_EFFICIENCY", 0.95) ** 0.5),
        discharge_efficiency=env_float("BATTERY_DISCHARGE_EFFICIENCY", env_float("BATTERY_EFFICIENCY", 0.95) ** 0.5),
        min_soc_fraction=env_float("BATTERY_MIN_SOC_FRACTION", 0.10),
        max_soc_fraction=env_float("BATTERY_MAX_SOC_FRACTION", 0.95),
        initial_soc_fraction=env_float("BATTERY_INITIAL_SOC_FRACTION", 0.50),
        degradation_eur_per_mwh=env_float("BATTERY_DEGRADATION_EUR_PER_MWH", 2.5),
        max_daily_cycles=env_float("BATTERY_MAX_DAILY_CYCLES", 1.5),
        replacement_cost_eur_per_mwh=env_float("BATTERY_REPLACEMENT_EUR_PER_MWH", 120000),
        cycle_life_to_80pct=env_float("BATTERY_CYCLE_LIFE_TO_80PCT", 6000),
        calendar_fade_pct_per_year=env_float("BATTERY_CALENDAR_FADE_PCT_PER_YEAR", 1.8),
        degradation_reference_temp_c=env_float("BATTERY_DEGRADATION_REFERENCE_TEMP_C", 25),
        thermal_management_delta_c=env_float("BATTERY_THERMAL_MANAGEMENT_DELTA_C", 3),
    ),
)

config.data_dir.mkdir(parents=True, exist_ok=True)
config.manual_data_dir.mkdir(parents=True, exist_ok=True)
