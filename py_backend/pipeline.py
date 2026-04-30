from __future__ import annotations

from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Callable

from py_backend.config import config
from py_backend.connectors.admie import fetch_load_forecast as admie_load, fetch_res_generation as admie_res
from py_backend.connectors.commodity_prices import fetch_commodity_prices
from py_backend.connectors.entsoe import fetch_day_ahead_prices, fetch_load
from py_backend.connectors.manual_csv import (
    read_manual_cross_border,
    read_manual_asset_availability,
    read_manual_battery_health,
    read_manual_degradation_scenarios,
    read_manual_grid_outages,
    read_manual_load,
    read_manual_market_signals,
    read_manual_prices,
    read_manual_res,
    read_predicted_battery_health,
    synthetic_fallback,
    write_predicted_battery_health,
)
from py_backend.connectors.open_meteo import fetch_weather
from py_backend.connectors.open_meteo_dwd import fetch_dwd_icon_weather
from py_backend.connectors.open_meteo_ensemble import fetch_ensemble_weather
from py_backend.connectors.pvgis import fetch_solar_reference
from py_backend.models.battery_twin import generate_battery_twin
from py_backend.models.features import build_features
from py_backend.models.forecast_errors import predict_forecast_errors
from py_backend.models.price_forecast import forecast_prices
from py_backend.optimizer.battery import optimize_battery
from py_backend.storage.json_store import read_json, write_json
from py_backend.utils.time import normalize_date


def _safe(name: str, fn: Callable[[], list[dict]], warnings: list[str]) -> list[dict]:
    try:
        return fn()
    except Exception as exc:
        warnings.append(f"{name}: {exc}")
        return []


def _safe_dict(name: str, fn: Callable[[], dict], warnings: list[str]) -> dict:
    try:
        return fn()
    except Exception as exc:
        warnings.append(f"{name}: {exc}")
        return {}


def run_pipeline(date_input: str, initial_soc_fraction: float | None = None) -> dict:
    date_string = normalize_date(date_input)
    warnings: list[str] = []
    fallback = synthetic_fallback(date_string)
    battery, soc_source = _battery_for_run(date_string, initial_soc_fraction)

    # ── Weather ──────────────────────────────────────────────────────────────
    # Prefer 15-minute DWD ICON alignment for the 96 DAM MTUs.
    # Keep ensemble as fallback because it carries useful uncertainty bands.
    weather = _safe("Open-Meteo DWD ICON 15m", lambda: fetch_dwd_icon_weather(date_string), warnings)
    if not weather:
        weather = _safe("Open-Meteo ensemble", lambda: fetch_ensemble_weather(date_string), warnings)
    if not weather:
        weather = _safe("Open-Meteo", lambda: fetch_weather(date_string), warnings)

    # ── Solar reference (PVGIS climatological baseline) ──────────────────────
    solar_ref = _safe("PVGIS solar reference", lambda: fetch_solar_reference(date_string), warnings)

    # ── Commodity prices (gas + carbon → thermal marginal cost) ──────────────
    commodity = _safe_dict("commodity prices", lambda: fetch_commodity_prices(date_string), warnings)

    # ── Electricity prices ────────────────────────────────────────────────────
    manual_prices = read_manual_prices(date_string)
    prices = manual_prices or _safe("ENTSO-E prices", lambda: fetch_day_ahead_prices(date_string), warnings)

    # ── Load ──────────────────────────────────────────────────────────────────
    manual_load = read_manual_load(date_string)
    if manual_load:
        load = manual_load
    else:
        # Prefer ADMIE IPTO load forecast (Greece-specific, often more accurate)
        admie_lf = _safe("ADMIE load forecast", lambda: admie_load(date_string), warnings)
        if admie_lf:
            load = admie_lf
        else:
            load_forecast = _safe("ENTSO-E load forecast", lambda: fetch_load(date_string, "A01"), warnings)
            load_actual = _safe("ENTSO-E actual load", lambda: fetch_load(date_string, "A16"), warnings)
            load = _merge_load(load_forecast, load_actual)

    # ── RES generation ────────────────────────────────────────────────────────
    manual_res = read_manual_res(date_string)
    if not manual_res:
        # Try ADMIE SCADA for actual RES data (wind + solar breakdown)
        admie_res_data = _safe("ADMIE RES SCADA", lambda: admie_res(date_string), warnings)
        if admie_res_data:
            manual_res = admie_res_data

    # ── Cross-border / regional pressure ─────────────────────────────────────
    cross_border = read_manual_cross_border(date_string)
    market_signals = read_manual_market_signals(date_string)
    grid_outages = read_manual_grid_outages(date_string)
    asset_availability = read_manual_asset_availability(date_string)
    battery_health = read_manual_battery_health(date_string)
    if not battery_health:
        battery_health = read_predicted_battery_health(date_string)
    if not battery_health:
        battery_health = generate_battery_twin(date_string, weather, battery)
        write_predicted_battery_health(date_string, battery_health)
    degradation_scenarios = read_manual_degradation_scenarios()

    # ── Feature engineering ──────────────────────────────────────────────────
    features = build_features(
        date_string,
        prices,
        load,
        manual_res,
        cross_border,
        market_signals,
        grid_outages,
        asset_availability,
        battery_health,
        weather,
        fallback,
        solar_ref=solar_ref, commodity=commodity,
    )
    forecast_errors = predict_forecast_errors(features)
    forecasts = forecast_prices(features, forecast_errors, commodity=commodity)
    dispatch = optimize_battery(forecasts, battery)
    summary = _summary(date_string, features, forecast_errors, forecasts, dispatch, warnings, commodity, battery, soc_source)
    summary["degradationScenarios"] = degradation_scenarios

    write_json("features", date_string, features)
    write_json("forecast-errors", date_string, forecast_errors)
    write_json("forecasts", date_string, forecasts)
    write_json("dispatch", date_string, dispatch)
    write_json("summary", date_string, summary)

    return {
        "date": date_string,
        "summary": summary,
        "features": features,
        "forecastErrors": forecast_errors,
        "forecasts": forecasts,
        "dispatch": dispatch,
    }


def _battery_for_run(date_string: str, initial_soc_fraction: float | None):
    if initial_soc_fraction is None:
        carry = _carry_forward_soc_fraction(date_string)
        if carry is not None:
            return replace(config.battery, initial_soc_fraction=carry), "previous-dispatch"
        return replace(config.battery, initial_soc_fraction=config.battery.min_soc_fraction), "default-min-soc"
    value = max(config.battery.min_soc_fraction, min(config.battery.max_soc_fraction, initial_soc_fraction))
    return replace(config.battery, initial_soc_fraction=value), "manual-override"


def _carry_forward_soc_fraction(date_string: str) -> float | None:
    try:
        date_obj = datetime.fromisoformat(date_string).date()
    except ValueError:
        return None
    previous_date = (date_obj - timedelta(days=1)).isoformat()
    previous_dispatch = read_json("dispatch", previous_date, [])
    if not previous_dispatch:
        return None
    end_soc = previous_dispatch[-1].get("socMwh")
    try:
        value = float(end_soc) / max(0.001, config.battery.capacity_mwh)
    except (TypeError, ValueError):
        return None
    return max(config.battery.min_soc_fraction, min(config.battery.max_soc_fraction, value))


def _merge_load(forecast_rows: list[dict], actual_rows: list[dict]) -> list[dict]:
    actual = {row["timestamp"]: row["load"] for row in actual_rows}
    return [
        {
            "timestamp": row["timestamp"],
            "loadForecast": row["load"],
            "loadActual": actual.get(row["timestamp"]),
            "source": row.get("source", "entsoe-live"),
        }
        for row in forecast_rows
    ]


def _summary(
    date: str,
    features: list[dict],
    errors: list[dict],
    forecasts: list[dict],
    dispatch: list[dict],
    warnings: list[str],
    commodity: dict,
    battery,
    soc_source: str,
) -> dict:
    prices = [row["priceP50"] for row in forecasts]
    revenue = sum(row["expectedRevenueEur"] for row in dispatch)
    active_threshold_mw = 0.05
    charge_mwh = sum(row["chargeMw"] * 0.25 for row in dispatch)
    discharge_mwh = sum(row["dischargeMw"] * 0.25 for row in dispatch)
    degradation_cost_eur = sum(row.get("degradationCostEur", 0.0) for row in dispatch)
    capacity_fade_pct = sum(row.get("capacityFadePct", 0.0) for row in dispatch)
    remaining_capacity_mwh = battery.capacity_mwh * max(0.0, 1.0 - capacity_fade_pct / 100.0)
    source_counts = {
        "price": dict(Counter(row["source"]["price"] for row in features)),
        "load": dict(Counter(row["source"]["load"] for row in features)),
        "res": dict(Counter(row["source"]["res"] for row in features)),
        "crossBorder": dict(Counter(row["source"]["crossBorder"] for row in features)),
        "marketSignals": dict(Counter(row["source"]["marketSignals"] for row in features)),
        "gridOutages": dict(Counter(row["source"]["gridOutages"] for row in features)),
        "assetAvailability": dict(Counter(row["source"]["assetAvailability"] for row in features)),
        "batteryHealth": dict(Counter(row["source"]["batteryHealth"] for row in features)),
        "weather": dict(Counter(row["source"]["weather"] for row in features)),
    }
    data_quality = _data_quality(source_counts)
    return {
        "date": date,
        "avgPrice": round(sum(prices) / max(1, len(prices)), 2),
        "minPrice": min(prices) if prices else None,
        "maxPrice": max(prices) if prices else None,
        "expectedRevenue": round(revenue, 2),
        "anomalyIntervals": sum(1 for row in errors if row["anomalyScore"] > 0.65),
        "chargeIntervals": sum(1 for row in dispatch if row["chargeMw"] >= active_threshold_mw),
        "dischargeIntervals": sum(1 for row in dispatch if row["dischargeMw"] >= active_threshold_mw),
        "chargeMwh": round(charge_mwh, 3),
        "dischargeMwh": round(discharge_mwh, 3),
        "equivalentCycles": round(charge_mwh / max(0.001, battery.capacity_mwh), 3),
        "degradationCostEur": round(degradation_cost_eur, 2),
        "capacityFadePct": round(capacity_fade_pct, 6),
        "remainingCapacityMwhAfterDay": round(remaining_capacity_mwh, 3),
        "battery": {
            "capacityMwh": battery.capacity_mwh,
            "maxChargeMw": battery.max_charge_mw,
            "maxDischargeMw": battery.max_discharge_mw,
            "initialSocFraction": battery.initial_soc_fraction,
            "initialSocMwh": round(battery.capacity_mwh * battery.initial_soc_fraction, 3),
            "initialSocSource": soc_source,
            "minSocMwh": round(battery.capacity_mwh * battery.min_soc_fraction, 3),
            "maxSocMwh": round(battery.capacity_mwh * battery.max_soc_fraction, 3),
            "durationHours": round(
                battery.capacity_mwh
                / max(0.001, min(battery.max_charge_mw, battery.max_discharge_mw)),
                2,
            ),
            "maxDailyCycles": battery.max_daily_cycles,
        },
        "dataQuality": data_quality,
        "commodityPrices": {
            "ttfGasEurMwh": commodity.get("ttfGasEurMwh"),
            "euaCarbonEurTon": commodity.get("euaCarbonEurTon"),
            "thermalMarginalCostEurMwh": commodity.get("thermalMarginalCostEurMwh"),
        },
        "sources": source_counts,
        "warnings": warnings,
    }


def load_result(collection: str, date_string: str):
    return read_json(collection, normalize_date(date_string), [])


def _data_quality(source_counts: dict[str, dict[str, int]]) -> dict:
    warnings = []
    synthetic_fields = [
        name for name in ("price", "load", "res")
        if source_counts.get(name, {}).get("synthetic-fallback", 0) > 0
    ]
    if synthetic_fields:
        warnings.append(
            "Using synthetic fallback for "
            + ", ".join(synthetic_fields)
            + ". Forecasts and revenue are demo outputs, not tradeable market results."
        )
    if source_counts.get("weather", {}).get("missing", 0) > 0:
        warnings.append("Some weather intervals are missing live data and were filled by defaults.")
    duration = config.battery.capacity_mwh / max(
        0.001, min(config.battery.max_charge_mw, config.battery.max_discharge_mw)
    )
    if duration > 12:
        warnings.append(
            f"Battery config implies a {duration:.1f}-hour battery "
            f"({config.battery.capacity_mwh:g} MWh / {config.battery.max_charge_mw:g} MW). "
            "For typical BESS testing use 2-4 hours, e.g. 1 MW / 2-4 MWh."
        )
    elif duration < 0.5:
        warnings.append(
            f"Battery config implies only {duration:.2f} hours of duration; check MW/MWh settings."
        )
    return {
        "isTradeReady": not synthetic_fields,
        "warnings": warnings,
    }
