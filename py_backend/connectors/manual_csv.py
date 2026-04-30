from __future__ import annotations

import csv
import math
from pathlib import Path

from py_backend.config import config
from py_backend.utils.time import intervals_for_date, parse_timestamp


def _read_csv(name: str) -> list[dict]:
    target = config.manual_data_dir / name
    if not target.exists():
        return []
    with target.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = []
        for row in csv.DictReader(handle):
            normalized = {}
            for key, value in row.items():
                if key is None:
                    normalized["__extra__"] = [str(item).strip() for item in value]
                else:
                    normalized[key.strip().lower()] = str(value).strip()
            rows.append(normalized)
        return rows


def _num(value: object) -> float | None:
    try:
        number = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def read_manual_prices(date_string: str) -> list[dict]:
    rows = []
    for idx, row in enumerate(_read_csv("prices.csv")):
        ts = parse_timestamp(
            row.get("timestamp")
            or row.get("datetime")
            or row.get("time")
            or row.get("time interval")
            or row.get("mtu (utc)")
            or row.get("mtu (cet/cest)")
            or row.get("mtu"),
            date_string,
            int(row.get("mtu") or idx),
        )
        price = _num(
            row.get("price")
            or row.get("price amount")
            or row.get("mcp")
            or row.get("day-ahead")
            or row.get("day_ahead")
            or row.get("day-ahead price (eur/mwh)")
            or row.get("day ahead price (eur/mwh)")
            or row.get("day-ahead price")
            or row.get("day ahead price")
            or row.get("value")
        )
        if price is None:
            price = _first_number(row.get("__extra__", []))
        if ts[:10] == date_string and price is not None:
            rows.append({"timestamp": ts, "price": price, "source": row.get("source") or "manual-csv"})
    return rows


def _first_number(values: list[str]) -> float | None:
    for value in values:
        number = _num(value)
        if number is not None:
            return number
    return None


def read_manual_load(date_string: str) -> list[dict]:
    rows = []
    for idx, row in enumerate(_read_csv("load.csv")):
        ts = parse_timestamp(row.get("timestamp") or row.get("datetime") or row.get("time"), date_string, int(row.get("mtu") or idx))
        forecast = _num(row.get("load_forecast") or row.get("forecast") or row.get("day ahead total load forecast") or row.get("day_ahead"))
        actual = _num(row.get("load_actual") or row.get("actual") or row.get("actual total load"))
        if ts[:10] == date_string and forecast is not None:
            rows.append({"timestamp": ts, "loadForecast": forecast, "loadActual": actual, "source": "manual-csv"})
    return rows


def read_manual_res(date_string: str) -> list[dict]:
    rows = []
    for idx, row in enumerate(_read_csv("res.csv")):
        ts = parse_timestamp(row.get("timestamp") or row.get("datetime") or row.get("time"), date_string, int(row.get("mtu") or idx))
        forecast = _num(row.get("res_forecast") or row.get("forecast") or row.get("res"))
        actual = _num(row.get("res_actual") or row.get("actual"))
        if forecast is None and actual is not None:
            # Historical ENTSO-E generation exports often contain actual generation only.
            # Use it as the best available proxy so backtests can learn RES-price shape.
            forecast = actual
        if ts[:10] == date_string and forecast is not None:
            rows.append({"timestamp": ts, "resForecast": forecast, "resActual": actual, "source": "manual-csv"})
    return rows


def read_manual_cross_border(date_string: str) -> list[dict]:
    """Read optional imports/exports/ATC data.

    Expected columns:
      timestamp,imports_mw,exports_mw,ntc_import_mw,ntc_export_mw

    Positive imports_mw means energy scheduled into Greece.
    Positive exports_mw means energy scheduled out of Greece.
    """
    rows = []
    for idx, row in enumerate(_read_csv("cross_border.csv")):
        ts = parse_timestamp(row.get("timestamp") or row.get("datetime") or row.get("time"), date_string, int(row.get("mtu") or idx))
        imports = _num(row.get("imports_mw") or row.get("imports") or row.get("imp_mw"))
        exports = _num(row.get("exports_mw") or row.get("exports") or row.get("exp_mw"))
        ntc_import = _num(row.get("ntc_import_mw") or row.get("import_capacity_mw") or row.get("atc_import_mw"))
        ntc_export = _num(row.get("ntc_export_mw") or row.get("export_capacity_mw") or row.get("atc_export_mw"))
        if ts[:10] == date_string:
            rows.append(
                {
                    "timestamp": ts,
                    "importsMw": imports or 0.0,
                    "exportsMw": exports or 0.0,
                    "ntcImportMw": ntc_import or 0.0,
                    "ntcExportMw": ntc_export or 0.0,
                    "source": "manual-csv",
                }
            )
    return rows


def read_manual_market_signals(date_string: str) -> list[dict]:
    """Read optional market microstructure and behavioral signals.

    Expected columns can include:
      timestamp,block_reject_rate,curve_steepness,own_price_impact_eur_per_mw,
      competitor_discharge_penalty_eur_mwh,reserve_value_eur_mwh,
      soc_option_value_eur_mwh,transaction_cost_eur_mwh,imbalance_risk_eur_mwh,
      bid_clear_probability,second_auction_risk,price_floor_eur_mwh,price_cap_eur_mwh
    """
    rows = []
    for idx, row in enumerate(_read_csv("market_signals.csv")):
        ts = parse_timestamp(row.get("timestamp") or row.get("datetime") or row.get("time"), date_string, int(row.get("mtu") or idx))
        if ts[:10] == date_string:
            rows.append(
                {
                    "timestamp": ts,
                    "blockRejectRate": _num(row.get("block_reject_rate")) or 0.0,
                    "curveSteepness": _num(row.get("curve_steepness")) or 0.0,
                    "ownPriceImpactEurPerMw": _num(row.get("own_price_impact_eur_per_mw")) or 0.0,
                    "competitorDischargePenaltyEurMwh": _num(row.get("competitor_discharge_penalty_eur_mwh")) or 0.0,
                    "reserveValueEurMwh": _num(row.get("reserve_value_eur_mwh")) or 0.0,
                    "socOptionValueEurMwh": _num(row.get("soc_option_value_eur_mwh")) or 0.0,
                    "transactionCostEurMwh": _num(row.get("transaction_cost_eur_mwh")) or 0.0,
                    "imbalanceRiskEurMwh": _num(row.get("imbalance_risk_eur_mwh")) or 0.0,
                    "bidClearProbability": _num(row.get("bid_clear_probability")) or 1.0,
                    "secondAuctionRisk": _num(row.get("second_auction_risk")) or 0.0,
                    "priceFloorEurMwh": _num(row.get("price_floor_eur_mwh")),
                    "priceCapEurMwh": _num(row.get("price_cap_eur_mwh")),
                    "source": "manual-csv",
                }
            )
    return rows


def read_manual_asset_availability(date_string: str) -> list[dict]:
    """Read optional physical availability / derating data.

    Expected columns:
      timestamp,availability_factor,thermal_derating_factor,capacity_health_factor
    """
    rows = []
    for idx, row in enumerate(_read_csv("asset_availability.csv")):
        ts = parse_timestamp(row.get("timestamp") or row.get("datetime") or row.get("time"), date_string, int(row.get("mtu") or idx))
        if ts[:10] == date_string:
            rows.append(
                {
                    "timestamp": ts,
                    "availabilityFactor": _num(row.get("availability_factor")) or 1.0,
                    "thermalDeratingFactor": _num(row.get("thermal_derating_factor")) or 1.0,
                    "capacityHealthFactor": _num(row.get("capacity_health_factor")) or 1.0,
                    "source": "manual-csv",
                }
            )
    return rows


def read_manual_grid_outages(date_string: str) -> list[dict]:
    """Read optional grid/outage/unavailability signals.

    Expected columns:
      timestamp,planned_unavailability_mw,actual_unavailability_mw,total_unavailability_mw
    """
    rows = []
    for idx, row in enumerate(_read_csv("grid_outages.csv")):
        ts = parse_timestamp(row.get("timestamp") or row.get("datetime") or row.get("time"), date_string, int(row.get("mtu") or idx))
        if ts[:10] == date_string:
            rows.append(
                {
                    "timestamp": ts,
                    "plannedUnavailabilityMw": _num(row.get("planned_unavailability_mw")) or 0.0,
                    "actualUnavailabilityMw": _num(row.get("actual_unavailability_mw")) or 0.0,
                    "totalUnavailabilityMw": _num(row.get("total_unavailability_mw")) or 0.0,
                    "source": "manual-csv",
                }
            )
    return rows


def read_manual_battery_health(date_string: str) -> list[dict]:
    """Read optional BMS/EMS health telemetry or synthetic digital-twin output.

    Expected columns:
      timestamp,soh_pct,available_capacity_mwh,cell_temp_c,
      daily_throughput_mwh,cumulative_cycles
    """
    rows = []
    for idx, row in enumerate(_read_csv("battery_health.csv")):
        ts = parse_timestamp(row.get("timestamp") or row.get("datetime") or row.get("time"), date_string, int(row.get("mtu") or idx))
        if ts[:10] == date_string:
            rows.append(
                {
                    "timestamp": ts,
                    "sohPct": _num(row.get("soh_pct")) or 100.0,
                    "availableCapacityMwh": _num(row.get("available_capacity_mwh")),
                    "cellTempC": _num(row.get("cell_temp_c")),
                    "dailyThroughputMwh": _num(row.get("daily_throughput_mwh")) or 0.0,
                    "cumulativeCycles": _num(row.get("cumulative_cycles")) or 0.0,
                    "source": "manual-csv",
                }
            )
    return rows


def write_predicted_battery_health(date_string: str, rows: list[dict]) -> None:
    """Persist synthetic battery-twin predictions as editable CSV.

    These rows are predictions, not measured BMS/EMS telemetry. The model reads
    this CSV on later runs so users can inspect, edit, or replace assumptions.
    """
    target = config.manual_data_dir / "battery_health_predicted.csv"
    fieldnames = [
        "timestamp",
        "soh_pct",
        "available_capacity_mwh",
        "cell_temp_c",
        "daily_throughput_mwh",
        "cumulative_cycles",
        "thermal_derating_factor",
        "capacity_health_factor",
        "calendar_fade_pct",
        "round_trip_efficiency_pct",
        "baseline_year",
        "baseline_soh_pct",
        "baseline_annual_capacity_loss_pct",
        "baseline_avg_op_temp_c",
        "climate_temp_delta_c",
        "operating_soc_window",
        "prediction_confidence",
        "source",
    ]
    existing = [row for row in _read_csv("battery_health_predicted.csv") if not str(row.get("timestamp", "")).startswith(date_string)]
    predicted = []
    for row in rows:
        predicted.append(
            {
                "timestamp": row.get("timestamp", ""),
                "soh_pct": row.get("sohPct", ""),
                "available_capacity_mwh": row.get("availableCapacityMwh", ""),
                "cell_temp_c": row.get("cellTempC", ""),
                "daily_throughput_mwh": row.get("dailyThroughputMwh", ""),
                "cumulative_cycles": row.get("cumulativeCycles", ""),
                "thermal_derating_factor": row.get("thermalDeratingFactor", ""),
                "capacity_health_factor": row.get("capacityHealthFactor", ""),
                "calendar_fade_pct": row.get("calendarFadePct", ""),
                "round_trip_efficiency_pct": row.get("roundTripEfficiencyPct", ""),
                "baseline_year": row.get("baselineYear", ""),
                "baseline_soh_pct": row.get("baselineSohPct", ""),
                "baseline_annual_capacity_loss_pct": row.get("baselineAnnualCapacityLossPct", ""),
                "baseline_avg_op_temp_c": row.get("baselineAvgOpTempC", ""),
                "climate_temp_delta_c": row.get("climateTempDeltaC", ""),
                "operating_soc_window": row.get("operatingSocWindow", ""),
                "prediction_confidence": row.get("predictionConfidence", ""),
                "source": row.get("source", "battery-twin-weather-lfp"),
            }
        )
    with target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in existing + predicted:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def read_predicted_battery_health(date_string: str) -> list[dict]:
    rows = []
    for idx, row in enumerate(_read_csv("battery_health_predicted.csv")):
        ts = parse_timestamp(row.get("timestamp") or row.get("datetime") or row.get("time"), date_string, int(row.get("mtu") or idx))
        if ts[:10] == date_string:
            rows.append(
                {
                    "timestamp": ts,
                    "sohPct": _num(row.get("soh_pct")) or 100.0,
                    "availableCapacityMwh": _num(row.get("available_capacity_mwh")),
                    "cellTempC": _num(row.get("cell_temp_c")),
                    "dailyThroughputMwh": _num(row.get("daily_throughput_mwh")) or 0.0,
                    "cumulativeCycles": _num(row.get("cumulative_cycles")) or 0.0,
                    "thermalDeratingFactor": _num(row.get("thermal_derating_factor")) or 1.0,
                    "capacityHealthFactor": _num(row.get("capacity_health_factor")),
                    "calendarFadePct": _num(row.get("calendar_fade_pct")) or 0.0,
                    "roundTripEfficiencyPct": _num(row.get("round_trip_efficiency_pct")),
                    "baselineYear": _num(row.get("baseline_year")),
                    "baselineSohPct": _num(row.get("baseline_soh_pct")),
                    "baselineAnnualCapacityLossPct": _num(row.get("baseline_annual_capacity_loss_pct")),
                    "baselineAvgOpTempC": _num(row.get("baseline_avg_op_temp_c")),
                    "climateTempDeltaC": _num(row.get("climate_temp_delta_c")),
                    "operatingSocWindow": row.get("operating_soc_window") or "20-80%",
                    "predictionConfidence": _num(row.get("prediction_confidence")),
                    "source": "battery-twin-predicted-csv",
                }
            )
    return rows


def read_manual_warranty_limits() -> list[dict]:
    """Read optional yearly warranty limits.

    Expected columns:
      year,max_cycles,max_throughput_mwh,min_soc,max_soc,max_cell_temp_c
    """
    rows = []
    for row in _read_csv("warranty_limits.csv"):
        year = _num(row.get("year"))
        if year is None:
            continue
        rows.append(
            {
                "year": int(year),
                "maxCycles": _num(row.get("max_cycles")) or 0.0,
                "maxThroughputMwh": _num(row.get("max_throughput_mwh")) or 0.0,
                "minSoc": _num(row.get("min_soc")),
                "maxSoc": _num(row.get("max_soc")),
                "maxCellTempC": _num(row.get("max_cell_temp_c")),
                "source": "manual-csv",
            }
        )
    return rows


def read_manual_degradation_scenarios() -> list[dict]:
    """Read optional degradation scenario assumptions.

    Expected columns:
      scenario,cycle_life_to_80pct,calendar_fade_pct_per_year,
      thermal_derating_start_c,replacement_eur_per_mwh
    """
    rows = []
    for row in _read_csv("degradation_scenarios.csv"):
        scenario = row.get("scenario") or "base"
        rows.append(
            {
                "scenario": scenario,
                "cycleLifeTo80Pct": _num(row.get("cycle_life_to_80pct")),
                "calendarFadePctPerYear": _num(row.get("calendar_fade_pct_per_year")),
                "thermalDeratingStartC": _num(row.get("thermal_derating_start_c")),
                "replacementEurPerMwh": _num(row.get("replacement_eur_per_mwh")),
                "source": "manual-csv",
            }
        )
    return rows


def synthetic_fallback(date_string: str) -> list[dict]:
    rows = []
    for slot in intervals_for_date(date_string):
        hour = slot["hour"] + slot["minute"] / 60
        solar_shape = max(0, math.sin(((hour - 6) / 12) * math.pi))
        evening_shape = max(0, math.sin(((hour - 16) / 7) * math.pi))
        load_forecast = 3900 + 1800 * max(0, math.sin(((hour - 4) / 17) * math.pi)) + 300 * evening_shape
        res_forecast = 400 + 3900 * solar_shape
        net_load = load_forecast - res_forecast
        price = max(-5, 35 + net_load * 0.028 + evening_shape * 65 - solar_shape * 45)
        rows.append(
            {
                **slot,
                "price": round(price, 2),
                "loadForecast": round(load_forecast),
                "loadActual": None,
                "resForecast": round(res_forecast),
                "resActual": None,
                "source": "synthetic-fallback",
            }
        )
    return rows
