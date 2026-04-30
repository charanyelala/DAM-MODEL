from __future__ import annotations

from datetime import datetime, timezone

from py_backend.utils.time import intervals_for_date


def _by_ts(rows: list[dict]) -> dict[str, dict]:
    return {row["timestamp"]: row for row in rows}


def _nearest_hour(weather_map: dict[str, dict], timestamp: str) -> dict:
    exact = weather_map.get(timestamp)
    if exact:
        return exact
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(timezone.utc)
    hour_ts = dt.replace(minute=0, second=0, microsecond=0).isoformat().replace("+00:00", "Z")
    return weather_map.get(hour_ts, {})


def _mean(values: list[float]) -> float:
    valid = [v for v in values if isinstance(v, (int, float))]
    return sum(valid) / max(1, len(valid))


def build_features(
    date_string: str,
    prices: list[dict],
    load: list[dict],
    res: list[dict],
    cross_border: list[dict],
    market_signals: list[dict],
    grid_outages: list[dict],
    asset_availability: list[dict],
    battery_health: list[dict],
    weather: list[dict],
    fallback: list[dict],
    *,
    solar_ref: list[dict] | None = None,
    commodity: dict | None = None,
) -> list[dict]:
    price_map = _by_ts(prices)
    load_map = _by_ts(load)
    res_map = _by_ts(res)
    cross_border_map = _by_ts(cross_border)
    market_signal_map = _by_ts(market_signals)
    grid_outage_map = _by_ts(grid_outages)
    availability_map = _by_ts(asset_availability)
    battery_health_map = _by_ts(battery_health)
    fallback_map = _by_ts(fallback)
    weather_map = _by_ts(weather)
    solar_ref_map = _by_ts(solar_ref or [])

    avg_temp = _mean([row.get("temperature", 0) for row in weather])
    avg_cloud = _mean([row.get("cloudCover", 0) for row in weather])
    slots = intervals_for_date(date_string)

    # Commodity prices — single value applied to all intervals
    ttf = (commodity or {}).get("ttfGasEurMwh") or 35.0
    eua = (commodity or {}).get("euaCarbonEurTon") or 65.0
    thermal_mc = (commodity or {}).get("thermalMarginalCostEurMwh") or (ttf / 0.45 + eua * 0.37)

    rows = []
    for idx, slot in enumerate(slots):
        fb = fallback_map.get(slot["timestamp"], {})
        price_row = price_map.get(slot["timestamp"], {})
        load_row = load_map.get(slot["timestamp"], {})
        res_row = res_map.get(slot["timestamp"], {})
        border_row = cross_border_map.get(slot["timestamp"], {})
        market_row = market_signal_map.get(slot["timestamp"], {})
        outage_row = grid_outage_map.get(slot["timestamp"], {})
        availability_row = availability_map.get(slot["timestamp"], {})
        health_row = battery_health_map.get(slot["timestamp"], {})
        weather_row = _nearest_hour(weather_map, slot["timestamp"])
        solar_ref_row = _nearest_hour(solar_ref_map, slot["timestamp"])
        prev_slot = slots[idx - 1] if idx else slot
        prev_load = load_map.get(prev_slot["timestamp"], {}).get("loadForecast", fb.get("loadForecast", load_row.get("loadForecast", 0)))

        load_forecast = float(load_row.get("loadForecast", load_row.get("load", fb.get("loadForecast", 0))) or 0)
        load_actual = load_row.get("loadActual")
        res_forecast = float(res_row.get("resForecast", fb.get("resForecast", 0)) or 0)
        res_actual = res_row.get("resActual")
        price = price_row.get("price", fb.get("price"))
        cloud = float(weather_row.get("cloudCover", avg_cloud) or 0)
        temp = float(weather_row.get("temperature", avg_temp) or 0)
        load_ramp = load_forecast - float(prev_load or load_forecast)
        imports_mw = float(border_row.get("importsMw", 0) or 0)
        exports_mw = float(border_row.get("exportsMw", 0) or 0)
        ntc_import_mw = float(border_row.get("ntcImportMw", 0) or 0)
        ntc_export_mw = float(border_row.get("ntcExportMw", 0) or 0)
        net_imports_mw = imports_mw - exports_mw
        import_stress = imports_mw / ntc_import_mw if ntc_import_mw > 1 else 0.0
        export_constraint = exports_mw / ntc_export_mw if ntc_export_mw > 1 else 0.0

        # Ensemble uncertainty fields (present if using ensemble weather connector)
        cloud_p10 = float(weather_row.get("cloudCoverP10", cloud) or cloud)
        cloud_p90 = float(weather_row.get("cloudCoverP90", cloud) or cloud)
        solar_mw = float(weather_row.get("solarRadiation", 0) or 0)
        solar_p10 = float(weather_row.get("solarRadiationP10", solar_mw) or solar_mw)
        solar_p90 = float(weather_row.get("solarRadiationP90", solar_mw) or solar_mw)
        wind_speed = float(weather_row.get("windSpeed", 0) or 0)
        wind_p10 = float(weather_row.get("windSpeedP10", wind_speed) or wind_speed)
        wind_p90 = float(weather_row.get("windSpeedP90", wind_speed) or wind_speed)
        ensemble_members = int(weather_row.get("ensembleMembers", 1) or 1)

        # PVGIS solar reference — clear-sky baseline for this hour-of-year
        ghi_ref = float(solar_ref_row.get("ghiRefWm2", solar_mw) or solar_mw)
        pv_norm_ref = float(solar_ref_row.get("pvNormRef", 0) or 0)
        # Ratio: how cloudy vs climatological expectation (>1 = clearer than normal)
        solar_anomaly = (solar_mw / ghi_ref) if ghi_ref > 10 else 1.0

        rows.append({
            **slot,
            "price": float(price) if price is not None else None,
            "loadForecast": load_forecast,
            "loadActual": float(load_actual) if load_actual is not None else None,
            "resForecast": res_forecast,
            "resActual": float(res_actual) if res_actual is not None else None,
            "loadError": (float(load_actual) - load_forecast) if load_actual is not None else None,
            "resError": (float(res_actual) - res_forecast) if res_actual is not None else None,
            "netLoad": load_forecast - res_forecast,
            "loadRamp": load_ramp,
            "resShare": res_forecast / max(1, load_forecast),
            # Cross-border / regional pressure
            "importsMw": imports_mw,
            "exportsMw": exports_mw,
            "netImportsMw": net_imports_mw,
            "ntcImportMw": ntc_import_mw,
            "ntcExportMw": ntc_export_mw,
            "importStress": min(max(import_stress, 0.0), 2.0),
            "exportConstraint": min(max(export_constraint, 0.0), 2.0),
            # Market microstructure / behavioral / option-value signals
            "blockRejectRate": min(max(float(market_row.get("blockRejectRate", 0) or 0), 0.0), 1.0),
            "curveSteepness": max(float(market_row.get("curveSteepness", 0) or 0), 0.0),
            "ownPriceImpactEurPerMw": max(float(market_row.get("ownPriceImpactEurPerMw", 0) or 0), 0.0),
            "competitorDischargePenaltyEurMwh": max(float(market_row.get("competitorDischargePenaltyEurMwh", 0) or 0), 0.0),
            "reserveValueEurMwh": max(float(market_row.get("reserveValueEurMwh", 0) or 0), 0.0),
            "socOptionValueEurMwh": max(float(market_row.get("socOptionValueEurMwh", 0) or 0), 0.0),
            "transactionCostEurMwh": max(float(market_row.get("transactionCostEurMwh", 0) or 0), 0.0),
            "imbalanceRiskEurMwh": max(float(market_row.get("imbalanceRiskEurMwh", 0) or 0), 0.0),
            "bidClearProbability": min(max(float(market_row.get("bidClearProbability", 1) or 1), 0.0), 1.0),
            "secondAuctionRisk": min(max(float(market_row.get("secondAuctionRisk", 0) or 0), 0.0), 1.0),
            "priceFloorEurMwh": market_row.get("priceFloorEurMwh"),
            "priceCapEurMwh": market_row.get("priceCapEurMwh"),
            # Grid/outage/unavailability signals. Current ENTSO-E bulk export in this
            # project is consumption-unit unavailability, not full generation outages.
            "plannedUnavailabilityMw": max(float(outage_row.get("plannedUnavailabilityMw", 0) or 0), 0.0),
            "actualUnavailabilityMw": max(float(outage_row.get("actualUnavailabilityMw", 0) or 0), 0.0),
            "totalUnavailabilityMw": max(float(outage_row.get("totalUnavailabilityMw", 0) or 0), 0.0),
            # Physical availability / derating
            "availabilityFactor": min(max(float(availability_row.get("availabilityFactor", 1) or 1), 0.0), 1.0),
            "thermalDeratingFactor": min(
                max(
                    float(
                        availability_row.get(
                            "thermalDeratingFactor",
                            health_row.get("thermalDeratingFactor", 1),
                        )
                        or 1
                    ),
                    0.0,
                ),
                1.0,
            ),
            "capacityHealthFactor": min(
                max(
                    float(
                        availability_row.get(
                            "capacityHealthFactor",
                            health_row.get("capacityHealthFactor", (float(health_row.get("sohPct", 100) or 100) / 100.0)),
                        )
                    ),
                    0.0,
                ),
                1.0,
            ),
            "healthAvailableCapacityMwh": health_row.get("availableCapacityMwh"),
            "healthCellTempC": health_row.get("cellTempC"),
            "batteryTwinCalendarFadePct": float(health_row.get("calendarFadePct", 0) or 0),
            "batteryTwinRtePct": health_row.get("roundTripEfficiencyPct"),
            "batteryTwinBaselineYear": health_row.get("baselineYear"),
            "batteryTwinBaselineSohPct": health_row.get("baselineSohPct"),
            "batteryTwinBaselineAnnualCapacityLossPct": health_row.get("baselineAnnualCapacityLossPct"),
            "batteryTwinBaselineAvgOpTempC": health_row.get("baselineAvgOpTempC"),
            "batteryTwinClimateTempDeltaC": health_row.get("climateTempDeltaC"),
            "batteryTwinOperatingSocWindow": health_row.get("operatingSocWindow"),
            "dailyThroughputMwh": float(health_row.get("dailyThroughputMwh", 0) or 0),
            "cumulativeCycles": float(health_row.get("cumulativeCycles", 0) or 0),
            # Weather
            "temperature": temp,
            "cloudCover": cloud,
            "cloudCoverP10": cloud_p10,
            "cloudCoverP90": cloud_p90,
            "cloudVolatility": abs(cloud - avg_cloud),
            "cloudUncertainty": cloud_p90 - cloud_p10,
            "solarRadiation": solar_mw,
            "solarRadiationP10": solar_p10,
            "solarRadiationP90": solar_p90,
            "windSpeed": wind_speed,
            "windSpeedP10": wind_p10,
            "windSpeedP90": wind_p90,
            "precipitation": float(weather_row.get("precipitation", 0) or 0),
            "weatherStress": max(0, temp - 24) * 65 + max(0, 9 - temp) * 40,
            "ensembleMembers": ensemble_members,
            # PVGIS solar reference
            "ghiRefWm2": ghi_ref,
            "pvNormRef": pv_norm_ref,
            "solarAnomalyRatio": round(min(solar_anomaly, 3.0), 3),
            # Commodity prices (constant across day, from daily market close)
            "ttfGasEurMwh": ttf,
            "euaCarbonEurTon": eua,
            "thermalMarginalCostEurMwh": thermal_mc,
            "source": {
                "price": price_row.get("source", fb.get("source", "missing")),
                "load": load_row.get("source", fb.get("source", "missing")),
                "res": res_row.get("source", fb.get("source", "missing")),
                "crossBorder": border_row.get("source", "missing"),
                "marketSignals": market_row.get("source", "missing"),
                "gridOutages": outage_row.get("source", "missing"),
                "assetAvailability": availability_row.get("source", "default"),
                "batteryHealth": health_row.get("source", "synthetic-model"),
                "weather": weather_row.get("source", "missing"),
            },
        })
    return rows
