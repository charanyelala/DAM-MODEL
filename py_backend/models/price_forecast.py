from __future__ import annotations


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def forecast_prices(
    features: list[dict],
    errors: list[dict],
    *,
    commodity: dict | None = None,
) -> list[dict]:
    error_map = {row["timestamp"]: row for row in errors}

    thermal_mc = (commodity or {}).get("thermalMarginalCostEurMwh")
    if not thermal_mc and features:
        thermal_mc = features[0].get("thermalMarginalCostEurMwh", 78.0)
    if not thermal_mc:
        thermal_mc = 78.0

    rows = []
    for row in features:
        err = error_map[row["timestamp"]]
        solar_surplus = row["resShare"] > 0.55 and 8 <= row["hour"] <= 16
        evening = 17 <= row["hour"] <= 21
        corrected_net_load = row["netLoad"] + err["expectedResidualDemandErrorMw"]
        stress_score, surplus_score = _stress_surplus(row, err, thermal_mc)

        base = row["price"]
        if base is None:
            scarcity_premium = max(0, corrected_net_load - 4000) * 0.038
            base = thermal_mc + scarcity_premium

            if solar_surplus:
                res_discount = 85 * _clamp(row["resShare"] - 0.45, 0, 0.85)
                cloud_uncertainty = row.get("cloudUncertainty", 0)
                res_discount *= _clamp(1 - cloud_uncertainty / 100, 0.3, 1.0)
                base -= res_discount

            if evening:
                base += 45 + max(0, corrected_net_load - 4200) * 0.018

        spike_prob = _clamp(
            (corrected_net_load - 4200) / 1800
            + (0.25 if evening else 0)
            + max(0, thermal_mc - 80) / 300
            + stress_score * 0.18,
            0.01,
            0.97,
        )
        neg_prob = _clamp(
            (row["resShare"] - 0.62) * 2.2
            + (0.12 if 9 <= row["hour"] <= 15 else -0.1)
            + (row.get("solarAnomalyRatio", 1.0) - 1.0) * 0.15
            + surplus_score * 0.18,
            0.01,
            0.95,
        )

        ensemble_spread = row.get("solarRadiationP90", 0) - row.get("solarRadiationP10", 0)
        base_uncertainty = 8 + abs(err["expectedResidualDemandErrorMw"]) * 0.035 + err["anomalyScore"] * 28
        weather_uncertainty = ensemble_spread * 0.04
        uncertainty = base_uncertainty + weather_uncertainty

        p50 = _clamp(base, -20, 500)
        ambient_temp_c = float(row.get("temperature", 25.0) or 25.0)
        cell_temp_c = row.get("healthCellTempC")
        if cell_temp_c is None:
            cell_temp_c = estimate_cell_temp_c(
                ambient_temp_c,
                config.battery,
                max(config.battery.max_charge_mw, config.battery.max_discharge_mw),
            )
        degradation_cost = cycle_degradation_cost_eur_per_mwh(
            battery=config.battery,
            cell_temp_c=cell_temp_c,
            power_mw=max(config.battery.max_charge_mw, config.battery.max_discharge_mw),
        )
        stress_premium = 22.0 * stress_score
        surplus_discount = 18.0 * surplus_score
        market_impact_full_power = row.get("ownPriceImpactEurPerMw", 0.0) * config.battery.max_discharge_mw
        competitor_penalty = row.get("competitorDischargePenaltyEurMwh", 0.0)
        reserve_value = row.get("reserveValueEurMwh", 0.0)
        soc_option_value = row.get("socOptionValueEurMwh", 0.0)
        transaction_cost = row.get("transactionCostEurMwh", 0.0)
        imbalance_risk = row.get("imbalanceRiskEurMwh", 0.0)
        bid_clear_probability = row.get("bidClearProbability", 1.0)
        second_auction_penalty = 12.0 * row.get("secondAuctionRisk", 0.0)

        adjusted_discharge_value = _clamp(
            (
                p50
                + stress_premium
                - market_impact_full_power
                - competitor_penalty
                - transaction_cost
                - imbalance_risk
                - second_auction_penalty
            )
            * bid_clear_probability
            + reserve_value * (1 - bid_clear_probability),
            -100,
            700,
        )
        adjusted_charge_cost = _clamp(
            p50
            - surplus_discount
            + market_impact_full_power
            + transaction_cost
            + imbalance_risk
            + second_auction_penalty,
            -100,
            700,
        )
        adjusted_hold_value = _clamp(soc_option_value + reserve_value, 0, 200)
        if row.get("priceFloorEurMwh") is not None:
            adjusted_charge_cost = max(float(row["priceFloorEurMwh"]), adjusted_charge_cost)
        if row.get("priceCapEurMwh") is not None:
            adjusted_discharge_value = min(float(row["priceCapEurMwh"]), adjusted_discharge_value)
        regime = _regime(row, p50, spike_prob, neg_prob)

        rows.append(
            {
                "timestamp": row["timestamp"],
                "mtu": row["mtu"],
                "priceP10": round(_clamp(p50 - uncertainty, -100, 500), 2),
                "priceP50": round(p50, 2),
                "priceP90": round(_clamp(p50 + uncertainty * (1 + spike_prob), -100, 700), 2),
                "expectedPrice": round(p50, 2),
                "stressScore": round(stress_score, 3),
                "surplusScore": round(surplus_score, 3),
                "adjustedDischargeValue": round(adjusted_discharge_value, 2),
                "adjustedChargeCost": round(adjusted_charge_cost, 2),
                "adjustedHoldValue": round(adjusted_hold_value, 2),
                "ownMarketImpactEurMwh": round(market_impact_full_power, 2),
                "competitorPenaltyEurMwh": round(competitor_penalty, 2),
                "reserveValueEurMwh": round(reserve_value, 2),
                "socOptionValueEurMwh": round(soc_option_value, 2),
                "transactionCostEurMwh": round(transaction_cost, 2),
                "imbalanceRiskEurMwh": round(imbalance_risk, 2),
                "bidClearProbability": round(bid_clear_probability, 3),
                "secondAuctionRisk": round(row.get("secondAuctionRisk", 0.0), 3),
                "availabilityFactor": round(row.get("availabilityFactor", 1.0), 3),
                "thermalDeratingFactor": round(row.get("thermalDeratingFactor", 1.0), 3),
                "capacityHealthFactor": round(row.get("capacityHealthFactor", 1.0), 3),
                "ambientTempC": round(ambient_temp_c, 2),
                "cellTempC": round(cell_temp_c, 2),
                "degradationCostEurPerMwh": round(degradation_cost, 4),
                "thermalMarginalCost": round(thermal_mc, 2),
                "probabilityNegative": round(neg_prob, 3),
                "probabilitySpike": round(spike_prob, 3),
                "confidence": round(
                    _clamp(
                        1
                        - err["anomalyScore"] * 0.55
                        + (1 if row.get("ensembleMembers", 1) > 10 else 0) * 0.05,
                        0.2,
                        0.95,
                    ),
                    3,
                ),
                "regime": regime,
                "note": _note(regime),
            }
        )
    return rows


def _stress_surplus(row: dict, err: dict, thermal_mc: float) -> tuple[float, float]:
    """Normalized system signals used by the battery objective.

    Stress raises discharge value. Surplus lowers effective charge cost.

    Stress_t = LoadError + ImportStress + Gas/Carbon + Scarcity
    Surplus_t = RESForecast + ExportConstraint + CurtailmentRisk
    """
    load_error_score = _clamp(err["expectedResidualDemandErrorMw"] / 900, -1, 1)
    import_stress = _clamp(row.get("importStress", 0.0), 0, 1.5)
    export_constraint = _clamp(row.get("exportConstraint", 0.0), 0, 1.5)
    outage_stress = _clamp(row.get("totalUnavailabilityMw", 0.0) / 1200, 0, 1.0)
    block_fragility = _clamp(row.get("blockRejectRate", 0.0) + row.get("curveSteepness", 0.0) / 100.0, 0, 1.5)
    gas_carbon_stress = _clamp((thermal_mc - 90) / 140, 0, 1)
    scarcity = _clamp((row["netLoad"] - 3800) / 2200, 0, 1)
    res_surplus = _clamp((row["resShare"] - 0.45) / 0.35, 0, 1.5)
    curtailment_risk = _clamp(export_constraint + res_surplus - 0.5, 0, 1.5)

    stress = (
        0.35 * max(0, load_error_score)
        + 0.25 * import_stress
        + 0.20 * gas_carbon_stress
        + 0.12 * scarcity
        + 0.03 * outage_stress
        + 0.05 * block_fragility
    )
    surplus = (
        0.45 * res_surplus
        + 0.30 * export_constraint
        + 0.25 * curtailment_risk
        + 0.15 * max(0, -load_error_score)
    )
    return _clamp(stress, 0, 1.5), _clamp(surplus, 0, 1.5)


def _regime(row: dict, price: float, spike_prob: float, neg_prob: float) -> str:
    if neg_prob > 0.55 or price <= 5:
        return "zero-negative-risk"
    if spike_prob > 0.65 or price >= 150:
        return "spike-risk"
    if row["resShare"] > 0.48 and 8 <= row["hour"] <= 16:
        return "solar-surplus"
    if 17 <= row["hour"] <= 21:
        return "evening-scarcity"
    return "normal"


def _note(regime: str) -> str:
    return {
        "zero-negative-risk": "High renewable share and low residual demand can push prices toward zero or negative.",
        "spike-risk": "High residual demand, import stress, or evening scarcity raise price spike risk.",
        "solar-surplus": "Solar surplus or export constraints are likely to suppress market clearing prices.",
        "evening-scarcity": "Solar output falls while load remains high, so thermal units can set price.",
        "normal": "Market conditions look close to normal.",
    }[regime]
from py_backend.config import config
from py_backend.models.degradation import cycle_degradation_cost_eur_per_mwh, estimate_cell_temp_c
