from __future__ import annotations


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def predict_forecast_errors(features: list[dict]) -> list[dict]:
    rows = []
    for row in features:
        morning_ramp = max(0, row["loadRamp"] - 50) if 7 <= row["hour"] <= 11 else 0
        heat = max(0, row["temperature"] - 24) * 70
        cold = max(0, 8 - row["temperature"]) * 45
        cloud = row["cloudVolatility"] * 8 if 8 <= row["hour"] <= 16 else 0
        solar_under = max(0, row["cloudCover"] - 45) * 18 if 8 <= row["hour"] <= 16 else 0

        # Ensemble spread: higher spread → higher expected forecast error
        # solarRadiationP90 - solarRadiationP10 gives MW spread across ensemble members
        solar_ensemble_spread = row.get("solarRadiationP90", 0) - row.get("solarRadiationP10", 0)
        cloud_uncertainty = row.get("cloudUncertainty", 0)  # cloud P90 - P10

        load_error = row["loadError"] if row["loadError"] is not None else heat + cold + morning_ramp * 0.8 + cloud - 120
        res_error = row["resError"] if row["resError"] is not None else (
            -solar_under
            + max(0, row["solarRadiation"] - 600) * 0.35
            - solar_ensemble_spread * 0.08  # high ensemble spread → larger expected RES error
        )
        residual_error = load_error - res_error

        # Anomaly score: incorporate ensemble spread as additional uncertainty signal
        base_anomaly = abs(residual_error) / 900 + row["cloudVolatility"] / 100
        ensemble_penalty = min(solar_ensemble_spread / 800, 0.25)  # up to +0.25 for high spread
        cloud_penalty = min(cloud_uncertainty / 80, 0.15)
        anomaly = _clamp(base_anomaly + ensemble_penalty + cloud_penalty, 0, 1)

        # Solar anomaly ratio: >1.2 or <0.8 means unusual solar conditions
        solar_anomaly = row.get("solarAnomalyRatio", 1.0)
        if solar_anomaly > 1.3 or solar_anomaly < 0.7:
            anomaly = _clamp(anomaly + 0.1, 0, 1)

        label = "normal"
        if anomaly > 0.65 and residual_error > 0:
            label = "underforecast-risk"
        elif anomaly > 0.65 and residual_error < 0:
            label = "overforecast-risk"

        rows.append({
            "timestamp": row["timestamp"],
            "mtu": row["mtu"],
            "expectedLoadErrorMw": round(load_error, 1),
            "expectedResErrorMw": round(res_error, 1),
            "expectedResidualDemandErrorMw": round(residual_error, 1),
            "probabilityUnderforecast": round(_clamp(0.25 + residual_error / 1200, 0.02, 0.98), 3),
            "probabilityOverforecast": round(_clamp(0.25 - residual_error / 1400, 0.02, 0.98), 3),
            "anomalyScore": round(anomaly, 3),
            "ensembleSolarSpreadWm2": round(solar_ensemble_spread, 1),
            "cloudUncertainty": round(cloud_uncertainty, 1),
            "label": label,
            "note": "Residual demand may deviate from forecast." if anomaly >= 0.35 else "Forecast-error risk is normal for this interval.",
        })
    return rows
