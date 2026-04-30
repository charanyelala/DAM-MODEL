from __future__ import annotations

import os
import sys
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from py_backend.config import config
from py_backend.connectors.manual_csv import synthetic_fallback
from py_backend.models.features import build_features
from py_backend.models.forecast_errors import predict_forecast_errors
from py_backend.models.price_forecast import forecast_prices
from py_backend.optimizer.battery import optimize_battery
from py_backend.groq_summary import explain_run
from py_backend.utils.time import parse_timestamp


def test_pipeline_rows():
    date = "2026-04-29"
    fallback = synthetic_fallback(date)
    features = build_features(date, [], [], [], [], [], [], [], [], [], fallback)
    errors = predict_forecast_errors(features)
    forecasts = forecast_prices(features, errors)
    dispatch = optimize_battery(forecasts, config.battery)
    assert len(features) == 96
    assert len(errors) == 96
    assert len(forecasts) == 96
    assert len(dispatch) == 96


def test_optimizer_bounds():
    date = "2026-04-29"
    fallback = synthetic_fallback(date)
    features = build_features(date, [], [], [], [], [], [], [], [], [], fallback)
    forecasts = forecast_prices(features, predict_forecast_errors(features))
    dispatch = optimize_battery(forecasts, config.battery)
    min_soc = config.battery.capacity_mwh * config.battery.min_soc_fraction - 0.001
    max_soc = config.battery.capacity_mwh * config.battery.max_soc_fraction + 0.001
    for row in dispatch:
        assert min_soc <= row["socMwh"] <= max_soc
        assert not (row["chargeMw"] > 0 and row["dischargeMw"] > 0)


def test_optimizer_respects_low_initial_soc():
    date = "2026-04-29"
    fallback = synthetic_fallback(date)
    features = build_features(date, [], [], [], [], [], [], [], [], [], fallback)
    forecasts = forecast_prices(features, predict_forecast_errors(features))
    low_soc_battery = replace(config.battery, initial_soc_fraction=config.battery.min_soc_fraction)
    dispatch = optimize_battery(forecasts, low_soc_battery)
    min_soc = low_soc_battery.capacity_mwh * low_soc_battery.min_soc_fraction - 0.001
    assert min(row["socMwh"] for row in dispatch) >= min_soc


def test_anomaly_detection():
    date = "2026-04-29"
    fallback = synthetic_fallback(date)
    features = build_features(date, [], [], [], [], [], [], [], [], [], fallback)
    for row in features:
        if 9 <= row["hour"] <= 12:
            row["loadActual"] = row["loadForecast"] + 900
            row["loadError"] = 900
    errors = predict_forecast_errors(features)
    assert any(row["label"] == "underforecast-risk" for row in errors)


def test_groq_local_fallback_without_key():
    previous_key = os.environ.pop("GROQ_API_KEY", None)
    previous_model = os.environ.get("GROQ_MODEL")
    try:
        summary = {
            "avgPrice": 80.0,
            "minPrice": 20.0,
            "maxPrice": 140.0,
            "expectedRevenue": 1200.0,
            "anomalyIntervals": 1,
            "dataQuality": {"isTradeReady": False, "warnings": ["Synthetic price data is present."]},
        }
        forecasts = [{"timestamp": "2026-04-29T18:00:00Z", "priceP50": 140.0}]
        errors = [{"timestamp": "2026-04-29T17:00:00Z", "anomalyScore": 0.8, "label": "underforecast-risk"}]
        dispatch = [{"timestamp": "2026-04-29T18:00:00Z", "action": "discharge"}]
        result = explain_run(summary, forecasts, errors, dispatch)
        assert result["enabled"] is False
        assert result["source"] == "local-fallback"
        assert "Market view:" in result["summary"]
        assert "Trade-ready status: false" in result["summary"]
    finally:
        if previous_key is not None:
            os.environ["GROQ_API_KEY"] = previous_key
        if previous_model is not None:
            os.environ["GROQ_MODEL"] = previous_model


def test_ambiguous_uploaded_timestamps_prefer_target_date():
    assert parse_timestamp("01/05/2026 00:15:00", "2026-05-01", 0) == "2026-05-01T00:15:00Z"
    assert parse_timestamp("01/05/2026 00:15:00", "2026-01-05", 0) == "2026-01-05T00:15:00Z"
    assert parse_timestamp("05/01 00:30", "2026-05-01", 0) == "2026-05-01T00:30:00Z"


def main():
    tests = [
        test_pipeline_rows,
        test_optimizer_bounds,
        test_optimizer_respects_low_initial_soc,
        test_anomaly_detection,
        test_groq_local_fallback_without_key,
        test_ambiguous_uploaded_timestamps_prefer_target_date,
    ]
    for test in tests:
        test()
        print(f"ok - {test.__name__}")
    print(f"{len(tests)} tests passed")


if __name__ == "__main__":
    main()
