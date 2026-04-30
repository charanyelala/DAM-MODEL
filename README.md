# Hellas DAM BESS Intelligence

Hellas DAM BESS Intelligence is a Python-based forecasting and optimization platform for the Greek Day-Ahead Market. It predicts 96 quarter-hour price intervals, estimates forecast-error risk, and optimizes a utility-scale battery dispatch schedule with state-of-charge, efficiency, degradation, and data-quality constraints.

The demo is configured for a 330 MW / 790 MWh Greece BESS use case, but the battery settings can be changed through environment variables.

## What It Does

- Forecasts day-ahead electricity prices for Greece across 96 market time units.
- Detects residual-demand forecast-error risk from load, RES, weather, and market signals.
- Optimizes battery charge/discharge decisions using SciPy HiGHS linear programming.
- Models battery degradation from throughput, temperature, available capacity, and synthetic or measured health data.
- Shows data-quality coverage so judges can see when output is based on live/manual data versus fallback data.
- Serves a browser dashboard from the Python backend with no frontend build step.

## Architecture

```text
frontend/
  index.html        Static dashboard shell
  app.js            Dashboard views, charts, upload UI, and API calls
  style.css         Dashboard styling

py_backend/
  server.py         HTTP server, static assets, API routes, upload handler
  pipeline.py       Data loading, feature engineering, forecasts, dispatch, storage
  config.py         Environment and battery configuration
  connectors/       Open-Meteo, PVGIS, ADMIE/IPTO, ENTSO-E, manual CSV readers
  models/           Battery twin, feature model, forecast-error model, price model
  optimizer/        Battery dispatch optimizer
  storage/          JSON output persistence
  utils/            Time and interval helpers

scripts/
  download_entsoe_data.py
  import_entsoe_folder.py
  import_henex_folder.py

py_tests/
  run_tests.py      Dependency-light regression tests
```

## Quick Start

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
python -m py_backend.server
```

Open:

```text
http://localhost:8080
```

Run tests:

```bash
python py_tests/run_tests.py
```

## Configuration

Important `.env` values:

```text
PORT=8080
DATA_DIR=./data/store
MANUAL_DATA_DIR=./data/manual
ENTSOE_TOKEN=
BATTERY_CAPACITY_MWH=790
BATTERY_MAX_CHARGE_MW=330
BATTERY_MAX_DISCHARGE_MW=330
BATTERY_MIN_SOC_FRACTION=0.10
BATTERY_MAX_SOC_FRACTION=0.95
BATTERY_MAX_DAILY_CYCLES=1.5
```

`ENTSOE_TOKEN` is optional. Without it, the app still runs with included/manual CSV data and synthetic fallback logic.

## Data Sources

The system supports:

- Open-Meteo weather forecasts, including DWD ICON and ensemble data.
- PVGIS solar reference data.
- ADMIE/IPTO load and RES endpoints where available.
- ENTSO-E Transparency API day-ahead prices, load, and cross-border data.
- Manual CSV fallback files in `data/manual`.
- HEnEx and ENTSO-E bulk exports through the importer scripts.

Large raw market exports are intentionally excluded from git. To rebuild local CSV inputs, place raw exports under `data/manual/entsoe` or `data/manual/henex` and run:

```bash
python scripts/import_entsoe_folder.py --input data/manual/entsoe --output data/manual
python scripts/import_henex_folder.py --input data/manual/henex --output data/manual --only-results
```

## Core Calculation Flow

1. Normalize the selected date into 96 quarter-hour market intervals.
2. Load price, load, RES, cross-border, outage, asset, battery-health, weather, and commodity inputs.
3. Fill missing critical inputs with synthetic fallback data so the demo always runs.
4. Build features such as residual demand, stress score, surplus score, thermal marginal cost, weather uncertainty, and source coverage.
5. Predict forecast-error risk, including underforecast and overforecast probabilities.
6. Produce P10/P50/P90 price forecasts.
7. Run the battery optimizer.
8. Persist `features`, `forecast-errors`, `forecasts`, `dispatch`, and `summary` JSON outputs under `data/store`.

## Optimization Model

The dispatch optimizer maximizes expected net value:

```text
sum_t dt * [
  discharge_mw_t * (adjusted_discharge_value_t - degradation_cost_t - hold_value_t)
  - charge_mw_t * (adjusted_charge_cost_t + degradation_cost_t)
]
```

Subject to:

- charge and discharge power limits
- state-of-charge dynamics
- minimum and maximum SOC
- end-of-day SOC floor
- daily cycle budget
- one-way charge/discharge efficiency

The default interval length is 0.25 hours. For the default 330 MW / 790 MWh asset:

```text
nominal duration = 790 / 330 = 2.39 hours
usable energy at 10%-95% SOC = 790 * 0.85 = 671.5 MWh
usable duration = 671.5 / 330 = 2.03 hours
```

## API

```text
GET  /api/health
GET  /api/sources
GET  /api/scheduler/status
POST /api/scheduler/trigger?date=2026-04-29
POST /api/pipeline/run?date=2026-04-29
GET  /api/summary/2026-04-29
GET  /api/features/2026-04-29
GET  /api/forecasts/2026-04-29
GET  /api/forecast-errors/2026-04-29
GET  /api/dispatch/2026-04-29
POST /api/manual/upload
GET  /api/groq/explain?date=2026-04-29
```

## Dashboard Pages

- Overview: executive summary, price surface, risk view, dispatch chart, and input coverage.
- Forecast: P10/P50/P90 prices, spike probability, negative-price probability, stress, surplus, and thermal marginal cost.
- Risk: anomaly score, residual-demand error, underforecast/overforecast risk, cloud uncertainty, and solar spread.
- Optimization: expected revenue, wear cost, capacity fade, charge/discharge energy, SOC path, and dispatch explainability.
- Battery Twin: synthetic or measured battery health, SOH, temperature, thermal derating, and degradation assumptions.
- Data Quality: source coverage and warnings that explain whether output is demo-grade or trade-ready.
- Intervals: 96-row audit table for every market time unit.
- Upload Data: manual CSV upload and optional API connection helpers.

## Hackathon Notes

This is an analytical decision-support prototype, not an executable trading system. Its value is the combination of market forecasting, explainable forecast-error risk, battery-aware optimization, and transparent data-quality gates.

For production use, replace synthetic fallback inputs with validated HEnEx, ADMIE/IPTO, ENTSO-E, BMS, EMS, and SCADA feeds; add market-rule-specific bid validation; and backtest against settled market results.
