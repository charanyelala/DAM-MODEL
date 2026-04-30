from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen

# Yahoo Finance unofficial v8 API — no auth, no key required
_YF_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=7d"
# TTF Dutch Natural Gas Futures (EUR/MWh equivalent after unit conversion)
_TTF_SYMBOL = "TTF=F"
# EU Carbon Allowances EUA — available on some Yahoo mirrors; fallback to hardcoded if absent
_EUA_SYMBOL = "CO2.EUA=F"

# Fallback values in EUR — updated for 2024/2025 market conditions
_TTF_FALLBACK_EUR_MWH = 35.0
_EUA_FALLBACK_EUR_TON = 65.0


def _yf_latest_close(symbol: str) -> float | None:
    url = _YF_URL.format(symbol=symbol)
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        result = data.get("chart", {}).get("result") or []
        if not result:
            return None
        meta = result[0].get("meta", {})
        price = meta.get("regularMarketPrice")
        return float(price) if price is not None else None
    except Exception:
        return None


def fetch_commodity_prices(date_string: str) -> dict:
    """Return TTF gas (EUR/MWh) and EUA carbon (EUR/tonne) for the given date."""
    ttf_raw = _yf_latest_close(_TTF_SYMBOL)
    eua = _yf_latest_close(_EUA_SYMBOL)

    # TTF on Yahoo Finance is quoted in EUR/MWh (Euronext TTF front-month)
    ttf = ttf_raw if ttf_raw and ttf_raw > 0 else _TTF_FALLBACK_EUR_MWH
    eua = eua if eua and eua > 0 else _EUA_FALLBACK_EUR_TON

    return {
        "date": date_string,
        "ttfGasEurMwh": round(ttf, 2),
        "euaCarbonEurTon": round(eua, 2),
        # Implied thermal marginal cost for a CCGT:
        # heat_rate ~0.45 (45% efficiency) → gas cost ÷ 0.45
        # carbon: 0.37 tCO2/MWh_el for CCGT × EUA price
        "thermalMarginalCostEurMwh": round(ttf / 0.45 + eua * 0.37, 2),
        "sourceTtf": "yahoo-finance" if ttf_raw else "fallback",
        "sourceEua": "yahoo-finance" if eua else "fallback",
    }
