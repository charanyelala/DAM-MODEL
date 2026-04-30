from __future__ import annotations

import json
import os
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_MODEL = "llama-3.3-70b-versatile"
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"


def explain_run(summary: dict, forecasts: list[dict], forecast_errors: list[dict], dispatch: list[dict]) -> dict:
    """Use Groq as an explanation layer over deterministic model output."""
    api_key = _env_value("GROQ_API_KEY")
    if not api_key:
        return {
            "enabled": False,
            "summary": _local_brief(summary, forecasts, forecast_errors, dispatch, "Groq is not configured. Add GROQ_API_KEY to .env to enable AI wording."),
            "model": None,
            "source": "local-fallback",
        }

    model = _env_value("GROQ_MODEL") or DEFAULT_MODEL
    payload = {
        "model": model,
        "temperature": 0.2,
        "max_tokens": 900,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You explain electricity market forecast and BESS optimization output. "
                    "Do not invent numeric forecasts. Use only the supplied data. "
                    "Be concise, practical, and separate model output from data-quality warnings. "
                    "Return plain text only, with no Markdown formatting."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(_brief_payload(summary, forecasts, forecast_errors, dispatch), ensure_ascii=False),
            },
        ],
    }

    request = Request(
        GROQ_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "dam-bess-platform/1.0",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        if exc.code in (401, 403, 429):
            return {
                "enabled": False,
                "model": payload["model"],
                "summary": _local_brief(summary, forecasts, forecast_errors, dispatch, _friendly_groq_error(exc.code, detail)),
                "source": "local-fallback",
            }
        raise RuntimeError(f"Groq API error {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"Groq API connection failed: {exc.reason}") from exc

    content = data["choices"][0]["message"]["content"].strip()
    return {
        "enabled": True,
        "model": payload["model"],
        "summary": content,
        "source": "groq",
    }


def _env_value(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        value = value[1:-1].strip()
    return value


def _friendly_groq_error(status_code: int, detail: str) -> str:
    try:
        parsed = json.loads(detail)
        error = parsed.get("error", {})
        if isinstance(error, dict):
            message = error.get("message") or detail
            code = error.get("code")
        else:
            message = str(error or detail)
            code = parsed.get("code")
    except json.JSONDecodeError:
        message = detail
        code = None

    if status_code == 401:
        return "Groq authentication failed. Check that GROQ_API_KEY in .env is valid, active, and has no extra quotes or spaces."
    if status_code == 403 and str(code) == "1010":
        return (
            "Groq denied this request with 403 / code 1010. This is an upstream Groq access block, "
            "not a forecast-engine failure. Check the Groq API key, account/project access, VPN/proxy/IP restrictions, "
            "and whether your current network is allowed by Groq. Numeric forecasts and BESS optimization still ran normally."
        )
    if status_code == 403:
        return f"Groq denied this request with 403. Check API key permissions, account access, and model availability. Details: {message}"
    if status_code == 429:
        return "Groq rate limit or quota was reached. Wait and try again, or check the Groq project quota/billing limits."
    return f"Groq request failed with HTTP {status_code}: {message}"


def _local_brief(summary: dict, forecasts: list[dict], forecast_errors: list[dict], dispatch: list[dict], reason: str) -> str:
    payload = _brief_payload(summary, forecasts, forecast_errors, dispatch)
    top_prices = payload["topPriceIntervals"]
    top_risks = payload["topRiskIntervals"]
    active_dispatch = payload["activeDispatchSample"]
    quality = summary.get("dataQuality", {})
    warnings = quality.get("warnings", []) if isinstance(quality, dict) else []

    highest = top_prices[0] if top_prices else {}
    riskiest = top_risks[0] if top_risks else {}
    charge_count = sum(1 for row in dispatch if row.get("action") == "charge")
    discharge_count = sum(1 for row in dispatch if row.get("action") == "discharge")
    active_times = ", ".join(_fmt_time(row.get("timestamp")) for row in active_dispatch[:4]) or "none"

    lines = [
        f"Groq note: {reason}",
        (
            "Market view: "
            f"P50 prices average {summary.get('avgPrice', 'n/a')} EUR/MWh, "
            f"ranging from {summary.get('minPrice', 'n/a')} to {summary.get('maxPrice', 'n/a')} EUR/MWh. "
            f"The highest forecast interval is {_fmt_time(highest.get('timestamp'))} at {highest.get('priceP50', 'n/a')} EUR/MWh."
        ),
        (
            "Forecast-error risks: "
            f"{summary.get('anomalyIntervals', 0)} intervals are flagged. "
            f"The largest risk sample is {_fmt_time(riskiest.get('timestamp'))} "
            f"({riskiest.get('label', 'unlabelled')}, score {riskiest.get('anomalyScore', 'n/a')})."
        ),
        (
            "Battery optimization: "
            f"expected revenue is {summary.get('expectedRevenue', 'n/a')} EUR with "
            f"{charge_count} charge and {discharge_count} discharge intervals. "
            f"Active dispatch starts around: {active_times}."
        ),
        (
            "What to improve before trade-ready use: "
            + ("; ".join(warnings[:3]) if warnings else "No data-quality warnings were reported by the engine.")
        ),
    ]
    if isinstance(quality, dict) and quality.get("isTradeReady") is False:
        lines.append("Trade-ready status: false. Treat this as a planning/demo explanation until missing or synthetic inputs are replaced.")
    return "\n\n".join(lines)


def _fmt_time(value) -> str:
    if not value:
        return "n/a"
    text = str(value)
    return text.replace("T", " ").replace("Z", "")


def _brief_payload(summary: dict, forecasts: list[dict], forecast_errors: list[dict], dispatch: list[dict]) -> dict:
    top_prices = sorted(forecasts, key=lambda row: row.get("priceP50", 0), reverse=True)[:5]
    top_risks = sorted(forecast_errors, key=lambda row: row.get("anomalyScore", 0), reverse=True)[:5]
    active_dispatch = [row for row in dispatch if row.get("action") and row.get("action") != "idle"][:12]
    return {
        "task": (
            "Write a dashboard-ready explanation with four sections: "
            "Market view, Forecast-error risks, Battery optimization, What to improve before trade-ready use."
        ),
        "summary": summary,
        "topPriceIntervals": top_prices,
        "topRiskIntervals": top_risks,
        "activeDispatchSample": active_dispatch,
        "rules": [
            "Do not create new price numbers.",
            "Mention if dataQuality.isTradeReady is false.",
            "Explain P10/P50/P90 as uncertainty range only if useful.",
            "Keep output under 250 words.",
            "Use plain text only; do not use Markdown bullets or headings.",
        ],
    }
