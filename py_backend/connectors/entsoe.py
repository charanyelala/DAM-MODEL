from __future__ import annotations

import re
from urllib.parse import urlencode
from urllib.request import urlopen

from py_backend.config import config
from py_backend.utils.time import entsoe_period

BASE_URL = "https://web-api.tp.entsoe.eu/api"


def _fetch(params: dict[str, str]) -> list[dict]:
    if not config.entsoe_token:
        return []
    query = urlencode({"securityToken": config.entsoe_token, **params})
    with urlopen(f"{BASE_URL}?{query}", timeout=45) as response:
        xml = response.read().decode("utf-8", errors="replace")
    return _extract_positions(xml)


def _extract_positions(xml: str) -> list[dict]:
    rows = []
    for period in re.findall(r"<Period>([\s\S]*?)</Period>", xml):
        start_match = re.search(r"<start>(.*?)</start>", period)
        if not start_match:
            continue
        from datetime import datetime, timedelta, timezone

        start = datetime.fromisoformat(start_match.group(1).replace("Z", "+00:00")).astimezone(timezone.utc)
        for point in re.findall(r"<Point>([\s\S]*?)</Point>", period):
            position_match = re.search(r"<position>(.*?)</position>", point)
            if not position_match:
                continue
            position = int(position_match.group(1))
            price_match = re.search(r"<price\.amount>(.*?)</price\.amount>", point)
            quantity_match = re.search(r"<quantity>(.*?)</quantity>", point)
            ts = start + timedelta(minutes=(position - 1) * 15)
            rows.append(
                {
                    "timestamp": ts.isoformat().replace("+00:00", "Z"),
                    "price": float(price_match.group(1)) if price_match else None,
                    "quantity": float(quantity_match.group(1)) if quantity_match else None,
                    "source": "entsoe-live",
                }
            )
    return rows


def fetch_day_ahead_prices(date_string: str) -> list[dict]:
    start, end = entsoe_period(date_string)
    rows = _fetch(
        {
            "documentType": "A44",
            "in_Domain": config.entsoe_domain,
            "out_Domain": config.entsoe_domain,
            "periodStart": start,
            "periodEnd": end,
        }
    )
    return [{"timestamp": row["timestamp"], "price": row["price"], "source": row["source"]} for row in rows if row["price"] is not None]


def fetch_load(date_string: str, process_type: str) -> list[dict]:
    start, end = entsoe_period(date_string)
    rows = _fetch(
        {
            "documentType": "A65",
            "processType": process_type,
            "outBiddingZone_Domain": config.entsoe_domain,
            "periodStart": start,
            "periodEnd": end,
        }
    )
    return [{"timestamp": row["timestamp"], "load": row["quantity"], "source": row["source"]} for row in rows if row["quantity"] is not None]
