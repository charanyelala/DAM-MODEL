from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

INTERVALS_PER_DAY = 96


def normalize_date(value: str | datetime | None = None) -> str:
    if value is None:
        return datetime.now(timezone.utc).date().isoformat()
    if isinstance(value, str):
        raw = value.strip()
        for candidate in _timestamp_candidates(raw, raw[:4] if raw[:4].isdigit() else None):
            return candidate.date().isoformat()
        return raw[:10]
    return value.date().isoformat()


def add_days(date_string: str, days: int) -> str:
    date = datetime.fromisoformat(f"{date_string}T00:00:00+00:00")
    return (date + timedelta(days=days)).date().isoformat()


def entsoe_period(date_string: str) -> tuple[str, str]:
    start = date_string.replace("-", "") + "0000"
    end = add_days(date_string, 1).replace("-", "") + "0000"
    return start, end


def intervals_for_date(date_string: str) -> list[dict]:
    start = datetime.fromisoformat(f"{date_string}T00:00:00+00:00")
    rows = []
    for mtu in range(INTERVALS_PER_DAY):
        ts = start + timedelta(minutes=15 * mtu)
        rows.append(
            {
                "mtu": mtu,
                "timestamp": ts.isoformat().replace("+00:00", "Z"),
                "hour": mtu // 4,
                "minute": (mtu % 4) * 15,
            }
        )
    return rows


def parse_timestamp(value: str | None, date_string: str, mtu: int) -> str:
    if value:
        raw = value.strip()
        candidates = _timestamp_candidates(raw, date_string)
        for candidate in candidates:
            if candidate.date().isoformat() == date_string:
                return candidate.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        for candidate in candidates:
            try:
                return candidate.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            except ValueError:
                pass
    return intervals_for_date(date_string)[mtu % INTERVALS_PER_DAY]["timestamp"]


def _timestamp_candidates(raw: str, date_string: str | None = None) -> list[datetime]:
    first = raw.split(" - ", 1)[0].strip()
    iso = first.replace(" ", "T")
    if iso.endswith("Z"):
        iso = iso[:-1] + "+00:00"
    if len(iso) > 10 and "+" not in iso[10:] and "-" not in iso[10:]:
        iso = iso + "+00:00"

    candidates = []
    seen = set()

    def add(candidate: datetime) -> None:
        if candidate.tzinfo is None:
            candidate = candidate.replace(tzinfo=timezone.utc)
        key = candidate.isoformat()
        if key not in seen:
            seen.add(key)
            candidates.append(candidate)

    try:
        add(datetime.fromisoformat(iso))
    except ValueError:
        pass

    for fmt in (
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%m/%d/%Y %H:%M",
        "%d/%m/%Y",
        "%m/%d/%Y",
    ):
        try:
            add(datetime.strptime(first, fmt).replace(tzinfo=timezone.utc))
        except ValueError:
            pass

    year = date_string[:4] if date_string and re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_string) else None
    match = re.fullmatch(r"(?P<date>\d{1,2}/\d{1,2})(?:[ T](?P<time>\d{1,2}:\d{2}(?::\d{2})?))?", first)
    if year and match:
        date_part = f"{match.group('date')}/{year}"
        inferred = f"{date_part} {match.group('time')}" if match.group("time") else date_part
        for fmt in ("%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%m/%d/%Y %H:%M", "%d/%m/%Y", "%m/%d/%Y"):
            try:
                add(datetime.strptime(inferred, fmt).replace(tzinfo=timezone.utc))
            except ValueError:
                pass
    return candidates
