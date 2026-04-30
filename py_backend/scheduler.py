from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Callable

_lock = threading.Lock()
_state: dict = {
    "lastRun": None,
    "lastStatus": None,
    "nextRun": None,
    "running": False,
    "enabled": False,
    "runHours": [],
}


def _next_run_at(run_hours: list[int]) -> datetime:
    now = datetime.now(timezone.utc)
    today = now.date()
    candidates: list[datetime] = []
    for h in run_hours:
        candidate = datetime(today.year, today.month, today.day, h, 0, 0, tzinfo=timezone.utc)
        if candidate > now + timedelta(seconds=30):
            candidates.append(candidate)
    if not candidates:
        tomorrow = today + timedelta(days=1)
        first_hour = min(run_hours)
        candidates = [datetime(tomorrow.year, tomorrow.month, tomorrow.day, first_hour, 0, 0, tzinfo=timezone.utc)]
    return min(candidates)


def start(run_fn: Callable[[str], dict], run_hours: list[int], enabled: bool = True) -> None:
    with _lock:
        _state["enabled"] = enabled
        _state["runHours"] = run_hours
        if enabled:
            _state["nextRun"] = _next_run_at(run_hours).isoformat()

    if not enabled:
        return

    def loop() -> None:
        while True:
            now = datetime.now(timezone.utc)
            next_run = _next_run_at(run_hours)
            with _lock:
                _state["nextRun"] = next_run.isoformat()
                already_running = _state["running"]

            should_run = now.hour in run_hours and now.minute == 0 and not already_running
            if should_run:
                with _lock:
                    _state["running"] = True
                try:
                    date_str = now.date().isoformat()
                    run_fn(date_str)
                    with _lock:
                        _state["lastRun"] = now.isoformat()
                        _state["lastStatus"] = "ok"
                except Exception as exc:  # noqa: BLE001
                    with _lock:
                        _state["lastRun"] = now.isoformat()
                        _state["lastStatus"] = f"error: {exc}"
                finally:
                    with _lock:
                        _state["running"] = False
                # sleep past the minute so we don't double-fire
                time.sleep(61)
            else:
                time.sleep(30)

    thread = threading.Thread(target=loop, daemon=True, name="pipeline-scheduler")
    thread.start()


def trigger(run_fn: Callable[[str], dict], date_str: str) -> dict:
    with _lock:
        if _state["running"]:
            return {"triggered": False, "reason": "already running"}
        _state["running"] = True

    def _run() -> None:
        try:
            run_fn(date_str)
            with _lock:
                _state["lastRun"] = datetime.now(timezone.utc).isoformat()
                _state["lastStatus"] = "ok"
        except Exception as exc:  # noqa: BLE001
            with _lock:
                _state["lastRun"] = datetime.now(timezone.utc).isoformat()
                _state["lastStatus"] = f"error: {exc}"
        finally:
            with _lock:
                _state["running"] = False

    threading.Thread(target=_run, daemon=True, name="pipeline-trigger").start()
    return {"triggered": True, "date": date_str}


def status() -> dict:
    with _lock:
        return dict(_state)
