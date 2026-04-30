from __future__ import annotations

import json
from email.parser import BytesParser
from email.policy import default
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from py_backend import scheduler
from py_backend.config import config
from py_backend.groq_summary import explain_run
from py_backend.pipeline import load_result, run_pipeline
from py_backend.storage.json_store import list_store
from py_backend.utils.time import normalize_date

FRONTEND_DIR = Path("frontend").resolve()

UPLOAD_TARGETS = {
    "prices": "prices.csv",
    "load": "load.csv",
    "res": "res.csv",
    "cross_border": "cross_border.csv",
    "market_signals": "market_signals.csv",
    "grid_outages": "grid_outages.csv",
    "asset_availability": "asset_availability.csv",
    "battery_health": "battery_health.csv",
}


class Handler(SimpleHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def do_GET(self):
        if self.path.startswith("/api/"):
            self._handle_api()
        else:
            self._serve_static()

    def do_POST(self):
        if self.path.startswith("/api/"):
            self._handle_api()
        else:
            self.send_error(404)

    def _handle_api(self):
        try:
            parsed = urlparse(self.path)
            parts = [part for part in parsed.path.split("/") if part]
            query = parse_qs(parsed.query)
            if parsed.path == "/api/health":
                return self._json({"status": "ok", "live": {"openMeteo": True, "entsoe": bool(config.entsoe_token)}, "storedFiles": list_store()})
            if parsed.path == "/api/sources":
                return self._json(_sources())
            if parsed.path == "/api/groq/explain":
                date = normalize_date(query.get("date", [None])[0])
                return self._json(_groq_explain(date))
            if parsed.path == "/api/scheduler/status":
                return self._json(scheduler.status())
            if parsed.path == "/api/scheduler/trigger" and self.command == "POST":
                date = normalize_date(query.get("date", [None])[0])
                return self._json(scheduler.trigger(run_pipeline, date))
            if parsed.path == "/api/manual/upload" and self.command == "POST":
                return self._json(self._handle_upload(query))
            if parsed.path == "/api/pipeline/run" and self.command == "POST":
                date = normalize_date(query.get("date", [None])[0])
                return self._json(run_pipeline(date, _initial_soc_fraction(query)))
            if len(parts) == 3 and parts[0] == "api":
                mapping = {
                    "forecasts": "forecasts",
                    "forecast-errors": "forecast-errors",
                    "dispatch": "dispatch",
                    "features": "features",
                    "summary": "summary",
                }
                collection = mapping.get(parts[1])
                if collection:
                    return self._json(load_result(collection, parts[2]))
            self._json({"error": "not found"}, status=404)
        except Exception as exc:
            self._json({"error": str(exc), "type": exc.__class__.__name__}, status=500)

    def _handle_upload(self, query: dict) -> dict:
        content_type = self.headers.get("content-type", "")
        if not content_type.startswith("multipart/form-data"):
            raise ValueError("Upload must use multipart/form-data.")
        length = int(self.headers.get("content-length", "0") or "0")
        if length <= 0:
            raise ValueError("Upload body is empty.")
        if length > 5_000_000:
            raise ValueError("Upload is too large. Keep each CSV under 5 MB.")

        body = self.rfile.read(length)
        message = BytesParser(policy=default).parsebytes(
            b"Content-Type: " + content_type.encode("utf-8") + b"\r\n"
            b"MIME-Version: 1.0\r\n\r\n" + body
        )
        fields: dict[str, str] = {}
        file_bytes: bytes | None = None
        uploaded_name = "upload.csv"
        for part in message.iter_parts():
            name = part.get_param("name", header="content-disposition")
            if not name:
                continue
            payload = part.get_payload(decode=True) or b""
            filename = part.get_filename()
            if filename:
                file_bytes = payload
                uploaded_name = filename
            else:
                fields[name] = payload.decode("utf-8", errors="replace").strip()

        dataset = fields.get("dataset") or query.get("dataset", [""])[0]
        mode = fields.get("mode") or query.get("mode", ["append"])[0]
        if dataset not in UPLOAD_TARGETS:
            raise ValueError(f"Unknown dataset '{dataset}'.")
        if mode not in ("append", "replace"):
            raise ValueError("Upload mode must be append or replace.")
        if not file_bytes:
            raise ValueError("No CSV file was uploaded.")

        target = config.manual_data_dir / UPLOAD_TARGETS[dataset]
        text = file_bytes.decode("utf-8-sig", errors="replace").replace("\r\n", "\n").replace("\r", "\n").strip()
        if not text:
            raise ValueError("Uploaded CSV is empty.")
        lines = [line for line in text.split("\n") if line.strip()]
        row_count = max(0, len(lines) - 1)
        if mode == "replace" or not target.exists() or target.stat().st_size == 0:
            target.write_text("\n".join(lines) + "\n", encoding="utf-8")
        else:
            existing = target.read_text(encoding="utf-8-sig")
            body_lines = lines[1:] if len(lines) > 1 else []
            with target.open("a", encoding="utf-8", newline="") as handle:
                if existing and not existing.endswith("\n"):
                    handle.write("\n")
                if body_lines:
                    handle.write("\n".join(body_lines) + "\n")

        return {
            "status": "ok",
            "dataset": dataset,
            "target": str(target),
            "uploadedFile": uploaded_name,
            "mode": mode,
            "rows": row_count,
        }

    def _serve_static(self):
        parsed = urlparse(self.path)
        relative = "index.html" if parsed.path == "/" else parsed.path.lstrip("/")
        target = (FRONTEND_DIR / relative).resolve()
        if not str(target).startswith(str(FRONTEND_DIR)) or not target.exists():
            self.send_error(404)
            return
        content_type = "text/html"
        if target.suffix == ".js":
            content_type = "text/javascript"
        elif target.suffix == ".css":
            content_type = "text/css"
        self.send_response(200)
        self.send_header("content-type", content_type)
        self.end_headers()
        self.wfile.write(target.read_bytes())

    def _json(self, value, status: int = 200):
        body = json.dumps(value, indent=2).encode("utf-8")
        self.send_response(status)
        self._cors()
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _cors(self):
        self.send_header("access-control-allow-origin", "*")
        self.send_header("access-control-allow-methods", "GET,POST,OPTIONS")
        self.send_header("access-control-allow-headers", "content-type")


def _sources() -> dict:
    return {
        "openMeteo": {"live": True, "auth": "none", "endpoint": "https://api.open-meteo.com/v1/forecast"},
        "entsoe": {
            "live": bool(config.entsoe_token),
            "auth": "ENTSOE_TOKEN required",
            "endpoint": "https://web-api.tp.entsoe.eu/api",
            "domainGreece": config.entsoe_domain,
        },
        "henex": {
            "liveParsing": False,
            "reason": "Use manual CSV fallback until XLSX parser dependency is added.",
            "resultsSummaryPattern": "https://www.enexgroup.gr/documents/20126/366820/YYYYMMDD_EL-DAM_ResultsSummary_EN_v01.xlsx",
        },
    }


def _groq_explain(date: str) -> dict:
    summary = load_result("summary", date)
    forecasts = load_result("forecasts", date)
    forecast_errors = load_result("forecast-errors", date)
    dispatch = load_result("dispatch", date)
    if not summary or not forecasts or not forecast_errors or not dispatch:
        raise ValueError(f"No complete model output exists for {date}. Run the model first.")
    return explain_run(summary, forecasts, forecast_errors, dispatch)


def _initial_soc_fraction(query: dict) -> float | None:
    raw = query.get("initialSocPct", query.get("initial_soc_pct", [None]))[0]
    if raw in (None, ""):
        return None
    try:
        value = float(raw) / 100.0
    except (TypeError, ValueError):
        raise ValueError("initialSocPct must be a number from 0 to 100.")
    return max(0.0, min(1.0, value))


def main():
    scheduler.start(run_pipeline, config.auto_run_hours, enabled=config.auto_run_enabled)
    if config.auto_run_enabled:
        print(f"Scheduler enabled — auto-runs at UTC hours: {config.auto_run_hours}")
    server = ThreadingHTTPServer(("localhost", config.port), Handler)
    print(f"Python Greece DAM BESS platform running at http://localhost:{config.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
