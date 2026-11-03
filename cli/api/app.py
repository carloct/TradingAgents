"""The web API: start analyses, follow them live, read their reports.

Started by ``tradingagents serve``. It runs beside the CLI on the same results,
memory log and checkpoints: a run started from either shows up in the other's
history, and the two cannot run the same checkpointed analysis at once.
"""

from __future__ import annotations

import asyncio
import datetime
import hmac
import json
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from starlette.concurrency import run_in_threadpool

import tradingagents
from cli.api import store as s
from cli.api.runner import RunManager
from cli.api.schemas import RunRequest
from cli.api.settings import ApiSettings
from cli.models import AnalystType, AssetType
from cli.prompts import (
    detect_asset_type,
    filter_analysts_for_asset_type,
    parse_analysis_date,
    parse_analysts,
    parse_ticker,
)
from tradingagents.dataflows.symbols import safe_ticker_component
from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.memory import TradingMemoryLog
from tradingagents.reporting import analyst_names

# How often a live event stream looks for new events, and how long it may stay
# silent before it sends a comment so proxies keep the connection open.
POLL_SECONDS = 0.5
HEARTBEAT_SECONDS = 15.0


# The report page loads and runs nothing (see report_html), and nothing here is
# meant to be framed. Sent as a header too: a <meta> policy cannot forbid framing.
_CONTENT_SECURITY_POLICY = (
    "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; "
    "form-action 'none'; frame-ancestors 'none'"
)
_SECURITY_HEADERS = [
    (b"x-content-type-options", b"nosniff"),
    (b"x-frame-options", b"DENY"),
    # A report opened as ...?access_token=<key> would otherwise pass that address
    # on to a site it links to (browsers send only the origin by default, but a
    # page or browser can widen that).
    (b"referrer-policy", b"no-referrer"),
    # Runs, reports and the memory log are private: no proxy or browser cache keeps them.
    (b"cache-control", b"no-store"),
]
# The interactive docs load Swagger UI and ReDoc from a CDN and run scripts.
_DOCS_PATHS = ("/docs", "/redoc")


class SecurityHeaders:
    """Add the security headers to every response.

    Plain ASGI rather than ``BaseHTTPMiddleware``, which wraps streaming
    responses and gets in the way of a live event stream noticing a disconnect.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        docs = scope["path"].startswith(_DOCS_PATHS)

        async def send_with_headers(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                # A header the response sets itself wins (the event stream's Cache-Control).
                present = {k.lower() for k, _ in headers}
                headers += [(k, v) for k, v in _SECURITY_HEADERS if k not in present]
                if not docs:
                    headers.append((b"content-security-policy", _CONTENT_SECURITY_POLICY.encode()))
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_with_headers)


def _store(request: Request) -> s.RunStore:
    return request.app.state.store


def _manager(request: Request) -> RunManager:
    return request.app.state.manager


Store = Annotated[s.RunStore, Depends(_store)]
Manager = Annotated[RunManager, Depends(_manager)]


def _public_run(run: dict) -> dict:
    """A run as the API shows it: analysts by the names users pick them by, no local paths."""
    run = dict(run)
    run["analysts"] = analyst_names(run["analysts"])
    run["has_report"] = bool(run.pop("report_dir", None))
    return run


def create_app(settings: ApiSettings | None = None, manager_class=RunManager) -> FastAPI:
    settings = settings or ApiSettings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store = s.RunStore(settings.db_path)
        manager = manager_class(store, settings)
        app.state.store, app.state.manager = store, manager
        manager.recover()
        yield
        # The store stays open: a worker may still write its last event before
        # the process exits, and each write is committed on its own.
        manager.shutdown()

    app = FastAPI(
        title="TradingAgents API",
        version=tradingagents.__version__,
        lifespan=lifespan,
        docs_url="/docs" if settings.docs else None,
        redoc_url="/redoc" if settings.docs else None,
        openapi_url="/openapi.json" if settings.docs else None,
    )
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=list(settings.cors_origins),
            allow_methods=["*"],
            allow_headers=["*"],
        )
    # Added last so it wraps everything, CORS responses included.
    app.add_middleware(SecurityHeaders)

    def require_key(request: Request) -> None:
        """Bearer token, X-API-Key header, or ``access_token`` in the query.

        The query form is for what a browser cannot send headers with: an
        EventSource stream, or a report opened in a new tab.
        """
        if settings.api_key is None:
            return
        header = request.headers.get("authorization", "")
        supplied = (header[7:].strip() if header.lower().startswith("bearer ") else None) \
            or request.headers.get("x-api-key") or request.query_params.get("access_token")
        if not supplied or not hmac.compare_digest(supplied.encode(), settings.api_key.encode()):
            raise HTTPException(status_code=401, detail="missing or wrong API key",
                                headers={"WWW-Authenticate": "Bearer"})

    def get_run_or_404(run_id: str, st: s.RunStore, with_result: bool = False) -> dict:
        run = st.get_run(run_id, with_result=with_result)
        if run is None:
            raise HTTPException(status_code=404, detail="no such run")
        return run

    @app.get("/healthz", include_in_schema=False)
    def healthz():
        return {"status": "ok", "version": tradingagents.__version__}

    api = "/api/v1"
    auth = [Depends(require_key)]

    @app.get(f"{api}/options", dependencies=auth)
    def options():
        """What a run can be configured with, and what it gets when the request says nothing."""
        all_analysts = list(AnalystType)
        return {
            "version": tradingagents.__version__,
            "analysts": {
                kind.value: analyst_names([a.value for a in filter_analysts_for_asset_type(all_analysts, kind)])
                for kind in AssetType
            },
            "defaults": {
                "llm_provider": DEFAULT_CONFIG.get("llm_provider"),
                "deep_think_llm": DEFAULT_CONFIG.get("deep_think_llm"),
                "quick_think_llm": DEFAULT_CONFIG.get("quick_think_llm"),
                "max_debate_rounds": DEFAULT_CONFIG.get("max_debate_rounds"),
                "max_risk_discuss_rounds": DEFAULT_CONFIG.get("max_risk_discuss_rounds"),
                "output_language": DEFAULT_CONFIG.get("output_language"),
                "checkpoint": settings.checkpoint_by_default,
            },
            "max_concurrent_runs": settings.max_concurrent_runs,
        }

    @app.post(f"{api}/runs", status_code=202, dependencies=auth)
    def start_run(body: RunRequest, st: Store, mgr: Manager):
        """Queue an analysis; follow it at ``/runs/{id}/events``."""
        try:
            ticker = parse_ticker(body.ticker)
            kind = detect_asset_type(ticker)
            trade_date = (parse_analysis_date(body.trade_date) if body.trade_date
                          else datetime.date.today().isoformat())
            chosen = (parse_analysts(",".join(body.analysts), kind) if body.analysts
                      else filter_analysts_for_asset_type(list(AnalystType), kind))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None
        active = st.active_run_for(ticker, trade_date)
        if active is not None:
            raise HTTPException(status_code=409, detail={
                "message": f"{ticker} on {trade_date} is already being analyzed",
                "run_id": active["id"]})
        run = st.create_run(ticker=ticker, trade_date=trade_date, asset_type=kind.value,
                            analysts=[a.value for a in chosen],
                            request=body.model_dump(mode="json", exclude_none=True))
        st.append_event(run["id"], "status", {"status": s.QUEUED})
        mgr.submit(run["id"])
        return _public_run(run)

    @app.get(f"{api}/runs", dependencies=auth)
    def list_runs(st: Store, ticker: str | None = None, status: str | None = None,
                  limit: Annotated[int, Query(ge=1, le=500)] = 50,
                  offset: Annotated[int, Query(ge=0)] = 0):
        return [_public_run(r) for r in st.list_runs(
            ticker=ticker.strip().upper() if ticker else None, status=status,
            limit=limit, offset=offset)]

    @app.get(f"{api}/runs/{{run_id}}", dependencies=auth)
    def get_run(run_id: str, st: Store):
        """The run, with its final state once completed."""
        return _public_run(get_run_or_404(run_id, st, with_result=True))

    @app.post(f"{api}/runs/{{run_id}}/cancel", dependencies=auth)
    def cancel_run(run_id: str, st: Store, mgr: Manager):
        run = get_run_or_404(run_id, st)
        if run["status"] not in s.ACTIVE or not mgr.cancel(run_id):
            raise HTTPException(status_code=409, detail=f"the run is {run['status']}, not active")
        return _public_run(get_run_or_404(run_id, st))

    @app.post(f"{api}/runs/{{run_id}}/retry", status_code=202, dependencies=auth)
    def retry_run(run_id: str, st: Store, mgr: Manager):
        """Run a failed, cancelled or interrupted run again; a checkpointed one resumes."""
        run = get_run_or_404(run_id, st)
        if run["status"] not in (s.FAILED, s.CANCELLED, s.INTERRUPTED):
            raise HTTPException(status_code=409, detail=f"the run is {run['status']}")
        active = st.active_run_for(run["ticker"], run["trade_date"])
        if active is not None:
            raise HTTPException(status_code=409, detail={
                "message": f"{run['ticker']} on {run['trade_date']} is already being analyzed",
                "run_id": active["id"]})
        st.update_run(run_id, status=s.QUEUED, finished_at=None, error=None)
        st.append_event(run_id, "status", {"status": s.QUEUED, "message": "Retrying"})
        mgr.submit(run_id)
        return _public_run(get_run_or_404(run_id, st))

    @app.delete(f"{api}/runs/{{run_id}}", status_code=204, dependencies=auth)
    def delete_run(run_id: str, st: Store):
        """Forget a finished run and its events; its saved report stays on disk."""
        run = get_run_or_404(run_id, st)
        if run["status"] in s.ACTIVE:
            raise HTTPException(status_code=409, detail="cancel the run first")
        st.delete_run(run_id)

    @app.get(f"{api}/runs/{{run_id}}/events", dependencies=auth)
    async def run_events(run_id: str, request: Request, st: Store,
                         after: Annotated[int, Query(ge=0)] = 0):
        """The run's events: a live Server-Sent Events stream when asked for
        ``text/event-stream`` (an EventSource does), else the events so far as JSON.

        A stream replays from ``after``, or from the browser's ``Last-Event-ID``
        when it reconnects, so nothing is missed across a dropped connection.
        """
        get_run_or_404(run_id, st)
        if "text/event-stream" not in request.headers.get("accept", ""):
            return await run_in_threadpool(st.events_after, run_id, after, 100_000)
        last_id = request.headers.get("last-event-id", "")
        start = int(last_id) if last_id.isdigit() else after
        return StreamingResponse(
            _event_stream(request, st, run_id, start),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.get(f"{api}/runs/{{run_id}}/report", dependencies=auth)
    def run_report(run_id: str, st: Store,
                   format: Annotated[str, Query(pattern="^(html|md)$")] = "html"):
        """The saved report, as the one-page HTML or the markdown."""
        run = get_run_or_404(run_id, st)
        if not run.get("report_dir"):
            raise HTTPException(status_code=404, detail="the run has no report yet")
        path = Path(run["report_dir"]) / f"complete_report.{format}"
        if not path.is_file():
            raise HTTPException(status_code=404, detail="the report file is gone")
        media = "text/html" if format == "html" else "text/markdown"
        return FileResponse(path, media_type=f"{media}; charset=utf-8")

    @app.get(f"{api}/history", dependencies=auth)
    def history(ticker: str | None = None, limit: Annotated[int, Query(ge=1, le=1000)] = 100):
        """Every finished analysis on this server, the CLI's included, newest first."""
        try:
            return _history_entries(ticker, limit)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from None

    @app.get(f"{api}/history/{{ticker}}/{{trade_date}}", dependencies=auth)
    def history_entry(ticker: str, trade_date: str):
        """A finished analysis's full state, as written to its state log."""
        path = _state_log_path(ticker, trade_date)
        if not path.is_file():
            raise HTTPException(status_code=404, detail="no analysis of that ticker on that date")
        return json.loads(path.read_text(encoding="utf-8"))

    @app.get(f"{api}/memory", dependencies=auth)
    def memory(ticker: str | None = None):
        """The memory log: past decisions, their realized returns and the lessons drawn."""
        entries = TradingMemoryLog(DEFAULT_CONFIG).load_entries()
        if ticker:
            entries = [e for e in entries if e.get("ticker", "").upper() == ticker.strip().upper()]
        return list(reversed(entries))

    return app


def _results_dir() -> Path:
    return Path(DEFAULT_CONFIG["results_dir"])


def _state_log_path(ticker: str, trade_date: str) -> Path:
    try:
        safe = safe_ticker_component(ticker.strip().upper())
        datetime.date.fromisoformat(trade_date)
    except ValueError:
        raise HTTPException(status_code=404, detail="no analysis of that ticker on that date") from None
    return _results_dir() / safe / "TradingAgentsStrategy_logs" / f"full_states_log_{trade_date}.json"


def _history_entries(ticker: str | None, limit: int) -> list[dict]:
    """Summaries of the state logs every run writes, CLI and API alike."""
    pattern = (f"{safe_ticker_component(ticker.strip().upper())}" if ticker else "*")
    logs = sorted(_results_dir().glob(f"{pattern}/TradingAgentsStrategy_logs/full_states_log_*.json"),
                  key=lambda p: p.stat().st_mtime, reverse=True)[:limit]
    entries = []
    for path in logs:
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        settings = state.get("run_settings") or {}
        entries.append({
            "ticker": state.get("company_of_interest"),
            "trade_date": state.get("trade_date"),
            "rating": state.get("final_rating"),
            "finished_at": datetime.datetime.fromtimestamp(
                path.stat().st_mtime, datetime.UTC).isoformat(timespec="seconds"),
            "analysts": analyst_names(settings.get("analysts")),
            "deep_think_llm": settings.get("deep_think_llm"),
            "quick_think_llm": settings.get("quick_think_llm"),
            "version": settings.get("version"),
        })
    return entries


def _sse(event: dict) -> str:
    data = json.dumps(event, ensure_ascii=False)
    return f"id: {event['seq']}\nevent: {event['type']}\ndata: {data}\n\n"


async def _event_stream(request: Request, st: s.RunStore, run_id: str, after: int):
    last, last_sent = after, time.monotonic()
    while True:
        if await request.is_disconnected():
            return
        events = await run_in_threadpool(st.events_after, run_id, last)
        for event in events:
            last = event["seq"]
            yield _sse(event)
        if events:
            last_sent = time.monotonic()
            continue
        run = await run_in_threadpool(st.get_run, run_id)
        if run is None or run["status"] in s.FINISHED:
            # A run's last event is written before its status, but possibly
            # after the read above: read once more before ending.
            for event in await run_in_threadpool(st.events_after, run_id, last):
                yield _sse(event)
            yield f"event: end\ndata: {json.dumps({'status': run['status'] if run else 'deleted'})}\n\n"
            return
        if time.monotonic() - last_sent >= HEARTBEAT_SECONDS:
            yield ": keep-alive\n\n"
            last_sent = time.monotonic()
        await asyncio.sleep(POLL_SECONDS)
