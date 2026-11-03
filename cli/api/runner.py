"""Running analyses in the background for the API, one worker thread per run.

A run follows the lifecycle the CLI and ``propagate()`` share —
``create_run_state`` → ``begin_checkpoint`` → ``stream_run`` →
``record_decision`` → ``clear_checkpoint_on_success`` — and records what it
streams as events the web app reads back.
"""

from __future__ import annotations

import copy
import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from cli.api import store as s
from cli.api.settings import ApiSettings
from cli.display import classify_message_type
from cli.stats_handler import StatsCallbackHandler
from tradingagents.agents.rating import run_rating
from tradingagents.default_config import DEFAULT_CONFIG
from tradingagents.graph.trading_graph import TradingAgentsGraph
from tradingagents.portfolio import PortfolioContext
from tradingagents.reporting import report_parts

logger = logging.getLogger(__name__)

# A tool's raw output (a price table, a filing) can run to many kilobytes; the
# event keeps enough to show what came back, the report keeps the analysis.
_MAX_DATA_CHARS = 4000

# Cancellation reasons: a user's cancel ends the run; a shutdown leaves it to
# resume when the server starts again.
_USER, _SHUTDOWN = "user", "shutdown"


class _Stopped(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def build_config(request: dict, settings: ApiSettings) -> dict:
    """The run's config: the server's defaults with the request's overrides."""
    config = copy.deepcopy(DEFAULT_CONFIG)
    provider = request.get("llm_provider")
    if provider and provider.lower() != (config.get("llm_provider") or "").lower():
        # The configured endpoint belongs to the configured provider; another
        # provider uses its own default (the CLI resets it the same way).
        config["backend_url"] = None
    if provider:
        config["llm_provider"] = provider.lower()
    for key in ("deep_think_llm", "quick_think_llm", "output_language"):
        if request.get(key):
            config[key] = request[key]
    if request.get("research_depth"):
        config["max_debate_rounds"] = config["max_risk_discuss_rounds"] = request["research_depth"]
    checkpoint = request.get("checkpoint")
    config["checkpoint_enabled"] = settings.checkpoint_by_default if checkpoint is None else checkpoint
    return config


def _message_event(message) -> dict | None:
    kind, content = classify_message_type(message)
    tool_calls = []
    for call in getattr(message, "tool_calls", None) or []:
        if isinstance(call, dict):
            tool_calls.append({"name": call.get("name"), "args": call.get("args")})
        else:
            tool_calls.append({"name": call.name, "args": call.args})
    content = (content or "").strip()
    if kind == "Control" or (not content and not tool_calls):
        return None
    event: dict[str, Any] = {"id": getattr(message, "id", None), "kind": kind, "content": content}
    if kind == "Data":
        event["tool"] = getattr(message, "name", None)
        if len(content) > _MAX_DATA_CHARS:
            event["content"] = content[:_MAX_DATA_CHARS]
            event["truncated"] = len(content)
    if tool_calls:
        event["tool_calls"] = tool_calls
    return event


def _serializable_state(final_state: dict) -> dict:
    """The final state without the message objects, for the run's stored result."""
    state = {k: v for k, v in final_state.items() if k != "messages"}
    state["final_rating"] = run_rating(final_state)
    return state


class RunManager:
    def __init__(self, store: s.RunStore, settings: ApiSettings, graph_class=TradingAgentsGraph):
        self.store = store
        self.settings = settings
        self._graph_class = graph_class
        self._executor = ThreadPoolExecutor(
            max_workers=settings.max_concurrent_runs, thread_name_prefix="tradingagents-run")
        # Reentrant: cancelling a queued future runs its done callback at once,
        # which takes the lock the cancel already holds.
        self._lock = threading.RLock()
        self._futures: dict[str, Future] = {}
        self._stop: dict[str, str] = {}
        self._closing = False

    # Scheduling

    def submit(self, run_id: str) -> None:
        with self._lock:
            if self._closing:
                raise RuntimeError("the server is shutting down")
            self._stop.pop(run_id, None)
            future = self._executor.submit(self._execute, run_id)
            self._futures[run_id] = future
        future.add_done_callback(lambda _f, rid=run_id: self._forget(rid, _f))

    def _forget(self, run_id: str, future: Future) -> None:
        with self._lock:
            if self._futures.get(run_id) is future:
                del self._futures[run_id]
                self._stop.pop(run_id, None)

    def cancel(self, run_id: str) -> bool:
        """Stop a queued or running run; False when it is not active here."""
        with self._lock:
            future = self._futures.get(run_id)
            if future is None:
                return False
            if future.cancel():   # still waiting for a worker: it never starts
                self._finish(run_id, s.CANCELLED, error="cancelled before it started")
                return True
            self._stop[run_id] = _USER
        # Seen between steps: a model call already under way finishes first.
        self.store.append_event(run_id, "log", {"message": "Cancelling after the current step"})
        return True

    def recover(self) -> None:
        """Pick up runs the last shutdown cut off.

        With one process owning the store, a run still queued or running here
        was cut off. A checkpointed one resumes from its last finished step.
        """
        for run in self.store.runs_with_status(*s.ACTIVE):
            if self.settings.resume_on_start and run["attempts"] < self.settings.max_attempts:
                self.store.update_run(run["id"], status=s.QUEUED)
                self.store.append_event(run["id"], "status", {
                    "status": s.QUEUED, "message": "The server restarted; the run will continue"})
                self.submit(run["id"])
            else:
                why = ("it was cut off by a restart too many times" if self.settings.resume_on_start
                       else "it was cut off by a restart")
                self._finish(run["id"], s.INTERRUPTED, error=why)

    def shutdown(self) -> None:
        """Stop taking runs and let running ones stop at their next step.

        Their status stays as it is, so ``recover`` resumes them on the next start.
        """
        with self._lock:
            self._closing = True
            for run_id in self._futures:
                self._stop[run_id] = _SHUTDOWN
        self._executor.shutdown(wait=False, cancel_futures=True)

    def active(self) -> int:
        with self._lock:
            return len(self._futures)

    # Execution

    def _check(self, run_id: str) -> None:
        with self._lock:
            reason = self._stop.get(run_id)
        if reason:
            raise _Stopped(reason)

    def _finish(self, run_id: str, status: str, **fields: Any) -> None:
        # The event is written before the status, so a reader that sees the
        # run finished has already been offered its last event.
        self.store.append_event(run_id, "status", {"status": status, **{
            k: v for k, v in fields.items() if k in ("error", "rating")}})
        self.store.update_run(run_id, status=status, finished_at=s.now(), **fields)

    def _execute(self, run_id: str) -> None:
        run = self.store.get_run(run_id)
        if run is None:
            return
        attempt = self.store.start_attempt(run_id)
        self.store.append_event(run_id, "status", {"status": s.RUNNING, "attempt": attempt})
        stats = StatsCallbackHandler()
        try:
            final_state, report_dir = self._run(run, stats)
        except _Stopped as stop:
            if stop.reason == _USER:
                self._finish(run_id, s.CANCELLED, stats=stats.get_stats())
            # On shutdown the run stays "running" for the next start to resume.
            return
        except Exception as exc:
            logger.exception("Run %s failed", run_id)
            self._finish(run_id, s.FAILED, error=f"{type(exc).__name__}: {exc}",
                         stats=stats.get_stats())
            return
        self._finish(run_id, s.COMPLETED, rating=run_rating(final_state),
                     report_dir=str(report_dir), stats=stats.get_stats(),
                     result=_serializable_state(final_state))

    def _run(self, run: dict, stats: StatsCallbackHandler):
        run_id, request = run["id"], run["request"]
        ticker, trade_date, asset_type = run["ticker"], run["trade_date"], run["asset_type"]
        emit = self.store.append_event

        self._check(run_id)
        config = build_config(request, self.settings)
        graph = self._graph_class(run["analysts"], config=config, debug=False, callbacks=[stats])
        portfolio = (PortfolioContext.model_validate(request["portfolio"])
                     if request.get("portfolio") else None)
        init_state = graph.create_run_state(ticker, trade_date, asset_type, portfolio)
        args = graph.propagator.get_graph_args(callbacks=[stats])

        # Messages an earlier attempt already recorded are not sent again when
        # a resumed run replays its saved state.
        seen = {e["data"].get("id") for e in self.store.events_after(run_id, 0, limit=100_000)
                if e["type"] == "message"}
        sections: dict[str, str] = {}
        last_stats: dict | None = None

        tid = graph.begin_checkpoint(ticker, trade_date, asset_type, portfolio)
        try:
            if tid is not None:
                args.setdefault("config", {}).setdefault("configurable", {})["thread_id"] = tid
                resuming = getattr(graph, "_resuming", False)
                emit(run_id, "log", {"message": "Resuming from the last saved step" if resuming
                                     else "Starting fresh"})
            final_state: dict = {}
            stream = graph.stream_run(graph.checkpoint_input(init_state), **args)
            try:
                for messages, chunk in stream:
                    self._check(run_id)
                    for message in messages:
                        event = _message_event(message)
                        if event is None or (event["id"] is not None and event["id"] in seen):
                            continue
                        seen.add(event["id"])
                        emit(run_id, "message", event)
                    if chunk is None:
                        continue
                    final_state.update(chunk)
                    for heading, folder, written in report_parts(chunk):
                        for agent, filename, text in written:
                            key = f"{folder}/{filename.removesuffix('.md')}"
                            if sections.get(key) != text:
                                sections[key] = text
                                emit(run_id, "report", {"section": key, "heading": heading,
                                                        "agent": agent, "content": text})
                    current = stats.get_stats()
                    if current != last_stats:
                        last_stats = current
                        emit(run_id, "stats", current)
            finally:
                stream.close()

            graph.record_decision(ticker, trade_date, final_state)
            graph.clear_checkpoint_on_success(ticker, trade_date, asset_type, portfolio)
        finally:
            graph.end_checkpoint()

        # Saved where the CLI saves its reports, so both appear in one place.
        report_file = graph.save_reports(final_state, ticker)
        return final_state, report_file.parent
