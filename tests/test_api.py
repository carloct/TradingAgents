"""The web API (`tradingagents serve`): runs start, stream, finish and survive a restart.

Runs go through the real graph with the scripted models and offline vendors of
the end-to-end test, so what the API streams is what a run produces.
"""

from __future__ import annotations

import threading
import time

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402
from typer.testing import CliRunner  # noqa: E402

import cli.main as cli_main  # noqa: E402
from cli.api import app as api_app, store as s  # noqa: E402
from cli.api.runner import build_config  # noqa: E402
from cli.api.settings import ApiSettings  # noqa: E402
from tests.test_graph_end_to_end import (  # noqa: E402, F401  (offline is a fixture)
    TRADE_DATE,
    ScriptedModel,
    _Client,
    offline,
)
from tradingagents.default_config import DEFAULT_CONFIG  # noqa: E402
from tradingagents.graph import trading_graph  # noqa: E402

KEY = "secret-key"
AUTH = {"Authorization": f"Bearer {KEY}"}


class GatedModel(ScriptedModel):
    """Waits for the test to open the gate before answering, so a run stays active."""

    gate: threading.Event

    def _generate(self, *args, **kwargs):
        assert self.gate.wait(10), "the test never opened the gate"
        return super()._generate(*args, **kwargs)


@pytest.fixture
def paths(tmp_path, monkeypatch):
    for key, value in (("results_dir", tmp_path / "results"), ("data_cache_dir", tmp_path / "cache"),
                       ("memory_log_path", tmp_path / "memory" / "log.md")):
        monkeypatch.setitem(DEFAULT_CONFIG, key, str(value))
    monkeypatch.setattr(api_app, "POLL_SECONDS", 0.02)
    return tmp_path


@pytest.fixture
def model(monkeypatch):
    holder = {"model": ScriptedModel()}
    monkeypatch.setattr(trading_graph, "create_tier_client",
                        lambda config, tier, **k: _Client(holder["model"]))
    return holder


def _client(paths, **settings) -> TestClient:
    settings = {"api_key": KEY, "db_path": paths / "api" / "runs.db", **settings}
    return TestClient(api_app.create_app(ApiSettings(**settings)))


def _wait(client, run_id, statuses=s.FINISHED, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = client.get(f"/api/v1/runs/{run_id}", headers=AUTH).json()
        if run["status"] in statuses:
            return run
        time.sleep(0.02)
    raise AssertionError(f"run {run_id} stayed {run['status']}")


def _start(client, **body):
    return client.post("/api/v1/runs", headers=AUTH,
                       json={"ticker": "NVDA", "trade_date": TRADE_DATE, **body})


@pytest.mark.unit
def test_every_route_but_the_health_check_needs_the_key(paths):
    with _client(paths) as client:
        assert client.get("/healthz").status_code == 200
        assert client.get("/api/v1/runs").status_code == 401
        assert client.get("/api/v1/runs", headers={"Authorization": "Bearer wrong"}).status_code == 401
        for ok in ({"headers": AUTH}, {"headers": {"X-API-Key": KEY}},
                   {"params": {"access_token": KEY}}):
            assert client.get("/api/v1/runs", **ok).status_code == 200, ok


@pytest.mark.unit
@pytest.mark.usefixtures("offline")
def test_a_run_streams_its_reports_and_ends_with_a_saved_decision(paths, model):
    with _client(paths) as client:
        started = _start(client, analysts=["market", "sentiment"])
        assert started.status_code == 202
        run_id = started.json()["id"]
        assert started.json()["analysts"] == ["market", "sentiment"]

        run = _wait(client, run_id)

        assert run["status"] == "completed", run.get("error")
        assert run["rating"] == run["result"]["final_rating"] == "Overweight"
        assert run["has_report"] and "report_dir" not in run
        assert run["stats"]["llm_calls"] > 0

        events = client.get(f"/api/v1/runs/{run_id}/events", headers=AUTH).json()
        assert [e["seq"] for e in events] == list(range(1, len(events) + 1))
        statuses = [e["data"]["status"] for e in events if e["type"] == "status"]
        assert statuses == ["queued", "running", "completed"]
        sections = {e["data"]["section"] for e in events if e["type"] == "report"}
        assert {"1_analysts/market", "1_analysts/sentiment", "5_portfolio/decision"} <= sections
        assert "1_analysts/fundamentals" not in sections
        tool_calls = [c["name"] for e in events if e["type"] == "message"
                      for c in e["data"].get("tool_calls", [])]
        assert "get_stock_data" in tool_calls

        page = client.get(f"/api/v1/runs/{run_id}/report", params={"access_token": KEY})
        assert page.status_code == 200 and page.headers["content-type"].startswith("text/html")
        markdown = client.get(f"/api/v1/runs/{run_id}/report?format=md", headers=AUTH)
        assert "Rating: Overweight" in markdown.text

        # The run is in the history and memory the CLI reads and writes.
        history = client.get("/api/v1/history", headers=AUTH).json()
        assert [(h["ticker"], h["trade_date"], h["rating"]) for h in history] == [
            ("NVDA", TRADE_DATE, "Overweight")]
        state = client.get(f"/api/v1/history/NVDA/{TRADE_DATE}", headers=AUTH).json()
        assert state["final_rating"] == "Overweight"
        memory = client.get("/api/v1/memory", headers=AUTH).json()
        assert [(m["ticker"], m["rating"], m["pending"]) for m in memory] == [("NVDA", "Overweight", True)]


@pytest.mark.unit
@pytest.mark.usefixtures("offline")
def test_the_event_stream_replays_from_the_last_event_the_browser_saw(paths, model):
    with _client(paths) as client:
        run_id = _start(client, analysts=["market"]).json()["id"]
        _wait(client, run_id)
        stream = {"accept": "text/event-stream", **AUTH}

        full = client.get(f"/api/v1/runs/{run_id}/events", headers=stream)
        resumed = client.get(f"/api/v1/runs/{run_id}/events", headers={**stream, "Last-Event-ID": "2"})

    assert full.headers["content-type"].startswith("text/event-stream")
    ids = [int(line[4:]) for line in full.text.splitlines() if line.startswith("id: ")]
    assert ids[0] == 1 and full.text.rstrip().endswith('data: {"status": "completed"}')
    assert [int(line[4:]) for line in resumed.text.splitlines() if line.startswith("id: ")] == ids[2:]


@pytest.mark.unit
@pytest.mark.parametrize(("body", "message"), [
    ({"ticker": "NVDA", "trade_date": "2999-01-01"}, "in the future"),
    ({"ticker": "NVDA", "trade_date": "09/01/2026"}, "not a date"),
    ({"ticker": "NVDA", "analysts": ["astrology"]}, "unknown analyst"),
    ({"ticker": "BTC-USD", "analysts": ["fundamentals"]}, "not available for crypto"),
    ({"ticker": "../etc"}, "not a ticker"),
])
def test_a_request_the_cli_would_refuse_is_refused_before_a_run_starts(paths, body, message):
    with _client(paths) as client:
        response = client.post("/api/v1/runs", headers=AUTH, json=body)
        assert response.status_code == 422
        assert message in response.json()["detail"]
        assert client.get("/api/v1/runs", headers=AUTH).json() == []


@pytest.mark.unit
def test_an_unknown_setting_is_refused_rather_than_ignored(paths):
    with _client(paths) as client:
        response = _start(client, reserch_depth=3)
        assert response.status_code == 422


@pytest.mark.unit
@pytest.mark.usefixtures("offline")
def test_a_run_can_be_cancelled_then_retried_and_one_at_a_time_per_ticker_and_date(paths, model):
    model["model"] = GatedModel(gate=threading.Event())
    with _client(paths) as client:
        run_id = _start(client, analysts=["market"]).json()["id"]
        _wait(client, run_id, {"running"})

        duplicate = _start(client, analysts=["news"])
        assert duplicate.status_code == 409 and duplicate.json()["detail"]["run_id"] == run_id

        assert client.post(f"/api/v1/runs/{run_id}/cancel", headers=AUTH).status_code == 200
        model["model"].gate.set()
        assert _wait(client, run_id)["status"] == "cancelled"
        assert client.post(f"/api/v1/runs/{run_id}/cancel", headers=AUTH).status_code == 409

        assert client.post(f"/api/v1/runs/{run_id}/retry", headers=AUTH).status_code == 202
        run = _wait(client, run_id)
        assert run["status"] == "completed" and run["attempts"] == 2

        assert client.delete(f"/api/v1/runs/{run_id}", headers=AUTH).status_code == 204
        assert client.get(f"/api/v1/runs/{run_id}", headers=AUTH).status_code == 404


@pytest.mark.unit
@pytest.mark.usefixtures("offline")
def test_a_failed_run_reports_why(paths, model):
    model["model"] = ScriptedModel(fail_at=3)
    with _client(paths) as client:
        run = _wait(client, _start(client, analysts=["market"]).json()["id"])
    assert run["status"] == "failed"
    assert "provider unavailable" in run["error"]


@pytest.mark.unit
@pytest.mark.usefixtures("offline")
def test_a_run_cut_off_by_a_restart_continues_on_the_next_start(paths, model):
    db = paths / "api" / "runs.db"
    store = s.RunStore(db)
    cut_off = store.create_run(ticker="NVDA", trade_date=TRADE_DATE, asset_type="stock",
                               analysts=["market"], request={"ticker": "NVDA"})
    store.start_attempt(cut_off["id"])
    gave_up = store.create_run(ticker="AAPL", trade_date=TRADE_DATE, asset_type="stock",
                               analysts=["market"], request={"ticker": "AAPL"})
    for _ in range(3):
        store.start_attempt(gave_up["id"])
    store.close()

    with _client(paths) as client:
        resumed = _wait(client, cut_off["id"])
        abandoned = client.get(f"/api/v1/runs/{gave_up['id']}", headers=AUTH).json()

    assert resumed["status"] == "completed" and resumed["attempts"] == 2
    assert abandoned["status"] == "interrupted" and "too many times" in abandoned["error"]


@pytest.mark.unit
def test_a_request_overrides_only_what_it_names():
    settings = ApiSettings()
    plain = build_config({"ticker": "NVDA"}, settings)
    assert plain["llm_provider"] == DEFAULT_CONFIG["llm_provider"]
    assert plain["checkpoint_enabled"] is True

    config = build_config({"ticker": "NVDA", "research_depth": 3, "checkpoint": False,
                           "llm_provider": "Anthropic", "deep_think_llm": "claude-opus-5-5"},
                          settings)
    assert config["max_debate_rounds"] == config["max_risk_discuss_rounds"] == 3
    assert config["checkpoint_enabled"] is False
    assert config["llm_provider"] == "anthropic" and config["deep_think_llm"] == "claude-opus-5-5"
    assert config["backend_url"] is None   # another provider's endpoint is not carried over


@pytest.mark.unit
def test_settings_are_read_from_the_environment(monkeypatch, tmp_path):
    monkeypatch.setenv("TRADINGAGENTS_API_KEY", KEY)
    monkeypatch.setenv("TRADINGAGENTS_API_MAX_RUNS", "4")
    monkeypatch.setenv("TRADINGAGENTS_API_CORS_ORIGINS", "https://app.example.com, http://localhost:5173")
    monkeypatch.setenv("TRADINGAGENTS_API_DB_PATH", str(tmp_path / "runs.db"))
    settings = ApiSettings.from_env()
    assert (settings.api_key, settings.max_concurrent_runs) == (KEY, 4)
    assert settings.cors_origins == ("https://app.example.com", "http://localhost:5173")
    assert settings.db_path == tmp_path / "runs.db"

    monkeypatch.setenv("TRADINGAGENTS_API_MAX_RUNS", "many")
    with pytest.raises(ValueError, match="TRADINGAGENTS_API_MAX_RUNS"):
        ApiSettings.from_env()


@pytest.mark.unit
def test_serve_refuses_to_start_without_a_key_except_locally_with_no_auth(monkeypatch, tmp_path):
    import uvicorn

    served = []
    monkeypatch.setattr(uvicorn, "run", lambda app, **kw: served.append(kw))
    monkeypatch.setenv("TRADINGAGENTS_API_DB_PATH", str(tmp_path / "runs.db"))
    runner = CliRunner()

    refused = runner.invoke(cli_main.app, ["serve"])
    assert refused.exit_code == 1 and "TRADINGAGENTS_API_KEY" in refused.output
    exposed = runner.invoke(cli_main.app, ["serve", "--no-auth", "--host", "0.0.0.0"])
    assert exposed.exit_code == 1 and served == []

    assert runner.invoke(cli_main.app, ["serve", "--no-auth"]).exit_code == 0
    monkeypatch.setenv("TRADINGAGENTS_API_KEY", KEY)
    assert runner.invoke(cli_main.app, ["serve", "--host", "0.0.0.0", "--port", "9000"]).exit_code == 0
    assert [(kw["host"], kw["port"], kw["workers"]) for kw in served] == [
        ("127.0.0.1", 8000, 1), ("0.0.0.0", 9000, 1)]
