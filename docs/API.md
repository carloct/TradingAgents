# Web API

`tradingagents serve` runs a web API beside the CLI, so a web app can start analyses and follow them live. It uses the same settings, results, memory log and checkpoints as the CLI, so a run started from either appears in the other's history.

```bash
pip install ".[api]"
export TRADINGAGENTS_API_KEY=a-long-random-string   # required: every run spends provider credit
TRADINGAGENTS_API_DOCS=true tradingagents serve      # http://127.0.0.1:8000, interactive docs at /docs
```

## Endpoints

All under `/api/v1`, and all need the key except `GET /healthz`.

| Endpoint | |
|---|---|
| `POST /runs` | Queue an analysis: `{"ticker": "NVDA", "trade_date": "2026-09-23", "analysts": ["market", "news"]}`. Also `research_depth`, `llm_provider`, `deep_think_llm`, `quick_think_llm`, `output_language`, `checkpoint`, `portfolio`. Omitted fields take the server's defaults; an unknown field is refused. Returns `409` while the same ticker and date is already being analyzed. |
| `GET /runs/{id}/events` | The run's progress as Server-Sent Events: `status`, `log`, `message` (agent messages and tool calls), `report` (each section as it is written), `stats` (LLM and tool calls, tokens), then `end`. Reconnecting replays what was missed, from `Last-Event-ID` or `?after=<seq>`. Without `Accept: text/event-stream` it returns the events so far as JSON. |
| `GET /runs/{id}` | The run's status, rating, token counts and error, and its final state once completed. |
| `GET /runs/{id}/report` | The saved report as one HTML page (`?format=md` for markdown). |
| `POST /runs/{id}/cancel` | Stop a run after its current step; a model call already under way finishes first. |
| `POST /runs/{id}/retry` | Run a failed, cancelled or interrupted run again; a checkpointed one resumes where it stopped. |
| `DELETE /runs/{id}` | Forget a finished run and its events; its saved report stays on disk. |
| `GET /runs` | API runs, newest first; filter with `?ticker=` and `?status=`. |
| `GET /history`, `GET /history/{ticker}/{date}` | Every finished analysis on the server, the CLI's included, and one's full state. |
| `GET /memory` | The memory log: past decisions, their realized returns and the lessons drawn. |
| `GET /options` | What a run can be configured with, and the server's defaults. |

A run's status is one of `queued`, `running`, `completed`, `failed`, `cancelled` or `interrupted`.

## Authentication

Send the key as `Authorization: Bearer <key>` or `X-API-Key: <key>`. A browser `EventSource` and a report opened in a new tab cannot send headers, so those take `?access_token=<key>`. A key in a URL can end up in proxy access logs; to avoid that, read the event stream with `fetch` (for example `@microsoft/fetch-event-source`) and send the header.

Without `TRADINGAGENTS_API_KEY` the server refuses to start. `tradingagents serve --no-auth` serves without a key, on 127.0.0.1 only.

Every response tells the browser not to cache it (`Cache-Control: no-store`), not to show it in a frame, and not to pass its address on to a site it links to (`Referrer-Policy: no-referrer`), so a report opened with `?access_token=` does not leak the key through its links. The report page loads and runs nothing.

### Hardening the server

- Set a spending limit for the provider key: anyone holding the API key can start runs.
- Behind Cloudflare, accept ports 80 and 443 only from [Cloudflare's addresses](https://www.cloudflare.com/ips/), use SSL mode **Full (strict)**, and keep the Dokploy panel (port 3000) off the public internet.
- Rotate the key if it has been pasted anywhere it should not stay: change `TRADINGAGENTS_API_KEY` and redeploy.

## Settings

A run's settings come from the same `TRADINGAGENTS_*` variables the CLI reads (provider, models, depth, language), overridden per request. The server's own:

| Variable | Default | |
|---|---|---|
| `TRADINGAGENTS_API_KEY` | none | Required. |
| `TRADINGAGENTS_API_MAX_RUNS` | `2` | Runs at once; the rest queue. |
| `TRADINGAGENTS_API_CORS_ORIGINS` | none | Comma-separated browser origins, when the web app is served from another domain. |
| `TRADINGAGENTS_API_DB_PATH` | `~/.tradingagents/api/runs.db` | Where runs and their events are kept. |
| `TRADINGAGENTS_API_CHECKPOINT` | `true` | Checkpoint runs unless a request says otherwise, so a cut-off run resumes. |
| `TRADINGAGENTS_API_RESUME_ON_START` | `true` | Continue runs a restart cut off. |
| `TRADINGAGENTS_API_MAX_ATTEMPTS` | `3` | Starts a run gets before it is left `interrupted`. |
| `TRADINGAGENTS_API_DOCS` | `false` | Serve the interactive docs at `/docs`, `/redoc` and `/openapi.json`. They show anyone how the API is laid out, though not its data. |

## Restarts and the CLI

Runs and their events are kept on disk. A run cut off by a restart or redeploy is picked up when the server starts again and, being checkpointed, resumes from its last finished step. The server runs as a single process: run one replica.

The CLI and the API cannot run the same checkpointed analysis (ticker, date and settings) at once: the second is refused while the first runs.

## Deploying (Dokploy)

`docker-compose.api.yml` builds the image's `api` stage, with a health check, and keeps its data in the `tradingagents_data` volume.

1. Create a Compose service from this repository, branch `deploy`, compose path `docker-compose.api.yml`.
2. Under Environment, set `TRADINGAGENTS_API_KEY` and your provider keys (and `TRADINGAGENTS_API_CORS_ORIGINS` if the web app is on another domain).
3. Add a domain for service `api`, port 8000.

The CLI is in the same container: open its terminal and run, for example, `tradingagents --ticker NVDA --date 2026-09-23 --analysts market,news --save --no-show`.

Locally, `docker compose -f docker-compose.api.yml up -d` runs the same container, sharing the `tradingagents_data` volume with `docker compose run --rm tradingagents`.

## Keeping up with upstream

This fork's `main` mirrors [TauricResearch/TradingAgents](https://github.com/TauricResearch/TradingAgents); its own changes live on `deploy`. To take in upstream changes:

```bash
gh repo sync carloct/TradingAgents -b main
git fetch origin
git checkout deploy && git merge origin/main
git push
```
