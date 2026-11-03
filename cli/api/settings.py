"""The API server's own settings, read from TRADINGAGENTS_API_* variables.

These configure the server, not a run: a run's settings come from
DEFAULT_CONFIG (the same TRADINGAGENTS_* variables the CLI reads) plus what the
request overrides.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

_TRUE = ("true", "1", "yes", "on")
_FALSE = ("false", "0", "no", "off")


def _env(name: str) -> str | None:
    """The variable's value, with an empty one read as unset (as DEFAULT_CONFIG does)."""
    value = os.environ.get(name)
    return value.strip() if value and value.strip() else None


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw = _env(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"Invalid value for {name}: expected a whole number, got {raw!r}") from None
    if value < minimum:
        raise ValueError(f"Invalid value for {name}: must be at least {minimum}, got {value}")
    return value


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name)
    if raw is None:
        return default
    if raw.lower() in _TRUE:
        return True
    if raw.lower() in _FALSE:
        return False
    raise ValueError(f"Invalid value for {name}: expected a boolean, got {raw!r}")


def _default_db_path() -> Path:
    return Path(os.path.expanduser("~")) / ".tradingagents" / "api" / "runs.db"


@dataclass(frozen=True)
class ApiSettings:
    # None means no key: the server refuses to start unless auth is explicitly off.
    api_key: str | None = None
    # Runs at once. A run mostly waits on LLM calls, so a few fit in one container;
    # each also spends provider credit and rate limit.
    max_concurrent_runs: int = 2
    # Browser origins allowed to call the API from another domain (CORS).
    cors_origins: tuple[str, ...] = ()
    # The job store: on the mounted volume so runs survive a redeploy.
    db_path: Path = field(default_factory=_default_db_path)
    # Checkpoint API runs unless a request says otherwise, so a run cut off by a
    # redeploy resumes from its last finished step instead of starting over.
    checkpoint_by_default: bool = True
    # Pick up runs a restart cut off; each run is started at most this many times.
    resume_on_start: bool = True
    max_attempts: int = 3
    # The interactive docs (/docs, /redoc, /openapi.json). Off by default: they
    # show anyone how the API is laid out, though not its data.
    docs: bool = False

    @classmethod
    def from_env(cls) -> ApiSettings:
        origins = _env("TRADINGAGENTS_API_CORS_ORIGINS")
        db_path = _env("TRADINGAGENTS_API_DB_PATH")
        return cls(
            api_key=_env("TRADINGAGENTS_API_KEY"),
            max_concurrent_runs=_env_int("TRADINGAGENTS_API_MAX_RUNS", 2),
            cors_origins=tuple(o.strip() for o in origins.split(",") if o.strip()) if origins else (),
            db_path=Path(db_path) if db_path else _default_db_path(),
            checkpoint_by_default=_env_bool("TRADINGAGENTS_API_CHECKPOINT", True),
            resume_on_start=_env_bool("TRADINGAGENTS_API_RESUME_ON_START", True),
            max_attempts=_env_int("TRADINGAGENTS_API_MAX_ATTEMPTS", 3),
            docs=_env_bool("TRADINGAGENTS_API_DOCS", False),
        )
