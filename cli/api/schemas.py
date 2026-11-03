"""What a request to start a run may carry."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from tradingagents.portfolio import PortfolioContext


class RunRequest(BaseModel):
    # An unknown field is refused, so a misspelled setting is not silently ignored.
    model_config = ConfigDict(extra="forbid")

    ticker: str = Field(description="Ticker to analyze, e.g. NVDA, 0700.HK or BTC-USD")
    trade_date: str | None = Field(
        default=None, description="Analysis date, YYYY-MM-DD, today or earlier; omit for today")
    analysts: list[str] | None = Field(
        default=None,
        description="market, sentiment, news, fundamentals; omit for all the asset type allows")
    research_depth: int | None = Field(
        default=None, ge=1, le=10,
        description="Rounds of both the research and the risk debate; omit for the server default")
    llm_provider: str | None = Field(default=None, description="Omit for the server default")
    deep_think_llm: str | None = Field(default=None, description="Model for the two managers")
    quick_think_llm: str | None = Field(
        default=None, description="Model for the analysts, researchers, debaters and trader")
    output_language: str | None = Field(default=None, description="Language of the reports")
    checkpoint: bool | None = Field(
        default=None,
        description="Save the run after each step so a restart resumes it; omit for the server default")
    portfolio: PortfolioContext | None = Field(
        default=None, description="Current holdings and cash, so the decision is sized against them")
