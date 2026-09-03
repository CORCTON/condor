"""Collect public Gate.io perpetual positioning and microstructure data."""

from __future__ import annotations

import asyncio
import math
import time
from typing import Any

import aiohttp
from pydantic import BaseModel, Field
from telegram.ext import ContextTypes

from condor.reports import ReportBuilder
from routines.base import RoutineResult

CATEGORY = "Market Data"

_API_ROOT = "https://api.gateio.ws/api/v4"


class Config(BaseModel):
    """Inspect Gate.io perpetual funding, open interest, liquidations, ratios, and L2."""

    trading_pair: str = Field(
        default="BTC-USDT", description="Gate.io perpetual pair, e.g. BTC-USDT"
    )
    history_hours: int = Field(
        default=24, ge=1, le=168, description="Contract-statistics lookback"
    )
    interval: str = Field(
        default="1h",
        pattern=r"^(5m|30m|1h|4h|1d)$",
        description="Gate.io contract-statistics interval",
    )
    order_book_depth: int = Field(
        default=20, ge=5, le=100, description="Order book levels per side"
    )


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


async def _get(session: aiohttp.ClientSession, path: str, params=None) -> Any:
    async with session.get(f"{_API_ROOT}{path}", params=params) as response:
        response.raise_for_status()
        return await response.json(content_type=None)


def _contract_pair(value: str) -> tuple[str, str]:
    pair = value.strip().upper().replace("_", "-")
    base, separator, quote = pair.rpartition("-")
    if not separator or not base or quote not in {"USDT", "USD", "BTC"}:
        raise ValueError("trading_pair must look like BTC-USDT")
    return f"{base}_{quote}", quote.lower()


def _ticker(payload: Any, contract: str) -> dict:
    rows = payload if isinstance(payload, list) else []
    return next(
        (
            row
            for row in rows
            if isinstance(row, dict) and row.get("contract") == contract
        ),
        {},
    )


def _book_metrics(payload: Any, multiplier: float) -> dict[str, float | None]:
    def levels(side: str) -> list[tuple[float, float]]:
        rows = payload.get(side, []) if isinstance(payload, dict) else []
        result = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            price = _number(row.get("p") or row.get("price"))
            contracts = _number(row.get("s") or row.get("size"))
            if price and price > 0 and contracts:
                result.append((price, abs(contracts) * multiplier))
        return result

    bids, asks = levels("bids"), levels("asks")
    if not bids or not asks:
        return {}
    best_bid, best_ask = bids[0][0], asks[0][0]
    mid = (best_bid + best_ask) / 2
    bid_quote = sum(price * amount for price, amount in bids)
    ask_quote = sum(price * amount for price, amount in asks)
    total_quote = bid_quote + ask_quote
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread_bps": (best_ask - best_bid) / mid * 10_000,
        "bid_depth_quote": bid_quote,
        "ask_depth_quote": ask_quote,
        "book_imbalance": (
            (bid_quote - ask_quote) / total_quote if total_quote > 0 else None
        ),
    }


def _fmt(value: float | None, decimals: int = 4) -> str:
    return "—" if value is None else f"{value:,.{decimals}f}"


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> RoutineResult:
    contract, settlement = _contract_pair(config.trading_pair)
    now = int(time.time())
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        contract_info, tickers, stats, order_book = await asyncio.gather(
            _get(session, f"/futures/{settlement}/contracts/{contract}"),
            _get(session, f"/futures/{settlement}/tickers"),
            _get(
                session,
                f"/futures/{settlement}/contract_stats",
                {
                    "contract": contract,
                    "from": now - config.history_hours * 3600,
                    "to": now,
                    "interval": config.interval,
                },
            ),
            _get(
                session,
                f"/futures/{settlement}/order_book",
                {
                    "contract": contract,
                    "limit": config.order_book_depth,
                    "with_id": "true",
                },
            ),
        )

    ticker = _ticker(tickers, contract)
    multiplier = _number(contract_info.get("quanto_multiplier")) or 1.0
    mark_price = _number(ticker.get("mark_price"))
    index_price = _number(ticker.get("index_price"))
    funding_rate = _number(ticker.get("funding_rate"))
    open_interest_contracts = _number(ticker.get("total_size"))
    open_interest_base = (
        abs(open_interest_contracts) * multiplier
        if open_interest_contracts is not None
        else None
    )
    open_interest_quote = (
        open_interest_base * mark_price
        if open_interest_base is not None and mark_price is not None
        else None
    )
    basis_bps = (
        (mark_price / index_price - 1) * 10_000
        if mark_price is not None and index_price
        else None
    )

    stats_rows = (
        [row for row in stats if isinstance(row, dict)]
        if isinstance(stats, list)
        else []
    )
    stats_rows.sort(key=lambda row: _number(row.get("time")) or 0)
    first_oi = _number(stats_rows[0].get("open_interest")) if stats_rows else None
    last_oi = _number(stats_rows[-1].get("open_interest")) if stats_rows else None
    oi_change_pct = (
        (last_oi / first_oi - 1) * 100
        if first_oi is not None and first_oi != 0 and last_oi is not None
        else None
    )
    long_liq_contracts = sum(
        abs(_number(row.get("long_liq_size")) or 0) for row in stats_rows
    )
    short_liq_contracts = sum(
        abs(_number(row.get("short_liq_size")) or 0) for row in stats_rows
    )
    latest_stats = stats_rows[-1] if stats_rows else {}
    book = _book_metrics(order_book, multiplier)

    observed = {
        "Pair": contract.replace("_", "-"),
        "Mark": _fmt(mark_price, 8),
        "Index": _fmt(index_price, 8),
        "Basis (bps)": _fmt(basis_bps, 2),
        "Funding": _fmt(funding_rate, 8),
        "Open interest (quote)": _fmt(open_interest_quote, 0),
        f"OI change ({config.history_hours}h %)": _fmt(oi_change_pct, 2),
        "Account L/S ratio": _fmt(_number(latest_stats.get("lsr_account")), 4),
        "Taker L/S ratio": _fmt(_number(latest_stats.get("lsr_taker")), 4),
        "Long liquidations (base)": _fmt(long_liq_contracts * multiplier, 4),
        "Short liquidations (base)": _fmt(short_liq_contracts * multiplier, 4),
        "L2 spread (bps)": _fmt(book.get("spread_bps"), 2),
        "L2 imbalance": _fmt(book.get("book_imbalance"), 4),
    }

    builder = ReportBuilder("Gate.io Derivatives Snapshot")
    builder.source("routine", "gate_io_derivatives_snapshot")
    builder.tags(["gate.io", "perpetual", "funding", "open-interest", "liquidations"])
    builder.section(
        "01 / PUBLIC DERIVATIVES CONTEXT",
        "Observed derivatives positioning and L2 context; no account credentials or orders are used.",
    )
    builder.kpi("Mark price", observed["Mark"])
    builder.kpi("Funding rate", observed["Funding"])
    builder.kpi("Open interest", observed["Open interest (quote)"])
    builder.table([observed], list(observed))
    builder.markdown(
        "Funding, open interest, liquidations, ratios, and a single order-book snapshot "
        "are contextual observations. They are not standalone directional signals."
    )
    builder.manual_order()
    report_id = await builder.save()
    return RoutineResult(
        text=(
            f"Gate.io derivatives snapshot for {observed['Pair']}: funding "
            f"{observed['Funding']}, OI {observed['Open interest (quote)']}, "
            f"{config.history_hours}h OI change {observed[f'OI change ({config.history_hours}h %)']}. "
            f"Report: {report_id}"
        ),
        table_data=[observed],
        table_columns=list(observed),
    )
