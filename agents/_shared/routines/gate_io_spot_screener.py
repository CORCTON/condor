"""Rank liquid Gate.io spot markets and inspect their executable order books."""

from __future__ import annotations

import asyncio
import logging
import math
from typing import Any

from pydantic import BaseModel, Field
from telegram.ext import ContextTypes

from condor.fetchers.market_data import fetch_tickers
from condor.reports import ReportBuilder
from config_manager import get_client, get_config_manager
from routines.base import RoutineResult

CATEGORY = "Market Data"

logger = logging.getLogger(__name__)

_STABLE_BASES = frozenset(
    {
        "BUSD",
        "DAI",
        "FDUSD",
        "PYUSD",
        "TUSD",
        "USD",
        "USDC",
        "USDD",
        "USDE",
        "USDG",
        "USDT",
    }
)
_LEVERAGED_SUFFIXES = ("3L", "3S", "5L", "5S", "BULL", "BEAR", "UP", "DOWN")


class Config(BaseModel):
    """Rank liquid Gate.io USDT spot markets and measure spread, depth, and impact."""

    quote_asset: str = Field(default="USDT", description="Quote asset to scan")
    universe_size: int = Field(
        default=20,
        ge=5,
        le=50,
        description="Highest-volume markets to inspect before execution ranking",
    )
    top_n: int = Field(default=10, ge=1, le=20, description="Rows to return")
    order_book_depth: int = Field(
        default=20, ge=5, le=100, description="Order book levels per side"
    )
    quote_notional: float = Field(
        default=100.0,
        gt=0,
        le=100_000,
        description="Quote amount used for simulated buy/sell impact",
    )


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _levels(payload: Any, side: str) -> list[tuple[float, float]]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload = payload["data"]
    raw = payload.get(side, []) if isinstance(payload, dict) else []
    levels: list[tuple[float, float]] = []
    for item in raw:
        if isinstance(item, dict):
            price = _number(item.get("price") or item.get("p"))
            amount = _number(
                item.get("amount")
                or item.get("quantity")
                or item.get("size")
                or item.get("s")
            )
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            price, amount = _number(item[0]), _number(item[1])
        else:
            continue
        if price and price > 0 and amount and amount > 0:
            levels.append((price, abs(amount)))
    return levels


def _impact_bps(
    levels: list[tuple[float, float]], quote_notional: float, mid: float, buy: bool
) -> float | None:
    if buy:
        quote_left = quote_notional
        base_bought = 0.0
        for price, amount in levels:
            quote_taken = min(quote_left, price * amount)
            base_bought += quote_taken / price
            quote_left -= quote_taken
            if quote_left <= 1e-9:
                average = quote_notional / base_bought
                return max(0.0, (average / mid - 1.0) * 10_000)
        return None

    base_total = quote_notional / mid
    base_left = base_total
    quote_received = 0.0
    for price, amount in levels:
        base_taken = min(base_left, amount)
        quote_received += base_taken * price
        base_left -= base_taken
        if base_left <= 1e-12:
            average = quote_received / base_total
            return max(0.0, (1.0 - average / mid) * 10_000)
    return None


def _book_metrics(payload: Any, quote_notional: float) -> dict[str, float | None]:
    bids = _levels(payload, "bids")
    asks = _levels(payload, "asks")
    if not bids or not asks:
        return {}
    best_bid, best_ask = bids[0][0], asks[0][0]
    if best_ask < best_bid:
        return {}
    mid = (best_bid + best_ask) / 2
    bid_depth = sum(price * amount for price, amount in bids)
    ask_depth = sum(price * amount for price, amount in asks)
    total_depth = bid_depth + ask_depth
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread_bps": (best_ask - best_bid) / mid * 10_000,
        "bid_depth_quote": bid_depth,
        "ask_depth_quote": ask_depth,
        "book_imbalance": (
            (bid_depth - ask_depth) / total_depth if total_depth > 0 else None
        ),
        "buy_impact_bps": _impact_bps(asks, quote_notional, mid, True),
        "sell_impact_bps": _impact_bps(bids, quote_notional, mid, False),
    }


async def _client_for(context):
    server_name = getattr(context, "server_name", None)
    if server_name:
        return await get_config_manager().get_client(server_name)
    return await get_client(
        getattr(context, "_chat_id", 0), context=context if context else None
    )


async def _book(client, pair: str, depth: int) -> tuple[str, Any]:
    try:
        result = await client.market_data.get_order_book(
            connector_name="gate_io", trading_pair=pair, depth=depth
        )
        return pair, result
    except Exception as error:  # one unavailable market must not sink the scan
        logger.warning("Gate.io order book failed for %s: %s", pair, error)
        return pair, None


def _eligible(pair: str, quote_asset: str) -> bool:
    base, separator, quote = pair.upper().rpartition("-")
    return bool(
        separator
        and base
        and quote == quote_asset
        and base not in _STABLE_BASES
        and not base.endswith(_LEVERAGED_SUFFIXES)
    )


def _display(value: float | None, decimals: int = 2) -> str:
    return "—" if value is None else f"{value:,.{decimals}f}"


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> RoutineResult:
    quote_asset = config.quote_asset.strip().upper()
    client = await _client_for(context)
    tickers_payload = await fetch_tickers(client, "gate_io", strict=True)
    raw_tickers = tickers_payload.get("tickers", {})
    candidates: list[dict[str, Any]] = []
    for pair, ticker in raw_tickers.items():
        if not _eligible(pair, quote_asset) or not isinstance(ticker, dict):
            continue
        price = _number(ticker.get("price"))
        quote_volume = _number(ticker.get("quote_volume"))
        if price and price > 0 and quote_volume is not None and quote_volume >= 0:
            candidates.append(
                {"pair": pair.upper(), "price": price, "quote_volume": quote_volume}
            )
    candidates.sort(key=lambda row: row["quote_volume"], reverse=True)
    candidates = candidates[: config.universe_size]

    books = dict(
        await asyncio.gather(
            *(_book(client, row["pair"], config.order_book_depth) for row in candidates)
        )
    )
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        metrics = _book_metrics(books.get(candidate["pair"]), config.quote_notional)
        if not metrics:
            continue
        rows.append({**candidate, **metrics})

    def execution_cost(row: dict[str, Any]) -> float:
        values = (
            row.get("spread_bps"),
            row.get("buy_impact_bps"),
            row.get("sell_impact_bps"),
        )
        return sum(value if value is not None else 1_000_000 for value in values)

    rows.sort(key=lambda row: (-row["quote_volume"], execution_cost(row)))
    rows = rows[: config.top_n]
    report_rows = [
        {
            "Pair": row["pair"],
            "Price": _display(row["price"], 8),
            f"24h Volume ({quote_asset})": _display(row["quote_volume"], 0),
            "Spread (bps)": _display(row["spread_bps"], 2),
            "Bid Depth": _display(row["bid_depth_quote"], 0),
            "Ask Depth": _display(row["ask_depth_quote"], 0),
            "Imbalance": _display(row["book_imbalance"], 3),
            "Buy Impact (bps)": _display(row["buy_impact_bps"], 2),
            "Sell Impact (bps)": _display(row["sell_impact_bps"], 2),
        }
        for row in rows
    ]

    builder = ReportBuilder("Gate.io Spot Market Scan")
    builder.source("routine", "gate_io_spot_screener")
    builder.tags(["gate.io", "spot", "liquidity", "order-book"])
    builder.section(
        "01 / MARKET UNIVERSE",
        "Descriptive liquidity and execution measurements; this report does not select a trade.",
    )
    builder.kpi("Eligible markets inspected", str(len(candidates)))
    builder.kpi("Complete order books", str(len(rows)))
    builder.kpi("Impact notional", f"{config.quote_notional:,.2f} {quote_asset}")
    builder.table(report_rows, list(report_rows[0]) if report_rows else ["Pair"])
    builder.markdown(
        "Volume ranks the initial universe. Spread, depth, imbalance, and impact are "
        "point-in-time observations—not directional signals or expected returns."
    )
    builder.manual_order()
    report_id = await builder.save()

    if not rows:
        text = "Gate.io spot scan returned no complete market/order-book rows."
    else:
        leaders = ", ".join(row["pair"] for row in rows[:5])
        text = (
            f"Gate.io spot scan: {len(rows)} complete markets. "
            f"Highest-volume results: {leaders}. Report: {report_id}"
        )
    return RoutineResult(
        text=text,
        table_data=report_rows,
        table_columns=list(report_rows[0]) if report_rows else ["Pair"],
    )
