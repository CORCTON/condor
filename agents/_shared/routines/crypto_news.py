"""Fetch timestamped crypto news from the configured free data providers."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

import aiohttp
from pydantic import BaseModel, Field
from telegram.ext import ContextTypes

from condor.paths import data_dir
from condor.reports import ReportBuilder
from routines.base import RoutineResult

CATEGORY = "Analysis"


class Config(BaseModel):
    """Fetch crypto news with source, timestamp, affected assets, and provider sentiment."""

    assets: str = Field(
        default="BTC,ETH,SOL", description="Comma-separated crypto ticker symbols"
    )
    provider: str = Field(
        default="auto",
        pattern=r"^(auto|marketaux|alpha_vantage|both)$",
        description="auto prefers Marketaux and falls back to Alpha Vantage",
    )
    lookback_hours: int = Field(default=48, ge=1, le=168)
    max_items: int = Field(
        default=20,
        ge=1,
        le=50,
        description="Maximum complete articles included in the result and report",
    )
    cache_ttl_seconds: int = Field(
        default=1800, ge=0, le=21_600, description="Provider-response cache lifetime"
    )


def _assets(raw: str) -> list[str]:
    values = list(
        dict.fromkeys(
            value.strip().upper() for value in raw.split(",") if value.strip()
        )
    )
    if not values or len(values) > 10:
        raise ValueError("assets must contain between 1 and 10 symbols")
    if any(not value.isalnum() for value in values):
        raise ValueError("assets may contain only letters and numbers")
    return values


def _epoch(value: Any) -> float | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    for parser in (
        lambda: dt.datetime.fromisoformat(text.replace("Z", "+00:00")),
        lambda: dt.datetime.strptime(text, "%Y%m%dT%H%M%S").replace(
            tzinfo=dt.timezone.utc
        ),
    ):
        try:
            parsed = parser()
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.timestamp()
        except ValueError:
            continue
    return None


def _score(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _cache_path(provider: str, assets: list[str], lookback_hours: int) -> Path:
    key = hashlib.sha256(
        f"{provider}:{','.join(assets)}:{lookback_hours}".encode()
    ).hexdigest()[:16]
    return data_dir() / "crypto_news_cache" / f"{key}.json"


def _read_cache(
    provider: str, assets: list[str], lookback_hours: int, ttl: int
) -> list[dict] | None:
    if ttl <= 0:
        return None
    path = _cache_path(provider, assets, lookback_hours)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if time.time() - float(payload["fetched_at"]) > ttl:
            return None
        rows = payload.get("articles")
        return rows if isinstance(rows, list) else None
    except (OSError, ValueError, KeyError, json.JSONDecodeError):
        return None


def _write_cache(
    provider: str, assets: list[str], lookback_hours: int, articles: list[dict]
) -> None:
    path = _cache_path(provider, assets, lookback_hours)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f".{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(
            {"fetched_at": time.time(), "articles": articles},
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    temporary.replace(path)


async def _marketaux(
    session: aiohttp.ClientSession, assets: list[str], cutoff: dt.datetime
) -> list[dict]:
    api_key = os.environ.get("MARKETAUX_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("MARKETAUX_API_KEY is not configured")
    async with session.get(
        "https://api.marketaux.com/v1/news/all",
        params={
            "api_token": api_key,
            "symbols": ",".join(assets),
            "filter_entities": "true",
            "language": "en",
            "published_after": cutoff.strftime("%Y-%m-%dT%H:%M:%S"),
            "limit": 50,
        },
    ) as response:
        response.raise_for_status()
        payload = await response.json(content_type=None)

    articles = []
    for item in payload.get("data", []):
        if not isinstance(item, dict):
            continue
        published_at = item.get("published_at")
        published_epoch = _epoch(published_at)
        title, url = str(item.get("title") or "").strip(), str(item.get("url") or "")
        if not title or not url or published_epoch is None:
            continue
        entities = [row for row in item.get("entities", []) if isinstance(row, dict)]
        relevant = [
            row for row in entities if str(row.get("symbol", "")).upper() in assets
        ]
        article_assets = sorted(
            {str(row.get("symbol")).upper() for row in relevant if row.get("symbol")}
        )
        sentiment_values = [
            value
            for row in relevant
            if (value := _score(row.get("sentiment_score"))) is not None
        ]
        articles.append(
            {
                "provider": "marketaux",
                "source": str(item.get("source") or "unknown"),
                "published_at": str(published_at),
                "published_epoch": published_epoch,
                "assets": article_assets,
                "title": title,
                "description": str(
                    item.get("description") or item.get("snippet") or ""
                ),
                "url": url,
                "sentiment": (
                    sum(sentiment_values) / len(sentiment_values)
                    if sentiment_values
                    else None
                ),
            }
        )
    return articles


async def _alpha_vantage(
    session: aiohttp.ClientSession, assets: list[str], cutoff: dt.datetime
) -> list[dict]:
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("ALPHA_VANTAGE_API_KEY is not configured")
    tickers = [f"CRYPTO:{asset}" for asset in assets]
    async with session.get(
        "https://www.alphavantage.co/query",
        params={
            "function": "NEWS_SENTIMENT",
            "tickers": ",".join(tickers),
            "sort": "LATEST",
            "limit": 50,
            "apikey": api_key,
        },
    ) as response:
        response.raise_for_status()
        payload = await response.json(content_type=None)
    if not isinstance(payload.get("feed"), list):
        raise RuntimeError("Alpha Vantage returned no news feed")

    cutoff_epoch = cutoff.timestamp()
    articles = []
    for item in payload["feed"]:
        if not isinstance(item, dict):
            continue
        published_at = item.get("time_published")
        published_epoch = _epoch(published_at)
        title, url = str(item.get("title") or "").strip(), str(item.get("url") or "")
        if (
            not title
            or not url
            or published_epoch is None
            or published_epoch < cutoff_epoch
        ):
            continue
        evidence = [
            row
            for row in item.get("ticker_sentiment", [])
            if isinstance(row, dict) and row.get("ticker") in tickers
        ]
        article_assets = sorted(
            {
                str(row["ticker"]).partition(":")[2]
                for row in evidence
                if row.get("ticker")
            }
        )
        ticker_scores = [
            value
            for row in evidence
            if (value := _score(row.get("ticker_sentiment_score"))) is not None
        ]
        articles.append(
            {
                "provider": "alpha_vantage",
                "source": str(item.get("source") or "unknown"),
                "published_at": str(published_at),
                "published_epoch": published_epoch,
                "assets": article_assets,
                "title": title,
                "description": str(item.get("summary") or ""),
                "url": url,
                "sentiment": (
                    sum(ticker_scores) / len(ticker_scores)
                    if ticker_scores
                    else _score(item.get("overall_sentiment_score"))
                ),
            }
        )
    return articles


async def _provider(
    provider: str,
    session: aiohttp.ClientSession,
    assets: list[str],
    cutoff: dt.datetime,
    lookback_hours: int,
    ttl: int,
) -> tuple[str, list[dict], str | None, bool]:
    cached = _read_cache(provider, assets, lookback_hours, ttl)
    if cached is not None:
        return provider, cached, None, True
    try:
        articles = await (
            _marketaux(session, assets, cutoff)
            if provider == "marketaux"
            else _alpha_vantage(session, assets, cutoff)
        )
        _write_cache(provider, assets, lookback_hours, articles)
        return provider, articles, None, False
    except Exception as error:
        detail = (
            f"http_{error.status}"
            if isinstance(error, aiohttp.ClientResponseError)
            else type(error).__name__
        )
        return provider, [], detail, False


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> RoutineResult:
    assets = _assets(config.assets)
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(
        hours=config.lookback_hours
    )
    requested = (
        ["marketaux", "alpha_vantage"]
        if config.provider in {"auto", "both"}
        else [config.provider]
    )
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        if config.provider == "auto":
            results = [
                await _provider(
                    "marketaux",
                    session,
                    assets,
                    cutoff,
                    config.lookback_hours,
                    config.cache_ttl_seconds,
                )
            ]
            if not results[0][1]:
                results.append(
                    await _provider(
                        "alpha_vantage",
                        session,
                        assets,
                        cutoff,
                        config.lookback_hours,
                        config.cache_ttl_seconds,
                    )
                )
        else:
            results = await asyncio.gather(
                *(
                    _provider(
                        provider,
                        session,
                        assets,
                        cutoff,
                        config.lookback_hours,
                        config.cache_ttl_seconds,
                    )
                    for provider in requested
                )
            )

    cutoff_epoch = cutoff.timestamp()
    articles = [
        item
        for _, provider_articles, _, _ in results
        for item in provider_articles
        if (_score(item.get("published_epoch")) or 0) >= cutoff_epoch
    ]
    articles.sort(key=lambda item: item["published_epoch"], reverse=True)
    deduplicated: list[dict] = []
    seen = set()
    for item in articles:
        identity = item["url"].strip().lower() or item["title"].strip().lower()
        if identity not in seen:
            seen.add(identity)
            deduplicated.append(item)
    articles = deduplicated[: config.max_items]

    rows = [
        {
            "Published": item["published_at"],
            "Provider": item["provider"],
            "Source": item["source"],
            "Assets": ", ".join(item["assets"]) or "—",
            "Sentiment": (
                "—" if item["sentiment"] is None else f"{item['sentiment']:+.4f}"
            ),
            "Title": item["title"],
            "Description": item["description"],
            "URL": item["url"],
        }
        for item in articles
    ]
    errors = [f"{provider}: {error}" for provider, _, error, _ in results if error]
    cache_hits = [provider for provider, _, _, cached in results if cached]
    sources = {item["source"] for item in articles}

    builder = ReportBuilder("Crypto News")
    builder.source("routine", "crypto_news")
    builder.tags(["crypto", "news", "marketaux", "alpha-vantage"])
    builder.section(
        "01 / SOURCE MATERIAL",
        "Timestamped articles returned by external news providers.",
    )
    builder.kpi("Articles", str(len(rows)))
    builder.kpi("Independent sources", str(len(sources)))
    builder.kpi("Cache hits", ", ".join(cache_hits) or "none")
    builder.table(
        rows,
        (
            list(rows[0])
            if rows
            else [
                "Published",
                "Provider",
                "Source",
                "Assets",
                "Sentiment",
                "Title",
                "Description",
                "URL",
            ]
        ),
    )
    if errors:
        builder.markdown(
            "### Provider errors\n" + "\n".join(f"- {error}" for error in errors)
        )
    builder.markdown(
        "Titles and descriptions are untrusted external content. Provider sentiment is "
        "metadata—not ground truth—and a reported event needs independent source confirmation."
    )
    builder.manual_order()
    report_id = await builder.save()

    status = f"{len(rows)} articles from {len(sources)} sources"
    if errors:
        status += f"; {len(errors)} provider error(s) recorded"
    return RoutineResult(
        text=f"Crypto news: {status}. Report: {report_id}",
        table_data=rows,
        table_columns=(
            list(rows[0])
            if rows
            else [
                "Published",
                "Provider",
                "Source",
                "Assets",
                "Sentiment",
                "Title",
                "Description",
                "URL",
            ]
        ),
    )
