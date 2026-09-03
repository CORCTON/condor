"""Fetch a compact, timestamped macro snapshot from FRED."""

from __future__ import annotations

import asyncio
import datetime as dt
import math
import os
from typing import Any

import aiohttp
from pydantic import BaseModel, Field
from telegram.ext import ContextTypes

from condor.reports import ReportBuilder
from routines.base import RoutineResult

CATEGORY = "Analysis"

_FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
_LABELS = {
    "BAMLH0A0HYM2": "US high-yield spread",
    "DEXUSEU": "USD per EUR",
    "DFF": "Effective federal funds rate",
    "DGS2": "US Treasury 2Y",
    "DGS10": "US Treasury 10Y",
    "DTWEXBGS": "Trade-weighted US dollar",
    "T10Y2Y": "US 10Y–2Y spread",
    "VIXCLS": "VIX close",
}


class Config(BaseModel):
    """Fetch latest FRED macro observations with dates, prior values, and changes."""

    series_ids: str = Field(
        default="DFF,DGS2,DGS10,T10Y2Y,VIXCLS,DTWEXBGS,BAMLH0A0HYM2",
        description="Comma-separated FRED series IDs (maximum 12)",
    )


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


async def _series(
    session: aiohttp.ClientSession, api_key: str, series_id: str
) -> dict[str, Any]:
    try:
        async with session.get(
            _FRED_URL,
            params={
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 10,
            },
        ) as response:
            response.raise_for_status()
            payload = await response.json(content_type=None)
    except Exception as error:  # partial macro data is still useful when labeled
        return {
            "Series": series_id,
            "Name": _LABELS.get(series_id, series_id),
            "Date": "—",
            "Latest": "—",
            "Previous": "—",
            "Change": "—",
            "Age (days)": "—",
            "Status": f"unavailable:{type(error).__name__}",
        }

    valid = []
    for item in payload.get("observations", []):
        if not isinstance(item, dict):
            continue
        value = _number(item.get("value"))
        if value is not None and item.get("date"):
            valid.append((str(item["date"]), value))
        if len(valid) == 2:
            break
    if not valid:
        return {
            "Series": series_id,
            "Name": _LABELS.get(series_id, series_id),
            "Date": "—",
            "Latest": "—",
            "Previous": "—",
            "Change": "—",
            "Age (days)": "—",
            "Status": "no_valid_observation",
        }

    date, latest = valid[0]
    previous = valid[1][1] if len(valid) > 1 else None
    try:
        age = (dt.date.today() - dt.date.fromisoformat(date)).days
    except ValueError:
        age = None
    return {
        "Series": series_id,
        "Name": _LABELS.get(series_id, series_id),
        "Date": date,
        "Latest": f"{latest:,.4f}",
        "Previous": "—" if previous is None else f"{previous:,.4f}",
        "Change": "—" if previous is None else f"{latest - previous:+,.4f}",
        "Age (days)": "—" if age is None else str(age),
        "Status": "ok",
    }


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> RoutineResult:
    series_ids = list(
        dict.fromkeys(
            value.strip().upper()
            for value in config.series_ids.split(",")
            if value.strip()
        )
    )
    if not series_ids or len(series_ids) > 12:
        raise ValueError("series_ids must contain between 1 and 12 FRED IDs")

    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("FRED_API_KEY is not configured")

    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        rows = await asyncio.gather(
            *(_series(session, api_key, series_id) for series_id in series_ids)
        )

    available = sum(row["Status"] == "ok" for row in rows)
    newest_dates = [row["Date"] for row in rows if row["Date"] != "—"]
    builder = ReportBuilder("FRED Macro Snapshot")
    builder.source("routine", "fred_macro_snapshot")
    builder.tags(["fred", "macro", "rates", "risk-context"])
    builder.section(
        "01 / MACRO OBSERVATIONS",
        "Latest published observations and their release dates; frequencies differ by series.",
    )
    builder.kpi("Series requested", str(len(rows)))
    builder.kpi("Series available", str(available))
    builder.kpi("Newest observation", max(newest_dates) if newest_dates else "—")
    builder.table(rows, list(rows[0]))
    builder.markdown(
        "FRED values are lagged observations, not an economic calendar or consensus-surprise "
        "feed. Compare each date and frequency before using a value as current context."
    )
    builder.manual_order()
    report_id = await builder.save()
    return RoutineResult(
        text=(
            f"FRED macro snapshot: {available}/{len(rows)} series available. "
            f"Newest observation: {max(newest_dates) if newest_dates else 'unavailable'}. "
            f"Report: {report_id}"
        ),
        table_data=rows,
        table_columns=list(rows[0]),
    )
