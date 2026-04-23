"""
Financial Modeling Prep (FMP) — free tier, no credit card.
Register at https://financialmodelingprep.com/register to get a free API key.
Free tier: 250 requests/day.

Provides: analyst upgrades/downgrades with firm name, price targets,
          analyst estimates, and consensus ratings.

Set FMP_API_KEY in config.py or leave blank to skip this source.
"""

import concurrent.futures
import re
import traceback
from datetime import date, timedelta
from urllib.parse import quote

try:
    import requests
    import pandas as pd
except ImportError:
    requests = None
    pd = None

_CUTOFF_DAYS = 30

FMP_BASE = "https://financialmodelingprep.com/api/v3"

BUY_GRADES = {"buy", "strong buy", "outperform", "overweight", "positive",
              "accumulate", "market outperform", "sector outperform", "add",
              "strong-buy", "market-outperform"}

# Allow US tickers (AAPL, BRK-B) and HK (0700.HK) — letters, digits, dot, dash only.
_TICKER_RE = re.compile(r"^[A-Za-z0-9.\-]{1,12}$")


def _safe_ticker(ticker):
    """Return URL-safe ticker or None if it fails whitelist."""
    if not ticker or not _TICKER_RE.match(str(ticker)):
        return None
    return quote(str(ticker), safe="")


def _get(endpoint, params, api_key):
    if requests is None or not api_key:
        return None
    params["apikey"] = api_key
    try:
        resp = requests.get(f"{FMP_BASE}/{endpoint}", params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, dict) and data.get("Error Message"):
            return None
        return data
    except Exception:
        return None


def get_upgrades_downgrades(ticker, api_key):
    """Get analyst upgrades/downgrades for a ticker."""
    safe = _safe_ticker(ticker)
    if not safe:
        return []
    data = _get(f"upgrades-downgrades/{safe}", {}, api_key)
    if not data:
        return []
    cutoff = date.today() - timedelta(days=_CUTOFF_DAYS)
    results = []
    for item in data[:20]:
        action = str(item.get("action", "")).lower()
        new_grade = str(item.get("newGrade", "")).lower()
        item_date_str = str(item.get("publishedDate", ""))[:10]
        try:
            if date.fromisoformat(item_date_str) < cutoff:
                continue
        except Exception:
            pass
        if action in ("upgrade", "initiated", "reiterated", "resumed") or \
           new_grade in BUY_GRADES:
            results.append({
                "date": item_date_str,
                "brokerage": item.get("gradingCompany", ""),
                "action": action,
                "from_grade": item.get("previousGrade", ""),
                "to_grade": item.get("newGrade", ""),
                "news_headline": item.get("newsTitle", ""),
                "news_url": item.get("newsURL", ""),
            })
    return results


def get_analyst_estimates(ticker, api_key):
    """Get annual EPS and revenue consensus estimates."""
    safe = _safe_ticker(ticker)
    if not safe:
        return {}
    data = _get(f"analyst-estimates/{safe}", {"limit": 2}, api_key)
    if not data or not isinstance(data, list):
        return {}
    next_yr = data[0] if data else {}
    return {
        "est_revenue": next_yr.get("estimatedRevenueAvg"),
        "est_eps": next_yr.get("estimatedEpsAvg"),
        "est_net_income": next_yr.get("estimatedNetIncomeAvg"),
        "period": next_yr.get("date", ""),
        "n_analysts_eps": next_yr.get("numberAnalystEstimatedEps"),
    }


def get_price_target(ticker, api_key):
    """Get latest analyst price target consensus."""
    safe = _safe_ticker(ticker)
    if not safe:
        return {}
    data = _get(f"price-target-consensus/{safe}", {}, api_key)
    if not data or not isinstance(data, dict):
        return {}
    return {
        "target_consensus": data.get("targetConsensus"),
        "target_high": data.get("targetHigh"),
        "target_low": data.get("targetLow"),
        "last_updated": str(data.get("lastUpdated", ""))[:10],
    }


def _fmp_bundle(ticker, api_key):
    return (
        ticker,
        get_upgrades_downgrades(ticker, api_key),
        get_analyst_estimates(ticker, api_key),
        get_price_target(ticker, api_key),
    )


def enrich_with_fmp(stock_list, api_key):
    """
    Add FMP analyst data to each stock dict.
    Skips gracefully if api_key is empty.
    """
    if not api_key or requests is None:
        return stock_list, None

    tickers = [s.get("ticker", "") for s in stock_list if s.get("ticker")]
    lookup = {s.get("ticker"): s for s in stock_list if s.get("ticker")}

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(_fmp_bundle, t, api_key) for t in tickers]
        for fut in concurrent.futures.as_completed(futures):
            try:
                ticker, upgrades, estimates, pt = fut.result(timeout=60)
            except Exception:
                continue
            stock = lookup.get(ticker)
            if stock is not None:
                stock["fmp_upgrades"] = upgrades
                stock["fmp_estimates"] = estimates
                stock["fmp_price_target"] = pt

    return stock_list, None
