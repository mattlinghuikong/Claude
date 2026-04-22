"""
Financial Modeling Prep (FMP) — free tier, no credit card.
Register at https://financialmodelingprep.com/register to get a free API key.
Free tier: 250 requests/day.

Provides: analyst upgrades/downgrades with firm name, price targets,
          analyst estimates, and consensus ratings.

Set FMP_API_KEY in config.py or leave blank to skip this source.
"""

import traceback
from datetime import date, timedelta

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
    data = _get(f"upgrades-downgrades/{ticker}", {}, api_key)
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
    data = _get(f"analyst-estimates/{ticker}", {"limit": 2}, api_key)
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
    data = _get(f"price-target-consensus/{ticker}", {}, api_key)
    if not data or not isinstance(data, dict):
        return {}
    return {
        "target_consensus": data.get("targetConsensus"),
        "target_high": data.get("targetHigh"),
        "target_low": data.get("targetLow"),
        "last_updated": str(data.get("lastUpdated", ""))[:10],
    }


def enrich_with_fmp(stock_list, api_key):
    """
    Add FMP analyst data to each stock dict.
    Skips gracefully if api_key is empty.
    """
    if not api_key or requests is None:
        return stock_list, None

    for stock in stock_list:
        ticker = stock.get("ticker", "")
        if not ticker:
            continue
        upgrades = get_upgrades_downgrades(ticker, api_key)
        estimates = get_analyst_estimates(ticker, api_key)
        pt = get_price_target(ticker, api_key)
        stock["fmp_upgrades"] = upgrades
        stock["fmp_estimates"] = estimates
        stock["fmp_price_target"] = pt

    return stock_list, None
