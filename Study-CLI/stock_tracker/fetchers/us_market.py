"""
US market analyst data via yfinance.
Since free sources don't expose individual analyst win rates,
we track analyst calls ourselves in SQLite and rank by self-computed win rate.
For fresh installs, we rank by: consensus strength + price target upside.
"""

import sys
import traceback
from datetime import date, timedelta

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    yf = None
    pd = None

from config import US_WATCHLIST, MIN_UPSIDE_PCT, MIN_BUY_RATINGS


BUY_RATINGS = {"buy", "strong buy", "outperform", "overweight", "positive",
               "accumulate", "add", "market outperform", "sector outperform"}
SELL_RATINGS = {"sell", "underperform", "underweight", "negative", "reduce"}


def _safe_float(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except Exception:
        return default


def fetch_ticker_data(symbol):
    """Fetch analyst consensus + fundamentals for one ticker."""
    if yf is None:
        return None
    try:
        t = yf.Ticker(symbol)
        info = t.info or {}

        current_price = _safe_float(info.get("currentPrice") or info.get("regularMarketPrice"))
        target_mean = _safe_float(info.get("targetMeanPrice"))
        target_high = _safe_float(info.get("targetHighPrice"))
        target_low = _safe_float(info.get("targetLowPrice"))
        n_analysts = int(_safe_float(info.get("numberOfAnalystOpinions", 0)))
        recommendation = str(info.get("recommendationKey", "")).lower()

        upside = None
        if current_price > 0 and target_mean > 0:
            upside = round((target_mean - current_price) / current_price * 100, 1)

        # Count buy vs hold vs sell from recommendations history
        strong_buy = int(_safe_float(info.get("recommendationMean", 0)))  # 1=strong buy, 5=sell
        # yfinance gives recommendationMean (1=strong buy … 5=sell), lower = more bullish
        rec_mean = _safe_float(info.get("recommendationMean"), 3.0)

        # Get recent upgrades/downgrades
        recent_upgrades = _get_recent_upgrades(t, symbol)

        return {
            "ticker": symbol,
            "name": info.get("longName") or info.get("shortName") or symbol,
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
            "current_price": current_price,
            "target_mean": target_mean,
            "target_high": target_high,
            "target_low": target_low,
            "upside_pct": upside,
            "n_analysts": n_analysts,
            "recommendation": recommendation,
            "rec_mean": rec_mean,   # 1=strong buy, 5=sell
            "market_cap": info.get("marketCap"),
            "pe_ratio": _safe_float(info.get("trailingPE")),
            "forward_pe": _safe_float(info.get("forwardPE")),
            "revenue_growth": _safe_float(info.get("revenueGrowth")),
            "earnings_growth": _safe_float(info.get("earningsGrowth")),
            "profit_margin": _safe_float(info.get("profitMargins")),
            "debt_to_equity": _safe_float(info.get("debtToEquity")),
            "roe": _safe_float(info.get("returnOnEquity")),
            "eps_forward": _safe_float(info.get("forwardEps")),
            "eps_ttm": _safe_float(info.get("trailingEps")),
            "52w_high": _safe_float(info.get("fiftyTwoWeekHigh")),
            "52w_low": _safe_float(info.get("fiftyTwoWeekLow")),
            "business_summary": (info.get("longBusinessSummary") or "")[:300],
            "recent_upgrades": recent_upgrades,
        }
    except Exception as e:
        return {"ticker": symbol, "error": str(e)}


def _get_recent_upgrades(ticker_obj, symbol, days=30):
    """Return list of recent upgrade events for a ticker."""
    upgrades = []
    try:
        df = ticker_obj.upgrades_downgrades
        if df is None or df.empty:
            return upgrades
        cutoff = pd.Timestamp(date.today() - timedelta(days=days), tz="UTC")
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC")
        recent = df[df.index >= cutoff].reset_index()
        for _, row in recent.iterrows():
            action = str(row.get("Action", "")).lower()
            to_grade = str(row.get("ToGrade", "")).strip()
            from_grade = str(row.get("FromGrade", "")).strip()
            firm = str(row.get("Firm", "")).strip()
            dt = str(row.get("GradeDate", row.get("Date", "")))[:10]
            if to_grade.lower() in BUY_RATINGS or action in ("up", "init"):
                upgrades.append({
                    "firm": firm,
                    "action": action,
                    "from_grade": from_grade,
                    "to_grade": to_grade,
                    "date": dt,
                })
    except Exception:
        pass
    return upgrades


def score_stock(data):
    """Higher score = more analyst conviction. Used for ranking."""
    if not data or "error" in data:
        return -999
    score = 0
    rec_mean = data.get("rec_mean", 3.0)
    # rec_mean: 1=strong buy, 5=sell → invert so lower=better becomes higher score
    score += (5 - rec_mean) * 20
    upside = data.get("upside_pct") or 0
    score += min(upside, 60)  # cap contribution at 60 pts
    score += min(data.get("n_analysts", 0), 30)
    score += len(data.get("recent_upgrades", [])) * 5
    return score


def build_us_report_data(watchlist=None):
    """
    Fetch data for watchlist, score, and return top candidates.
    Returns list of stock dicts sorted by analyst conviction score.
    """
    if yf is None:
        return [], "yfinance not installed"

    symbols = watchlist or US_WATCHLIST
    results = []
    for symbol in symbols:
        data = fetch_ticker_data(symbol)
        if data and "error" not in data:
            upside = data.get("upside_pct") or 0
            n = data.get("n_analysts", 0)
            if upside >= MIN_UPSIDE_PCT and n >= MIN_BUY_RATINGS:
                data["score"] = score_stock(data)
                results.append(data)

    results.sort(key=lambda x: x.get("score", 0), reverse=True)
    return results, None
