"""
US market analyst data via yfinance.
Since free sources don't expose individual analyst win rates,
we track analyst calls ourselves in SQLite and rank by self-computed win rate.
For fresh installs, we rank by: consensus strength + price target upside.
"""

import concurrent.futures
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
from fetchers.price_fallback import bulk_fallback_history


BUY_RATINGS = {"buy", "strong buy", "outperform", "overweight", "positive",
               "accumulate", "add", "market outperform", "sector outperform"}
SELL_RATINGS = {"sell", "underperform", "underweight", "negative", "reduce"}


def _safe_float(val, default=0.0):
    try:
        return float(val) if val is not None else default
    except Exception:
        return default


def _momentum_from_closes(closes):
    """Compute momentum stats from a pandas Close series."""
    out = {"ret_1m": None, "ret_3m": None, "ret_6m": None,
           "above_200dma": None, "dist_52w_high_pct": None}
    if closes is None:
        return out
    try:
        closes = closes.dropna()
        if closes.empty:
            return out
        last = float(closes.iloc[-1])

        def _ret(n_days):
            if len(closes) <= n_days:
                return None
            prev = float(closes.iloc[-n_days - 1])
            if prev <= 0:
                return None
            return round((last - prev) / prev * 100, 2)

        out["ret_1m"] = _ret(21)
        out["ret_3m"] = _ret(63)
        out["ret_6m"] = _ret(126)
        if len(closes) >= 200:
            ma200 = float(closes.tail(200).mean())
            out["above_200dma"] = bool(last >= ma200)
        w52_high = float(closes.max())
        if w52_high > 0:
            out["dist_52w_high_pct"] = round((w52_high - last) / w52_high * 100, 2)
    except Exception:
        pass
    return out


def bulk_history(symbols, period="1y"):
    """One HTTP call for all symbols' price history. Returns dict[symbol -> Close series].
    Replaces ~N per-ticker .history() calls with a single batch download."""
    if yf is None or pd is None or not symbols:
        return {}
    result = {}
    try:
        df = yf.download(
            " ".join(symbols), period=period, interval="1d",
            auto_adjust=True, progress=False, group_by="ticker",
            threads=True,
        )
        if df is None or df.empty:
            return {}
        if len(symbols) == 1:
            # Single-ticker download returns flat columns, not MultiIndex.
            sym = symbols[0]
            if "Close" in df.columns:
                result[sym] = df["Close"]
        else:
            for sym in symbols:
                try:
                    sub = df[sym]
                    if "Close" in sub.columns:
                        result[sym] = sub["Close"]
                except (KeyError, Exception):
                    continue
    except Exception:
        return {}
    return result


def _earnings_days_from_info(info):
    """Extract days-until-next-earnings from info dict. Avoids the per-ticker
    .calendar network call (saves ~one round-trip per stock)."""
    ts = info.get("earningsTimestamp") or info.get("earningsTimestampStart")
    if not ts:
        return None
    try:
        from datetime import datetime as _dt
        ed = _dt.fromtimestamp(int(ts)).date()
        delta = (ed - date.today()).days
        if delta < -7 or delta > 365:
            return None
        return int(delta)
    except Exception:
        return None


def fetch_ticker_data(symbol, price_series=None):
    """Fetch analyst consensus + fundamentals for one ticker.

    `price_series` — optional pre-fetched pandas Close series (from bulk_history).
    When present, we skip the extra per-ticker .history() round-trip."""
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

        # yfinance gives recommendationMean (1=strong buy … 5=sell), lower = more bullish
        rec_mean = _safe_float(info.get("recommendationMean"), 3.0)

        # Get recent upgrades/downgrades
        recent_upgrades = _get_recent_upgrades(t, symbol)

        # Price momentum — prefer the bulk-fetched series; fall back to per-ticker.
        if price_series is None:
            try:
                hist = t.history(period="1y", interval="1d", auto_adjust=True)
                price_series = hist["Close"] if hist is not None and not hist.empty else None
            except Exception:
                price_series = None
        momentum = _momentum_from_closes(price_series)
        earnings_in_days = _earnings_days_from_info(info)

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
            # New money-making signals
            "ret_1m": momentum["ret_1m"],
            "ret_3m": momentum["ret_3m"],
            "ret_6m": momentum["ret_6m"],
            "above_200dma": momentum["above_200dma"],
            "dist_52w_high_pct": momentum["dist_52w_high_pct"],
            "earnings_in_days": earnings_in_days,
            "beta": _safe_float(info.get("beta")),
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


def _fetch_many(symbols, max_workers=8):
    """Fetch many tickers in parallel. Bulk-fetches price history up front so
    each per-ticker worker only needs one info call. If yfinance's bulk
    download misses tickers (Yahoo regularly 404s on HK), fill in gaps from
    Stooq as a fallback source."""
    results = []
    if not symbols:
        return results

    history_map = bulk_history(symbols, period="1y")
    missing = [s for s in symbols if s not in history_map]
    if missing:
        history_map.update(bulk_fallback_history(missing))

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {
            ex.submit(fetch_ticker_data, s, history_map.get(s)): s
            for s in symbols
        }
        for fut in concurrent.futures.as_completed(future_map):
            try:
                data = fut.result(timeout=30)
            except Exception:
                data = None
            if data:
                results.append(data)
    return results


def build_us_report_data(watchlist=None):
    """
    Fetch data for watchlist, score, and return top candidates.
    Returns list of stock dicts sorted by analyst conviction score.
    """
    if yf is None:
        return [], "yfinance not installed"

    symbols = watchlist or US_WATCHLIST
    raw = _fetch_many(symbols)
    results = []
    for data in raw:
        if "error" in data:
            continue
        upside = data.get("upside_pct") or 0
        n = data.get("n_analysts", 0)
        if upside >= MIN_UPSIDE_PCT and n >= MIN_BUY_RATINGS:
            data["score"] = score_stock(data)
            results.append(data)

    results.sort(key=lambda x: x.get("score", 0), reverse=True)
    return results, None
