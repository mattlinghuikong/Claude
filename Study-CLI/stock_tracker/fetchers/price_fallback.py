"""
Free, keyless fallback price sources used when yfinance fails.

Two independent sources so a single-point outage doesn't block the run:

- Stooq (https://stooq.com)    — CSV endpoint, daily history, US/HK/CN tickers
- Sina Finance (hq.sinajs.cn)  — real-time quote, US/HK/CN tickers

Both return a canonical {"price": float, "closes": pandas.Series|None} dict.
"""

from __future__ import annotations

import concurrent.futures
import io
import re
from datetime import date, timedelta

try:
    import requests
    import pandas as pd
except ImportError:
    requests = None
    pd = None


_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

_TICKER_RE = re.compile(r"^[A-Za-z0-9.\-]{1,12}$")


def _is_hk(symbol: str) -> bool:
    return symbol.upper().endswith(".HK")


def _is_cn(symbol: str) -> bool:
    return bool(re.match(r"^\d{6}(\.(SH|SZ))?$", symbol))


# ── Stooq ────────────────────────────────────────────────────────────────────

def _stooq_symbol(symbol: str) -> str | None:
    """Translate our ticker format into Stooq's."""
    s = symbol.upper()
    if _is_hk(s):
        # 0700.HK -> 0700.hk
        return s.lower()
    if _is_cn(s):
        # 600519 -> 600519.cn (Shanghai) or 000001.cn (Shenzhen) — Stooq uses .cn suffix
        base = s.split(".")[0]
        return f"{base}.cn"
    if _TICKER_RE.match(s):
        return f"{s}.us"
    return None


def stooq_history(symbol: str, days: int = 365, timeout: float = 8.0):
    """Return pandas.Series of daily closes from Stooq, or None on failure.

    Tries HTTPS first; some environments (e.g. Python 3.14 + macOS) reject
    Stooq's CA chain, so we retry over HTTP before giving up."""
    if requests is None or pd is None:
        return None
    s = _stooq_symbol(symbol)
    if not s:
        return None
    end = date.today()
    begin = end - timedelta(days=days)
    qs = f"s={s}&i=d&d1={begin.strftime('%Y%m%d')}&d2={end.strftime('%Y%m%d')}"
    for scheme in ("https", "http"):
        url = f"{scheme}://stooq.com/q/d/l/?{qs}"
        try:
            r = requests.get(url, headers=_HEADERS, timeout=timeout,
                             allow_redirects=False)
            if r.status_code != 200 or not r.text or \
               r.text.lower().startswith("no data"):
                continue
            df = pd.read_csv(io.StringIO(r.text))
            if df.empty or "Close" not in df.columns:
                continue
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
                df = df.dropna(subset=["Date"]).sort_values("Date").set_index("Date")
            return df["Close"].dropna()
        except Exception:
            continue
    return None


# ── Sina Finance ─────────────────────────────────────────────────────────────
# Sina's hq.sinajs.cn endpoint returns tick-level quotes. Format varies by market.

def _sina_symbol(symbol: str) -> str | None:
    s = symbol.upper()
    if _is_hk(s):
        # 0700.HK -> rt_hk00700
        base = s.split(".")[0].zfill(5)
        return f"rt_hk{base}"
    if _is_cn(s):
        # 600519 -> sh600519 ; 000001 -> sz000001
        base = s.split(".")[0]
        exch = "sh" if base.startswith(("6", "9")) else "sz"
        return f"{exch}{base}"
    if _TICKER_RE.match(s):
        # US ticker — Sina supports gb_aapl format
        return f"gb_{s.lower()}"
    return None


def sina_quote(symbol: str, timeout: float = 5.0):
    """Return current price as float, or None on failure."""
    if requests is None:
        return None
    s = _sina_symbol(symbol)
    if not s:
        return None
    # Sina only serves this endpoint cleanly over HTTP. HTTPS redirects to
    # a different CA chain that often fails cert validation on modern Python.
    url = f"http://hq.sinajs.cn/list={s}"
    try:
        r = requests.get(
            url,
            headers={**_HEADERS, "Referer": "https://finance.sina.com.cn/"},
            timeout=timeout,
            allow_redirects=False,
        )
        if r.status_code != 200 or "=" not in r.text:
            return None
        # Response: var hq_str_<sym>="...,price,...";
        payload = r.text.split('"', 2)
        if len(payload) < 2:
            return None
        fields = payload[1].split(",")
        if not fields:
            return None
        # Price position varies by market.
        if s.startswith("gb_"):
            # US: 1 = current price
            return float(fields[1]) if len(fields) > 1 and fields[1] else None
        if s.startswith("rt_hk"):
            # HK: 6 = last price
            return float(fields[6]) if len(fields) > 6 and fields[6] else None
        # CN A-share: 3 = current price
        return float(fields[3]) if len(fields) > 3 and fields[3] else None
    except Exception:
        return None


# ── Unified fallback ─────────────────────────────────────────────────────────

def get_price_and_history(symbol: str):
    """Try sources in order; return (current_price, closes_series)."""
    closes = stooq_history(symbol)
    price = None
    if closes is not None and not closes.empty:
        try:
            price = float(closes.iloc[-1])
        except Exception:
            price = None
    if price is None:
        price = sina_quote(symbol)
    return price, closes


def bulk_fallback_history(symbols, workers=8):
    """Parallel Stooq history fetch for all symbols. Returns dict[sym -> Close series]."""
    out = {}
    if not symbols:
        return out
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        future_map = {ex.submit(stooq_history, s): s for s in symbols}
        for fut in concurrent.futures.as_completed(future_map):
            sym = future_map[fut]
            try:
                series = fut.result(timeout=12)
            except Exception:
                series = None
            if series is not None and not series.empty:
                out[sym] = series
    return out
