"""
Hong Kong market data via yfinance.
Same analyst-consensus approach as the US fetcher.
"""

import traceback
from datetime import date, timedelta

try:
    import yfinance as yf
    import pandas as pd
except ImportError:
    yf = None
    pd = None

from config import HK_WATCHLIST, MIN_UPSIDE_PCT, MIN_BUY_RATINGS
from fetchers.us_market import fetch_ticker_data, score_stock


def build_hk_report_data(watchlist=None):
    """Fetch HK watchlist and return top candidates by analyst conviction."""
    if yf is None:
        return [], "yfinance not installed"

    symbols = watchlist or HK_WATCHLIST
    results = []
    for symbol in symbols:
        data = fetch_ticker_data(symbol)
        if data and "error" not in data:
            upside = data.get("upside_pct") or 0
            n = data.get("n_analysts", 0)
            # HK stocks often have fewer analysts — lower threshold
            if upside >= MIN_UPSIDE_PCT and n >= max(MIN_BUY_RATINGS - 1, 1):
                data["score"] = score_stock(data)
                results.append(data)

    results.sort(key=lambda x: x.get("score", 0), reverse=True)
    return results, None
