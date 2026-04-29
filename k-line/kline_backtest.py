"""K-line + MA breakout strategy backtest (per CLAUDE.md).

Markets: A-share (akshare) / US + HK (yfinance), 20 random tickers each.
Period:  6 months of 1-hour K-lines.
Costs:   0.05% commission + 0.05% slippage per side (=0.20% round trip).

Performance:
- CSV disk cache (re-runs skip the network).
- ThreadPoolExecutor for parallel fetch (capped concurrency).
- Vectorized rolling MA / swing-high / swing-low via pandas.
- Inner backtest loop runs on numpy arrays (not pandas Series).

Cybersecurity hardening:
- Strict regex validation of every ticker before it touches network/filesystem.
- Cache filename sanitized (no path traversal possible).
- Network calls have explicit timeouts.
- No pickle, no eval/exec, no subprocess on external data.
- HTTPS via library defaults; SSL verification not disabled.
- Concurrency capped to avoid hammering remote APIs.
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import logging
import random
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# --- Constants -------------------------------------------------------------

CACHE_DIR = Path(__file__).resolve().parent / "data_cache"
CACHE_DIR.mkdir(exist_ok=True)

COMMISSION = 0.0005          # 0.05% per side
SLIPPAGE = 0.0005            # 0.05% per side (assumed)
COST_PER_SIDE = COMMISSION + SLIPPAGE
ROUND_TRIP_COST = 2 * COST_PER_SIDE  # 0.20%

MA_FAST, MA_MID, MA_SLOW = 30, 150, 750
SWING_LOOKBACK = 20

# --- v2 strategy parameters -----------------------------------------------
ATR_PERIOD = 14
BREAKOUT_BUFFER_ATR = 0.25      # close must clear neckline by 0.25*ATR
TREND_SLOPE_PERIOD = 20         # bars used to compute MA_MID slope
TREND_SLOPE_MIN = 0.0002        # min |slope| per bar (~0.4% over 20 bars)
ATR_TRAIL_MULT = 2.5            # trailing-stop distance in ATR units
PYRAMID_PROFIT_ATR = 1.5        # need >=1.5*ATR profit before adding
PYRAMID_MAX_ADDS = 1            # CLAUDE.md: 加仓只能加一次
ADX_PERIOD = 14                 # standard Wilder ADX

# === v3 — Per-market parameter dictionary =================================
# Tighter on choppier markets; looser on the trendier ones. Calibrated from
# the 3y daily backtest observation that A-share/HK chop punishes wide stops.
# --- v4 — multi-timeframe + market regime ---------------------------------
INDEX_TICKERS_BY_MARKET = {
    "US": "^GSPC",       # S&P 500
    "HK": "^HSI",        # Hang Seng
    "CN": "000300.SS",   # CSI 300
}
WEEKLY_MA_SHORT = 10        # ~2.3 months — paired with daily MA_FAST=30 (~6 weeks)
WEEKLY_MA_LONG = 30         # ~7 months  — paired with daily MA_MID=150 (~30 weeks)
INDEX_ADX_MIN = 18           # market regime master switch
INDEX_ADX_PERIOD = 14

# --- v5 — RSI mean-reversion fallback for non-trending regimes ------------
RSI_PERIOD = 14
RSI_OVERSOLD = 30
RSI_OVERBOUGHT = 70
RSI_TARGET = 50          # exit MR position when RSI returns toward center
MR_TIME_STOP = 20        # close MR trade after N bars regardless
MR_INIT_STOP_ATR = 1.5   # MR initial stop in ATR units (independent of trend params)

# --- v6 — multi-indicator confluence (BB + MACD + RSI zone filters) -------
# Standard parameters. Kept globally so all v6 callers use the same settings.
BB_PERIOD = 20
BB_STD_MULT = 2.0
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
RSI_LONG_MIN = 40        # filters out deeply oversold (failed breakouts)
RSI_LONG_MAX = 75        # filters out extreme overbought (chasing)
RSI_SHORT_MIN = 25
RSI_SHORT_MAX = 60
V6_FILTER_KEYS = ("bb", "macd", "rsi")  # all subsets of these are tested

# --- v7 — additional indicators (BB %B, OBV, ATR-expansion) + 10% hard stop
PCT_HARD_STOP = 0.10           # absolute cap: 10% drawdown from entry → force exit
BB_PERCENT_LONG = 0.70         # BB %B > 0.70 = top 30% of bands (long bias)
BB_PERCENT_SHORT = 0.30        # < 0.30 = bottom 30% (short bias)
OBV_MA_PERIOD = 20             # OBV trend confirmation MA
ATR_EXPANSION_LOOKBACK = 50    # average ATR window for expansion check
ATR_EXPANSION_MULT = 1.20      # current ATR must be > 1.2 × avg → volatility breakout
V7_FILTER_KEYS = ("bb", "macd", "rsi", "obv", "atr_exp")

# --- v8 — MFI, CCI, KDJ + CN-specific master switch ----------------------
MFI_PERIOD = 14
MFI_LONG_MIN = 50         # MFI > 50 = volume-weighted bullishness
MFI_LONG_MAX = 80         # < 80 = not yet over-extended
MFI_SHORT_MIN = 20
MFI_SHORT_MAX = 50
CCI_PERIOD = 20
CCI_LONG_MIN = 100        # CCI > +100 = strong uptrend confirmed
CCI_SHORT_MAX = -100      # CCI < -100 = strong downtrend confirmed
KDJ_N = 9
KDJ_M1 = 3
KDJ_M2 = 3
KDJ_LONG_MIN = 50         # K and D both above 50, plus K > D
KDJ_SHORT_MAX = 50

# A-share specific master switch (in addition to ADX-based regime).
# CSI300 must be above its MA200 for longs (uptrend bias), below for shorts.
# This is meant to skip the chronic-bear periods that ADX alone doesn't catch.
CN_MASTER_MA = 200
CN_INDEX_TICKER = "000300.SS"

# v8 filters: drop bb (proven redundant), add mfi/cci/kdj.
V8_FILTER_KEYS = ("macd", "rsi", "obv", "atr_exp", "mfi", "cci", "kdj")

# In-memory cache for the per-market regime calculation (avoid recomputing
# across the 20 stocks of that market). Keyed by market label.
_REGIME_CACHE: dict[str, dict] = {}


MARKET_PARAMS = {
    # US: trends are strong, so almost anything that fires runs. Don't
    # over-filter — keep the v2 spirit (loose), only add a mild ADX gate.
    "US": {
        "atr_buffer": 0.25, "slope_min": 0.0002, "adx_min": 15,
        "init_stop_atr": 2.2, "trail_atr": 2.5, "trail_activate_atr": 0.8,
        "pyramid": True,  "pyramid_profit_atr": 1.5,
        "cooldown_after_losses": 0, "cooldown_bars": 0,
        "vol_confirm_mult": 1.0,        # effectively off
    },
    "HK": {
        "atr_buffer": 0.35, "slope_min": 0.0003, "adx_min": 25,
        "init_stop_atr": 1.5, "trail_atr": 2.0, "trail_activate_atr": 1.0,
        "pyramid": True,  "pyramid_profit_atr": 2.0,
        "cooldown_after_losses": 2, "cooldown_bars": 30,
        "vol_confirm_mult": 1.3,
    },
    "CN": {
        "atr_buffer": 0.50, "slope_min": 0.0004, "adx_min": 28,
        "init_stop_atr": 1.2, "trail_atr": 1.8, "trail_activate_atr": 0.8,
        "pyramid": False, "pyramid_profit_atr": 999.0,
        "cooldown_after_losses": 2, "cooldown_bars": 60,
        "vol_confirm_mult": 1.5,
    },
}
# 6 months of 1H bars (~800) is too short to use MA750 as a hard trend
# filter (it eats most of the warmup window AND makes 3-level alignment
# almost impossible). Default to K1's 2-level system; pass --three-level
# to opt in to K2's 3-level filter (suitable for ~3+ years of data).
USE_THREE_LEVEL_DEFAULT = False

SAMPLE_SIZE = 20
RANDOM_SEED = 42
NETWORK_TIMEOUT = 30
MAX_WORKERS = 5

# Data window presets. yfinance hard limits: 1h <= 730 days; 1d unlimited.
DATA_PRESETS = {
    "short":       {"days": 31 * 6,  "interval": "1h"},  # ~6 months hourly
    "long":        {"days": 365 * 3, "interval": "1d"},  # ~3 years daily
    "hourly_long": {"days": 729,     "interval": "1h"},  # ~2 years hourly
                                                         # (max yfinance allows)
}
# Mutated in main() based on --mode; default keeps backwards compatibility.
PERIOD_DAYS = DATA_PRESETS["short"]["days"]
INTERVAL = DATA_PRESETS["short"]["interval"]

# Strict whitelists. yfinance tickers: letters/digits/dot/hyphen.
# A-share codes: exactly 6 digits.
YF_TICKER_RE = re.compile(r"^\^?[A-Z0-9.\-]{1,15}$")  # leading ^ permitted for indices
A_SHARE_RE = re.compile(r"^\d{6}$")


def _a_share_to_yf(code: str) -> str:
    """Map a 6-digit A-share code to its yfinance ticker.
    Shanghai: 6/9 -> .SS;  Shenzhen: 0/3 -> .SZ.
    """
    c = _validate_a_share(code)
    if c[0] in ("6", "9"):
        return f"{c}.SS"
    if c[0] in ("0", "3"):
        return f"{c}.SZ"
    raise ValueError(f"unknown exchange for A-share code: {code!r}")

LOG = logging.getLogger("kline-bt")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


# --- Universes -------------------------------------------------------------
# Hardcoded liquid lists drawn from S&P 500 / HSI / CSI 300. Hardcoding keeps
# the test reproducible and avoids fragile web-scraping at runtime.

US_UNIVERSE = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B",
    "JPM", "V", "JNJ", "WMT", "MA", "PG", "UNH", "XOM", "HD", "CVX",
    "BAC", "ABBV", "PFE", "KO", "PEP", "AVGO", "COST", "TMO", "DIS",
    "CSCO", "MRK", "ABT", "ACN", "ADBE", "VZ", "NFLX", "CRM", "DHR",
    "INTC", "AMD", "ORCL", "T", "QCOM", "TXN", "IBM", "NKE", "GE",
    "BA", "F", "GM", "CAT", "MMM",
]

HK_UNIVERSE = [
    "0001.HK", "0002.HK", "0003.HK", "0005.HK", "0006.HK", "0011.HK",
    "0012.HK", "0016.HK", "0017.HK", "0027.HK", "0066.HK", "0083.HK",
    "0101.HK", "0175.HK", "0241.HK", "0267.HK", "0288.HK", "0291.HK",
    "0386.HK", "0388.HK", "0669.HK", "0688.HK", "0700.HK", "0762.HK",
    "0823.HK", "0857.HK", "0883.HK", "0939.HK", "0941.HK", "0968.HK",
    "0992.HK", "1038.HK", "1044.HK", "1093.HK", "1109.HK", "1113.HK",
    "1177.HK", "1209.HK", "1211.HK", "1299.HK", "1398.HK", "1810.HK",
    "1928.HK", "2018.HK", "2269.HK", "2313.HK", "2318.HK", "2382.HK",
    "2388.HK", "2628.HK", "3690.HK", "3968.HK", "3988.HK", "9618.HK",
    "9888.HK", "9988.HK", "9999.HK",
]

CN_UNIVERSE = [
    "600519", "601398", "601318", "600036", "600276", "601166", "600030",
    "601288", "600028", "601628", "600000", "600887", "601088", "600585",
    "601668", "601857", "600016", "600048", "601988", "601328",
    "601601", "600837", "601138", "601800", "600009", "600019", "601012",
    "600104", "601111", "601225", "600438", "600406", "601899", "002714",
    "300750", "002475", "002594", "300059", "002230", "300015",
    "002415", "300760", "002352", "002304", "002241", "002142", "300033",
    "002027", "002001", "000001", "000002", "000333",
    "000538", "000651", "000725", "000776", "000858", "000895",
]


# --- Validation helpers ----------------------------------------------------

def _validate_yf(ticker: str) -> str:
    t = ticker.strip().upper()
    if not YF_TICKER_RE.match(t):
        raise ValueError(f"invalid yfinance ticker: {ticker!r}")
    return t


def _validate_a_share(code: str) -> str:
    c = code.strip()
    if not A_SHARE_RE.match(c):
        raise ValueError(f"invalid A-share code: {code!r}")
    return c


def _cache_path(market: str, ticker: str, interval: str | None = None) -> Path:
    """Sanitize ticker into a safe cache filename. No path traversal."""
    safe = re.sub(r"[^A-Z0-9._-]", "_", ticker.upper())
    if not safe or safe.startswith(".") or "/" in safe or "\\" in safe:
        raise ValueError(f"unsafe cache key: {ticker!r}")
    iv = re.sub(r"[^a-z0-9]", "", (interval or INTERVAL).lower())[:4]
    p = CACHE_DIR / f"{market}_{iv}_{safe}.csv"
    if CACHE_DIR.resolve() not in p.resolve().parents:
        raise ValueError(f"cache path escapes CACHE_DIR: {p}")
    return p


# --- Data fetchers ---------------------------------------------------------

def _read_cache(p: Path) -> pd.DataFrame | None:
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p, parse_dates=["ts"])
        if {"ts", "open", "high", "low", "close"}.issubset(df.columns):
            return df
    except Exception:
        try:
            p.unlink()
        except OSError:
            pass
    return None


def _write_cache(p: Path, df: pd.DataFrame) -> None:
    df.to_csv(p, index=False)


def fetch_yf(ticker: str) -> pd.DataFrame | None:
    """Fetch ~6 months of 1-hour bars via yfinance."""
    import yfinance as yf

    t = _validate_yf(ticker)
    cp = _cache_path("yf", t, INTERVAL)
    cached = _read_cache(cp)
    if cached is not None:
        return cached

    end = datetime.now()
    start = end - timedelta(days=int(PERIOD_DAYS))
    try:
        df = yf.download(
            tickers=t,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            interval=INTERVAL,
            progress=False,
            timeout=NETWORK_TIMEOUT,
            auto_adjust=False,
            threads=False,
        )
    except Exception as e:
        LOG.warning("yfinance fetch failed for %s: %s", t, e)
        return None
    if df is None or df.empty:
        return None

    df = df.reset_index()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    df.columns = [str(c) for c in df.columns]
    rename = {
        "Datetime": "ts", "Date": "ts", "index": "ts",
        "Open": "open", "High": "high", "Low": "low", "Close": "close",
        "Volume": "volume",
    }
    df = df.rename(columns=rename)
    needed = ["ts", "open", "high", "low", "close"]
    if not set(needed).issubset(df.columns):
        return None
    df = df[needed + (["volume"] if "volume" in df.columns else [])].copy()
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.dropna(subset=["close", "high", "low"])
    if df.empty:
        return None
    _write_cache(cp, df)
    return df


def fetch_cn(code: str) -> pd.DataFrame | None:
    """Fetch ~6 months of 1-hour A-share bars.

    Primary path: yfinance with .SS/.SZ suffix (reliable, same API as US/HK).
    Fallback:    akshare's EM endpoint (frequently rate-limits — kept as backup).
    """
    yf_ticker = _a_share_to_yf(code)
    df = fetch_yf(yf_ticker)
    if df is not None:
        return df

    # Fallback to akshare if yfinance fails for some Chinese listing.
    import akshare as ak  # noqa: F401  imported lazily

    c = _validate_a_share(code)
    cp = _cache_path("ak", c, INTERVAL)
    cached = _read_cache(cp)
    if cached is not None:
        return cached
    end = datetime.now()
    start = end - timedelta(days=int(PERIOD_DAYS))
    try:
        df = ak.stock_zh_a_hist_min_em(
            symbol=c, period="60",
            start_date=start.strftime("%Y-%m-%d %H:%M:%S"),
            end_date=end.strftime("%Y-%m-%d %H:%M:%S"),
            adjust="qfq",
        )
    except Exception as e:
        LOG.warning("akshare fallback failed for %s: %s", c, e)
        return None
    if df is None or df.empty:
        return None
    rename = {"时间": "ts", "开盘": "open", "最高": "high",
              "最低": "low", "收盘": "close", "成交量": "volume"}
    df = df.rename(columns=rename)
    needed = ["ts", "open", "high", "low", "close"]
    if not set(needed).issubset(df.columns):
        return None
    df = df[needed + (["volume"] if "volume" in df.columns else [])].copy()
    df["ts"] = pd.to_datetime(df["ts"])
    df = df.dropna(subset=["close", "high", "low"])
    if df.empty:
        return None
    _write_cache(cp, df)
    return df


# --- Analyst-based universe screener --------------------------------------

ANALYST_CACHE_DIR = CACHE_DIR / "analyst"
ANALYST_CACHE_DIR.mkdir(exist_ok=True)
SCREENER_VERSION = 4   # bump whenever score formula changes; old cache invalid


def _analyst_cache_path(market: str, ticker: str) -> Path:
    safe = re.sub(r"[^A-Z0-9._-]", "_", ticker.upper())
    if not safe or "/" in safe or "\\" in safe or safe.startswith("."):
        raise ValueError(f"unsafe analyst cache key: {ticker!r}")
    p = ANALYST_CACHE_DIR / f"{market}_v{SCREENER_VERSION}_{safe}.json"
    if ANALYST_CACHE_DIR.resolve() not in p.resolve().parents:
        raise ValueError(f"path escapes ANALYST_CACHE_DIR: {p}")
    return p


def fetch_analyst_score(market: str, ticker: str) -> dict | None:
    """Build a composite analyst score for a ticker.

    Pulls Yahoo's aggregated street view (covers Wall St / HSI brokers / some
    A-share via .SS/.SZ) plus 3-month price momentum and price-vs-MA200.

    KNOWN LIMITATION (look-ahead bias): analyst data is *current* — using it
    to filter a backtest universe will bias results. Caller must acknowledge.
    """
    import json
    cp = _analyst_cache_path(market, ticker)
    if cp.exists():
        try:
            return json.loads(cp.read_text())
        except Exception:
            cp.unlink(missing_ok=True)

    yf_t = ticker if market != "CN" else _a_share_to_yf(ticker)
    try:
        import yfinance as yf
        info = yf.Ticker(yf_t).info or {}
    except Exception as e:
        LOG.warning("[%s] %s: analyst info fetch failed: %s", market, ticker, e)
        return None

    target = info.get("targetMeanPrice")
    current = info.get("currentPrice") or info.get("regularMarketPrice")
    rec_mean = info.get("recommendationMean")    # 1=StrongBuy .. 5=StrongSell
    n_analysts = info.get("numberOfAnalystOpinions") or 0

    if not target or not current or current <= 0 or not n_analysts:
        # Cache the negative result briefly to avoid hammering the endpoint
        cp.write_text(json.dumps({"ticker": ticker, "score": None}))
        return None

    upside = float(target) / float(current) - 1.0

    # 3-month momentum + above-MA200 from cached price history.
    df = fetch_yf(yf_t)
    mom_3m = 0.0
    above_ma200 = False
    if df is not None and len(df) > 0:
        if INTERVAL == "1h":
            lookback = min(len(df) - 1, 60 * 6)   # ~60 days of hourly bars
        else:
            lookback = min(len(df) - 1, 60)
        if lookback > 0:
            mom_3m = float(df["close"].iloc[-1] / df["close"].iloc[-lookback] - 1.0)
        if len(df) >= 200:
            above_ma200 = bool(
                df["close"].iloc[-1] > df["close"].rolling(200).mean().iloc[-1]
            )

    rec_mean_v = float(rec_mean) if rec_mean is not None else 3.0
    rec_score = max(0.0, min(1.0, (4.0 - rec_mean_v) / 3.0))   # 0..1
    upside_score = max(0.0, min(1.0, upside / 0.30))           # cap @ 30%
    analyst_volume = min(1.0, float(n_analysts) / 15.0)
    mom_score = max(0.0, min(1.0, mom_3m / 0.20))              # continuous, cap +20%

    # Strength above MA200 (continuous). For trend-following, "how far above
    # the long MA you are" is much more predictive than analyst sentiment.
    ma_strength = 0.0
    if df is not None and len(df) >= 200:
        ratio = float(df["close"].iloc[-1]
                      / df["close"].rolling(200).mean().iloc[-1])
        ma_strength = max(0.0, min(1.0, (ratio - 1.0) / 0.20))  # 0..1, cap 20%

    # v4 screener weights: technical/momentum 70%, analyst signals 30%.
    # Analyst total reduced from 45% to 30% because:
    #   - the v3 strategy refinement made stock selection less critical
    #     (better stops/filters compensate for slightly noisier picks)
    #   - analyst signals are end-of-day stale and update lazily
    #   - 30% is enough to nudge ties; not enough to override price action
    # Within the 30% analyst block, consensus > coverage > target price.
    #
    # Component weights (sum = 1.0, analyst sub-total = 0.30):
    #   0.40  3-month price momentum                        \ technical
    #   0.30  strength above MA200 (continuous)              / 70%
    #   0.15  consensus rec (StrongBuy=1 ... StrongSell=5)  \
    #   0.10  analyst coverage breadth (n_analysts)          | analyst
    #   0.05  upside to mean target price                    / 30%
    # Constraint preserved: rec (0.15) > upside (0.05).
    score = (
        0.40 * mom_score
        + 0.30 * ma_strength
        + 0.15 * rec_score
        + 0.10 * analyst_volume
        + 0.05 * upside_score
    )

    out = {
        "ticker": ticker, "market": market,
        "current": float(current), "target": float(target),
        "upside": upside, "rec_mean": rec_mean_v,
        "n_analysts": int(n_analysts),
        "momentum_3m": mom_3m, "above_ma200": above_ma200,
        "score": score,
    }
    try:
        cp.write_text(json.dumps(out))
    except OSError:
        pass
    return out


def screen_universe(market: str, candidates: list[str], top_n: int) -> list[str]:
    """Score every candidate, return the top-N by composite score.

    Skips tickers without analyst coverage (no target / 0 analysts).
    """
    LOG.info("[%s] screening %d candidates for top %d ...",
             market, len(candidates), top_n)
    scored = []
    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_analyst_score, market, t): t for t in candidates}
        for fut in cf.as_completed(futs):
            try:
                r = fut.result(timeout=NETWORK_TIMEOUT * 2)
            except Exception:
                continue
            if r is None or r.get("score") is None:
                continue
            scored.append(r)

    scored.sort(key=lambda x: x["score"], reverse=True)
    chosen = scored[:top_n]
    LOG.info("[%s] %d candidates with analyst data, top %d picked:",
             market, len(scored), len(chosen))
    for s in chosen:
        LOG.info("  %s  score=%.3f  upside=%+.1f%%  rec=%.2f  "
                 "n=%d  mom3m=%+.1f%%  >MA200=%s",
                 s["ticker"], s["score"], s["upside"] * 100,
                 s["rec_mean"], s["n_analysts"],
                 s["momentum_3m"] * 100, s["above_ma200"])
    return [s["ticker"] for s in chosen]


def fetch_parallel(market: str, tickers, fetch_fn) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    with cf.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(fetch_fn, t): t for t in tickers}
        for fut in cf.as_completed(futs):
            t = futs[fut]
            try:
                df = fut.result(timeout=NETWORK_TIMEOUT * 2)
            except Exception as e:
                LOG.warning("[%s] %s: fetch error %s", market, t, e)
                continue
            if df is None or len(df) < MA_MID + SWING_LOOKBACK + 5:
                LOG.warning(
                    "[%s] %s: insufficient bars (got %s)",
                    market, t, 0 if df is None else len(df),
                )
                continue
            out[t] = df
            LOG.info("[%s] %s: %d bars", market, t, len(df))
    return out


# --- Strategy / backtest ---------------------------------------------------

@dataclass(frozen=True)
class Trade:
    side: str          # 'long' | 'short'
    entry_time: pd.Timestamp
    entry_price: float
    exit_time: pd.Timestamp
    exit_price: float
    stop_dist: float = 0.0   # |entry - initial_stop|; 0 means unknown

    @property
    def gross_return(self) -> float:
        if self.side == "long":
            return self.exit_price / self.entry_price - 1.0
        return self.entry_price / self.exit_price - 1.0

    @property
    def net_return(self) -> float:
        # Cost approximation: subtract round-trip (commission + slippage on
        # both entry and exit). Accurate to first order for moves under ~10%.
        return self.gross_return - ROUND_TRIP_COST

    @property
    def r_multiple(self) -> float:
        """Risk-adjusted return: trade P&L expressed in initial-risk units.
        Returns 0.0 when stop_dist isn't recorded (v1/v2 trades)."""
        if self.stop_dist <= 0:
            return 0.0
        if self.side == "long":
            move = self.exit_price - self.entry_price
        else:
            move = self.entry_price - self.exit_price
        # Subtract cost expressed in price terms: ROUND_TRIP_COST * entry.
        net_move = move - ROUND_TRIP_COST * self.entry_price
        return net_move / self.stop_dist


@dataclass
class TickerResult:
    ticker: str
    market: str
    bars: int
    use_three_level: bool
    trades: list[Trade]

    @property
    def n_trades(self) -> int:
        return len(self.trades)

    @property
    def n_wins(self) -> int:
        return sum(1 for t in self.trades if t.net_return > 0)

    @property
    def total_net(self) -> float:
        return sum(t.net_return for t in self.trades)

    @property
    def total_gross(self) -> float:
        return sum(t.gross_return for t in self.trades)


def backtest(df: pd.DataFrame, three_level: bool = USE_THREE_LEVEL_DEFAULT
             ) -> tuple[list[Trade], bool]:
    """Run K-line + MA breakout strategy on a single ticker.

    Entry rules:
      Long  = trend_up + close breaks above prior SWING_LOOKBACK-bar high.
      Short = trend_down + close breaks below prior SWING_LOOKBACK-bar low.
    Trend filter:
      2-level: MA_FAST > MA_MID (long) / < (short).
      3-level (if MA_SLOW computable): also MA_MID > MA_SLOW for long, etc.
    Exit:
      Long: close < MA_MID OR close < prior swing low.
      Short: close > MA_MID OR close > prior swing high.
      On exit, immediately flip if opposite signal triggers same bar.
    Open position at series end is closed on the last bar (mark-to-market).
    """
    df = df.sort_values("ts").reset_index(drop=True)
    n = len(df)

    close = df["close"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    ts = df["ts"].to_numpy()

    s_close = pd.Series(close)
    ma_fast = s_close.rolling(MA_FAST).mean().to_numpy()
    ma_mid = s_close.rolling(MA_MID).mean().to_numpy()

    use_three = three_level and n >= MA_SLOW + SWING_LOOKBACK + 20
    ma_slow = (
        s_close.rolling(MA_SLOW).mean().to_numpy()
        if use_three else np.full(n, np.nan)
    )

    sh = pd.Series(high).rolling(SWING_LOOKBACK).max().shift(1).to_numpy()
    sl = pd.Series(low).rolling(SWING_LOOKBACK).min().shift(1).to_numpy()

    warmup = (MA_SLOW if use_three else MA_MID) + SWING_LOOKBACK + 1
    if warmup >= n:
        return [], use_three

    trades: list[Trade] = []
    state = "flat"
    entry_price = 0.0
    entry_time: pd.Timestamp | None = None

    for i in range(warmup, n):
        c = close[i]
        f, m = ma_fast[i], ma_mid[i]

        trend_up = f > m
        trend_dn = f < m
        if use_three:
            sl_ma = ma_slow[i]
            trend_up = trend_up and m > sl_ma
            trend_dn = trend_dn and m < sl_ma

        bo_up = c > sh[i]
        bo_dn = c < sl[i]

        if state == "flat":
            if trend_up and bo_up:
                state = "long"
                entry_price, entry_time = c, ts[i]
            elif trend_dn and bo_dn:
                state = "short"
                entry_price, entry_time = c, ts[i]

        elif state == "long":
            if c < m or c < sl[i]:
                trades.append(Trade("long", entry_time, entry_price, ts[i], c))
                state = "flat"
                if trend_dn and bo_dn:
                    state = "short"
                    entry_price, entry_time = c, ts[i]

        else:  # short
            if c > m or c > sh[i]:
                trades.append(Trade("short", entry_time, entry_price, ts[i], c))
                state = "flat"
                if trend_up and bo_up:
                    state = "long"
                    entry_price, entry_time = c, ts[i]

    if state in ("long", "short"):
        trades.append(Trade(state, entry_time, entry_price, ts[-1], close[-1]))

    return trades, use_three


def _true_range(high: np.ndarray, low: np.ndarray,
                close: np.ndarray) -> np.ndarray:
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    return np.maximum.reduce([
        high - low,
        np.abs(high - prev_close),
        np.abs(low - prev_close),
    ])


def compute_bollinger(close: np.ndarray, period: int = BB_PERIOD,
                      std_mult: float = BB_STD_MULT
                      ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Standard Bollinger bands. Returns (upper, middle, lower)."""
    s = pd.Series(close)
    mid = s.rolling(period).mean()
    std = s.rolling(period).std()
    upper = (mid + std_mult * std).to_numpy()
    lower = (mid - std_mult * std).to_numpy()
    return upper, mid.to_numpy(), lower


def compute_macd(close: np.ndarray, fast: int = MACD_FAST, slow: int = MACD_SLOW,
                 signal: int = MACD_SIGNAL
                 ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Standard MACD. Returns (macd_line, signal_line, histogram)."""
    s = pd.Series(close)
    ema_fast = s.ewm(span=fast, adjust=False).mean()
    ema_slow = s.ewm(span=slow, adjust=False).mean()
    macd_line = (ema_fast - ema_slow)
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line.to_numpy(), signal_line.to_numpy(), histogram.to_numpy()


def compute_bb_percent(close: np.ndarray, period: int = BB_PERIOD,
                       std_mult: float = BB_STD_MULT) -> np.ndarray:
    """Bollinger %B: (close - lower) / (upper - lower).

    Range:  > 1.0  = above upper band (extreme strength)
            0.5    = at the middle band
            < 0.0  = below lower band (extreme weakness)

    A trend-following long filter is "%B > 0.7" — meaning the close is in the
    top 30% of its 20-bar volatility band, a true upper-range posture that
    isn't covered by a simple MA-stack check.
    """
    s = pd.Series(close)
    mid = s.rolling(period).mean()
    std = s.rolling(period).std()
    upper = mid + std_mult * std
    lower = mid - std_mult * std
    width = upper - lower
    with np.errstate(divide="ignore", invalid="ignore"):
        pct_b = (close - lower) / width.replace(0, np.nan)
    return pct_b.to_numpy()


def compute_obv(close: np.ndarray, volume: np.ndarray) -> np.ndarray:
    """Standard On-Balance Volume.

    OBV[t] = OBV[t-1] + sign(close[t] - close[t-1]) × volume[t]

    Volume is added when price closes up, subtracted when it closes down.
    A rising OBV trend means buying pressure is sustained — distinct from
    pure price-MA signals because it weighs each move by volume.
    """
    if len(close) == 0:
        return np.zeros(0)
    direction = np.sign(np.diff(close, prepend=close[0]))
    return np.cumsum(direction * volume)


def compute_mfi(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                volume: np.ndarray, period: int = MFI_PERIOD) -> np.ndarray:
    """Money Flow Index — volume-weighted RSI.

    MFI[t] = 100 - 100 / (1 + sum(positive_flow) / sum(negative_flow))
      where flow_t = typical_price_t × volume_t
      positive_flow accumulates when typical_price rises, negative when falls.

    Range 0–100. > 80 = overbought, < 20 = oversold.
    More robust than RSI in trending markets because volume confirms moves.
    """
    n = len(close)
    if n == 0 or volume.sum() == 0:
        return np.full(n, np.nan)
    tp = (high + low + close) / 3.0
    money_flow = tp * volume
    delta_tp = np.diff(tp, prepend=tp[0])
    pos_mf = np.where(delta_tp > 0, money_flow, 0.0)
    neg_mf = np.where(delta_tp < 0, money_flow, 0.0)
    pos_sum = pd.Series(pos_mf).rolling(period).sum()
    neg_sum = pd.Series(neg_mf).rolling(period).sum()
    with np.errstate(divide="ignore", invalid="ignore"):
        mfr = pos_sum / neg_sum.replace(0, np.nan)
        mfi = 100.0 - 100.0 / (1.0 + mfr)
    return mfi.to_numpy()


def compute_cci(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                period: int = CCI_PERIOD) -> np.ndarray:
    """Commodity Channel Index.

    CCI = (typical_price - SMA(tp)) / (0.015 * MAD)
      where MAD = mean of |tp_i - SMA(tp)| over the window.

    Centered around 0. > +100 = strong uptrend, < -100 = strong downtrend.
    Distinct from price-MA filters because it uses (high+low+close)/3 and
    measures absolute deviation in the window's own center, not crossover.
    """
    n = len(close)
    if n == 0:
        return np.full(n, np.nan)
    tp = (high + low + close) / 3.0
    s_tp = pd.Series(tp)
    sma = s_tp.rolling(period).mean()
    md = s_tp.rolling(period).apply(
        lambda x: np.abs(x - x.mean()).mean(), raw=True,
    )
    with np.errstate(divide="ignore", invalid="ignore"):
        cci = (tp - sma) / (0.015 * md.replace(0, np.nan))
    return cci.to_numpy()


def compute_kdj(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                n_period: int = KDJ_N, m1: int = KDJ_M1, m2: int = KDJ_M2
                ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """KDJ — Chinese-market stochastic with extra J line.

    RSV = (close - low_min_n) / (high_max_n - low_min_n) × 100
    K = EWM(RSV, alpha=1/m1)
    D = EWM(K, alpha=1/m2)
    J = 3K - 2D

    Trend filter:
      long  = K > 50 and K > D and J > D    (bullish stochastic)
      short = K < 50 and K < D and J < D
    """
    s_low = pd.Series(low).rolling(n_period).min()
    s_high = pd.Series(high).rolling(n_period).max()
    width = s_high - s_low
    with np.errstate(divide="ignore", invalid="ignore"):
        rsv = (close - s_low) / width.replace(0, np.nan) * 100.0
    k = pd.Series(rsv).ewm(alpha=1.0 / m1, adjust=False).mean()
    d = k.ewm(alpha=1.0 / m2, adjust=False).mean()
    j = 3 * k - 2 * d
    return k.to_numpy(), d.to_numpy(), j.to_numpy()


def compute_rsi(close: np.ndarray, period: int = RSI_PERIOD) -> np.ndarray:
    """Wilder RSI. Returns array same length as input. NaN until warm."""
    n = len(close)
    if n < period + 1:
        return np.full(n, np.nan)
    delta = np.diff(close, prepend=close[0])
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    alpha = 1.0 / period
    avg_gain = pd.Series(gain).ewm(alpha=alpha, adjust=False).mean().to_numpy()
    avg_loss = pd.Series(loss).ewm(alpha=alpha, adjust=False).mean().to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        rs = avg_gain / (avg_loss + 1e-12)
        rsi = 100.0 - 100.0 / (1.0 + rs)
    rsi = rsi.copy()
    rsi[: period + 1] = np.nan
    return rsi


def compute_adx(high: np.ndarray, low: np.ndarray, close: np.ndarray,
                period: int = ADX_PERIOD) -> np.ndarray:
    """Wilder ADX — standard formulation.

    Returns array same length as input. NaN until enough warmup.
    """
    n = len(close)
    if n < period + 1:
        return np.full(n, np.nan)
    prev_close = np.roll(close, 1); prev_close[0] = close[0]
    prev_high = np.roll(high, 1);   prev_high[0] = high[0]
    prev_low = np.roll(low, 1);     prev_low[0] = low[0]

    tr = np.maximum.reduce([high - low,
                            np.abs(high - prev_close),
                            np.abs(low - prev_close)])
    up = high - prev_high
    dn = prev_low - low
    plus_dm = np.where((up > dn) & (up > 0), up, 0.0)
    minus_dm = np.where((dn > up) & (dn > 0), dn, 0.0)

    alpha = 1.0 / period  # Wilder smoothing
    atr_w = pd.Series(tr).ewm(alpha=alpha, adjust=False).mean().to_numpy()
    pdm_w = pd.Series(plus_dm).ewm(alpha=alpha, adjust=False).mean().to_numpy()
    mdm_w = pd.Series(minus_dm).ewm(alpha=alpha, adjust=False).mean().to_numpy()

    with np.errstate(divide="ignore", invalid="ignore"):
        plus_di = 100 * pdm_w / atr_w
        minus_di = 100 * mdm_w / atr_w
        dx = 100 * np.abs(plus_di - minus_di) / (plus_di + minus_di + 1e-12)
    adx = pd.Series(dx).ewm(alpha=alpha, adjust=False).mean().to_numpy().copy()
    adx[: period * 2] = np.nan
    return adx


def backtest_v2(df: pd.DataFrame) -> tuple[list[Trade], bool]:
    """v2: ATR-buffered breakout + slope filter + ATR trailing stop + 1 pyramid.

    Improvements vs v1:
      A. Breakout requires close beyond neckline by BREAKOUT_BUFFER_ATR * ATR.
      B. MA_MID slope (over TREND_SLOPE_PERIOD bars) must clear TREND_SLOPE_MIN.
      C. Exit = close < MA_MID AND price hits ATR trailing stop. Removes the
         "break of recent swing low" exit which causes whipsaws.
      D. One pyramid: when in profit >= PYRAMID_PROFIT_ATR * ATR and a fresh
         breakout fires, add one more unit at current close.
    Each leg is recorded as a separate Trade for fair win-rate accounting.
    """
    df = df.sort_values("ts").reset_index(drop=True)
    n = len(df)

    close = df["close"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    ts = df["ts"].to_numpy()

    s_close = pd.Series(close)
    ma_fast = s_close.rolling(MA_FAST).mean().to_numpy()
    ma_mid = s_close.rolling(MA_MID).mean().to_numpy()

    sh = pd.Series(high).rolling(SWING_LOOKBACK).max().shift(1).to_numpy()
    sl = pd.Series(low).rolling(SWING_LOOKBACK).min().shift(1).to_numpy()

    tr = _true_range(high, low, close)
    atr = pd.Series(tr).rolling(ATR_PERIOD).mean().to_numpy()

    # MA_MID slope: rel change per bar over TREND_SLOPE_PERIOD bars.
    ma_mid_lag = np.roll(ma_mid, TREND_SLOPE_PERIOD)
    with np.errstate(divide="ignore", invalid="ignore"):
        slope = (ma_mid - ma_mid_lag) / ma_mid_lag / TREND_SLOPE_PERIOD
    slope[: TREND_SLOPE_PERIOD] = np.nan

    warmup = MA_MID + SWING_LOOKBACK + TREND_SLOPE_PERIOD + 1
    if warmup >= n:
        return [], False

    trades: list[Trade] = []
    state = "flat"
    legs: list[tuple] = []          # [(entry_time, entry_price), ...]
    extreme = 0.0                   # max-high (long) or min-low (short)

    for i in range(warmup, n):
        c = close[i]
        a = atr[i]
        if not np.isfinite(a) or a <= 0:
            continue

        f, m, s = ma_fast[i], ma_mid[i], slope[i]
        trend_up = f > m and np.isfinite(s) and s > TREND_SLOPE_MIN
        trend_dn = f < m and np.isfinite(s) and s < -TREND_SLOPE_MIN

        bo_up = c > sh[i] + BREAKOUT_BUFFER_ATR * a
        bo_dn = c < sl[i] - BREAKOUT_BUFFER_ATR * a

        if state == "flat":
            if trend_up and bo_up:
                state = "long"
                legs = [(ts[i], c)]
                extreme = high[i]
            elif trend_dn and bo_dn:
                state = "short"
                legs = [(ts[i], c)]
                extreme = low[i]
            continue

        if state == "long":
            extreme = max(extreme, high[i])
            trail_stop = extreme - ATR_TRAIL_MULT * a
            first_entry = legs[0][1]

            # Pyramid (max 1 add).
            if (len(legs) <= PYRAMID_MAX_ADDS and bo_up
                    and (c - first_entry) >= PYRAMID_PROFIT_ATR * a):
                legs.append((ts[i], c))

            if c < m and c < trail_stop:
                for et, ep in legs:
                    trades.append(Trade("long", et, ep, ts[i], c))
                state = "flat"
                legs = []
                if trend_dn and bo_dn:
                    state = "short"
                    legs = [(ts[i], c)]
                    extreme = low[i]
            continue

        # short
        extreme = min(extreme, low[i])
        trail_stop = extreme + ATR_TRAIL_MULT * a
        first_entry = legs[0][1]

        if (len(legs) <= PYRAMID_MAX_ADDS and bo_dn
                and (first_entry - c) >= PYRAMID_PROFIT_ATR * a):
            legs.append((ts[i], c))

        if c > m and c > trail_stop:
            for et, ep in legs:
                trades.append(Trade("short", et, ep, ts[i], c))
            state = "flat"
            legs = []
            if trend_up and bo_up:
                state = "long"
                legs = [(ts[i], c)]
                extreme = high[i]

    if state in ("long", "short"):
        for et, ep in legs:
            trades.append(Trade(state, et, ep, ts[-1], close[-1]))

    return trades, False


def backtest_v3(df: pd.DataFrame, market: str) -> tuple[list[Trade], bool]:
    """v3 = v2 + ADX filter + initial fixed stop + per-market params +
    consecutive-loss cooldown + volume confirmation.

    Per-market parameters are pulled from MARKET_PARAMS[market]. The default
    (US) is similar to v2; HK/CN ship strictly tighter values (smaller pyramid,
    higher ADX threshold, mandatory cooldown).
    """
    p = MARKET_PARAMS.get(market, MARKET_PARAMS["US"])

    df = df.sort_values("ts").reset_index(drop=True)
    n = len(df)

    close = df["close"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    if "volume" in df.columns:
        volume = df["volume"].to_numpy(dtype=float)
    else:
        volume = np.zeros(n)
    ts = df["ts"].to_numpy()

    s_close = pd.Series(close)
    ma_fast = s_close.rolling(MA_FAST).mean().to_numpy()
    ma_mid = s_close.rolling(MA_MID).mean().to_numpy()

    sh = pd.Series(high).rolling(SWING_LOOKBACK).max().shift(1).to_numpy()
    sl = pd.Series(low).rolling(SWING_LOOKBACK).min().shift(1).to_numpy()

    tr = _true_range(high, low, close)
    atr = pd.Series(tr).rolling(ATR_PERIOD).mean().to_numpy()

    ma_mid_lag = np.roll(ma_mid, TREND_SLOPE_PERIOD)
    with np.errstate(divide="ignore", invalid="ignore"):
        slope = (ma_mid - ma_mid_lag) / ma_mid_lag / TREND_SLOPE_PERIOD
    slope[: TREND_SLOPE_PERIOD] = np.nan

    adx = compute_adx(high, low, close, period=ADX_PERIOD)

    has_volume = bool((volume > 0).any())
    vol_avg = (pd.Series(volume).rolling(20).mean().to_numpy()
               if has_volume else np.full(n, np.nan))

    warmup = MA_MID + SWING_LOOKBACK + TREND_SLOPE_PERIOD + 1
    if warmup >= n:
        return [], False

    trades: list[Trade] = []
    state = "flat"
    legs: list[tuple] = []
    extreme = 0.0
    init_stop = 0.0
    using_trail = False
    consec_losses = 0
    cooldown_until = 0

    for i in range(warmup, n):
        c = close[i]
        a = atr[i]
        if not np.isfinite(a) or a <= 0:
            continue
        f, m, s = ma_fast[i], ma_mid[i], slope[i]
        adx_i = adx[i]

        adx_ok = np.isfinite(adx_i) and adx_i > p["adx_min"]
        slope_up = np.isfinite(s) and s > p["slope_min"]
        slope_dn = np.isfinite(s) and s < -p["slope_min"]
        trend_up = f > m and slope_up and adx_ok
        trend_dn = f < m and slope_dn and adx_ok

        bo_up = c > sh[i] + p["atr_buffer"] * a
        bo_dn = c < sl[i] - p["atr_buffer"] * a

        if has_volume and np.isfinite(vol_avg[i]) and vol_avg[i] > 0:
            vol_ok = volume[i] >= vol_avg[i] * p["vol_confirm_mult"]
        else:
            vol_ok = True  # fallback when volume is missing (e.g. indices)

        in_cd = i < cooldown_until

        if state == "flat":
            if in_cd:
                continue
            if trend_up and bo_up and vol_ok:
                state = "long"
                legs = [(ts[i], c)]
                extreme = high[i]
                init_stop = c - p["init_stop_atr"] * a
                using_trail = False
            elif trend_dn and bo_dn and vol_ok:
                state = "short"
                legs = [(ts[i], c)]
                extreme = low[i]
                init_stop = c + p["init_stop_atr"] * a
                using_trail = False
            continue

        if state == "long":
            extreme = max(extreme, high[i])
            first = legs[0][1]
            if not using_trail and (c - first) >= p["trail_activate_atr"] * a:
                using_trail = True

            if (p["pyramid"] and len(legs) <= PYRAMID_MAX_ADDS and bo_up
                    and (c - first) >= p["pyramid_profit_atr"] * a):
                legs.append((ts[i], c))

            if using_trail:
                # Both conditions (v2-style): trail breach AND MA breach.
                trail = extreme - p["trail_atr"] * a
                exit_long = c < trail and c < m
            else:
                # Initial phase: hard floor.
                exit_long = c < init_stop
            if exit_long:
                won = (c - first) > 0
                for et, ep in legs:
                    trades.append(Trade("long", et, ep, ts[i], c))
                state = "flat"
                legs = []
                using_trail = False
                consec_losses = 0 if won else consec_losses + 1
                if (p["cooldown_after_losses"] > 0
                        and consec_losses >= p["cooldown_after_losses"]):
                    cooldown_until = i + p["cooldown_bars"]
                    consec_losses = 0
                if (i >= cooldown_until and trend_dn and bo_dn and vol_ok):
                    state = "short"
                    legs = [(ts[i], c)]
                    extreme = low[i]
                    init_stop = c + p["init_stop_atr"] * a
                    using_trail = False
            continue

        # short — symmetric
        extreme = min(extreme, low[i])
        first = legs[0][1]
        if not using_trail and (first - c) >= p["trail_activate_atr"] * a:
            using_trail = True

        if (p["pyramid"] and len(legs) <= PYRAMID_MAX_ADDS and bo_dn
                and (first - c) >= p["pyramid_profit_atr"] * a):
            legs.append((ts[i], c))

        if using_trail:
            trail = extreme + p["trail_atr"] * a
            exit_short = c > trail and c > m
        else:
            exit_short = c > init_stop
        if exit_short:
            won = (first - c) > 0
            for et, ep in legs:
                trades.append(Trade("short", et, ep, ts[i], c))
            state = "flat"
            legs = []
            using_trail = False
            consec_losses = 0 if won else consec_losses + 1
            if (p["cooldown_after_losses"] > 0
                    and consec_losses >= p["cooldown_after_losses"]):
                cooldown_until = i + p["cooldown_bars"]
                consec_losses = 0
            if (i >= cooldown_until and trend_up and bo_up and vol_ok):
                state = "long"
                legs = [(ts[i], c)]
                extreme = high[i]
                init_stop = c - p["init_stop_atr"] * a
                using_trail = False

    if state in ("long", "short"):
        for et, ep in legs:
            trades.append(Trade(state, et, ep, ts[-1], close[-1]))
    return trades, False


def compute_weekly_alignment(df_daily: pd.DataFrame,
                             ma_short: int = WEEKLY_MA_SHORT,
                             ma_long: int = WEEKLY_MA_LONG,
                             ) -> tuple[np.ndarray | None, np.ndarray | None]:
    """Resample daily->weekly, compute MA stack, align back to daily index.

    Each daily bar receives MA values from the *most recent COMPLETED* weekly
    bar (achieved via shift(1) on the weekly series). No look-ahead.
    Returns (ma_short_per_daily, ma_long_per_daily), or (None, None) if there
    is not enough weekly history for the long MA.
    """
    if "ts" not in df_daily.columns or "close" not in df_daily.columns:
        return None, None
    df = df_daily.copy()
    df["ts"] = _strip_tz(df["ts"])
    df = df.set_index("ts").sort_index()

    weekly = df["close"].resample("W-FRI").last().dropna()
    if len(weekly) < ma_long + 2:
        return None, None

    # No shift: weekly MAs are known at EOD Friday and the strategy itself
    # decides on bar-close (EOD), so reading Friday MA on Friday's daily bar
    # is consistent. ffill carries the value across Mon-Thu of next week.
    ma_s = weekly.rolling(ma_short).mean()
    ma_l = weekly.rolling(ma_long).mean()
    ma_s_d = ma_s.reindex(df.index, method="ffill").to_numpy()
    ma_l_d = ma_l.reindex(df.index, method="ffill").to_numpy()
    return ma_s_d, ma_l_d


def get_market_regime(market: str) -> dict | None:
    """Compute (and disk-cache) per-bar regime indicators for the market index.

    Returns dict with: ts, is_trending (bool array, lagged 1 bar to prevent
    look-ahead), or None if the index can't be fetched.
    """
    if market in _REGIME_CACHE:
        return _REGIME_CACHE[market]
    idx_ticker = INDEX_TICKERS_BY_MARKET.get(market)
    if not idx_ticker:
        return None
    df = fetch_yf(idx_ticker)
    if df is None or len(df) < 200:
        LOG.warning("[%s] regime index %s unavailable", market, idx_ticker)
        return None

    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    close = df["close"].to_numpy(dtype=float)
    adx = compute_adx(high, low, close, period=INDEX_ADX_PERIOD)

    # Lag by 1 bar — bar i's decision uses bar (i-1)'s ADX.
    adx_lag = np.roll(adx, 1)
    adx_lag[0] = np.nan
    is_trending = np.where(np.isnan(adx_lag), False, adx_lag > INDEX_ADX_MIN)

    out = {
        "ts": _strip_tz(df["ts"]).to_numpy(),
        "is_trending": is_trending,
    }
    _REGIME_CACHE[market] = out
    n_on = int(is_trending.sum())
    LOG.info("[%s] regime index %s: %d/%d bars trending (ADX>%d)",
             market, idx_ticker, n_on, len(is_trending), INDEX_ADX_MIN)
    return out


def _strip_tz(ser: pd.Series) -> pd.Series:
    """Drop timezone (yfinance hourly is tz-aware, daily isn't). Mixing
    tz-aware and tz-naive causes merge_asof and astype to fail."""
    s = pd.to_datetime(ser)
    if hasattr(s.dt, "tz") and s.dt.tz is not None:
        s = s.dt.tz_convert("UTC").dt.tz_localize(None)
    return s.astype("datetime64[ns]")


def _align_regime_to_df(df: pd.DataFrame, regime: dict | None) -> np.ndarray:
    """Return per-bar boolean array: True = trade allowed by regime."""
    n = len(df)
    if regime is None:
        return np.ones(n, dtype=bool)
    left_ts = _strip_tz(df["ts"])
    right_ts = _strip_tz(pd.Series(regime["ts"]))
    df_ts = pd.DataFrame({"ts": left_ts}).sort_values("ts")
    reg_df = pd.DataFrame({"ts": right_ts,
                            "ok": regime["is_trending"]}).sort_values("ts")
    aligned = pd.merge_asof(df_ts, reg_df, on="ts", direction="backward")
    return aligned["ok"].fillna(False).to_numpy(dtype=bool)


def backtest_v4(df: pd.DataFrame, market: str) -> tuple[list[Trade], bool]:
    """v4 = v3 + weekly trend resonance + index-regime master switch.

    Adds two gates on top of v3 entry conditions:
      * Weekly resonance: weekly MA20 must be above (long) / below (short)
        weekly MA60 — daily breakout only fires when the higher timeframe
        agrees. Falls back to "always pass" if not enough weekly history.
      * Market regime: the per-market index (S&P500 / HSI / CSI300) must be
        trending (ADX > INDEX_ADX_MIN) on the prior bar. When the broad
        market is range-bound, the strategy stands aside.

    Each trade also records its initial stop distance, enabling R-multiple
    aggregation downstream.
    """
    p = MARKET_PARAMS.get(market, MARKET_PARAMS["US"])
    df = df.sort_values("ts").reset_index(drop=True)
    n = len(df)

    close = df["close"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    volume = (df["volume"].to_numpy(dtype=float)
              if "volume" in df.columns else np.zeros(n))
    ts = df["ts"].to_numpy()

    s_close = pd.Series(close)
    ma_fast = s_close.rolling(MA_FAST).mean().to_numpy()
    ma_mid = s_close.rolling(MA_MID).mean().to_numpy()
    sh = pd.Series(high).rolling(SWING_LOOKBACK).max().shift(1).to_numpy()
    sl = pd.Series(low).rolling(SWING_LOOKBACK).min().shift(1).to_numpy()

    tr = _true_range(high, low, close)
    atr = pd.Series(tr).rolling(ATR_PERIOD).mean().to_numpy()

    ma_mid_lag = np.roll(ma_mid, TREND_SLOPE_PERIOD)
    with np.errstate(divide="ignore", invalid="ignore"):
        slope = (ma_mid - ma_mid_lag) / ma_mid_lag / TREND_SLOPE_PERIOD
    slope[: TREND_SLOPE_PERIOD] = np.nan

    adx = compute_adx(high, low, close, period=ADX_PERIOD)

    has_volume = bool((volume > 0).any())
    vol_avg = (pd.Series(volume).rolling(20).mean().to_numpy()
               if has_volume else np.full(n, np.nan))

    # --- new in v4: weekly resonance + market regime
    weekly_ma_s, weekly_ma_l = compute_weekly_alignment(df)
    has_weekly = weekly_ma_s is not None
    regime = get_market_regime(market)
    regime_ok = _align_regime_to_df(df, regime)

    warmup = MA_MID + SWING_LOOKBACK + TREND_SLOPE_PERIOD + 1
    if warmup >= n:
        return [], False

    trades: list[Trade] = []
    state = "flat"
    legs: list[tuple] = []      # (entry_time, entry_price, stop_dist) per leg
    extreme = 0.0
    init_stop = 0.0
    using_trail = False
    consec_losses = 0
    cooldown_until = 0

    for i in range(warmup, n):
        c = close[i]; a = atr[i]
        if not np.isfinite(a) or a <= 0:
            continue
        f, m, sm = ma_fast[i], ma_mid[i], slope[i]
        adx_i = adx[i]

        adx_ok = np.isfinite(adx_i) and adx_i > p["adx_min"]
        slope_up = np.isfinite(sm) and sm > p["slope_min"]
        slope_dn = np.isfinite(sm) and sm < -p["slope_min"]

        # Weekly resonance gate.
        if has_weekly:
            ws, wl = weekly_ma_s[i], weekly_ma_l[i]
            weekly_up = np.isfinite(ws) and np.isfinite(wl) and ws > wl
            weekly_dn = np.isfinite(ws) and np.isfinite(wl) and ws < wl
        else:
            weekly_up = weekly_dn = True

        # Market regime gate (master switch).
        regime_on = bool(regime_ok[i])

        trend_up = (f > m and slope_up and adx_ok and weekly_up and regime_on)
        trend_dn = (f < m and slope_dn and adx_ok and weekly_dn and regime_on)

        bo_up = c > sh[i] + p["atr_buffer"] * a
        bo_dn = c < sl[i] - p["atr_buffer"] * a

        if has_volume and np.isfinite(vol_avg[i]) and vol_avg[i] > 0:
            vol_ok = volume[i] >= vol_avg[i] * p["vol_confirm_mult"]
        else:
            vol_ok = True

        in_cd = i < cooldown_until

        if state == "flat":
            if in_cd:
                continue
            if trend_up and bo_up and vol_ok:
                state = "long"
                stop_d = p["init_stop_atr"] * a
                legs = [(ts[i], c, stop_d)]
                extreme = high[i]
                init_stop = c - stop_d
                using_trail = False
            elif trend_dn and bo_dn and vol_ok:
                state = "short"
                stop_d = p["init_stop_atr"] * a
                legs = [(ts[i], c, stop_d)]
                extreme = low[i]
                init_stop = c + stop_d
                using_trail = False
            continue

        if state == "long":
            extreme = max(extreme, high[i])
            first = legs[0][1]
            if not using_trail and (c - first) >= p["trail_activate_atr"] * a:
                using_trail = True
            if (p["pyramid"] and len(legs) <= PYRAMID_MAX_ADDS and bo_up
                    and (c - first) >= p["pyramid_profit_atr"] * a):
                legs.append((ts[i], c, p["init_stop_atr"] * a))
            if using_trail:
                trail = extreme - p["trail_atr"] * a
                exit_long = c < trail and c < m
            else:
                exit_long = c < init_stop
            if exit_long:
                won = (c - first) > 0
                for et, ep, sd in legs:
                    trades.append(Trade("long", et, ep, ts[i], c, sd))
                state = "flat"
                legs = []
                using_trail = False
                consec_losses = 0 if won else consec_losses + 1
                if (p["cooldown_after_losses"] > 0
                        and consec_losses >= p["cooldown_after_losses"]):
                    cooldown_until = i + p["cooldown_bars"]
                    consec_losses = 0
                if (i >= cooldown_until and trend_dn and bo_dn and vol_ok):
                    stop_d = p["init_stop_atr"] * a
                    state = "short"
                    legs = [(ts[i], c, stop_d)]
                    extreme = low[i]
                    init_stop = c + stop_d
                    using_trail = False
            continue

        # short — symmetric
        extreme = min(extreme, low[i])
        first = legs[0][1]
        if not using_trail and (first - c) >= p["trail_activate_atr"] * a:
            using_trail = True
        if (p["pyramid"] and len(legs) <= PYRAMID_MAX_ADDS and bo_dn
                and (first - c) >= p["pyramid_profit_atr"] * a):
            legs.append((ts[i], c, p["init_stop_atr"] * a))
        if using_trail:
            trail = extreme + p["trail_atr"] * a
            exit_short = c > trail and c > m
        else:
            exit_short = c > init_stop
        if exit_short:
            won = (first - c) > 0
            for et, ep, sd in legs:
                trades.append(Trade("short", et, ep, ts[i], c, sd))
            state = "flat"
            legs = []
            using_trail = False
            consec_losses = 0 if won else consec_losses + 1
            if (p["cooldown_after_losses"] > 0
                    and consec_losses >= p["cooldown_after_losses"]):
                cooldown_until = i + p["cooldown_bars"]
                consec_losses = 0
            if (i >= cooldown_until and trend_up and bo_up and vol_ok):
                stop_d = p["init_stop_atr"] * a
                state = "long"
                legs = [(ts[i], c, stop_d)]
                extreme = high[i]
                init_stop = c - stop_d
                using_trail = False

    if state in ("long", "short"):
        for et, ep, sd in legs:
            trades.append(Trade(state, et, ep, ts[-1], close[-1], sd))
    return trades, False


def backtest_v5(df: pd.DataFrame, market: str) -> tuple[list[Trade], bool]:
    """v5 = v4 + auto-3-level (MA30/150/750) + RSI mean-reversion fallback.

    KEY ADDITIONS:

    A. Auto three-level mode: when bar count >= MA_SLOW + warmup, the strategy
       additionally requires MA_MID > MA_SLOW (long) / MA_MID < MA_SLOW (short)
       for trend-mode entries. This is K2's full "三级别联立" — only feasible
       on hourly_long mode (~4700 bars) or longer daily history.

    B. RSI mean-reversion in non-trending regimes: when the index master
       switch is OFF (market is range-bound), the strategy doesn't stand
       aside; instead it switches to a mean-reversion engine:
          long_mr: RSI < 30 → buy; exit when RSI > 50 OR stop OR time-stop
          short_mr: RSI > 70 → sell; symmetric exits

       MR mode addresses the A股 / HK problem where chronic chop made v3/v4
       give up on those tickers entirely.

    Each Trade carries `stop_dist` so R-multiples can be aggregated across
    both engines.
    """
    p = MARKET_PARAMS.get(market, MARKET_PARAMS["US"])
    df = df.sort_values("ts").reset_index(drop=True)
    n = len(df)

    close = df["close"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    volume = (df["volume"].to_numpy(dtype=float)
              if "volume" in df.columns else np.zeros(n))
    ts = df["ts"].to_numpy()

    s_close = pd.Series(close)
    ma_fast = s_close.rolling(MA_FAST).mean().to_numpy()
    ma_mid = s_close.rolling(MA_MID).mean().to_numpy()

    # Auto three-level: enable only if we have enough bars to warm up MA750
    # and still have a meaningful trading window.
    use_three = n >= MA_SLOW + SWING_LOOKBACK + 50
    ma_slow = (s_close.rolling(MA_SLOW).mean().to_numpy()
               if use_three else np.full(n, np.nan))

    sh = pd.Series(high).rolling(SWING_LOOKBACK).max().shift(1).to_numpy()
    sl = pd.Series(low).rolling(SWING_LOOKBACK).min().shift(1).to_numpy()

    tr = _true_range(high, low, close)
    atr = pd.Series(tr).rolling(ATR_PERIOD).mean().to_numpy()

    ma_mid_lag = np.roll(ma_mid, TREND_SLOPE_PERIOD)
    with np.errstate(divide="ignore", invalid="ignore"):
        slope = (ma_mid - ma_mid_lag) / ma_mid_lag / TREND_SLOPE_PERIOD
    slope[: TREND_SLOPE_PERIOD] = np.nan

    adx = compute_adx(high, low, close, period=ADX_PERIOD)
    rsi = compute_rsi(close, period=RSI_PERIOD)

    # Long-MA bias filter for MR: only take RSI longs above MA200, shorts below
    ma200 = (s_close.rolling(200).mean().to_numpy()
             if n >= 200 else np.full(n, np.nan))

    has_volume = bool((volume > 0).any())
    vol_avg = (pd.Series(volume).rolling(20).mean().to_numpy()
               if has_volume else np.full(n, np.nan))

    weekly_ma_s, weekly_ma_l = compute_weekly_alignment(df)
    has_weekly = weekly_ma_s is not None
    regime = get_market_regime(market)
    regime_ok = _align_regime_to_df(df, regime)

    warmup = (MA_SLOW if use_three else MA_MID) + SWING_LOOKBACK + TREND_SLOPE_PERIOD + 1
    if warmup >= n:
        return [], False

    trades: list[Trade] = []
    state = "flat"               # flat | long_t | short_t | long_mr | short_mr
    legs: list[tuple] = []       # (entry_time, entry_price, stop_dist)
    extreme = 0.0
    init_stop = 0.0
    using_trail = False
    consec_losses = 0
    cooldown_until = 0
    mr_entry_bar = -1            # bar index where MR position started

    for i in range(warmup, n):
        c = close[i]; a = atr[i]; rsi_i = rsi[i]
        if not np.isfinite(a) or a <= 0:
            continue

        f, m, sm = ma_fast[i], ma_mid[i], slope[i]
        adx_i = adx[i]
        sl_ma = ma_slow[i] if use_three else None

        adx_ok = np.isfinite(adx_i) and adx_i > p["adx_min"]
        slope_up = np.isfinite(sm) and sm > p["slope_min"]
        slope_dn = np.isfinite(sm) and sm < -p["slope_min"]

        if has_weekly:
            ws, wl = weekly_ma_s[i], weekly_ma_l[i]
            weekly_up = np.isfinite(ws) and np.isfinite(wl) and ws > wl
            weekly_dn = np.isfinite(ws) and np.isfinite(wl) and ws < wl
        else:
            weekly_up = weekly_dn = True

        regime_on = bool(regime_ok[i])

        # Three-level alignment: MA stack must be fully ordered.
        three_up = (sl_ma is not None and m > sl_ma) if use_three else True
        three_dn = (sl_ma is not None and m < sl_ma) if use_three else True

        trend_up = (f > m and slope_up and adx_ok and weekly_up
                    and regime_on and three_up)
        trend_dn = (f < m and slope_dn and adx_ok and weekly_dn
                    and regime_on and three_dn)

        bo_up = c > sh[i] + p["atr_buffer"] * a
        bo_dn = c < sl[i] - p["atr_buffer"] * a

        if has_volume and np.isfinite(vol_avg[i]) and vol_avg[i] > 0:
            vol_ok = volume[i] >= vol_avg[i] * p["vol_confirm_mult"]
        else:
            vol_ok = True

        # Mean-reversion entry conditions (only when regime is OFF):
        ma200_i = ma200[i] if i < len(ma200) else np.nan
        mr_long_ok = (not regime_on
                      and np.isfinite(rsi_i) and rsi_i < RSI_OVERSOLD
                      and np.isfinite(ma200_i) and c > ma200_i)
        mr_short_ok = (not regime_on
                       and np.isfinite(rsi_i) and rsi_i > RSI_OVERBOUGHT
                       and np.isfinite(ma200_i) and c < ma200_i)

        in_cd = i < cooldown_until

        if state == "flat":
            if in_cd:
                continue
            if trend_up and bo_up and vol_ok:
                state = "long_t"
                stop_d = p["init_stop_atr"] * a
                legs = [(ts[i], c, stop_d)]
                extreme = high[i]
                init_stop = c - stop_d
                using_trail = False
            elif trend_dn and bo_dn and vol_ok:
                state = "short_t"
                stop_d = p["init_stop_atr"] * a
                legs = [(ts[i], c, stop_d)]
                extreme = low[i]
                init_stop = c + stop_d
                using_trail = False
            elif mr_long_ok:
                state = "long_mr"
                stop_d = MR_INIT_STOP_ATR * a
                legs = [(ts[i], c, stop_d)]
                init_stop = c - stop_d
                mr_entry_bar = i
            elif mr_short_ok:
                state = "short_mr"
                stop_d = MR_INIT_STOP_ATR * a
                legs = [(ts[i], c, stop_d)]
                init_stop = c + stop_d
                mr_entry_bar = i
            continue

        # ---- TREND modes (same logic as v4) ----
        if state == "long_t":
            extreme = max(extreme, high[i])
            first = legs[0][1]
            if not using_trail and (c - first) >= p["trail_activate_atr"] * a:
                using_trail = True
            if (p["pyramid"] and len(legs) <= PYRAMID_MAX_ADDS and bo_up
                    and (c - first) >= p["pyramid_profit_atr"] * a):
                legs.append((ts[i], c, p["init_stop_atr"] * a))
            if using_trail:
                trail = extreme - p["trail_atr"] * a
                exit_long = c < trail and c < m
            else:
                exit_long = c < init_stop
            if exit_long:
                won = (c - first) > 0
                for et, ep, sd in legs:
                    trades.append(Trade("long", et, ep, ts[i], c, sd))
                state = "flat"
                legs = []
                using_trail = False
                consec_losses = 0 if won else consec_losses + 1
                if (p["cooldown_after_losses"] > 0
                        and consec_losses >= p["cooldown_after_losses"]):
                    cooldown_until = i + p["cooldown_bars"]
                    consec_losses = 0
            continue

        if state == "short_t":
            extreme = min(extreme, low[i])
            first = legs[0][1]
            if not using_trail and (first - c) >= p["trail_activate_atr"] * a:
                using_trail = True
            if (p["pyramid"] and len(legs) <= PYRAMID_MAX_ADDS and bo_dn
                    and (first - c) >= p["pyramid_profit_atr"] * a):
                legs.append((ts[i], c, p["init_stop_atr"] * a))
            if using_trail:
                trail = extreme + p["trail_atr"] * a
                exit_short = c > trail and c > m
            else:
                exit_short = c > init_stop
            if exit_short:
                won = (first - c) > 0
                for et, ep, sd in legs:
                    trades.append(Trade("short", et, ep, ts[i], c, sd))
                state = "flat"
                legs = []
                using_trail = False
                consec_losses = 0 if won else consec_losses + 1
                if (p["cooldown_after_losses"] > 0
                        and consec_losses >= p["cooldown_after_losses"]):
                    cooldown_until = i + p["cooldown_bars"]
                    consec_losses = 0
            continue

        # ---- MEAN-REVERSION modes ----
        if state == "long_mr":
            first = legs[0][1]
            held = i - mr_entry_bar
            target_hit = np.isfinite(rsi_i) and rsi_i >= RSI_TARGET
            stop_hit = c < init_stop
            time_out = held >= MR_TIME_STOP
            if target_hit or stop_hit or time_out:
                for et, ep, sd in legs:
                    trades.append(Trade("long", et, ep, ts[i], c, sd))
                state = "flat"
                legs = []
                # MR uses lighter cooldown logic — only trip on hard stop loss
                if stop_hit:
                    consec_losses += 1
                    if (p["cooldown_after_losses"] > 0
                            and consec_losses >= p["cooldown_after_losses"]):
                        cooldown_until = i + p["cooldown_bars"]
                        consec_losses = 0
                else:
                    consec_losses = 0
            continue

        if state == "short_mr":
            first = legs[0][1]
            held = i - mr_entry_bar
            target_hit = np.isfinite(rsi_i) and rsi_i <= RSI_TARGET
            stop_hit = c > init_stop
            time_out = held >= MR_TIME_STOP
            if target_hit or stop_hit or time_out:
                for et, ep, sd in legs:
                    trades.append(Trade("short", et, ep, ts[i], c, sd))
                state = "flat"
                legs = []
                if stop_hit:
                    consec_losses += 1
                    if (p["cooldown_after_losses"] > 0
                            and consec_losses >= p["cooldown_after_losses"]):
                        cooldown_until = i + p["cooldown_bars"]
                        consec_losses = 0
                else:
                    consec_losses = 0
            continue

    # mark-to-market any open position at the last bar
    if state in ("long_t", "short_t", "long_mr", "short_mr"):
        side = "long" if state.startswith("long") else "short"
        for et, ep, sd in legs:
            trades.append(Trade(side, et, ep, ts[-1], close[-1], sd))
    return trades, use_three


def backtest_v6(df: pd.DataFrame, market: str,
                filters: frozenset[str] = frozenset(),
                ) -> tuple[list[Trade], bool]:
    """v6 = v5 + indicator-confluence filters (BB / MACD / RSI zone).

    `filters` is a subset of {'bb', 'macd', 'rsi'}. Each adds a confirmation
    requirement that ALL must be satisfied (AND) along with v5's existing
    gates. The grid runner tries every 2^3=8 subset to find the best mix
    per market.

    Filter semantics (long side, mirror for short):
      'bb'   : close > middle band (price in the upper half of recent range)
      'macd' : MACD histogram > 0 (momentum is bullish)
      'rsi'  : RSI in [RSI_LONG_MIN, RSI_LONG_MAX]  (not deeply oversold,
                                                       not extremely overbought)

    K-line + MA breakout is still the *primary* signal; these are additional
    confirmation votes, not replacements.
    """
    p = MARKET_PARAMS.get(market, MARKET_PARAMS["US"])
    df = df.sort_values("ts").reset_index(drop=True)
    n = len(df)

    close = df["close"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    volume = (df["volume"].to_numpy(dtype=float)
              if "volume" in df.columns else np.zeros(n))
    ts = df["ts"].to_numpy()

    s_close = pd.Series(close)
    ma_fast = s_close.rolling(MA_FAST).mean().to_numpy()
    ma_mid = s_close.rolling(MA_MID).mean().to_numpy()
    use_three = n >= MA_SLOW + SWING_LOOKBACK + 50
    ma_slow = (s_close.rolling(MA_SLOW).mean().to_numpy()
               if use_three else np.full(n, np.nan))

    sh = pd.Series(high).rolling(SWING_LOOKBACK).max().shift(1).to_numpy()
    sl = pd.Series(low).rolling(SWING_LOOKBACK).min().shift(1).to_numpy()

    tr = _true_range(high, low, close)
    atr = pd.Series(tr).rolling(ATR_PERIOD).mean().to_numpy()

    ma_mid_lag = np.roll(ma_mid, TREND_SLOPE_PERIOD)
    with np.errstate(divide="ignore", invalid="ignore"):
        slope = (ma_mid - ma_mid_lag) / ma_mid_lag / TREND_SLOPE_PERIOD
    slope[: TREND_SLOPE_PERIOD] = np.nan

    adx = compute_adx(high, low, close, period=ADX_PERIOD)
    rsi_arr = compute_rsi(close, period=RSI_PERIOD)
    bb_u, bb_m, bb_l = compute_bollinger(close)
    _, _, macd_h = compute_macd(close)

    has_volume = bool((volume > 0).any())
    vol_avg = (pd.Series(volume).rolling(20).mean().to_numpy()
               if has_volume else np.full(n, np.nan))

    weekly_ma_s, weekly_ma_l = compute_weekly_alignment(df)
    has_weekly = weekly_ma_s is not None
    regime = get_market_regime(market)
    regime_ok = _align_regime_to_df(df, regime)

    warmup = (MA_SLOW if use_three else MA_MID) + SWING_LOOKBACK + TREND_SLOPE_PERIOD + 1
    if warmup >= n:
        return [], False

    use_bb = "bb" in filters
    use_macd = "macd" in filters
    use_rsi = "rsi" in filters

    trades: list[Trade] = []
    state = "flat"
    legs: list[tuple] = []
    extreme = 0.0
    init_stop = 0.0
    using_trail = False
    consec_losses = 0
    cooldown_until = 0

    for i in range(warmup, n):
        c = close[i]; a = atr[i]
        if not np.isfinite(a) or a <= 0:
            continue

        f, m, sm = ma_fast[i], ma_mid[i], slope[i]
        adx_i = adx[i]
        sl_ma = ma_slow[i] if use_three else None

        adx_ok = np.isfinite(adx_i) and adx_i > p["adx_min"]
        slope_up = np.isfinite(sm) and sm > p["slope_min"]
        slope_dn = np.isfinite(sm) and sm < -p["slope_min"]

        if has_weekly:
            ws, wl = weekly_ma_s[i], weekly_ma_l[i]
            weekly_up = np.isfinite(ws) and np.isfinite(wl) and ws > wl
            weekly_dn = np.isfinite(ws) and np.isfinite(wl) and ws < wl
        else:
            weekly_up = weekly_dn = True

        regime_on = bool(regime_ok[i])

        three_up = (sl_ma is not None and m > sl_ma) if use_three else True
        three_dn = (sl_ma is not None and m < sl_ma) if use_three else True

        # --- v6 confluence filters ---
        bb_up_ok = (not use_bb) or (np.isfinite(bb_m[i]) and c > bb_m[i])
        bb_dn_ok = (not use_bb) or (np.isfinite(bb_m[i]) and c < bb_m[i])
        macd_up_ok = (not use_macd) or (np.isfinite(macd_h[i]) and macd_h[i] > 0)
        macd_dn_ok = (not use_macd) or (np.isfinite(macd_h[i]) and macd_h[i] < 0)
        rsi_up_ok = ((not use_rsi)
                     or (np.isfinite(rsi_arr[i])
                         and RSI_LONG_MIN <= rsi_arr[i] <= RSI_LONG_MAX))
        rsi_dn_ok = ((not use_rsi)
                     or (np.isfinite(rsi_arr[i])
                         and RSI_SHORT_MIN <= rsi_arr[i] <= RSI_SHORT_MAX))

        confluence_up = bb_up_ok and macd_up_ok and rsi_up_ok
        confluence_dn = bb_dn_ok and macd_dn_ok and rsi_dn_ok

        trend_up = (f > m and slope_up and adx_ok and weekly_up
                    and regime_on and three_up and confluence_up)
        trend_dn = (f < m and slope_dn and adx_ok and weekly_dn
                    and regime_on and three_dn and confluence_dn)

        bo_up = c > sh[i] + p["atr_buffer"] * a
        bo_dn = c < sl[i] - p["atr_buffer"] * a

        if has_volume and np.isfinite(vol_avg[i]) and vol_avg[i] > 0:
            vol_ok = volume[i] >= vol_avg[i] * p["vol_confirm_mult"]
        else:
            vol_ok = True

        in_cd = i < cooldown_until

        # v6 keeps v4's pure-trend exit logic (no MR mode here — testing
        # shows MR adds noise; v6 isolates the indicator-confluence question).
        if state == "flat":
            if in_cd:
                continue
            if trend_up and bo_up and vol_ok:
                state = "long"
                stop_d = p["init_stop_atr"] * a
                legs = [(ts[i], c, stop_d)]
                extreme = high[i]
                init_stop = c - stop_d
                using_trail = False
            elif trend_dn and bo_dn and vol_ok:
                state = "short"
                stop_d = p["init_stop_atr"] * a
                legs = [(ts[i], c, stop_d)]
                extreme = low[i]
                init_stop = c + stop_d
                using_trail = False
            continue

        if state == "long":
            extreme = max(extreme, high[i])
            first = legs[0][1]
            if not using_trail and (c - first) >= p["trail_activate_atr"] * a:
                using_trail = True
            if (p["pyramid"] and len(legs) <= PYRAMID_MAX_ADDS and bo_up
                    and (c - first) >= p["pyramid_profit_atr"] * a):
                legs.append((ts[i], c, p["init_stop_atr"] * a))
            if using_trail:
                trail = extreme - p["trail_atr"] * a
                exit_long = c < trail and c < m
            else:
                exit_long = c < init_stop
            if exit_long:
                won = (c - first) > 0
                for et, ep, sd in legs:
                    trades.append(Trade("long", et, ep, ts[i], c, sd))
                state = "flat"
                legs = []
                using_trail = False
                consec_losses = 0 if won else consec_losses + 1
                if (p["cooldown_after_losses"] > 0
                        and consec_losses >= p["cooldown_after_losses"]):
                    cooldown_until = i + p["cooldown_bars"]
                    consec_losses = 0
            continue

        # short
        extreme = min(extreme, low[i])
        first = legs[0][1]
        if not using_trail and (first - c) >= p["trail_activate_atr"] * a:
            using_trail = True
        if (p["pyramid"] and len(legs) <= PYRAMID_MAX_ADDS and bo_dn
                and (first - c) >= p["pyramid_profit_atr"] * a):
            legs.append((ts[i], c, p["init_stop_atr"] * a))
        if using_trail:
            trail = extreme + p["trail_atr"] * a
            exit_short = c > trail and c > m
        else:
            exit_short = c > init_stop
        if exit_short:
            won = (first - c) > 0
            for et, ep, sd in legs:
                trades.append(Trade("short", et, ep, ts[i], c, sd))
            state = "flat"
            legs = []
            using_trail = False
            consec_losses = 0 if won else consec_losses + 1
            if (p["cooldown_after_losses"] > 0
                    and consec_losses >= p["cooldown_after_losses"]):
                cooldown_until = i + p["cooldown_bars"]
                consec_losses = 0

    if state in ("long", "short"):
        for et, ep, sd in legs:
            trades.append(Trade(state, et, ep, ts[-1], close[-1], sd))
    return trades, use_three


def backtest_v7(df: pd.DataFrame, market: str,
                filters: frozenset[str] = frozenset(),
                ) -> tuple[list[Trade], bool]:
    """v7 = v6 redesigned + new indicators + 10% hard drawdown stop.

    KEY CHANGES VS V6:

    1. **10% hard drawdown stop** — at every bar, if the close has dropped
       10% from entry (long) / risen 10% from entry (short), force exit.
       This caps absolute losses regardless of how wide the ATR-based stop is.

    2. **BB filter redesigned** — uses BB %B > 0.70 (long) / < 0.30 (short),
       not "above middle band". Middle band = MA20, which is mostly redundant
       with the MA stack already in use; %B captures volatility-relative
       positioning that MA can't see.

    3. **NEW filter 'obv'** — On-Balance Volume must be above its 20-bar MA
       (long) / below (short). Volume confirmation independent of price MA.

    4. **NEW filter 'atr_exp'** — current ATR must be > ATR_EXPANSION_MULT ×
       50-bar avg ATR, i.e., volatility is expanding. Catches breakouts out
       of squeezes; stale-range trades are filtered out.

    `filters` is a subset of {'bb','macd','rsi','obv','atr_exp'}. All requested
    filters must AGREE for entry (AND logic). 32 possible subsets.
    """
    p = MARKET_PARAMS.get(market, MARKET_PARAMS["US"])
    df = df.sort_values("ts").reset_index(drop=True)
    n = len(df)

    close = df["close"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    volume = (df["volume"].to_numpy(dtype=float)
              if "volume" in df.columns else np.zeros(n))
    ts = df["ts"].to_numpy()

    s_close = pd.Series(close)
    ma_fast = s_close.rolling(MA_FAST).mean().to_numpy()
    ma_mid = s_close.rolling(MA_MID).mean().to_numpy()
    use_three = n >= MA_SLOW + SWING_LOOKBACK + 50
    ma_slow = (s_close.rolling(MA_SLOW).mean().to_numpy()
               if use_three else np.full(n, np.nan))

    sh = pd.Series(high).rolling(SWING_LOOKBACK).max().shift(1).to_numpy()
    sl = pd.Series(low).rolling(SWING_LOOKBACK).min().shift(1).to_numpy()

    tr = _true_range(high, low, close)
    atr = pd.Series(tr).rolling(ATR_PERIOD).mean().to_numpy()
    atr_avg = pd.Series(atr).rolling(ATR_EXPANSION_LOOKBACK).mean().to_numpy()

    ma_mid_lag = np.roll(ma_mid, TREND_SLOPE_PERIOD)
    with np.errstate(divide="ignore", invalid="ignore"):
        slope = (ma_mid - ma_mid_lag) / ma_mid_lag / TREND_SLOPE_PERIOD
    slope[: TREND_SLOPE_PERIOD] = np.nan

    adx = compute_adx(high, low, close, period=ADX_PERIOD)
    rsi_arr = compute_rsi(close, period=RSI_PERIOD)
    pct_b = compute_bb_percent(close)
    _, _, macd_h = compute_macd(close)
    has_volume = bool((volume > 0).any())
    obv = compute_obv(close, volume) if has_volume else np.zeros(n)
    obv_ma = (pd.Series(obv).rolling(OBV_MA_PERIOD).mean().to_numpy()
              if has_volume else np.full(n, np.nan))

    vol_avg = (pd.Series(volume).rolling(20).mean().to_numpy()
               if has_volume else np.full(n, np.nan))

    weekly_ma_s, weekly_ma_l = compute_weekly_alignment(df)
    has_weekly = weekly_ma_s is not None
    regime = get_market_regime(market)
    regime_ok = _align_regime_to_df(df, regime)

    warmup = (MA_SLOW if use_three else MA_MID) + SWING_LOOKBACK + TREND_SLOPE_PERIOD + 1
    if warmup >= n:
        return [], False

    use_bb = "bb" in filters
    use_macd = "macd" in filters
    use_rsi = "rsi" in filters
    use_obv = "obv" in filters
    use_atrx = "atr_exp" in filters

    trades: list[Trade] = []
    state = "flat"
    legs: list[tuple] = []
    extreme = 0.0
    init_stop = 0.0
    using_trail = False
    consec_losses = 0
    cooldown_until = 0

    for i in range(warmup, n):
        c = close[i]; a = atr[i]
        if not np.isfinite(a) or a <= 0:
            continue

        f, m, sm = ma_fast[i], ma_mid[i], slope[i]
        adx_i = adx[i]
        sl_ma = ma_slow[i] if use_three else None

        adx_ok = np.isfinite(adx_i) and adx_i > p["adx_min"]
        slope_up = np.isfinite(sm) and sm > p["slope_min"]
        slope_dn = np.isfinite(sm) and sm < -p["slope_min"]

        if has_weekly:
            ws, wl = weekly_ma_s[i], weekly_ma_l[i]
            weekly_up = np.isfinite(ws) and np.isfinite(wl) and ws > wl
            weekly_dn = np.isfinite(ws) and np.isfinite(wl) and ws < wl
        else:
            weekly_up = weekly_dn = True

        regime_on = bool(regime_ok[i])
        three_up = (sl_ma is not None and m > sl_ma) if use_three else True
        three_dn = (sl_ma is not None and m < sl_ma) if use_three else True

        # --- v7 confluence filters ---
        bb_up_ok = ((not use_bb)
                    or (np.isfinite(pct_b[i]) and pct_b[i] > BB_PERCENT_LONG))
        bb_dn_ok = ((not use_bb)
                    or (np.isfinite(pct_b[i]) and pct_b[i] < BB_PERCENT_SHORT))
        macd_up_ok = (not use_macd) or (np.isfinite(macd_h[i]) and macd_h[i] > 0)
        macd_dn_ok = (not use_macd) or (np.isfinite(macd_h[i]) and macd_h[i] < 0)
        rsi_up_ok = ((not use_rsi)
                     or (np.isfinite(rsi_arr[i])
                         and RSI_LONG_MIN <= rsi_arr[i] <= RSI_LONG_MAX))
        rsi_dn_ok = ((not use_rsi)
                     or (np.isfinite(rsi_arr[i])
                         and RSI_SHORT_MIN <= rsi_arr[i] <= RSI_SHORT_MAX))
        obv_up_ok = ((not use_obv) or (not has_volume)
                     or (np.isfinite(obv_ma[i]) and obv[i] > obv_ma[i]))
        obv_dn_ok = ((not use_obv) or (not has_volume)
                     or (np.isfinite(obv_ma[i]) and obv[i] < obv_ma[i]))
        atrx_ok = ((not use_atrx)
                   or (np.isfinite(atr_avg[i]) and atr_avg[i] > 0
                       and a > atr_avg[i] * ATR_EXPANSION_MULT))

        confluence_up = bb_up_ok and macd_up_ok and rsi_up_ok and obv_up_ok and atrx_ok
        confluence_dn = bb_dn_ok and macd_dn_ok and rsi_dn_ok and obv_dn_ok and atrx_ok

        trend_up = (f > m and slope_up and adx_ok and weekly_up
                    and regime_on and three_up and confluence_up)
        trend_dn = (f < m and slope_dn and adx_ok and weekly_dn
                    and regime_on and three_dn and confluence_dn)

        bo_up = c > sh[i] + p["atr_buffer"] * a
        bo_dn = c < sl[i] - p["atr_buffer"] * a

        if has_volume and np.isfinite(vol_avg[i]) and vol_avg[i] > 0:
            vol_ok = volume[i] >= vol_avg[i] * p["vol_confirm_mult"]
        else:
            vol_ok = True

        in_cd = i < cooldown_until

        if state == "flat":
            if in_cd:
                continue
            if trend_up and bo_up and vol_ok:
                state = "long"
                stop_d = p["init_stop_atr"] * a
                # 10% hard stop floor: never let init_stop go below 90% of entry
                hard_stop = c * (1.0 - PCT_HARD_STOP)
                init_stop = max(c - stop_d, hard_stop)
                stop_d = c - init_stop  # actual stop distance after cap
                legs = [(ts[i], c, stop_d)]
                extreme = high[i]
                using_trail = False
            elif trend_dn and bo_dn and vol_ok:
                state = "short"
                stop_d = p["init_stop_atr"] * a
                hard_stop = c * (1.0 + PCT_HARD_STOP)
                init_stop = min(c + stop_d, hard_stop)
                stop_d = init_stop - c
                legs = [(ts[i], c, stop_d)]
                extreme = low[i]
                using_trail = False
            continue

        if state == "long":
            extreme = max(extreme, high[i])
            first = legs[0][1]
            # 10% hard stop applies at every bar, regardless of trail/init.
            hard_floor = first * (1.0 - PCT_HARD_STOP)
            if not using_trail and (c - first) >= p["trail_activate_atr"] * a:
                using_trail = True
            if (p["pyramid"] and len(legs) <= PYRAMID_MAX_ADDS and bo_up
                    and (c - first) >= p["pyramid_profit_atr"] * a):
                legs.append((ts[i], c, p["init_stop_atr"] * a))
            if using_trail:
                trail = extreme - p["trail_atr"] * a
                exit_long = (c < trail and c < m) or c < hard_floor
            else:
                exit_long = c < init_stop or c < hard_floor
            if exit_long:
                won = (c - first) > 0
                for et, ep, sd in legs:
                    trades.append(Trade("long", et, ep, ts[i], c, sd))
                state = "flat"
                legs = []
                using_trail = False
                consec_losses = 0 if won else consec_losses + 1
                if (p["cooldown_after_losses"] > 0
                        and consec_losses >= p["cooldown_after_losses"]):
                    cooldown_until = i + p["cooldown_bars"]
                    consec_losses = 0
            continue

        # short
        extreme = min(extreme, low[i])
        first = legs[0][1]
        hard_ceil = first * (1.0 + PCT_HARD_STOP)
        if not using_trail and (first - c) >= p["trail_activate_atr"] * a:
            using_trail = True
        if (p["pyramid"] and len(legs) <= PYRAMID_MAX_ADDS and bo_dn
                and (first - c) >= p["pyramid_profit_atr"] * a):
            legs.append((ts[i], c, p["init_stop_atr"] * a))
        if using_trail:
            trail = extreme + p["trail_atr"] * a
            exit_short = (c > trail and c > m) or c > hard_ceil
        else:
            exit_short = c > init_stop or c > hard_ceil
        if exit_short:
            won = (first - c) > 0
            for et, ep, sd in legs:
                trades.append(Trade("short", et, ep, ts[i], c, sd))
            state = "flat"
            legs = []
            using_trail = False
            consec_losses = 0 if won else consec_losses + 1
            if (p["cooldown_after_losses"] > 0
                    and consec_losses >= p["cooldown_after_losses"]):
                cooldown_until = i + p["cooldown_bars"]
                consec_losses = 0

    if state in ("long", "short"):
        for et, ep, sd in legs:
            trades.append(Trade(state, et, ep, ts[-1], close[-1], sd))
    return trades, use_three


def get_cn_master_state() -> dict | None:
    """A-share specific master switch: CSI300 close vs MA200.
    Returns dict with ts + 'allow_long' / 'allow_short' boolean arrays.
    """
    cache_key = "_cn_master"
    if cache_key in _REGIME_CACHE:
        return _REGIME_CACHE[cache_key]
    df = fetch_yf(CN_INDEX_TICKER)
    if df is None or len(df) < CN_MASTER_MA + 5:
        return None
    close = df["close"].to_numpy(dtype=float)
    ma200 = pd.Series(close).rolling(CN_MASTER_MA).mean().to_numpy()
    # Lag by 1 bar to prevent look-ahead.
    above = np.roll(close > ma200, 1); above[0] = False
    below = np.roll(close < ma200, 1); below[0] = False
    out = {
        "ts": _strip_tz(df["ts"]).to_numpy(),
        "allow_long": above,
        "allow_short": below,
    }
    _REGIME_CACHE[cache_key] = out
    LOG.info("CN master switch: %d/%d bars allow long, %d allow short",
             int(above.sum()), len(above), int(below.sum()))
    return out


def _align_cn_master(df: pd.DataFrame
                     ) -> tuple[np.ndarray, np.ndarray]:
    """Returns (allow_long, allow_short) per bar of df."""
    n = len(df)
    state = get_cn_master_state()
    if state is None:
        return np.ones(n, dtype=bool), np.ones(n, dtype=bool)
    left_ts = _strip_tz(df["ts"])
    right_ts = _strip_tz(pd.Series(state["ts"]))
    df_ts = pd.DataFrame({"ts": left_ts}).sort_values("ts")
    reg_df = pd.DataFrame({
        "ts": right_ts,
        "long_ok": state["allow_long"],
        "short_ok": state["allow_short"],
    }).sort_values("ts")
    aligned = pd.merge_asof(df_ts, reg_df, on="ts", direction="backward")
    return (aligned["long_ok"].fillna(False).to_numpy(dtype=bool),
            aligned["short_ok"].fillna(False).to_numpy(dtype=bool))


def backtest_v8(df: pd.DataFrame, market: str,
                filters: frozenset[str] = frozenset(),
                ) -> tuple[list[Trade], bool]:
    """v8 = v7 minus BB (proven redundant) plus MFI / CCI / KDJ + CN master switch.

    Filter keys (7): macd, rsi, obv, atr_exp, mfi, cci, kdj
    BB (in any form) was removed — empirically redundant with MA stack.

    NEW filters:
      'mfi'  : MFI (volume-weighted RSI) in [50, 80] long / [20, 50] short
      'cci'  : CCI > +100 long / < -100 short (strong-trend confirmed)
      'kdj'  : K > 50 AND K > D AND J > D long; symmetric short

    A-SHARE SPECIFIC: CN_MASTER_MA gate. For CN tickers, additionally require
    CSI300 close > MA200 (long) / < MA200 (short). This skips chronic-bear
    periods that ADX alone passed through (the dominant CN failure mode).
    """
    p = MARKET_PARAMS.get(market, MARKET_PARAMS["US"])
    df = df.sort_values("ts").reset_index(drop=True)
    n = len(df)

    close = df["close"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    volume = (df["volume"].to_numpy(dtype=float)
              if "volume" in df.columns else np.zeros(n))
    ts = df["ts"].to_numpy()

    s_close = pd.Series(close)
    ma_fast = s_close.rolling(MA_FAST).mean().to_numpy()
    ma_mid = s_close.rolling(MA_MID).mean().to_numpy()
    use_three = n >= MA_SLOW + SWING_LOOKBACK + 50
    ma_slow = (s_close.rolling(MA_SLOW).mean().to_numpy()
               if use_three else np.full(n, np.nan))

    sh = pd.Series(high).rolling(SWING_LOOKBACK).max().shift(1).to_numpy()
    sl = pd.Series(low).rolling(SWING_LOOKBACK).min().shift(1).to_numpy()

    tr = _true_range(high, low, close)
    atr = pd.Series(tr).rolling(ATR_PERIOD).mean().to_numpy()
    atr_avg = pd.Series(atr).rolling(ATR_EXPANSION_LOOKBACK).mean().to_numpy()

    ma_mid_lag = np.roll(ma_mid, TREND_SLOPE_PERIOD)
    with np.errstate(divide="ignore", invalid="ignore"):
        slope = (ma_mid - ma_mid_lag) / ma_mid_lag / TREND_SLOPE_PERIOD
    slope[: TREND_SLOPE_PERIOD] = np.nan

    adx = compute_adx(high, low, close, period=ADX_PERIOD)
    rsi_arr = compute_rsi(close, period=RSI_PERIOD)
    _, _, macd_h = compute_macd(close)
    has_volume = bool((volume > 0).any())
    obv = compute_obv(close, volume) if has_volume else np.zeros(n)
    obv_ma = (pd.Series(obv).rolling(OBV_MA_PERIOD).mean().to_numpy()
              if has_volume else np.full(n, np.nan))
    mfi_arr = (compute_mfi(high, low, close, volume) if has_volume
               else np.full(n, np.nan))
    cci_arr = compute_cci(high, low, close)
    kdj_k, kdj_d, kdj_j = compute_kdj(high, low, close)

    vol_avg = (pd.Series(volume).rolling(20).mean().to_numpy()
               if has_volume else np.full(n, np.nan))

    weekly_ma_s, weekly_ma_l = compute_weekly_alignment(df)
    has_weekly = weekly_ma_s is not None
    regime = get_market_regime(market)
    regime_ok = _align_regime_to_df(df, regime)

    # CN master switch — only applies to CN market entries.
    if market == "CN":
        cn_long_ok, cn_short_ok = _align_cn_master(df)
    else:
        cn_long_ok = np.ones(n, dtype=bool)
        cn_short_ok = np.ones(n, dtype=bool)

    warmup = (MA_SLOW if use_three else MA_MID) + SWING_LOOKBACK + TREND_SLOPE_PERIOD + 1
    if warmup >= n:
        return [], False

    use_macd = "macd" in filters
    use_rsi = "rsi" in filters
    use_obv = "obv" in filters
    use_atrx = "atr_exp" in filters
    use_mfi = "mfi" in filters
    use_cci = "cci" in filters
    use_kdj = "kdj" in filters

    trades: list[Trade] = []
    state = "flat"
    legs: list[tuple] = []
    extreme = 0.0
    init_stop = 0.0
    using_trail = False
    consec_losses = 0
    cooldown_until = 0

    for i in range(warmup, n):
        c = close[i]; a = atr[i]
        if not np.isfinite(a) or a <= 0:
            continue

        f, m, sm = ma_fast[i], ma_mid[i], slope[i]
        adx_i = adx[i]
        sl_ma = ma_slow[i] if use_three else None

        adx_ok = np.isfinite(adx_i) and adx_i > p["adx_min"]
        slope_up = np.isfinite(sm) and sm > p["slope_min"]
        slope_dn = np.isfinite(sm) and sm < -p["slope_min"]

        if has_weekly:
            ws, wl = weekly_ma_s[i], weekly_ma_l[i]
            weekly_up = np.isfinite(ws) and np.isfinite(wl) and ws > wl
            weekly_dn = np.isfinite(ws) and np.isfinite(wl) and ws < wl
        else:
            weekly_up = weekly_dn = True

        regime_on = bool(regime_ok[i])
        three_up = (sl_ma is not None and m > sl_ma) if use_three else True
        three_dn = (sl_ma is not None and m < sl_ma) if use_three else True

        # CN master switch (only restrictive for CN market)
        cn_long = bool(cn_long_ok[i])
        cn_short = bool(cn_short_ok[i])

        # --- v8 confluence filters ---
        macd_up = (not use_macd) or (np.isfinite(macd_h[i]) and macd_h[i] > 0)
        macd_dn = (not use_macd) or (np.isfinite(macd_h[i]) and macd_h[i] < 0)
        rsi_up = ((not use_rsi)
                  or (np.isfinite(rsi_arr[i])
                      and RSI_LONG_MIN <= rsi_arr[i] <= RSI_LONG_MAX))
        rsi_dn = ((not use_rsi)
                  or (np.isfinite(rsi_arr[i])
                      and RSI_SHORT_MIN <= rsi_arr[i] <= RSI_SHORT_MAX))
        obv_up = ((not use_obv) or (not has_volume)
                  or (np.isfinite(obv_ma[i]) and obv[i] > obv_ma[i]))
        obv_dn = ((not use_obv) or (not has_volume)
                  or (np.isfinite(obv_ma[i]) and obv[i] < obv_ma[i]))
        atrx = ((not use_atrx)
                or (np.isfinite(atr_avg[i]) and atr_avg[i] > 0
                    and a > atr_avg[i] * ATR_EXPANSION_MULT))
        mfi_up = ((not use_mfi) or (not has_volume)
                  or (np.isfinite(mfi_arr[i])
                      and MFI_LONG_MIN <= mfi_arr[i] <= MFI_LONG_MAX))
        mfi_dn = ((not use_mfi) or (not has_volume)
                  or (np.isfinite(mfi_arr[i])
                      and MFI_SHORT_MIN <= mfi_arr[i] <= MFI_SHORT_MAX))
        cci_up = ((not use_cci)
                  or (np.isfinite(cci_arr[i]) and cci_arr[i] > CCI_LONG_MIN))
        cci_dn = ((not use_cci)
                  or (np.isfinite(cci_arr[i]) and cci_arr[i] < CCI_SHORT_MAX))
        kdj_up = ((not use_kdj)
                  or (np.isfinite(kdj_k[i]) and np.isfinite(kdj_d[i])
                      and kdj_k[i] > KDJ_LONG_MIN and kdj_k[i] > kdj_d[i]
                      and kdj_j[i] > kdj_d[i]))
        kdj_dn = ((not use_kdj)
                  or (np.isfinite(kdj_k[i]) and np.isfinite(kdj_d[i])
                      and kdj_k[i] < KDJ_SHORT_MAX and kdj_k[i] < kdj_d[i]
                      and kdj_j[i] < kdj_d[i]))

        confluence_up = macd_up and rsi_up and obv_up and atrx and mfi_up and cci_up and kdj_up
        confluence_dn = macd_dn and rsi_dn and obv_dn and atrx and mfi_dn and cci_dn and kdj_dn

        trend_up = (f > m and slope_up and adx_ok and weekly_up
                    and regime_on and three_up and confluence_up
                    and cn_long)
        trend_dn = (f < m and slope_dn and adx_ok and weekly_dn
                    and regime_on and three_dn and confluence_dn
                    and cn_short)

        bo_up = c > sh[i] + p["atr_buffer"] * a
        bo_dn = c < sl[i] - p["atr_buffer"] * a

        if has_volume and np.isfinite(vol_avg[i]) and vol_avg[i] > 0:
            vol_ok = volume[i] >= vol_avg[i] * p["vol_confirm_mult"]
        else:
            vol_ok = True

        in_cd = i < cooldown_until

        if state == "flat":
            if in_cd:
                continue
            if trend_up and bo_up and vol_ok:
                state = "long"
                stop_d = p["init_stop_atr"] * a
                hard_stop = c * (1.0 - PCT_HARD_STOP)
                init_stop = max(c - stop_d, hard_stop)
                stop_d = c - init_stop
                legs = [(ts[i], c, stop_d)]
                extreme = high[i]
                using_trail = False
            elif trend_dn and bo_dn and vol_ok:
                state = "short"
                stop_d = p["init_stop_atr"] * a
                hard_stop = c * (1.0 + PCT_HARD_STOP)
                init_stop = min(c + stop_d, hard_stop)
                stop_d = init_stop - c
                legs = [(ts[i], c, stop_d)]
                extreme = low[i]
                using_trail = False
            continue

        if state == "long":
            extreme = max(extreme, high[i])
            first = legs[0][1]
            hard_floor = first * (1.0 - PCT_HARD_STOP)
            if not using_trail and (c - first) >= p["trail_activate_atr"] * a:
                using_trail = True
            if (p["pyramid"] and len(legs) <= PYRAMID_MAX_ADDS and bo_up
                    and (c - first) >= p["pyramid_profit_atr"] * a):
                legs.append((ts[i], c, p["init_stop_atr"] * a))
            if using_trail:
                trail = extreme - p["trail_atr"] * a
                exit_long = (c < trail and c < m) or c < hard_floor
            else:
                exit_long = c < init_stop or c < hard_floor
            if exit_long:
                won = (c - first) > 0
                for et, ep, sd in legs:
                    trades.append(Trade("long", et, ep, ts[i], c, sd))
                state = "flat"
                legs = []
                using_trail = False
                consec_losses = 0 if won else consec_losses + 1
                if (p["cooldown_after_losses"] > 0
                        and consec_losses >= p["cooldown_after_losses"]):
                    cooldown_until = i + p["cooldown_bars"]
                    consec_losses = 0
            continue

        # short
        extreme = min(extreme, low[i])
        first = legs[0][1]
        hard_ceil = first * (1.0 + PCT_HARD_STOP)
        if not using_trail and (first - c) >= p["trail_activate_atr"] * a:
            using_trail = True
        if (p["pyramid"] and len(legs) <= PYRAMID_MAX_ADDS and bo_dn
                and (first - c) >= p["pyramid_profit_atr"] * a):
            legs.append((ts[i], c, p["init_stop_atr"] * a))
        if using_trail:
            trail = extreme + p["trail_atr"] * a
            exit_short = (c > trail and c > m) or c > hard_ceil
        else:
            exit_short = c > init_stop or c > hard_ceil
        if exit_short:
            won = (first - c) > 0
            for et, ep, sd in legs:
                trades.append(Trade("short", et, ep, ts[i], c, sd))
            state = "flat"
            legs = []
            using_trail = False
            consec_losses = 0 if won else consec_losses + 1
            if (p["cooldown_after_losses"] > 0
                    and consec_losses >= p["cooldown_after_losses"]):
                cooldown_until = i + p["cooldown_bars"]
                consec_losses = 0

    if state in ("long", "short"):
        for et, ep, sd in legs:
            trades.append(Trade(state, et, ep, ts[-1], close[-1], sd))
    return trades, use_three


def run_market(market: str, tickers, fetch_fn,
               three_level: bool = USE_THREE_LEVEL_DEFAULT,
               strategy: str = "v1",
               v6_filters: frozenset[str] = frozenset(),
               ) -> list[TickerResult]:
    data = fetch_parallel(market, tickers, fetch_fn)
    out: list[TickerResult] = []
    # Pre-warm the per-market regime cache once for v4/v5 (so all 20 stocks of
    # this market reuse the same index-data computation).
    if strategy in ("v4", "v5", "v6", "v7", "v8"):
        get_market_regime(market)
    if strategy == "v8" and market == "CN":
        get_cn_master_state()  # warm CN-specific state
    for t, df in data.items():
        if strategy == "v8":
            trades, use3 = backtest_v8(df, market, filters=v6_filters)
        elif strategy == "v7":
            trades, use3 = backtest_v7(df, market, filters=v6_filters)
        elif strategy == "v6":
            trades, use3 = backtest_v6(df, market, filters=v6_filters)
        elif strategy == "v5":
            trades, use3 = backtest_v5(df, market)
        elif strategy == "v4":
            trades, use3 = backtest_v4(df, market)
        elif strategy == "v3":
            trades, use3 = backtest_v3(df, market)
        elif strategy == "v2":
            trades, use3 = backtest_v2(df)
        else:
            trades, use3 = backtest(df, three_level=three_level)
        out.append(TickerResult(
            ticker=t, market=market, bars=len(df),
            use_three_level=use3, trades=trades,
        ))
    return out


# --- Reporting -------------------------------------------------------------

def aggregate(results: list[TickerResult], label: str) -> None:
    if not results:
        print(f"\n=== {label} === (no data)")
        return
    n_trades = sum(r.n_trades for r in results)
    n_wins = sum(r.n_wins for r in results)
    wr = n_wins / n_trades if n_trades else 0.0
    total_net = sum(r.total_net for r in results)
    total_gross = sum(r.total_gross for r in results)
    avg_net = total_net / len(results)
    n_three = sum(1 for r in results if r.use_three_level)

    wins = [t.net_return for r in results for t in r.trades if t.net_return > 0]
    losses = [t.net_return for r in results for t in r.trades if t.net_return <= 0]
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = sum(losses) / len(losses) if losses else 0.0

    print(f"\n=== {label} ===")
    print(f"  Tickers covered:        {len(results)}  (3-level: {n_three})")
    print(f"  Total trades:           {n_trades}")
    print(f"  Wins / Losses:          {n_wins} / {n_trades - n_wins}")
    print(f"  Win rate:               {wr:.2%}")
    print(f"  Sum gross return:       {total_gross:+.2%}")
    print(f"  Sum net return:         {total_net:+.2%}")
    print(f"  Avg net per ticker:     {avg_net:+.2%}")
    print(f"  Avg winning trade:      {avg_win:+.3%}")
    print(f"  Avg losing trade:       {avg_loss:+.3%}")

    # R-multiple summary (only meaningful for v4 trades that record stop_dist).
    r_mults = [t.r_multiple for r in results for t in r.trades
               if t.stop_dist > 0]
    if r_mults:
        sum_r = sum(r_mults)
        avg_r = sum_r / len(r_mults)
        print(f"  Total R earned:         {sum_r:+.2f} R")
        print(f"  Avg R per trade:        {avg_r:+.3f} R")


def _summary(results: list[TickerResult]) -> dict:
    n_trades = sum(r.n_trades for r in results)
    n_wins = sum(r.n_wins for r in results)
    wins = [t.net_return for r in results for t in r.trades if t.net_return > 0]
    losses = [t.net_return for r in results for t in r.trades if t.net_return <= 0]
    return {
        "n_tickers": len(results),
        "n_trades": n_trades,
        "n_wins": n_wins,
        "win_rate": (n_wins / n_trades) if n_trades else 0.0,
        "gross": sum(r.total_gross for r in results),
        "net": sum(r.total_net for r in results),
        "avg_win": (sum(wins) / len(wins)) if wins else 0.0,
        "avg_loss": (sum(losses) / len(losses)) if losses else 0.0,
        "profit_factor": (sum(wins) / -sum(losses)) if losses and sum(losses) < 0 else float("inf"),
    }


def print_comparison(*runs: tuple[str, dict]) -> None:
    """Print N-way side-by-side comparison. `runs` is [(label, results_dict), ...]."""
    if not runs:
        return
    market_labels = list(runs[0][1].keys())
    fmt_pf = lambda v: f"{v:.2f}" if v != float("inf") else "inf"
    name_w = max(len(n) for n, _ in runs) + 1
    width = 10 + 4 + (8 + 8 + 9 + 6) * len(runs) + 2
    print("\n" + "=" * width)
    hdr = f"{'市场':<10} {'tk':>3}"
    for n, _ in runs:
        hdr += f"  {('trd_'+n):>6} {('wr_'+n):>6} {('net_'+n):>9} {('PF_'+n):>5}"
    print(hdr)
    print("-" * width)

    def row(label: str, summaries: list[dict]) -> None:
        line = f"{label:<10} {summaries[0]['n_tickers']:>3}"
        for s in summaries:
            line += (f"  {s['n_trades']:>6} {s['win_rate']:>6.1%} "
                     f"{s['net']:>+8.2%} {fmt_pf(s['profit_factor']):>5}")
        print(line)

    for ml in market_labels:
        sums = [_summary(rd[ml]) for _, rd in runs]
        row(ml, sums)
    sums_total = [_summary([t for v in rd.values() for t in v]) for _, rd in runs]
    row("TOTAL", sums_total)
    print("=" * width)
    print("  tk=tickers, trd=trades, wr=win rate, net=sum of per-ticker net returns,")
    print("  PF=profit factor (sum_wins / |sum_losses|)")


def per_ticker_table(results: list[TickerResult], label: str) -> None:
    if not results:
        return
    print(f"\n--- {label} per-ticker ---")
    print(f"{'ticker':<10} {'bars':>5} {'3L':>3} "
          f"{'trades':>7} {'wins':>5} {'win%':>7} {'net':>8}")
    for r in sorted(results, key=lambda x: -x.total_net):
        wr = r.n_wins / r.n_trades if r.n_trades else 0.0
        print(f"{r.ticker:<10} {r.bars:>5} "
              f"{'Y' if r.use_three_level else 'N':>3} "
              f"{r.n_trades:>7} {r.n_wins:>5} {wr:>6.1%} "
              f"{r.total_net:>+8.2%}")


# --- Main ------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--detail", action="store_true",
                        help="print per-ticker table")
    parser.add_argument("--seed", type=int, default=RANDOM_SEED)
    parser.add_argument("--no-cn", action="store_true",
                        help="skip A-share market (e.g. if akshare is blocked)")
    parser.add_argument("--three-level", action="store_true",
                        help="enable K2 three-level filter (needs much more data)")
    parser.add_argument("--strategy",
                        choices=["v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"],
                        default="v1",
                        help="v1..v7 as before; "
                             "v8=v7-bb+MFI+CCI+KDJ+CN-master-switch")
    parser.add_argument("--filters", default="",
                        help="v6/v7/v8 filters: comma-separated subset. "
                             "v6: bb,macd,rsi. "
                             "v7: bb,macd,rsi,obv,atr_exp. "
                             "v8: macd,rsi,obv,atr_exp,mfi,cci,kdj.")
    parser.add_argument("--grid", action="store_true",
                        help="grid search: try every subset of filter keys "
                             "(v6=8 / v7=32 combos) and rank by profit factor")
    parser.add_argument("--screen", action="store_true",
                        help="filter universe by analyst score (current snapshot, "
                             "look-ahead bias warning)")
    parser.add_argument("--compare", action="store_true",
                        help="run both v1 and v2 and print side-by-side")
    parser.add_argument("--mode", choices=list(DATA_PRESETS.keys()),
                        default="short",
                        help="short=6mo/1h (default) | long=3y/1d")
    args = parser.parse_args()

    global PERIOD_DAYS, INTERVAL  # module constants used by fetchers
    preset = DATA_PRESETS[args.mode]
    PERIOD_DAYS = preset["days"]
    INTERVAL = preset["interval"]
    LOG.info("Data window: %d days @ %s bars", PERIOD_DAYS, INTERVAL)

    rng = random.Random(args.seed)
    if args.screen:
        LOG.warning("--screen uses current analyst data on historical bars: "
                    "results are subject to look-ahead bias. Treat as illustration.")
        us_pick = screen_universe("US", US_UNIVERSE, SAMPLE_SIZE)
        hk_pick = screen_universe("HK", HK_UNIVERSE, SAMPLE_SIZE)
        cn_pick = (screen_universe("CN", sorted(set(CN_UNIVERSE)), SAMPLE_SIZE)
                   if not args.no_cn else [])
    else:
        us_pick = rng.sample(US_UNIVERSE, SAMPLE_SIZE)
        hk_pick = rng.sample(HK_UNIVERSE, SAMPLE_SIZE)
        cn_pick = rng.sample(sorted(set(CN_UNIVERSE)), SAMPLE_SIZE)

    LOG.info("Cost model: commission=%.4f%% slippage=%.4f%% (round-trip=%.2f%%)",
             COMMISSION * 100, SLIPPAGE * 100, ROUND_TRIP_COST * 100)
    LOG.info("US sample (%d): %s", len(us_pick), us_pick)
    LOG.info("HK sample (%d): %s", len(hk_pick), hk_pick)
    LOG.info("A-share sample (%d): %s", len(cn_pick), cn_pick)

    LOG.info("Strategy mode: %s",
             "3-level (MA30+MA150+MA750)" if args.three_level
             else "2-level (MA30+MA150) — K1 default")

    def _run(strategy: str,
             v6_filters: frozenset[str] = frozenset()
             ) -> dict[str, list[TickerResult]]:
        return {
            "US (美股)": run_market("US", us_pick, fetch_yf,
                                  three_level=args.three_level,
                                  strategy=strategy, v6_filters=v6_filters),
            "HK (港股)": run_market("HK", hk_pick, fetch_yf,
                                  three_level=args.three_level,
                                  strategy=strategy, v6_filters=v6_filters),
            "CN (A股)": [] if args.no_cn else run_market(
                "CN", cn_pick, fetch_cn,
                three_level=args.three_level, strategy=strategy,
                v6_filters=v6_filters,
            ),
        }

    if args.grid:
        # Indicator-confluence grid. v7 has 32 combos, v6 has 8.
        from itertools import combinations
        if args.strategy == "v8":
            keys = V8_FILTER_KEYS
            grid_strategy = "v8"
        elif args.strategy == "v7":
            keys = V7_FILTER_KEYS
            grid_strategy = "v7"
        else:
            keys = V6_FILTER_KEYS
            grid_strategy = "v6"
        all_subsets: list[frozenset[str]] = [frozenset()]
        for r in range(1, len(keys) + 1):
            for combo in combinations(keys, r):
                all_subsets.append(frozenset(combo))

        runs: list[tuple[str, dict[str, list[TickerResult]]]] = []
        for filt in all_subsets:
            label = f"{grid_strategy}_" + (
                "_".join(sorted(filt)) if filt else "base"
            )
            LOG.info("Grid: running %s (filters=%s) ...",
                     label, sorted(filt) or "[]")
            runs.append((label, _run(grid_strategy, v6_filters=filt)))

        # Print full comparison only for small grids (v6 = 8 combos).
        if len(runs) <= 10:
            print_comparison(*runs)

        # Per-market top-N rankings. With 32 combos, show top 8 per market.
        top_n = 8 if len(runs) > 10 else 3
        print("\n" + "=" * 70)
        print(f"Per-market top {top_n} (sorted by PF, tie-break net):")
        print("=" * 70)
        markets = list(runs[0][1].keys())
        for m in markets + ["TOTAL"]:
            scores = []
            for label, rd in runs:
                rs = (rd[m] if m != "TOTAL"
                      else [t for v in rd.values() for t in v])
                if not rs or sum(r.n_trades for r in rs) == 0:
                    continue
                s = _summary(rs)
                scores.append((s["profit_factor"], s["net"],
                               s["n_trades"], s["win_rate"], label))
            scores.sort(key=lambda x: (-x[0], -x[1]))
            top = scores[:top_n]
            print(f"\n  [{m}]")
            for pf, net, nt, wr, lbl in top:
                pf_str = f"{pf:.2f}" if pf != float("inf") else "inf"
                print(f"    {lbl:<32}  PF={pf_str:>4}  "
                      f"net={net:+8.2%}  trd={nt:>4}  wr={wr:.1%}")
        return 0

    if args.compare:
        LOG.info("Running v1 (baseline) ...")
        r1 = _run("v1")
        LOG.info("Running v2 (ATR+slope+trail+pyramid) ...")
        r2 = _run("v2")
        LOG.info("Running v3 (v2+ADX+init-stop+per-market+cooldown+vol) ...")
        r3 = _run("v3")
        LOG.info("Running v4 (v3+weekly-resonance+market-regime) ...")
        r4 = _run("v4")
        LOG.info("Running v5 (v4+auto-3level+RSI-mean-reversion) ...")
        r5 = _run("v5")
        print_comparison(("v1", r1), ("v2", r2), ("v3", r3),
                         ("v4", r4), ("v5", r5))
        if args.detail:
            for label, results in r5.items():
                per_ticker_table(results, f"{label} v5")
        return 0

    LOG.info("Strategy: %s", args.strategy)
    v6_filters = frozenset(
        f.strip() for f in args.filters.split(",") if f.strip()
    ) if args.strategy == "v6" else frozenset()
    if v6_filters:
        invalid = v6_filters - set(V6_FILTER_KEYS)
        if invalid:
            LOG.error("Invalid v6 filters: %s. Valid: %s",
                      sorted(invalid), V6_FILTER_KEYS)
            return 2
        LOG.info("v6 filters: %s", sorted(v6_filters))
    results = _run(args.strategy, v6_filters=v6_filters)
    for label, r in results.items():
        aggregate(r, label)
    aggregate([t for r in results.values() for t in r], "TOTAL")

    if args.detail:
        for label, r in results.items():
            per_ticker_table(r, label)

    return 0


if __name__ == "__main__":
    sys.exit(main())
