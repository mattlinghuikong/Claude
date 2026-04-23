"""
China A-share analyst data via akshare (Eastmoney source).
Eastmoney publishes analyst accuracy rankings publicly — no API key needed.
"""

import re
import signal
import traceback
from datetime import date, timedelta

_CN_TICKER_RE = re.compile(r"^\d{6}$")

try:
    import akshare as ak
    import pandas as pd
except ImportError:
    ak = None
    pd = None


class _Timeout(Exception):
    pass


def _timed(fn, timeout=15):
    """
    Hard wall-clock timeout using SIGALRM — must be called from the main thread.
    Unlike ThreadPoolExecutor-based timeouts, SIGALRM actually interrupts blocking
    I/O so hung HTTP calls don't leak background threads that exhaust connections.
    """
    def _handler(signum, frame):
        raise _Timeout()

    prev = signal.signal(signal.SIGALRM, _handler)
    signal.alarm(max(1, int(timeout)))
    try:
        return fn()
    except _Timeout:
        return None
    except Exception:
        return None
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, prev)


REPORT_LOOKBACK_DAYS = 30


def _is_recent(date_str, days=REPORT_LOOKBACK_DAYS):
    """Return True if date_str (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS) is within the last N days."""
    try:
        cutoff = date.today() - timedelta(days=days)
        return date.fromisoformat(str(date_str)[:10]) >= cutoff
    except Exception:
        return True  # unparseable dates pass through


def _safe_float(val, default=0.0):
    try:
        return float(str(val).replace("%", "").replace(",", ""))
    except Exception:
        return default


def _get_col(row, *candidates, default=""):
    """Return the first matching column value from a row dict."""
    for key in candidates:
        v = row.get(key)
        if v is not None and str(v).strip() not in ("", "nan", "None", "--"):
            return v
    return default


def get_top_analysts(year=None, top_n=20):
    """
    Return top N analysts from Eastmoney ranked by YTD return.

    Actual columns returned by stock_analyst_rank_em (2026):
        序号, 分析师名称, 分析师单位, 年度指数,
        2026年收益率, 3个月收益率, 6个月收益率, 12个月收益率,
        成分股个数, 2026最新个股评级-股票名称, 2026最新个股评级-股票代码,
        分析师ID, 行业代码, 行业, 更新日期, 年度
    """
    if ak is None:
        return [], "akshare not installed"
    if year is None:
        year = str(date.today().year)
    try:
        df = ak.stock_analyst_rank_em(year=year)
        if df is None or df.empty:
            df = ak.stock_analyst_rank_em(year=str(int(year) - 1))
        if df is None or df.empty:
            return [], "No data returned from Eastmoney"

        df.columns = [c.strip() for c in df.columns]
        cols = list(df.columns)

        # Dynamically find the YTD return column (e.g. "2026年收益率")
        ytd_col = next((c for c in cols if "年收益率" in c), None)
        # 12-month return as avg_return proxy
        m12_col = next((c for c in cols if "12个月收益率" in c), None)
        # Component stock count
        count_col = next((c for c in cols if "成分股" in c or "评级数量" in c or "研报数量" in c), None)
        # Latest recommended stock fields
        latest_name_col = next((c for c in cols if "最新个股评级" in c and "名称" in c), None)
        latest_code_col = next((c for c in cols if "最新个股评级" in c and "代码" in c), None)

        analysts = []
        for _, row in df.head(top_n).iterrows():
            entry = {
                "analyst":     str(_get_col(row, "分析师名称", "分析师", default="Unknown")),
                "firm":        str(_get_col(row, "分析师单位", "所属机构", default="Unknown")),
                "analyst_id":  str(_get_col(row, "分析师ID", default="")),
                "ytd_return":  _safe_float(row.get(ytd_col, 0) if ytd_col else 0),
                "avg_return":  _safe_float(row.get(m12_col, 0) if m12_col else 0),
                "total_calls": int(_safe_float(row.get(count_col, 0) if count_col else 0)),
                "year":        year,
                # Latest pick already embedded in the rank table
                "latest_stock_name": str(row.get(latest_name_col, "") if latest_name_col else ""),
                "latest_stock_code": str(row.get(latest_code_col, "") if latest_code_col else ""),
                "updated_date": str(_get_col(row, "更新日期", default="")),
            }
            # Expose ytd_return as win_rate for display compatibility
            entry["win_rate"] = entry["ytd_return"]
            analysts.append(entry)
        return analysts, None
    except Exception as e:
        return [], f"akshare error: {e}\n{traceback.format_exc()}"


def get_analyst_picks(analyst_id, top_n=5):
    """
    Get an analyst's currently tracked stocks.

    Uses indicator="最新跟踪成分股" (valid values: "最新跟踪成分股",
    "历史跟踪成分股", "历史指数").

    Actual columns returned:
        序号, 股票代码, 股票名称, 调入日期, 最新评级日期,
        当前评级名称, 成交价格(前复权), 最新价格, 阶段涨跌幅

    NOTE: These are ongoing tracked positions, not timestamped publications.
    An analyst adds a stock and keeps it until they exit — we use a 365-day
    window so active holdings aren't silently dropped.
    """
    if ak is None or not analyst_id:
        return []
    try:
        df = ak.stock_analyst_detail_em(analyst_id=analyst_id, indicator="最新跟踪成分股")
        if df is None or df.empty:
            return []
        df.columns = [c.strip() for c in df.columns]
        picks = []
        for _, row in df.iterrows():
            if len(picks) >= top_n:
                break
            # Use 365-day window — these are ongoing positions, not new reports
            report_date = str(_get_col(row, "最新评级日期", "调入日期", default=""))
            if not _is_recent(report_date, days=365):
                continue

            ticker       = str(_get_col(row, "股票代码", default=""))
            name         = str(_get_col(row, "股票名称", default=""))
            rating       = str(_get_col(row, "当前评级名称", default=""))
            current_price = _safe_float(_get_col(row, "最新价格", default=0))
            entry_price  = _safe_float(_get_col(row, "成交价格(前复权)", default=0))
            change_pct   = _safe_float(_get_col(row, "阶段涨跌幅", default=0))

            picks.append({
                "ticker":        ticker,
                "name":          name,
                "rating":        rating,
                "price_target":  0.0,   # not provided by this endpoint
                "report_title":  "",    # not provided by this endpoint
                "report_date":   report_date,
                "entry_price":   entry_price,
                "current_price": current_price,
                "change_pct_since_call": change_pct,
            })
        return picks
    except Exception:
        return []


def get_stock_fundamentals(ticker):
    """Fetch basic fundamental data for a CN A-share ticker."""
    if ak is None:
        return {}
    try:
        info = ak.stock_individual_info_em(symbol=ticker)
        if info is None or info.empty:
            return {}
        result = {}
        for _, row in info.iterrows():
            key = str(row.iloc[0]).strip()
            val = str(row.iloc[1]).strip()
            result[key] = val
        return result
    except Exception:
        return {}


def get_current_price(ticker):
    """Get current price for a CN stock from spot data."""
    if ak is None:
        return None
    try:
        df = ak.stock_zh_a_spot_em()
        if df is None or df.empty:
            return None
        row = df[df["代码"] == ticker]
        if row.empty:
            return None
        return _safe_float(row.iloc[0].get("最新价", 0)) or None
    except Exception:
        return None




def _sector_default_pe(ticker):
    """Return a reasonable default forward PE for the stock's exchange segment."""
    if ticker.startswith("688"):
        return 35.0   # STAR Market — high-growth tech/biotech
    if ticker.startswith("300"):
        return 28.0   # ChiNext — growth SMEs
    if ticker.startswith(("600", "601", "603", "605")):
        return 12.0   # Shanghai mainboard — blue-chips, cyclicals
    if ticker.startswith(("000", "001", "002", "003")):
        return 15.0   # Shenzhen mainboard
    return 15.0


def _em_analyst_targets(tickers, per_ticker_timeout=6, budget=20, workers=6):
    """
    Fetch median analyst target price from Eastmoney research reports
    (direct API call — akshare drops the indvAimPriceT/indvAimPriceL fields).

    Returns dict: {ticker: median_analyst_target}. Only populated when ≥1 report
    has a non-zero target price in the last 180 days.

    Parallelized across a small thread pool. `budget` is a soft wall-clock cap.
    """
    try:
        import requests as _req
        import time as _t
        import concurrent.futures as _cf
    except ImportError:
        return {}

    url = "https://reportapi.eastmoney.com/report/list"
    begin = (date.today() - timedelta(days=180)).isoformat()
    end_str = f"{date.today().year + 1}-01-01"

    valid = [t for t in tickers if _CN_TICKER_RE.match(str(t))]
    if not valid:
        return {}

    def _one(ticker):
        try:
            params = {
                "code": ticker, "pageSize": "50", "pageNo": "1",
                "p": "1", "pageNum": "1", "pageNumber": "1", "qType": "0",
                "industryCode": "*", "industry": "*", "rating": "*",
                "ratingChange": "*", "beginTime": begin, "endTime": end_str,
                "orgCode": "", "rcode": "", "fields": "",
            }
            r = _req.get(url, params=params, timeout=per_ticker_timeout)
            rows = r.json().get("data") or []
            pts = [float(row["indvAimPriceT"]) for row in rows
                   if str(row.get("indvAimPriceT", "")).strip() not in ("", "0", "null", "None")
                   and float(row.get("indvAimPriceT", 0) or 0) > 0]
            if pts:
                pts.sort()
                return ticker, pts[len(pts) // 2]
        except Exception:
            pass
        return ticker, None

    targets = {}
    deadline = _t.time() + budget
    with _cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_one, t) for t in valid]
        for fut in _cf.as_completed(futures):
            if _t.time() >= deadline:
                break
            try:
                ticker, val = fut.result(timeout=per_ticker_timeout + 2)
            except Exception:
                continue
            if val is not None:
                targets[ticker] = val
    return targets


def get_cn_price_targets(ticker_price_map, ths_timeout=8, em_budget=20):
    """
    Fetch and compute price targets for CN stocks using two sources:

    Source 1 — Eastmoney research report consensus (actual analyst targets):
      Calls the Eastmoney report API directly to extract indvAimPriceT (analyst-set
      target price) from the last 180 days of reports.  Takes ~3-8s per stock with
      a 2.5s throttle between requests; capped by em_budget seconds total.

    Source 2 — THS consensus EPS × constant-PE model (fallback):
      target = current_price × (next_year_EPS / this_year_EPS)
      Preserves each stock's market-implied PE; handles high-PE growth stocks
      correctly (e.g. stocks at 150x PE get a target reflecting EPS growth, not
      a misleading 15x sector-default multiple).
      Guards against lumpy current-year EPS (one-time gains common in CN biotechs)
      by detecting a >50% EPS drop and using sector default PE as fallback.

    Returns dict: {ticker: target_price}
    """
    if ak is None or not ticker_price_map:
        return {}

    import time as _t
    tickers = list(ticker_price_map.keys())
    this_year  = str(date.today().year)
    next_year  = str(date.today().year + 1)
    year_after = str(date.today().year + 2)

    # ── Source 1: Actual analyst targets from Eastmoney reports ──────────────
    em_targets = _em_analyst_targets(tickers, budget=em_budget)

    # ── Source 2: THS consensus EPS — parallelized per-ticker ────────────────
    ths_eps_this: dict[str, float]  = {}
    ths_eps_next: dict[str, float]  = {}
    ths_eps_after: dict[str, float] = {}
    ths_targets = [t for t in tickers if t not in em_targets]

    def _fetch_ths(ticker):
        try:
            val = ak.stock_profit_forecast_ths(symbol=ticker, indicator="预测年报每股收益")
            return ticker, val
        except Exception:
            return ticker, None

    import concurrent.futures as _cf
    if ths_targets:
        with _cf.ThreadPoolExecutor(max_workers=6) as ex:
            futs = [ex.submit(_fetch_ths, t) for t in ths_targets]
            for fut in _cf.as_completed(futs):
                try:
                    ticker, val = fut.result(timeout=ths_timeout)
                except Exception:
                    continue
                if val is None or val.empty:
                    continue
                val.columns = [c.strip() for c in val.columns]
                for _, row in val.iterrows():
                    yr  = str(row.get("年度", ""))
                    eps = _safe_float(row.get("均值", 0))
                    if yr.startswith(this_year)  and ticker not in ths_eps_this:
                        ths_eps_this[ticker]  = eps
                    if yr.startswith(next_year)  and ticker not in ths_eps_next:
                        ths_eps_next[ticker]  = eps
                    if yr.startswith(year_after) and ticker not in ths_eps_after:
                        ths_eps_after[ticker] = eps
                if ticker not in ths_eps_next and not val.empty:
                    ths_eps_next[ticker] = _safe_float(val.iloc[0].get("均值", 0))

    # ── Compute fallback targets for stocks without EM data ───────────────────
    targets = dict(em_targets)   # start from actual analyst targets

    for ticker in tickers:
        if ticker in targets:
            continue
        price = ticker_price_map.get(ticker) or 0.0
        eps0  = ths_eps_this.get(ticker,  0.0)
        eps1  = ths_eps_next.get(ticker,  0.0)
        eps2  = ths_eps_after.get(ticker, 0.0)

        if eps1 <= 0:
            continue

        if eps0 > 0 and price > 0 and eps1 >= eps0 * 0.5:
            # Normal case: EPS growing or declining moderately — constant-PE model.
            # If the result is below current price but eps2 shows recovery, use
            # the 2027→2028 forward path instead so cyclical stocks (shipping,
            # materials) get a positive target during the trough year.
            t = round(price * eps1 / eps0, 2)
            if t <= price and eps2 > eps1:
                targets[ticker] = round(price * eps2 / eps1, 2)
            else:
                targets[ticker] = t
        elif eps0 > 0 and price > 0 and eps1 < eps0 * 0.5 and eps2 > eps1:
            # Lumpy current-year EPS (e.g. CN biotech one-time milestone in 2026):
            # skip the inflated base year and use the 2027→2028 growth rate instead.
            targets[ticker] = round(price * eps2 / eps1, 2)
        elif eps0 <= 0 and eps1 > 0:
            # Pre-profit stock turning profitable: sector default PE
            targets[ticker] = round(eps1 * _sector_default_pe(ticker), 2)
        # else: no meaningful target can be derived — leave blank

    return targets


def build_cn_report_data(top_n_analysts=20, picks_per_analyst=3):
    """
    Main entry point: returns a list of analyst dicts, each with their
    top recent picks enriched with current price and fundamentals.
    """
    analysts, err = get_top_analysts(top_n=top_n_analysts)
    if err and not analysts:
        return [], err

    # Parallelize the 20 per-analyst picks fetches — each hits Eastmoney
    # once, so sequential would cost ~2-5s × 20 = 40-100s on wall time.
    import concurrent.futures as _cf
    analyst_picks_map = {}
    with _cf.ThreadPoolExecutor(max_workers=6) as ex:
        futs = {
            ex.submit(get_analyst_picks, a["analyst_id"], picks_per_analyst): a["analyst_id"]
            for a in analysts
        }
        for fut in _cf.as_completed(futs):
            analyst_id = futs[fut]
            try:
                analyst_picks_map[analyst_id] = fut.result(timeout=15)
            except Exception:
                analyst_picks_map[analyst_id] = []

    # Build unique ticker→price map for batch target fetch
    ticker_price_map = {}
    for picks in analyst_picks_map.values():
        for p in picks:
            t = p.get("ticker", "")
            price = p.get("current_price") or 0
            if t and t not in ticker_price_map:
                ticker_price_map[t] = price

    # Batch-fetch implied price targets from THS + Eastmoney in parallel
    price_targets = get_cn_price_targets(ticker_price_map) if ticker_price_map else {}

    report_data = []
    for analyst in analysts:
        picks = analyst_picks_map.get(analyst["analyst_id"], [])
        enriched_picks = []
        for pick in picks:
            price = pick.get("current_price") or None
            target = price_targets.get(pick.get("ticker", ""), 0.0)
            upside = None
            if price and target and price > 0 and target > price:
                upside = round((target - price) / price * 100, 1)
            else:
                target = 0.0   # suppress targets at or below current price
            enriched_picks.append({
                **pick,
                "current_price": price,
                "price_target":  target,
                "upside_pct":    upside,
                "fundamentals":  {},
            })

        report_data.append({
            "analyst":     analyst["analyst"],
            "firm":        analyst["firm"],
            "win_rate":    analyst["win_rate"],
            "avg_return":  analyst["avg_return"],
            "total_calls": analyst["total_calls"],
            "picks":       enriched_picks,
        })

    return report_data, None
