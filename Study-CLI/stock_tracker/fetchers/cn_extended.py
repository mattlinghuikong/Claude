"""
Extended China/HK data from akshare — all free, no API key needed.

Sources:
  - Eastmoney 东方财富: fund flows, sector rotation, hot stocks, research reports
  - Xueqiu 雪球:        HK/CN hot stocks by follower attention
  - CNINF 巨潮资讯:    official SSE/SZSE company announcements
"""

import time
import traceback
import concurrent.futures
from datetime import date, timedelta

REPORT_LOOKBACK_DAYS = 30
_CALL_TIMEOUT = 10        # seconds per akshare call before giving up
_CONTEXT_DEADLINE = 45    # absolute wall-clock cap on build_market_context
_CONTEXT_WORKERS = 4      # lower concurrency to avoid Eastmoney rate limits


def _is_recent(date_str, days=REPORT_LOOKBACK_DAYS):
    try:
        cutoff = date.today() - timedelta(days=days)
        return date.fromisoformat(str(date_str)[:10]) >= cutoff
    except Exception:
        return True  # unparseable dates pass through

try:
    import akshare as ak
    import pandas as pd
except ImportError:
    ak = None
    pd = None


def _timed(fn, timeout=_CALL_TIMEOUT):
    """Run fn() with a hard wall-clock timeout. Returns result or raises TimeoutError."""
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = ex.submit(fn)
    try:
        return future.result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        raise TimeoutError(f"akshare call timed out after {timeout}s")
    finally:
        ex.shutdown(wait=False)  # don't block — let the hung thread die on its own


def _retry(fn, retries=1, delay=2, timeout=_CALL_TIMEOUT):
    """Call fn() with timeout, retry once on failure."""
    last_err = None
    for attempt in range(retries + 1):
        try:
            return _timed(fn, timeout=timeout), None
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(delay)
    return None, str(last_err)


def _strip_exchange_prefix(code):
    """Remove SH/SZ/BJ prefix from ticker codes returned by some akshare functions."""
    for prefix in ("SH", "SZ", "BJ"):
        if code.startswith(prefix):
            return code[2:]
    return code

# Popular A-share + HK stocks to pull research reports for
REPORT_UNIVERSE = [
    "600519", "601318", "600036", "300750", "002594",  # Maotai, Ping An, CMB, CATL, BYD
    "000858", "000333", "002415", "601166", "600900",  # Wuliangye, Midea, Hikvision, CITIC, CNOOC
]


def _safe_float(val, default=0.0):
    try:
        return float(str(val).replace("%", "").replace(",", "").replace("亿", ""))
    except Exception:
        return default


# ─────────────────────────────────────────────
# OVERALL MARKET FUND FLOW
# ─────────────────────────────────────────────

def get_market_fund_flow():
    """
    Overall A-share market fund flow (main force net buy/sell).
    Source: Eastmoney.
    """
    if ak is None:
        return {}, "akshare not installed"
    try:
        df = _timed(lambda: ak.stock_market_fund_flow())
        if df is None or df.empty:
            return {}, "No market fund flow data"
        df.columns = [c.strip() for c in df.columns]
        row = df.iloc[0]
        return {col: str(row[col]) for col in df.columns}, None
    except Exception as e:
        return {}, f"market fund flow error: {e}"


# ─────────────────────────────────────────────
# SECTOR FUND FLOWS
# ─────────────────────────────────────────────

def get_sector_fund_flows(indicator="今日"):
    """
    Sector-level fund flow rankings from Eastmoney.
    indicator: "今日" | "5日" | "10日"
    NOTE: Only available during/after A-share trading hours (9:30am–3pm CST).
    Retries dropped — this is an optional context block, not worth burning
    wall time when the endpoint is sluggish.
    """
    if ak is None:
        return [], "akshare not installed"
    try:
        df, err = _retry(
            lambda: ak.stock_sector_fund_flow_rank(indicator=indicator, sector_type="行业资金流"),
            retries=0, delay=0
        )
        if err:
            return [], f"sector fund flow error: {err}"
        if df is None or df.empty:
            return [], "No sector fund flow data (market may be closed)"
        df.columns = [c.strip() for c in df.columns]
        results = []
        for _, row in df.head(10).iterrows():
            entry = {"sector": "", "main_net_inflow": 0.0, "main_net_pct": 0.0,
                     "leading_stock": "", "leading_pct": 0.0}
            for col in df.columns:
                cv = col.strip()
                if "行业" in cv or ("名称" in cv and "股" not in cv):
                    entry["sector"] = str(row[col])
                elif "主力净流入" in cv and "净额" in cv:
                    entry["main_net_inflow"] = _safe_float(row[col])
                elif "主力净流入" in cv and "占比" in cv:
                    entry["main_net_pct"] = _safe_float(row[col])
                elif "领涨" in cv or "涨幅最大" in cv:
                    if "涨幅" in cv or "幅" in cv:
                        entry["leading_pct"] = _safe_float(row[col])
                    else:
                        entry["leading_stock"] = str(row[col])
            results.append(entry)
        return results, None
    except Exception as e:
        return [], f"sector fund flow error: {e}"


def get_top_inflow_stocks(indicator="今日", top_n=20):
    """
    Individual stocks with largest net institutional inflows (主力净流入).
    Source: Eastmoney.
    """
    if ak is None:
        return [], "akshare not installed"
    try:
        df = _timed(lambda: ak.stock_individual_fund_flow_rank(indicator=indicator))
        if df is None or df.empty:
            return [], "No individual fund flow data"
        df.columns = [c.strip() for c in df.columns]
        results = []
        for _, row in df.head(top_n).iterrows():
            entry = {"ticker": "", "name": "", "latest_price": 0.0,
                     "change_pct": 0.0, "main_net_inflow": 0.0, "main_net_pct": 0.0}
            for col in df.columns:
                cv = col.strip()
                if cv == "代码":
                    entry["ticker"] = str(row[col])
                elif cv == "名称":
                    entry["name"] = str(row[col])
                elif "最新价" in cv:
                    entry["latest_price"] = _safe_float(row[col])
                elif "涨跌幅" in cv:
                    entry["change_pct"] = _safe_float(row[col])
                elif "主力净流入" in cv and "净额" in cv:
                    entry["main_net_inflow"] = _safe_float(row[col])
                elif "主力净流入" in cv and "占比" in cv:
                    entry["main_net_pct"] = _safe_float(row[col])
            results.append(entry)
        return results, None
    except Exception as e:
        return [], f"individual fund flow error: {e}"


# ─────────────────────────────────────────────
# HOT STOCKS — Eastmoney + Xueqiu + HK
# ─────────────────────────────────────────────

def get_hot_stocks_em(top_n=20):
    """Hot A-share stocks by search popularity on Eastmoney."""
    if ak is None:
        return []
    try:
        df = _timed(lambda: ak.stock_hot_rank_em())
        if df is None or df.empty:
            return []
        df.columns = [c.strip() for c in df.columns]
        results = []
        for _, row in df.head(top_n).iterrows():
            entry = {"rank": 0, "ticker": "", "name": "", "latest_price": 0.0,
                     "change_pct": 0.0, "hot_value": ""}
            for col in df.columns:
                cv = col.strip()
                if "排名" in cv or cv == "序号":
                    entry["rank"] = int(_safe_float(row[col]))
                elif cv == "代码":
                    entry["ticker"] = _strip_exchange_prefix(str(row[col]))
                elif "名称" in cv:
                    entry["name"] = str(row[col])
                elif "最新价" in cv:
                    entry["latest_price"] = _safe_float(row[col])
                elif "涨跌幅" in cv:
                    entry["change_pct"] = _safe_float(row[col])
                elif "热度" in cv:
                    entry["hot_value"] = str(row[col])
            results.append(entry)
        return results
    except Exception:
        return []


def get_hot_stocks_xueqiu(top_n=20):
    """Hot stocks by follower attention on Xueqiu 雪球."""
    if ak is None:
        return []
    for symbol in ("最热门", "最多关注", "沪深A股"):
        try:
            df = _timed(lambda: ak.stock_hot_follow_xq(symbol=symbol))
            if df is not None and not df.empty:
                df.columns = [c.strip() for c in df.columns]
                results = []
                for _, row in df.head(top_n).iterrows():
                    entry = {"rank": 0, "ticker": "", "name": "",
                             "follow_count": "", "change_pct": 0.0}
                    for col in df.columns:
                        cv = col.strip()
                        if "序号" in cv or "排名" in cv:
                            entry["rank"] = int(_safe_float(row[col]))
                        elif "代码" in cv:
                            entry["ticker"] = str(row[col])
                        elif "名称" in cv:
                            entry["name"] = str(row[col])
                        elif "关注" in cv:
                            entry["follow_count"] = str(row[col])
                        elif "涨跌幅" in cv:
                            entry["change_pct"] = _safe_float(row[col])
                    results.append(entry)
                return results
        except Exception:
            continue
    return []


def get_hk_hot_stocks_em(top_n=20):
    """Hot HK stocks by attention on Eastmoney."""
    if ak is None:
        return []
    try:
        df = _timed(lambda: ak.stock_hk_hot_rank_em())
        if df is None or df.empty:
            return []
        df.columns = [c.strip() for c in df.columns]
        results = []
        for _, row in df.head(top_n).iterrows():
            entry = {"rank": 0, "ticker": "", "name": "", "latest_price": 0.0,
                     "change_pct": 0.0}
            for col in df.columns:
                cv = col.strip()
                if "排名" in cv or "序号" in cv:
                    entry["rank"] = int(_safe_float(row[col]))
                elif "代码" in cv:
                    entry["ticker"] = str(row[col])
                elif "名称" in cv:
                    entry["name"] = str(row[col])
                elif "最新价" in cv or "现价" in cv:
                    entry["latest_price"] = _safe_float(row[col])
                elif "涨跌幅" in cv:
                    entry["change_pct"] = _safe_float(row[col])
            results.append(entry)
        return results
    except Exception:
        return []


# ─────────────────────────────────────────────
# RESEARCH REPORTS — per-stock from Eastmoney
# ─────────────────────────────────────────────

def get_research_reports_for_stock(ticker, top_n=5):
    """
    Analyst research reports for a specific A-share from Eastmoney.
    Returns title, analyst, firm, rating, price target, date.
    """
    if ak is None:
        return []
    try:
        df, err = _retry(lambda: ak.stock_research_report_em(symbol=ticker), retries=1, delay=3)
        if err or df is None:
            return []
        if df is None or df.empty:
            return []
        df.columns = [c.strip() for c in df.columns]
        results = []
        for _, row in df.iterrows():
            if len(results) >= top_n:
                break
            # Extract date first so we can filter before building full entry
            row_date = ""
            for col in df.columns:
                if "日期" in col.strip():
                    row_date = str(row[col])
                    break
            if not _is_recent(row_date):
                continue
            entry = {"title": "", "analyst": "", "firm": "",
                     "rating": "", "price_target": 0.0, "date": row_date, "summary": ""}
            for col in df.columns:
                cv = col.strip()
                if "报告" in cv and "名称" in cv or "标题" in cv:
                    entry["title"] = str(row[col])
                elif "分析师" in cv:
                    entry["analyst"] = str(row[col])
                elif "机构" in cv or "研究机构" in cv:
                    entry["firm"] = str(row[col])
                elif "评级" in cv:
                    entry["rating"] = str(row[col])
                elif "目标价" in cv:
                    entry["price_target"] = _safe_float(row[col])
                elif "摘要" in cv:
                    entry["summary"] = str(row[col])[:200]
            results.append(entry)
        return results
    except Exception:
        return []


def _reports_for_ticker(ticker, max_per_stock, buy_keywords):
    """Worker: fetch recent buy-rated research reports for one ticker."""
    out = []
    try:
        df, err = _retry(lambda: ak.stock_research_report_em(symbol=ticker), retries=0, delay=0)
        if err or df is None or df.empty:
            return out
        df.columns = [c.strip() for c in df.columns]
        count = 0
        for _, row in df.iterrows():
            if count >= max_per_stock:
                break
            row_date = ""
            for col in df.columns:
                if "日期" in col.strip():
                    row_date = str(row[col])
                    break
            if not _is_recent(row_date):
                continue
            rating = ""
            for col in df.columns:
                if "评级" in col.strip():
                    rating = str(row[col]).strip()
                    break
            if rating and not any(k in rating for k in buy_keywords):
                continue
            entry = {"ticker": ticker, "name": "", "title": "", "analyst": "",
                     "firm": "", "rating": rating, "price_target": 0.0, "date": ""}
            for col in df.columns:
                cv = col.strip()
                if "名称" in cv and "股票" in cv:
                    entry["name"] = str(row[col])
                elif ("报告" in cv and "名称" in cv) or "标题" in cv:
                    entry["title"] = str(row[col])
                elif "分析师" in cv:
                    entry["analyst"] = str(row[col])
                elif "机构" in cv:
                    entry["firm"] = str(row[col])
                elif "目标价" in cv:
                    entry["price_target"] = _safe_float(row[col])
                elif "日期" in cv:
                    entry["date"] = str(row[col])
            out.append(entry)
            count += 1
    except Exception:
        return out
    return out


def get_latest_reports_for_universe(symbols=None, max_per_stock=2, workers=6):
    """
    Pull recent buy-rated research reports for a universe of popular stocks.
    Parallelized across `workers` concurrent per-ticker fetches.
    Returns flat list sorted by date descending.
    """
    if ak is None:
        return [], "akshare not installed"
    universe = symbols or REPORT_UNIVERSE
    buy_keywords = {"买入", "增持", "强烈推荐", "推荐", "强买"}
    all_reports = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_reports_for_ticker, t, max_per_stock, buy_keywords)
                   for t in universe]
        for fut in concurrent.futures.as_completed(futures):
            try:
                rows = fut.result(timeout=_CALL_TIMEOUT + 5)
            except Exception:
                rows = []
            all_reports.extend(rows)

    all_reports.sort(key=lambda x: x.get("date", ""), reverse=True)
    return all_reports, None


# ─────────────────────────────────────────────
# COMPOSITE CONTEXT BUILDER
# ─────────────────────────────────────────────

def build_market_context():
    """
    Fetch macro-level market context. Each fetch is independent, so we run
    them in parallel — the overall wall time drops to ~max() of the slowest
    call instead of the sum of all of them.
    """
    # Each entry: (ctx_key, callable, postprocess). postprocess extracts the
    # stored value from whatever the fetcher returns.
    tasks = [
        ("market_fund_flow",    lambda: get_market_fund_flow(),                      lambda r: r[0] if isinstance(r, tuple) else r),
        ("sector_flows_today",  lambda: get_sector_fund_flows(indicator="今日"),      lambda r: r[0] if isinstance(r, tuple) else r),
        ("sector_flows_5d",     lambda: get_sector_fund_flows(indicator="5日"),       lambda r: r[0] if isinstance(r, tuple) else r),
        ("top_inflow_stocks",   lambda: get_top_inflow_stocks(indicator="今日", top_n=15), lambda r: r[0] if isinstance(r, tuple) else r),
        ("hot_em",              lambda: get_hot_stocks_em(top_n=15),                 lambda r: r),
        ("hot_xq",              lambda: get_hot_stocks_xueqiu(top_n=15),             lambda r: r),
        ("hk_hot",              lambda: get_hk_hot_stocks_em(top_n=10),              lambda r: r),
        ("latest_reports",      lambda: get_latest_reports_for_universe(max_per_stock=2), lambda r: r[0] if isinstance(r, tuple) else r),
    ]

    import time as _t
    deadline = _t.time() + _CONTEXT_DEADLINE
    ctx = {key: ({} if key == "market_fund_flow" else []) for key, _, _ in tasks}

    with concurrent.futures.ThreadPoolExecutor(max_workers=_CONTEXT_WORKERS) as ex:
        future_to_key = {
            ex.submit(fn): (key, post) for key, fn, post in tasks
        }
        for fut in concurrent.futures.as_completed(future_to_key):
            key, post = future_to_key[fut]
            remaining = max(1.0, deadline - _t.time())
            try:
                raw = fut.result(timeout=min(_CALL_TIMEOUT + 5, remaining))
                ctx[key] = post(raw)
            except Exception:
                pass  # keep the empty default
            if _t.time() >= deadline:
                # Hard stop — cancel anything unstarted; in-flight calls
                # will wrap up on their own timeouts.
                for other in future_to_key:
                    if not other.done():
                        other.cancel()
                break
    return ctx
