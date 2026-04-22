"""
aggregator.py — Unified priority-ranked stock list from all three markets.

Takes raw outputs from:
  - build_cn_report_data()  → cn_data  (list of analyst-entry dicts, analyst→picks)
  - build_us_report_data()  → us_stocks (list of stock dicts)
  - build_hk_report_data()  → hk_stocks (list of stock dicts)

Inverts CN data from analyst→picks into ticker→stock with recommending analysts,
collects upgrade/firm info for US/HK, computes a priority score, assigns tiers,
and returns a single structured dict ready for any renderer.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def _dedupe_firms(entries: list[dict], key: str = "firm") -> list[dict]:
    """Keep the first occurrence of each firm name (case-insensitive)."""
    seen: set[str] = set()
    out: list[dict] = []
    for e in entries:
        firm_lower = str(e.get(key, "")).strip().lower()
        if firm_lower and firm_lower not in seen:
            seen.add(firm_lower)
            out.append(e)
        elif not firm_lower:
            out.append(e)   # keep entries with no firm name rather than silently drop
    return out


# ---------------------------------------------------------------------------
# CN inversion: analyst→picks  →  ticker→stock
# ---------------------------------------------------------------------------

def _invert_cn_data(cn_data: list[dict]) -> dict[str, dict]:
    """
    cn_data is a list of analyst-entry dicts:
        {analyst, firm, win_rate, avg_return, total_calls, picks: [...]}

    Each pick is:
        {ticker, name, rating, price_target, report_title, report_date,
         current_price, upside_pct, fundamentals}

    Returns a dict keyed by ticker; each value is a merged stock dict with
    a 'recommenders' list containing the analysts who picked it.
    """
    ticker_map: dict[str, dict] = {}

    for analyst_entry in cn_data:
        analyst_name = analyst_entry.get("analyst", "")
        firm = analyst_entry.get("firm", "")
        win_rate = _safe_float(analyst_entry.get("win_rate", 0))
        avg_return = _safe_float(analyst_entry.get("avg_return", 0))

        for pick in analyst_entry.get("picks", []):
            ticker = str(pick.get("ticker", "")).strip()
            if not ticker:
                continue

            recommender_entry = {
                "name": analyst_name,
                "firm": firm,
                "win_rate": win_rate,
                "avg_return": avg_return,
            }

            if ticker not in ticker_map:
                # First time we see this ticker — seed the stock record
                ticker_map[ticker] = {
                    "ticker": ticker,
                    "name": pick.get("name", ticker),
                    "sector": "",
                    "industry": "",
                    "current_price": pick.get("current_price"),
                    "price_target": _safe_float(pick.get("price_target", 0)) or None,
                    "upside_pct": pick.get("upside_pct"),
                    "rating": pick.get("rating", ""),
                    "report_title": pick.get("report_title", ""),
                    "report_date": pick.get("report_date", ""),
                    "fundamentals": pick.get("fundamentals", {}),
                    # US/HK fields — not applicable for CN
                    "rec_mean": None,
                    "n_analysts": 0,
                    "pe_ratio": None,
                    "forward_pe": None,
                    "revenue_growth": None,
                    "earnings_growth": None,
                    "profit_margin": None,
                    "debt_to_equity": None,
                    "roe": None,
                    "eps_forward": None,
                    "eps_ttm": None,
                    "52w_high": None,
                    "52w_low": None,
                    "market_cap": None,
                    "business_summary": "",
                    "market": "CN",
                    "recommenders": [recommender_entry],
                }
            else:
                # Ticker already seen — add this analyst if not duplicated
                existing = ticker_map[ticker]
                # Avoid same analyst appearing twice
                existing_names = {r["name"] for r in existing["recommenders"]}
                if analyst_name not in existing_names:
                    existing["recommenders"].append(recommender_entry)

                # Keep the best (most recent / highest target) data
                existing_upside = existing.get("upside_pct")
                new_upside = pick.get("upside_pct")
                if new_upside is not None and (
                    existing_upside is None or new_upside > existing_upside
                ):
                    existing["upside_pct"] = new_upside
                    existing["price_target"] = _safe_float(pick.get("price_target", 0)) or existing.get("price_target")

    return ticker_map


# ---------------------------------------------------------------------------
# US/HK recommender collection
# ---------------------------------------------------------------------------

def _collect_us_hk_recommenders(stock: dict) -> list[dict]:
    """
    Gather upgrade/recommendation entries from recent_upgrades, finviz_upgrades,
    and fmp_upgrades, deduplicating by firm name.

    Returns a list of {firm, action, to_grade, date} dicts.
    """
    raw: list[dict] = []

    # yfinance recent upgrades
    for u in stock.get("recent_upgrades", []):
        raw.append({
            "firm": str(u.get("firm", "")).strip(),
            "action": str(u.get("action", "")).strip(),
            "to_grade": str(u.get("to_grade", "")).strip(),
            "from_grade": str(u.get("from_grade", "")).strip(),
            "date": str(u.get("date", "")).strip(),
            "_source": "yf",
        })

    # Finviz upgrades
    for u in stock.get("finviz_upgrades", []):
        # Finviz uses 'brokerage' and 'rating_change' keys
        raw.append({
            "firm": str(u.get("brokerage", "")).strip(),
            "action": str(u.get("action", "")).strip(),
            "to_grade": str(u.get("rating_change", "")).strip(),
            "from_grade": "",
            "date": str(u.get("date", "")).strip(),
            "_source": "finviz",
        })

    # FMP upgrades
    for u in stock.get("fmp_upgrades", []):
        raw.append({
            "firm": str(u.get("brokerage", u.get("gradingCompany", ""))).strip(),
            "action": str(u.get("action", "")).strip(),
            "to_grade": str(u.get("to_grade", u.get("newGrade", ""))).strip(),
            "from_grade": str(u.get("from_grade", u.get("previousGrade", ""))).strip(),
            "date": str(u.get("date", "")).strip(),
            "_source": "fmp",
        })

    # Deduplicate by firm (keep first occurrence = most recent source priority)
    deduped = _dedupe_firms(raw, key="firm")
    # Strip internal _source key before returning
    return [{k: v for k, v in r.items() if k != "_source"} for r in deduped]


# ---------------------------------------------------------------------------
# Priority score
# ---------------------------------------------------------------------------

def _compute_priority_score(
    recommender_count: int,
    upside_pct: float | None,
    rec_mean: float | None,
    market: str,
    recommenders: list[dict],
) -> float:
    """
    Priority formula:
      recommender_count * 20
      + min(upside_pct, 80)                      (capped)
      + (5 - rec_mean) * 8                       (US/HK only, rec_mean 1=strong buy)
      + avg_win_rate / 10                         (CN only)
    """
    score: float = recommender_count * 20

    upside = _safe_float(upside_pct, 0.0)
    score += min(upside, 80.0)

    if market in ("US", "HK") and rec_mean is not None:
        score += (5.0 - _safe_float(rec_mean, 3.0)) * 8.0
    elif market == "CN" and recommenders:
        win_rates = [_safe_float(r.get("win_rate", 0)) for r in recommenders]
        avg_win = sum(win_rates) / len(win_rates) if win_rates else 0.0
        score += avg_win / 10.0

    return round(score, 2)


# ---------------------------------------------------------------------------
# Tier assignment
# ---------------------------------------------------------------------------

def _assign_tier(
    recommender_count: int,
    upside_pct: float | None,
    rec_mean: float | None,
    market: str,
) -> int:
    """
    Tier 1 "High Conviction":
        recommender_count >= 3
        OR (recommender_count >= 2 AND upside >= 25)

    Tier 2 "Strong Picks":
        recommender_count >= 2
        OR (recommender_count == 1 AND upside >= 20 AND rec_mean <= 1.8 [US/HK])

    Tier 3 "Watch List": everything else
    """
    upside = _safe_float(upside_pct, 0.0)

    # Tier 1
    if recommender_count >= 3:
        return 1
    if recommender_count >= 2 and upside >= 25:
        return 1

    # Tier 2
    if recommender_count >= 2:
        return 2
    if recommender_count == 1 and upside >= 20:
        if market in ("US", "HK"):
            if rec_mean is not None and _safe_float(rec_mean, 3.0) <= 1.8:
                return 2
        else:
            # CN — upside alone is enough for tier 2 if single recommender
            return 2

    return 3


# ---------------------------------------------------------------------------
# Enrich a stock dict with aggregator fields
# ---------------------------------------------------------------------------

def _enrich_stock(stock: dict) -> dict:
    """
    Add recommender_count, priority_score, tier to an already-enriched stock dict
    (one that already has 'recommenders' and 'market').
    Returns the mutated dict.
    """
    recommenders = stock.get("recommenders", [])
    recommender_count = len(recommenders)
    upside_pct = stock.get("upside_pct")
    rec_mean = stock.get("rec_mean")
    market = stock.get("market", "US")

    priority_score = _compute_priority_score(
        recommender_count, upside_pct, rec_mean, market, recommenders
    )
    tier = _assign_tier(recommender_count, upside_pct, rec_mean, market)

    stock["recommender_count"] = recommender_count
    stock["priority_score"] = priority_score
    stock["tier"] = tier
    return stock


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def aggregate_all(
    us_stocks: list[dict],
    hk_stocks: list[dict],
    cn_data: list[dict],
) -> dict:
    """
    Aggregate stocks from all three markets into a unified, priority-ranked
    structure ready for rendering.

    Parameters
    ----------
    us_stocks : list[dict]
        Output of build_us_report_data() (optionally enriched by Finviz/FMP).
    hk_stocks : list[dict]
        Output of build_hk_report_data() (optionally enriched by FMP).
    cn_data : list[dict]
        Output of build_cn_report_data() — analyst→picks structure.

    Returns
    -------
    dict with keys:
        "tier1"  : list[dict]  — High Conviction stocks, sorted by priority_score desc
        "tier2"  : list[dict]  — Strong Picks, sorted by priority_score desc
        "tier3"  : list[dict]  — Watch List, sorted by priority_score desc
        "stats"  : dict        — Summary statistics
    """
    all_enriched: list[dict] = []

    # ── US stocks ────────────────────────────────────────────────────────────
    for stock in (us_stocks or []):
        if "error" in stock:
            continue
        s = dict(stock)  # shallow copy — don't mutate caller's data
        s["market"] = "US"
        s["recommenders"] = _collect_us_hk_recommenders(s)
        _enrich_stock(s)
        all_enriched.append(s)

    # ── HK stocks ────────────────────────────────────────────────────────────
    for stock in (hk_stocks or []):
        if "error" in stock:
            continue
        s = dict(stock)
        s["market"] = "HK"
        s["recommenders"] = _collect_us_hk_recommenders(s)
        _enrich_stock(s)
        all_enriched.append(s)

    # ── CN stocks (invert analyst→picks) ─────────────────────────────────────
    cn_ticker_map = _invert_cn_data(cn_data or [])
    for ticker, stock in cn_ticker_map.items():
        _enrich_stock(stock)
        all_enriched.append(stock)

    # ── Partition into tiers and sort within each ─────────────────────────────
    tier1: list[dict] = []
    tier2: list[dict] = []
    tier3: list[dict] = []

    for s in all_enriched:
        t = s.get("tier", 3)
        if t == 1:
            tier1.append(s)
        elif t == 2:
            tier2.append(s)
        else:
            tier3.append(s)

    tier1.sort(key=lambda x: x.get("priority_score", 0), reverse=True)
    tier2.sort(key=lambda x: x.get("priority_score", 0), reverse=True)
    tier3.sort(key=lambda x: x.get("priority_score", 0), reverse=True)

    # ── Stats ─────────────────────────────────────────────────────────────────
    all_stocks = tier1 + tier2 + tier3
    total = len(all_stocks)
    total_us = sum(1 for s in all_stocks if s.get("market") == "US")
    total_hk = sum(1 for s in all_stocks if s.get("market") == "HK")
    total_cn = sum(1 for s in all_stocks if s.get("market") == "CN")

    upsides = [
        _safe_float(s.get("upside_pct"))
        for s in all_stocks
        if s.get("upside_pct") is not None
    ]
    top_upside = round(max(upsides), 1) if upsides else 0.0

    recommender_counts = [s.get("recommender_count", 0) for s in all_stocks]
    avg_recommenders = (
        round(sum(recommender_counts) / len(recommender_counts), 2)
        if recommender_counts else 0.0
    )

    stats = {
        "total_stocks": total,
        "total_us": total_us,
        "total_hk": total_hk,
        "total_cn": total_cn,
        "top_upside": top_upside,
        "avg_recommenders": avg_recommenders,
    }

    return {
        "tier1": tier1,
        "tier2": tier2,
        "tier3": tier3,
        "stats": stats,
    }
