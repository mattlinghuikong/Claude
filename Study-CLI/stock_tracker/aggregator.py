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
    stock: dict | None = None,
) -> float:
    """
    Priority formula (money-making weighted):
      analyst_conviction = recommender_count * 20 + min(upside, 80) + (5 - rec_mean) * 8
      momentum           = +12 if 3m return in [5, 40], +6 if above 200DMA
                           -15 if 3m return < -15 (falling knife)
      quality            = +6 if margin >=15% AND ROE >=15%
                           -12 if rev_growth < -10% (revenue shrinking)
      valuation          = -10 if forward_pe > 60 without earnings growth >25%
      catalyst           = +8 if earnings within next 30 days
      analyst_quality    = CN only — weight recommenders by their historical
                           win rate instead of raw count.
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
        # Extra credit when multiple *high-accuracy* analysts converge
        high_wr = sum(1 for wr in win_rates if wr >= 60)
        score += high_wr * 6.0

    if stock is None:
        return round(score, 2)

    # ── Momentum ────────────────────────────────────────────────────────────
    ret_3m = stock.get("ret_3m")
    if ret_3m is not None:
        if 5.0 <= ret_3m <= 40.0:
            score += 12.0         # established uptrend
        elif ret_3m < -15.0:
            score -= 15.0         # falling knife — avoid
    if stock.get("above_200dma") is True:
        score += 6.0

    # ── Quality filter ──────────────────────────────────────────────────────
    margin = _safe_float(stock.get("profit_margin"), 0.0)
    roe = _safe_float(stock.get("roe"), 0.0)
    if margin >= 0.15 and roe >= 0.15:
        score += 6.0
    rev_g = stock.get("revenue_growth")
    if rev_g is not None and _safe_float(rev_g) < -0.10:
        score -= 12.0

    # ── Valuation sanity ────────────────────────────────────────────────────
    fpe = _safe_float(stock.get("forward_pe"), 0.0)
    earn_g = _safe_float(stock.get("earnings_growth"), 0.0)
    if fpe > 60.0 and earn_g < 0.25:
        score -= 10.0             # expensive without the growth to justify it

    # ── Catalyst: earnings announcement coming up ──────────────────────────
    eid = stock.get("earnings_in_days")
    if eid is not None and 0 <= eid <= 30:
        score += 8.0

    return round(score, 2)


# ---------------------------------------------------------------------------
# Tier assignment
# ---------------------------------------------------------------------------

def _assign_tier(
    recommender_count: int,
    upside_pct: float | None,
    rec_mean: float | None,
    market: str,
    stock: dict | None = None,
) -> int:
    """
    Tier 1 "High Conviction":
        recommender_count >= 3
        OR (recommender_count >= 2 AND upside >= 25)

    Tier 2 "Strong Picks":
        recommender_count >= 2
        OR (recommender_count == 1 AND upside >= 20 AND rec_mean <= 1.8 [US/HK])

    Tier 3 "Watch List": everything else

    Demotion rules (avoid putting risky stocks in tier 1/2):
        - 3-month return <= -20%  → demote one tier (falling knife)
        - Revenue growth <= -15%  → demote to tier 3 (deteriorating business)
    """
    upside = _safe_float(upside_pct, 0.0)

    # Compute the base tier from analyst conviction.
    if recommender_count >= 3:
        tier = 1
    elif recommender_count >= 2 and upside >= 25:
        tier = 1
    elif recommender_count >= 2:
        tier = 2
    elif recommender_count == 1 and upside >= 20:
        if market in ("US", "HK"):
            tier = 2 if (rec_mean is not None and _safe_float(rec_mean, 3.0) <= 1.8) else 3
        else:
            # CN — upside alone is enough for tier 2 if single recommender
            tier = 2
    else:
        tier = 3

    # Risk-based demotion.
    if stock is not None:
        ret_3m = stock.get("ret_3m")
        if ret_3m is not None and ret_3m <= -20.0 and tier < 3:
            tier += 1
        rev_g = stock.get("revenue_growth")
        if rev_g is not None and _safe_float(rev_g) <= -0.15:
            tier = 3

    return tier


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
        recommender_count, upside_pct, rec_mean, market, recommenders, stock
    )
    tier = _assign_tier(recommender_count, upside_pct, rec_mean, market, stock)

    stock["recommender_count"] = recommender_count
    stock["priority_score"] = priority_score
    stock["tier"] = tier
    return stock


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def select_market_top(
    stocks: list[dict],
    market: str,
    limit: int = 10,
    industry_cap: int = 2,
) -> list[dict]:
    """Greedy-pick the top N stocks for one market with industry diversification.

    Order: highest priority_score first. Stop when we've hit `limit` picks, or
    when we can't add a new stock without breaching `industry_cap` for its
    industry. CN stocks usually have blank industry — those are treated as
    unique (no cap applied).
    """
    filtered = [s for s in stocks if s.get("market") == market]
    filtered.sort(key=lambda x: x.get("priority_score", 0), reverse=True)

    picks: list[dict] = []
    industry_counts: dict[str, int] = {}
    for s in filtered:
        industry = (s.get("industry") or s.get("sector") or "").strip().lower()
        if industry:
            if industry_counts.get(industry, 0) >= industry_cap:
                continue
        picks.append(s)
        if industry:
            industry_counts[industry] = industry_counts.get(industry, 0) + 1
        if len(picks) >= limit:
            break
    return picks


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
        "all": all_stocks,
        "stats": stats,
    }
