"""
report_html_generator.py — Self-contained Apple-style HTML report generator.

Generates a beautiful, fully offline HTML file from the aggregated stock data
produced by aggregator.aggregate_all().

Usage:
    from aggregator import aggregate_all
    from report_html_generator import generate_html, save_html

    aggregated = aggregate_all(us_stocks, hk_stocks, cn_data)
    html = generate_html(aggregated, market_ctx)
    path = save_html(html)
"""

from __future__ import annotations

import os
import re
from datetime import date
from html import escape
from typing import Any

from config import REPORTS_DIR
from aggregator import select_market_top

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _safe_date(value: str | None) -> str:
    """Return value if it's a plain YYYY-MM-DD, else today's date.
    Prevents ../ path traversal when the date is used in a filename."""
    if value and _ISO_DATE_RE.match(value):
        return value
    return date.today().isoformat()


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _fmt_price(val: Any, currency: str = "$") -> str:
    try:
        f = float(val)
        if f == 0:
            return "N/A"
        return f"{currency}{f:,.2f}"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_pct(val: Any, plus: bool = True) -> str:
    try:
        f = float(val)
        sign = "+" if (plus and f >= 0) else ""
        return f"{sign}{f:.1f}%"
    except (TypeError, ValueError):
        return "N/A"


def _fmt_large(val: Any) -> str:
    """Format a large number (market cap) as $XB or $XM."""
    try:
        v = float(val)
        if v >= 1e12:
            return f"${v/1e12:.1f}T"
        if v >= 1e9:
            return f"${v/1e9:.1f}B"
        if v >= 1e6:
            return f"${v/1e6:.0f}M"
        return f"${v:,.0f}"
    except (TypeError, ValueError):
        return "N/A"


def _rec_label(rec_mean: Any) -> str:
    try:
        r = float(rec_mean)
        if r <= 1.5:
            return "Strong Buy"
        if r <= 2.5:
            return "Buy"
        if r <= 3.5:
            return "Hold"
        return "Sell"
    except (TypeError, ValueError):
        return "N/A"


def _upside_class(upside: Any) -> str:
    try:
        u = float(upside)
        if u >= 25:
            return "upside-high"
        if u >= 10:
            return "upside-mid"
        return "upside-low"
    except (TypeError, ValueError):
        return "upside-low"


def _upside_arrow(upside: Any) -> str:
    try:
        u = float(upside)
        return "▲" if u >= 0 else "▼"
    except (TypeError, ValueError):
        return ""


def _market_badge(market: str) -> str:
    classes = {
        "US": "badge-us",
        "HK": "badge-hk",
        "CN": "badge-cn",
    }
    cls = classes.get(market, "badge-us")
    return f'<span class="market-badge {cls}">{escape(market)}</span>'


def _tier_accent_class(tier: int) -> str:
    return {1: "tier-gold", 2: "tier-blue", 3: "tier-gray"}.get(tier, "tier-gray")


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

def _css() -> str:
    return """
    /* ── Reset & Base ─────────────────────────────────────────────────── */
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    :root {
        --bg:            #f5f5f7;
        --card:          #ffffff;
        --text-primary:  #1d1d1f;
        --text-secondary:#6e6e73;
        --accent:        #0071e3;
        --green:         #34c759;
        --red:           #ff3b30;
        --yellow:        #ff9f0a;
        --gold:          #FFD700;
        --border:        rgba(0,0,0,0.08);
        --shadow:        0 4px 24px rgba(0,0,0,0.08);
        --radius:        20px;
        --radius-sm:     12px;
        --radius-xs:     8px;
        --font:          -apple-system, BlinkMacSystemFont, "SF Pro Display",
                         "Helvetica Neue", Arial, sans-serif;
    }

    html { font-size: 16px; }

    body {
        font-family: var(--font);
        background: var(--bg);
        color: var(--text-primary);
        -webkit-font-smoothing: antialiased;
        -moz-osx-font-smoothing: grayscale;
        line-height: 1.5;
    }

    /* ── Hero Header ──────────────────────────────────────────────────── */
    .hero {
        background: #1d1d1f;
        color: #f5f5f7;
        padding: 72px 48px 60px;
        text-align: center;
        position: relative;
        overflow: hidden;
    }
    .hero::before {
        content: "";
        position: absolute;
        inset: 0;
        background: radial-gradient(ellipse 80% 60% at 50% -20%,
                    rgba(0,113,227,0.22) 0%, transparent 70%);
        pointer-events: none;
    }
    .hero-eyebrow {
        font-size: 13px;
        font-weight: 600;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--accent);
        margin-bottom: 14px;
    }
    .hero-title {
        font-size: clamp(40px, 6vw, 72px);
        font-weight: 700;
        letter-spacing: -0.03em;
        line-height: 1.05;
        color: #f5f5f7;
        margin-bottom: 12px;
    }
    .hero-date {
        font-size: 18px;
        color: rgba(245,245,247,0.55);
        font-weight: 400;
        margin-bottom: 48px;
    }

    /* Hero stat pills */
    .hero-stats {
        display: flex;
        justify-content: center;
        gap: 16px;
        flex-wrap: wrap;
    }
    .hero-stat {
        background: rgba(255,255,255,0.07);
        border: 1px solid rgba(255,255,255,0.10);
        border-radius: 100px;
        padding: 14px 28px;
        min-width: 160px;
        backdrop-filter: blur(8px);
        -webkit-backdrop-filter: blur(8px);
    }
    .hero-stat-value {
        font-size: 28px;
        font-weight: 700;
        color: #f5f5f7;
        letter-spacing: -0.02em;
        display: block;
    }
    .hero-stat-label {
        font-size: 12px;
        color: rgba(245,245,247,0.5);
        letter-spacing: 0.05em;
        text-transform: uppercase;
        display: block;
        margin-top: 2px;
    }

    /* ── Page Container ───────────────────────────────────────────────── */
    .container {
        max-width: 1280px;
        margin: 0 auto;
        padding: 0 32px;
    }

    /* ── Section Headers ──────────────────────────────────────────────── */
    .section {
        padding: 56px 0 16px;
    }
    .section-header {
        display: flex;
        align-items: baseline;
        gap: 14px;
        margin-bottom: 32px;
    }
    .section-title {
        font-size: 28px;
        font-weight: 700;
        letter-spacing: -0.02em;
        color: var(--text-primary);
    }
    .section-count {
        font-size: 14px;
        font-weight: 500;
        color: var(--text-secondary);
        background: rgba(0,0,0,0.05);
        border-radius: 100px;
        padding: 3px 12px;
    }

    /* ── Stock Card Grid ──────────────────────────────────────────────── */
    .card-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(380px, 1fr));
        gap: 24px;
    }

    /* ── Stock Card ───────────────────────────────────────────────────── */
    .stock-card {
        background: var(--card);
        border-radius: var(--radius);
        box-shadow: var(--shadow);
        border: 1px solid var(--border);
        overflow: hidden;
        transition: transform 0.25s cubic-bezier(0.25, 0.46, 0.45, 0.94),
                    box-shadow 0.25s cubic-bezier(0.25, 0.46, 0.45, 0.94);
        display: flex;
        flex-direction: column;
    }
    .stock-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 12px 40px rgba(0,0,0,0.12);
    }
    .stock-card.tier-gold  { border-top: 3px solid var(--gold); }
    .stock-card.tier-blue  { border-top: 3px solid var(--accent); }
    .stock-card.tier-gray  { border-top: 3px solid #d2d2d7; }

    /* Card Header */
    .card-header {
        padding: 20px 22px 16px;
        border-bottom: 1px solid rgba(0,0,0,0.05);
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 10px;
    }
    .card-header-left { flex: 1; min-width: 0; }
    .card-ticker {
        font-size: 22px;
        font-weight: 700;
        letter-spacing: -0.02em;
        color: var(--text-primary);
        display: inline-block;
        margin-right: 8px;
    }
    .card-name {
        font-size: 13px;
        color: var(--text-secondary);
        margin-top: 3px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .card-sector {
        font-size: 11px;
        color: var(--text-secondary);
        margin-top: 3px;
        opacity: 0.75;
    }

    /* Market Badges */
    .market-badge {
        display: inline-block;
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.06em;
        padding: 3px 9px;
        border-radius: 100px;
        vertical-align: middle;
    }
    .badge-us { background: rgba(0,113,227,0.10); color: #0071e3; }
    .badge-hk { background: rgba(255,59,148,0.10); color: #e3006b; }
    .badge-cn { background: rgba(255,120,0,0.10);  color: #e35c00; }

    /* Card Price Row */
    .card-price-row {
        padding: 14px 22px;
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
        border-bottom: 1px solid rgba(0,0,0,0.04);
        background: rgba(0,0,0,0.015);
    }
    .price-current {
        font-size: 20px;
        font-weight: 600;
        color: var(--text-primary);
        letter-spacing: -0.01em;
    }
    .price-arrow {
        font-size: 14px;
        color: var(--text-secondary);
        opacity: 0.6;
    }
    .price-target {
        font-size: 16px;
        font-weight: 500;
        color: var(--text-secondary);
    }
    .upside-badge {
        display: inline-flex;
        align-items: center;
        gap: 3px;
        font-size: 13px;
        font-weight: 600;
        padding: 3px 10px;
        border-radius: 100px;
        margin-left: auto;
    }
    .upside-high { background: rgba(52,199,89,0.12); color: #1d8a3a; }
    .upside-mid  { background: rgba(255,159,10,0.12); color: #b35c00; }
    .upside-low  { background: rgba(110,110,115,0.10); color: var(--text-secondary); }

    /* Card Body */
    .card-body { padding: 16px 22px; flex: 1; }

    /* Recommenders */
    .rec-row {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 10px;
        flex-wrap: wrap;
    }
    .rec-label {
        font-size: 12px;
        font-weight: 600;
        color: var(--text-secondary);
        white-space: nowrap;
    }
    .rec-chips { display: flex; flex-wrap: wrap; gap: 6px; }
    .chip {
        display: inline-flex;
        align-items: center;
        gap: 5px;
        font-size: 11px;
        font-weight: 500;
        padding: 4px 10px;
        border-radius: var(--radius-xs);
        border: 1px solid rgba(0,0,0,0.07);
        background: rgba(0,0,0,0.025);
        color: var(--text-primary);
        white-space: nowrap;
    }
    .chip-winrate {
        background: rgba(52,199,89,0.15);
        color: #1d8a3a;
        font-size: 10px;
        font-weight: 700;
        padding: 1px 6px;
        border-radius: 100px;
    }
    .chip-grade {
        font-size: 10px;
        font-weight: 600;
        color: var(--accent);
        opacity: 0.85;
    }

    /* Why Buy reasons */
    .why-buy {
        margin-top: 12px;
        padding-top: 12px;
        border-top: 1px solid rgba(0,0,0,0.05);
    }
    .why-buy-label {
        font-size: 10px;
        font-weight: 700;
        letter-spacing: 0.10em;
        text-transform: uppercase;
        color: var(--text-secondary);
        margin-bottom: 7px;
    }
    .why-buy ul {
        list-style: none;
        padding: 0;
        margin: 0;
    }
    .why-buy li {
        font-size: 12.5px;
        color: var(--text-secondary);
        padding: 3px 0 3px 16px;
        position: relative;
        line-height: 1.45;
    }
    .why-buy li::before {
        content: "•";
        position: absolute;
        left: 4px;
        color: var(--accent);
        font-weight: 700;
    }

    /* Fundamentals toggle */
    .fundamentals-section {
        margin-top: 12px;
        border-top: 1px solid rgba(0,0,0,0.05);
    }
    .fundamentals-toggle {
        width: 100%;
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 10px 0 4px;
        background: none;
        border: none;
        cursor: pointer;
        font-family: var(--font);
        font-size: 12px;
        font-weight: 600;
        color: var(--accent);
        text-align: left;
        -webkit-font-smoothing: antialiased;
    }
    .fundamentals-toggle:hover { opacity: 0.75; }
    .toggle-icon {
        font-size: 10px;
        transition: transform 0.2s ease;
        display: inline-block;
    }
    .fundamentals-body {
        display: none;
        padding: 8px 0 4px;
    }
    .fundamentals-body.open { display: block; }
    .fundamentals-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 4px 16px;
    }
    .fund-item {
        font-size: 11.5px;
        color: var(--text-secondary);
        display: flex;
        justify-content: space-between;
        padding: 3px 0;
        border-bottom: 1px solid rgba(0,0,0,0.03);
    }
    .fund-item strong {
        color: var(--text-primary);
        font-weight: 600;
    }
    .fund-positive { color: #1d8a3a; font-weight: 600; }
    .fund-negative { color: #c0392b; font-weight: 600; }

    /* ── Sector Flows Section ─────────────────────────────────────────── */
    .flows-section { padding: 48px 0 8px; }
    .flows-section .section-title {
        font-size: 22px;
        margin-bottom: 20px;
    }
    .flows-cards {
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
    }
    .flow-card {
        background: var(--card);
        border-radius: var(--radius-sm);
        box-shadow: 0 2px 12px rgba(0,0,0,0.06);
        border: 1px solid var(--border);
        padding: 14px 18px;
        min-width: 160px;
        flex: 1 1 160px;
        max-width: 220px;
    }
    .flow-sector {
        font-size: 12px;
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 6px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .flow-value {
        font-size: 18px;
        font-weight: 700;
        letter-spacing: -0.02em;
    }
    .flow-positive { color: var(--green); }
    .flow-negative { color: var(--red); }
    .flow-pct {
        font-size: 11px;
        color: var(--text-secondary);
        margin-top: 2px;
    }

    /* ── Empty state ─────────────────────────────────────────────────── */
    .empty-state {
        text-align: center;
        padding: 48px 24px;
        color: var(--text-secondary);
        font-size: 15px;
        background: var(--card);
        border-radius: var(--radius);
        border: 1px dashed rgba(0,0,0,0.10);
    }

    /* ── Footer ──────────────────────────────────────────────────────── */
    footer {
        margin-top: 72px;
        padding: 40px 32px;
        background: #1d1d1f;
        color: rgba(245,245,247,0.45);
        font-size: 12px;
        line-height: 1.7;
        text-align: center;
    }
    footer strong { color: rgba(245,245,247,0.70); font-weight: 600; }
    footer a { color: rgba(245,245,247,0.55); }
    .footer-sources {
        display: flex;
        justify-content: center;
        gap: 20px;
        flex-wrap: wrap;
        margin-bottom: 18px;
    }
    .footer-source {
        background: rgba(255,255,255,0.06);
        border-radius: 100px;
        padding: 4px 14px;
        font-size: 11px;
        font-weight: 500;
    }
    .footer-disclaimer {
        max-width: 680px;
        margin: 0 auto;
        opacity: 0.8;
    }

    /* ── Utilities ───────────────────────────────────────────────────── */
    .text-secondary { color: var(--text-secondary); }
    .mt-4 { margin-top: 4px; }
    .mt-8 { margin-top: 8px; }

    /* ── Featured (top picks) ─────────────────────────────────────────── */
    .featured-section {
        padding: 56px 0 8px;
    }
    .featured-section .section-title {
        font-size: 32px;
        letter-spacing: -0.02em;
    }
    .featured-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
        gap: 20px;
    }
    .featured-card {
        background: linear-gradient(135deg, #ffffff 0%, #fafbff 100%);
        border-radius: 22px;
        border: 1px solid rgba(0,113,227,0.18);
        box-shadow: 0 12px 40px rgba(0,113,227,0.10);
        padding: 24px 24px 20px;
        position: relative;
        overflow: hidden;
    }
    .featured-card::before {
        content: "";
        position: absolute;
        top: 0; left: 0;
        width: 100%;
        height: 4px;
        background: linear-gradient(90deg, var(--gold) 0%, var(--accent) 100%);
    }
    .featured-rank {
        display: inline-block;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        color: var(--accent);
        margin-bottom: 10px;
    }
    .featured-ticker {
        font-size: 28px;
        font-weight: 700;
        letter-spacing: -0.02em;
        margin-right: 10px;
    }
    .featured-name {
        font-size: 13px;
        color: var(--text-secondary);
        margin-top: 4px;
    }
    .featured-price {
        font-size: 22px;
        font-weight: 600;
        margin-top: 16px;
    }
    .featured-reasons {
        margin-top: 14px;
        font-size: 13px;
        color: var(--text-secondary);
        line-height: 1.6;
    }
    .featured-reasons li {
        list-style: none;
        padding: 4px 0 4px 18px;
        position: relative;
    }
    .featured-reasons li::before {
        content: "→";
        position: absolute;
        left: 0;
        color: var(--accent);
        font-weight: 700;
    }

    /* ── Market section headers ───────────────────────────────────────── */
    .market-section {
        padding: 48px 0 12px;
    }
    .market-section-header {
        display: flex;
        align-items: baseline;
        gap: 14px;
        margin-bottom: 24px;
        border-bottom: 1px solid var(--border);
        padding-bottom: 12px;
    }
    .market-section-title {
        font-size: 24px;
        font-weight: 700;
        letter-spacing: -0.02em;
    }
    .market-section-subtitle {
        font-size: 13px;
        color: var(--text-secondary);
        font-weight: 500;
    }

    /* ── Responsive ──────────────────────────────────────────────────── */
    @media (max-width: 768px) {
        .hero { padding: 48px 24px 44px; }
        .hero-stats { gap: 10px; }
        .hero-stat { min-width: 130px; padding: 12px 18px; }
        .container { padding: 0 16px; }
        .card-grid { grid-template-columns: 1fr; }
        .fundamentals-grid { grid-template-columns: 1fr; }
        .flows-cards { gap: 8px; }
    }
    """


# ---------------------------------------------------------------------------
# JavaScript
# ---------------------------------------------------------------------------

def _js() -> str:
    return """
    function toggleFundamentals(btn) {
        var card = btn.closest('.stock-card');
        var body = card.querySelector('.fundamentals-body');
        var icon = btn.querySelector('.toggle-icon');
        var isOpen = body.classList.contains('open');
        if (isOpen) {
            body.classList.remove('open');
            btn.querySelector('.toggle-text').textContent = 'Show Fundamentals';
            icon.style.transform = 'rotate(0deg)';
        } else {
            body.classList.add('open');
            btn.querySelector('.toggle-text').textContent = 'Hide Fundamentals';
            icon.style.transform = 'rotate(180deg)';
        }
    }
    """


# ---------------------------------------------------------------------------
# Build "why buy" reasons (port from report_generator)
# ---------------------------------------------------------------------------

def _build_reasons(stock: dict) -> list[str]:
    reasons: list[str] = []
    market = stock.get("market", "US")
    upside = stock.get("upside_pct") or 0
    rec_mean = stock.get("rec_mean")
    n = stock.get("n_analysts", 0) or 0
    recommenders = stock.get("recommenders", [])
    recommender_count = stock.get("recommender_count", 0)

    if recommender_count >= 2:
        names_str = " & ".join(
            r.get("firm") or r.get("name") or "analyst"
            for r in recommenders[:3]
        )
        reasons.append(f"{recommender_count} analysts independently recommended this stock ({names_str}).")

    if upside >= 30:
        reasons.append(f"Analysts see {upside:.0f}% upside to consensus target — significant re-rating potential.")
    elif upside >= 15:
        reasons.append(f"Analyst consensus target implies {upside:.0f}% upside from current price.")

    ret_3m = stock.get("ret_3m")
    if ret_3m is not None:
        if 5.0 <= ret_3m <= 40.0:
            reasons.append(f"Positive 3-month momentum: {ret_3m:+.1f}% — trend is confirming analyst thesis.")
        elif ret_3m <= -15.0:
            reasons.append(f"Warning: down {ret_3m:.1f}% over 3 months — wait for a base before buying.")

    eid = stock.get("earnings_in_days")
    if eid is not None and 0 <= eid <= 30:
        reasons.append(f"Earnings announcement in ~{eid} days — potential near-term catalyst.")

    if stock.get("above_200dma") is True:
        reasons.append("Price above 200-day moving average — primary uptrend intact.")

    if rec_mean is not None:
        if rec_mean <= 1.5 and n >= 5:
            reasons.append(f"Strong Buy consensus across {n} analysts — high conviction across the street.")
        elif rec_mean <= 2.0 and n > 0:
            reasons.append(f"Buy consensus from {n} analysts with aligned bullish price targets.")

    # CN-specific: report title
    if market == "CN":
        report = stock.get("report_title", "")
        if report:
            reasons.append(f'Latest research note: "{report}"')
        win_rates = [r.get("win_rate", 0) for r in recommenders if r.get("win_rate")]
        if win_rates:
            avg_wr = sum(win_rates) / len(win_rates)
            if avg_wr >= 60:
                reasons.append(f"Recommended by analysts with avg {avg_wr:.0f}% historical accuracy rate.")

    rev_g = stock.get("revenue_growth") or 0
    if rev_g >= 0.20:
        reasons.append(f"Revenue growing at {rev_g * 100:.0f}% YoY — above-market top-line expansion.")
    elif rev_g >= 0.10:
        reasons.append(f"Solid {rev_g * 100:.0f}% revenue growth supports earnings trajectory.")

    earn_g = stock.get("earnings_growth") or 0
    if earn_g >= 0.20:
        reasons.append(f"Earnings growing at {earn_g * 100:.0f}% YoY — strong EPS leverage.")

    margin = stock.get("profit_margin") or 0
    if margin >= 0.25:
        reasons.append(f"High-quality business: {margin * 100:.0f}% net profit margin.")

    roe = stock.get("roe") or 0
    if roe >= 0.20:
        reasons.append(f"Return on equity of {roe * 100:.0f}% — efficient capital allocation.")

    fpe = stock.get("forward_pe") or 0
    pe = stock.get("pe_ratio") or 0
    if 0 < fpe < 20:
        reasons.append(f"Reasonable valuation at {fpe:.1f}x forward earnings.")
    elif 0 < fpe < pe * 0.8 and pe > 0:
        reasons.append(f"Forward P/E of {fpe:.1f}x well below trailing P/E — earnings acceleration expected.")

    w52h = stock.get("52w_high") or 0
    price = stock.get("current_price") or 0
    if w52h > 0 and price > 0 and price < w52h * 0.85:
        pct_below = (w52h - price) / w52h * 100
        reasons.append(f"Trading {pct_below:.0f}% below 52-week high — potential mean-reversion opportunity.")

    if not reasons:
        reasons.append("Qualifies on analyst consensus and price target upside criteria.")

    return reasons[:5]  # cap at 5 bullets for card readability


# ---------------------------------------------------------------------------
# Recommender chips HTML
# ---------------------------------------------------------------------------

def _recommender_chips_html(stock: dict) -> str:
    recommenders = stock.get("recommenders", [])
    market = stock.get("market", "US")
    if not recommenders:
        return ""

    chips_html = ""
    for r in recommenders:
        if market == "CN":
            name = escape(str(r.get("name", "")))
            firm = escape(str(r.get("firm", "")))
            win_rate = r.get("win_rate", 0)
            label = f"{name} · {firm}" if firm else name
            wr_badge = ""
            if win_rate and float(win_rate) >= 60:
                wr_badge = f'<span class="chip-winrate">{float(win_rate):.0f}%</span>'
            chips_html += f'<span class="chip">{label}{wr_badge}</span>'
        else:
            firm = escape(str(r.get("firm", "Unknown")))
            to_grade = escape(str(r.get("to_grade", "")))
            action = str(r.get("action", "")).lower()
            action_icon = {"up": "↑", "init": "★", "down": "↓"}.get(action, "·")
            grade_html = f'<span class="chip-grade">{action_icon}{to_grade}</span>' if to_grade else ""
            chips_html += f'<span class="chip">{firm} {grade_html}</span>'

    return f'<div class="rec-chips">{chips_html}</div>'


# ---------------------------------------------------------------------------
# Fundamentals panel HTML
# ---------------------------------------------------------------------------

def _fundamentals_html(stock: dict) -> str:
    market = stock.get("market", "US")
    items: list[tuple[str, str]] = []

    def _pct_colored(val: Any, multiplier: float = 100.0) -> str:
        try:
            f = float(val) * multiplier
            cls = "fund-positive" if f >= 0 else "fund-negative"
            sign = "+" if f >= 0 else ""
            return f'<strong class="{cls}">{sign}{f:.1f}%</strong>'
        except (TypeError, ValueError):
            return "<strong>N/A</strong>"

    def _val(v: Any, fmt: str = "") -> str:
        if v is None or v == 0:
            return "<strong>N/A</strong>"
        try:
            f = float(v)
            if fmt == "x":
                return f"<strong>{f:.1f}x</strong>"
            if fmt == "pct":
                return _pct_colored(v)
            return f"<strong>{f:,.2f}</strong>"
        except (TypeError, ValueError):
            return f"<strong>{escape(str(v))}</strong>"

    if market in ("US", "HK"):
        pe = stock.get("pe_ratio")
        fpe = stock.get("forward_pe")
        rev_g = stock.get("revenue_growth")
        earn_g = stock.get("earnings_growth")
        margin = stock.get("profit_margin")
        roe = stock.get("roe")
        d_e = stock.get("debt_to_equity")
        eps_f = stock.get("eps_forward")
        eps_t = stock.get("eps_ttm")
        w52h = stock.get("52w_high")
        w52l = stock.get("52w_low")
        mcap = stock.get("market_cap")
        n_a = stock.get("n_analysts", 0)
        rec_mean = stock.get("rec_mean")

        items = [
            ("Trailing P/E",   _val(pe, "x")),
            ("Forward P/E",    _val(fpe, "x")),
            ("Rev Growth",     _pct_colored(rev_g) if rev_g is not None else "<strong>N/A</strong>"),
            ("Earnings Growth",_pct_colored(earn_g) if earn_g is not None else "<strong>N/A</strong>"),
            ("Profit Margin",  _pct_colored(margin) if margin is not None else "<strong>N/A</strong>"),
            ("ROE",            _pct_colored(roe) if roe is not None else "<strong>N/A</strong>"),
            ("Fwd EPS",        f"<strong>{_fmt_price(eps_f)}</strong>"),
            ("TTM EPS",        f"<strong>{_fmt_price(eps_t)}</strong>"),
            ("Debt/Equity",    _val(d_e)),
            ("Market Cap",     f"<strong>{_fmt_large(mcap)}</strong>"),
            ("52W High",       f"<strong>{_fmt_price(w52h)}</strong>"),
            ("52W Low",        f"<strong>{_fmt_price(w52l)}</strong>"),
            ("# Analysts",     f"<strong>{n_a}</strong>"),
            ("Consensus",      f"<strong>{_rec_label(rec_mean)}</strong>"),
        ]
    else:
        # CN — use fundamentals dict from akshare
        fundamentals = stock.get("fundamentals", {})
        key_fields = [
            ("总市值", "Market Cap"),
            ("市盈率(TTM)", "P/E (TTM)"),
            ("市净率", "P/B"),
            ("ROE", "ROE"),
            ("营收增速", "Rev Growth"),
            ("净利润增速", "Net Profit Growth"),
            ("毛利率", "Gross Margin"),
        ]
        for cn_key, label in key_fields:
            val = fundamentals.get(cn_key)
            if val and str(val) not in ("--", "None", ""):
                items.append((label, f"<strong>{escape(str(val))}</strong>"))

        rating = stock.get("rating", "")
        if rating:
            items.append(("Rating", f"<strong>{escape(rating)}</strong>"))

    if not items:
        return ""

    rows_html = "".join(
        f'<div class="fund-item"><span>{escape(label)}</span>{val_html}</div>'
        for label, val_html in items
    )

    return f"""
        <div class="fundamentals-section">
            <button class="fundamentals-toggle" onclick="toggleFundamentals(this)">
                <span class="toggle-text">Show Fundamentals</span>
                <span class="toggle-icon">▼</span>
            </button>
            <div class="fundamentals-body">
                <div class="fundamentals-grid">
                    {rows_html}
                </div>
            </div>
        </div>
    """


# ---------------------------------------------------------------------------
# Stock card HTML
# ---------------------------------------------------------------------------

def _stock_card_html(stock: dict) -> str:
    ticker = escape(str(stock.get("ticker", "?")))
    name = escape(str(stock.get("name", ticker)))
    sector = escape(str(stock.get("sector", "")))
    industry = escape(str(stock.get("industry", "")))
    market = stock.get("market", "US")
    tier = stock.get("tier", 3)
    upside = stock.get("upside_pct")
    current_price = stock.get("current_price")
    price_target = stock.get("price_target") or stock.get("target_mean")
    recommender_count = stock.get("recommender_count", 0)

    accent_cls = _tier_accent_class(tier)
    market_badge = _market_badge(market)

    # Currency symbol
    currency = "¥" if market == "CN" else ("HK$" if market == "HK" else "$")

    # Price row
    current_str = _fmt_price(current_price, currency)
    target_str = _fmt_price(price_target, currency)
    upside_cls = _upside_class(upside)
    arrow = _upside_arrow(upside)
    upside_str = _fmt_pct(upside) if upside is not None else "N/A"

    # Sector line
    sector_line = ""
    if sector or industry:
        parts = [p for p in [sector, industry] if p]
        sector_line = f'<div class="card-sector">{" / ".join(parts)}</div>'

    # Recommenders
    rec_label_text = (
        f"👥 {recommender_count} analyst{'s' if recommender_count != 1 else ''} recommended"
    )
    chips_html = _recommender_chips_html(stock)
    rec_row_html = f"""
        <div class="rec-row">
            <span class="rec-label">{rec_label_text}</span>
        </div>
        {chips_html}
    """

    # Why buy
    reasons = _build_reasons(stock)
    reasons_items = "".join(f"<li>{escape(r)}</li>" for r in reasons)
    why_buy_html = f"""
        <div class="why-buy">
            <div class="why-buy-label">Why Buy</div>
            <ul>{reasons_items}</ul>
        </div>
    """ if reasons else ""

    # Fundamentals
    fund_html = _fundamentals_html(stock)

    return f"""
    <div class="stock-card {accent_cls}">
        <div class="card-header">
            <div class="card-header-left">
                <div>
                    <span class="card-ticker">{ticker}</span>
                    {market_badge}
                </div>
                <div class="card-name">{name}</div>
                {sector_line}
            </div>
        </div>

        <div class="card-price-row">
            <span class="price-current">{current_str}</span>
            <span class="price-arrow">→</span>
            <span class="price-target">{target_str}</span>
            <span class="upside-badge {upside_cls}">{arrow} {upside_str}</span>
        </div>

        <div class="card-body">
            {rec_row_html}
            {why_buy_html}
            {fund_html}
        </div>
    </div>
    """


# ---------------------------------------------------------------------------
# Tier section HTML
# ---------------------------------------------------------------------------

def _tier_section_html(tier_num: int, stocks: list[dict]) -> str:
    labels = {
        1: ("🏆 High Conviction", "tier-gold"),
        2: ("⭐ Strong Picks",    "tier-blue"),
        3: ("📋 Watch List",      "tier-gray"),
    }
    title, _ = labels.get(tier_num, ("Unknown Tier", "tier-gray"))
    count = len(stocks)

    if not stocks:
        body = '<div class="empty-state">No stocks in this tier today.</div>'
    else:
        cards_html = "\n".join(_stock_card_html(s) for s in stocks)
        body = f'<div class="card-grid">{cards_html}</div>'

    return f"""
    <section class="section">
        <div class="section-header">
            <h2 class="section-title">{title}</h2>
            <span class="section-count">{count} stocks</span>
        </div>
        {body}
    </section>
    """


# ---------------------------------------------------------------------------
# Sector flows section HTML
# ---------------------------------------------------------------------------

def _sector_flows_html(market_ctx: dict) -> str:
    flows = market_ctx.get("sector_flows_today", [])
    if not flows:
        return ""

    cards_html = ""
    for flow in flows[:12]:
        sector_name = escape(str(flow.get("sector", "")))
        inflow = flow.get("main_net_inflow", 0)
        try:
            inflow_f = float(inflow)
        except (TypeError, ValueError):
            inflow_f = 0.0
        net_pct = flow.get("main_net_pct", 0)
        positive = inflow_f >= 0
        value_cls = "flow-positive" if positive else "flow-negative"
        sign = "▲" if positive else "▼"

        cards_html += f"""
        <div class="flow-card">
            <div class="flow-sector">{sector_name}</div>
            <div class="flow-value {value_cls}">{sign} {abs(inflow_f):.1f}亿</div>
            <div class="flow-pct">{_fmt_pct(net_pct)}</div>
        </div>
        """

    return f"""
    <section class="flows-section">
        <div class="section-header">
            <h2 class="section-title">📊 Sector Fund Flows</h2>
            <span class="section-count">Today · CN A-shares</span>
        </div>
        <div class="flows-cards">
            {cards_html}
        </div>
    </section>
    """


# ---------------------------------------------------------------------------
# Featured picks — top 3 across all markets
# ---------------------------------------------------------------------------

def _featured_card_html(stock: dict, rank: int) -> str:
    ticker = escape(str(stock.get("ticker", "?")))
    name = escape(str(stock.get("name", ticker)))
    market = stock.get("market", "US")
    currency = "¥" if market == "CN" else ("HK$" if market == "HK" else "$")
    current = _fmt_price(stock.get("current_price"), currency)
    target = _fmt_price(stock.get("price_target") or stock.get("target_mean"), currency)
    upside = stock.get("upside_pct")
    upside_str = _fmt_pct(upside) if upside is not None else "N/A"
    upside_cls = _upside_class(upside)
    market_badge = _market_badge(market)

    reasons = _build_reasons(stock)[:3]
    reasons_html = "".join(f"<li>{escape(r)}</li>" for r in reasons)

    return f"""
    <div class="featured-card">
        <div class="featured-rank">#{rank} · Most Worth Attention</div>
        <div>
            <span class="featured-ticker">{ticker}</span>
            {market_badge}
        </div>
        <div class="featured-name">{name}</div>
        <div class="featured-price">
            {current} <span class="price-arrow">→</span>
            <span class="price-target">{target}</span>
            <span class="upside-badge {upside_cls}" style="margin-left:10px;">{upside_str}</span>
        </div>
        <ul class="featured-reasons">{reasons_html}</ul>
    </div>
    """


def _featured_section_html(picks: list[dict]) -> str:
    if not picks:
        return ""
    cards = "\n".join(_featured_card_html(s, i + 1) for i, s in enumerate(picks))
    return f"""
    <section class="section featured-section">
        <div class="section-header">
            <h2 class="section-title">⭐ Most Worth Your Attention</h2>
            <span class="section-count">Top {len(picks)} · Across all markets</span>
        </div>
        <div class="featured-grid">{cards}</div>
    </section>
    """


# ---------------------------------------------------------------------------
# Per-market section (up to N picks, industry-capped)
# ---------------------------------------------------------------------------

def _market_section_html(market: str, label: str, subtitle: str, picks: list[dict]) -> str:
    if not picks:
        return ""
    cards_html = "\n".join(_stock_card_html(s) for s in picks)
    badge = _market_badge(market)
    return f"""
    <section class="market-section">
        <div class="market-section-header">
            <h2 class="market-section-title">{label} {badge}</h2>
            <span class="market-section-subtitle">{escape(subtitle)} · {len(picks)} picks</span>
        </div>
        <div class="card-grid">
            {cards_html}
        </div>
    </section>
    """


# ---------------------------------------------------------------------------
# Performance (historical picks) section
# ---------------------------------------------------------------------------

def _performance_html(performance_rows: list[dict]) -> str:
    """Render realised performance of past tier-1/2 picks."""
    if not performance_rows:
        return ""

    # Compute realised return per pick.
    prepared = []
    for r in performance_rows:
        pub = float(r.get("price_at_pub") or 0)
        cur = float(r.get("current_price") or 0)
        if pub <= 0 or cur <= 0:
            continue
        ret_pct = (cur - pub) / pub * 100
        prepared.append({**r, "ret_pct": ret_pct})

    if not prepared:
        return ""

    wins = sum(1 for p in prepared if p["ret_pct"] > 0)
    win_rate = wins / len(prepared) * 100
    avg_ret = sum(p["ret_pct"] for p in prepared) / len(prepared)

    # Show top 12 most recent.
    prepared.sort(key=lambda p: p.get("report_date", ""), reverse=True)
    rows_html = ""
    for p in prepared[:12]:
        ticker = escape(str(p.get("ticker", "")))
        name = escape(str(p.get("name", "")))
        ret_pct = p["ret_pct"]
        cls = "flow-positive" if ret_pct >= 0 else "flow-negative"
        sign = "▲" if ret_pct >= 0 else "▼"
        rd = escape(str(p.get("report_date", "")))
        tier_str = f"T{p.get('tier', '?')}"
        rows_html += f"""
        <div class="flow-card">
            <div class="flow-sector">{ticker} · {tier_str}</div>
            <div class="flow-value {cls}">{sign} {abs(ret_pct):.1f}%</div>
            <div class="flow-pct">{rd} · {name}</div>
        </div>
        """

    summary = (
        f'Historical picks: <strong>{win_rate:.0f}% win rate</strong> · '
        f'<strong>{avg_ret:+.1f}%</strong> avg return across {len(prepared)} picks'
    )

    return f"""
    <section class="flows-section">
        <div class="section-header">
            <h2 class="section-title">📈 Our Track Record</h2>
            <span class="section-count">Realized — past tier 1/2 picks</span>
        </div>
        <p class="text-secondary" style="margin-bottom:16px;font-size:13px;">{summary}</p>
        <div class="flows-cards">
            {rows_html}
        </div>
    </section>
    """


# ---------------------------------------------------------------------------
# Hero header HTML
# ---------------------------------------------------------------------------

def _hero_html(stats: dict, report_date: str, market_ctx: dict) -> str:
    total = stats.get("total_stocks", 0)
    top_upside = stats.get("top_upside", 0)
    avg_recs = stats.get("avg_recommenders", 0)

    markets_covered_parts = []
    if stats.get("total_us", 0):
        markets_covered_parts.append("US")
    if stats.get("total_hk", 0):
        markets_covered_parts.append("HK")
    if stats.get("total_cn", 0):
        markets_covered_parts.append("CN")
    markets_str = " · ".join(markets_covered_parts) if markets_covered_parts else "—"

    # Top pick ticker
    # Will be injected after aggregation if available (caller sets via market_ctx or we skip)
    top_pick = market_ctx.get("_top_pick_ticker", "—")

    return f"""
    <header class="hero">
        <div class="hero-eyebrow">Analyst Intelligence · Auto-Generated</div>
        <h1 class="hero-title">Daily Analyst Report</h1>
        <div class="hero-date">{escape(report_date)}</div>
        <div class="hero-stats">
            <div class="hero-stat">
                <span class="hero-stat-value">{total}</span>
                <span class="hero-stat-label">Total Stocks</span>
            </div>
            <div class="hero-stat">
                <span class="hero-stat-value">{markets_str}</span>
                <span class="hero-stat-label">Markets Covered</span>
            </div>
            <div class="hero-stat">
                <span class="hero-stat-value">{top_upside:.0f}%</span>
                <span class="hero-stat-label">Top Upside</span>
            </div>
            <div class="hero-stat">
                <span class="hero-stat-value">{top_pick}</span>
                <span class="hero-stat-label">Top Pick</span>
            </div>
        </div>
    </header>
    """


# ---------------------------------------------------------------------------
# Footer HTML
# ---------------------------------------------------------------------------

def _footer_html(report_date: str) -> str:
    sources = [
        "Yahoo Finance",
        "Eastmoney 东方财富",
        "Xueqiu 雪球",
        "Finviz",
        "FMP (optional)",
    ]
    sources_html = "".join(
        f'<span class="footer-source">{escape(s)}</span>' for s in sources
    )
    return f"""
    <footer>
        <div class="footer-sources">{sources_html}</div>
        <p class="footer-disclaimer">
            <strong>Disclaimer:</strong> This report is auto-generated from publicly available
            data and is for informational purposes only. It does not constitute investment advice.
            Past analyst accuracy does not guarantee future performance. Always conduct your own
            due diligence before making any investment decisions.
        </p>
        <p style="margin-top:14px; opacity:0.5;">
            Generated {escape(report_date)} · Stock Tracker by Claude Code
        </p>
    </footer>
    """


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_html(
    aggregated: dict,
    market_ctx: dict,
    report_date: str | None = None,
    performance_rows: list[dict] | None = None,
) -> str:
    """
    Generate a self-contained Apple-style HTML report.

    Parameters
    ----------
    aggregated : dict
        Output of aggregator.aggregate_all() —
        {"tier1": [...], "tier2": [...], "tier3": [...], "stats": {...}}
    market_ctx : dict
        Output of build_market_context() (may be empty dict).
        Optionally include "_top_pick_ticker" key to display in hero.
    report_date : str, optional
        ISO date string (YYYY-MM-DD). Defaults to today.

    Returns
    -------
    str : Full HTML document as a string.
    """
    today = _safe_date(report_date)
    stats = aggregated.get("stats", {})
    all_stocks = aggregated.get("all") or (
        aggregated.get("tier1", []) + aggregated.get("tier2", []) + aggregated.get("tier3", [])
    )
    all_sorted = sorted(all_stocks, key=lambda s: s.get("priority_score", 0), reverse=True)

    # Per-market top picks — max 10 each, max 2 per industry.
    us_picks = select_market_top(all_stocks, "US", limit=10, industry_cap=2)
    hk_picks = select_market_top(all_stocks, "HK", limit=10, industry_cap=2)
    cn_picks = select_market_top(all_stocks, "CN", limit=10, industry_cap=2)

    # Featured = top 3 overall, but each must come from a distinct market when
    # possible so the hero shows diversity.
    seen_markets: set[str] = set()
    featured: list[dict] = []
    for s in all_sorted:
        m = s.get("market", "")
        if m not in seen_markets:
            featured.append(s)
            seen_markets.add(m)
        if len(featured) >= 3:
            break
    # Fill the rest with highest-score remainders if fewer than 3 markets.
    for s in all_sorted:
        if len(featured) >= 3:
            break
        if s not in featured:
            featured.append(s)

    # Hero top-pick comes from featured.
    ctx = dict(market_ctx or {})
    if featured and "_top_pick_ticker" not in ctx:
        ctx["_top_pick_ticker"] = featured[0].get("ticker", "—")

    hero = _hero_html(stats, today, ctx)
    flows = _sector_flows_html(ctx)
    performance = _performance_html(performance_rows or [])
    featured_html = _featured_section_html(featured)
    us_html = _market_section_html("US", "US Equities — Top Picks", "NYSE / NASDAQ (max 2 per industry)", us_picks)
    hk_html = _market_section_html("HK", "Hong Kong — Top Picks", "HKEX (max 2 per industry)", hk_picks)
    cn_html = _market_section_html("CN", "China A-Shares — Top Picks", "Eastmoney / 东方财富 (max 2 per industry)", cn_picks)
    footer = _footer_html(today)

    css = _css()
    js = _js()

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="Daily Analyst Stock Report — {escape(today)}">
    <title>Daily Analyst Report · {escape(today)}</title>
    <style>{css}</style>
</head>
<body>

{hero}

<div class="container">

{featured_html}

{us_html}

{hk_html}

{cn_html}

{flows}

{performance}

</div>

{footer}

<script>{js}</script>
</body>
</html>"""


def save_html(html: str, report_date: str | None = None) -> str:
    """
    Save the HTML report to reports/<date>.html.

    Parameters
    ----------
    html : str
        Full HTML string from generate_html().
    report_date : str, optional
        ISO date string (YYYY-MM-DD). Defaults to today.

    Returns
    -------
    str : Absolute path to the saved file.
    """
    os.makedirs(REPORTS_DIR, exist_ok=True)
    today = _safe_date(report_date)
    path = os.path.join(REPORTS_DIR, f"{today}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    return path
