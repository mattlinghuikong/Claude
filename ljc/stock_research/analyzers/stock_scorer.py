"""Score and rank stocks across markets based on analyst recommendations."""

import pandas as pd
import numpy as np
from datetime import datetime
from config import (
    MIN_ANALYST_COVERAGE,
    WEIGHT_WIN_RATE,
    WEIGHT_ANALYST_COUNT,
    WEIGHT_EXCESS_RETURN,
    WEIGHT_RECENCY,
    TOP_STOCKS_FINAL,
)


class StockScorer:
    def score_cn(self, recs_df: pd.DataFrame) -> pd.DataFrame:
        if recs_df.empty:
            return pd.DataFrame()

        group_cols = ["ticker", "name", "market"]
        group_cols = [c for c in group_cols if c in recs_df.columns]

        agg: dict = {
            "analyst_win_rate": "mean",
            "analyst_excess_return": "mean",
        }
        if "analyst" in recs_df.columns:
            agg["analyst"] = "count"

        grouped = recs_df.groupby(group_cols).agg(agg).reset_index()

        if "analyst" in grouped.columns:
            grouped = grouped.rename(columns={"analyst": "analyst_count"})
        else:
            grouped["analyst_count"] = 1

        # CN analysts are sector specialists with little overlap — use min=1
        grouped = grouped[grouped["analyst_count"] >= 1]
        return self._compute_score(grouped)

    def score_hk(self, recs_df: pd.DataFrame) -> pd.DataFrame:
        if recs_df.empty:
            return pd.DataFrame()

        keep = ["ticker", "name", "market", "analyst_win_rate",
                "analyst_excess_return", "buy_count", "sector"]
        recs_df = recs_df[[c for c in keep if c in recs_df.columns]].copy()

        if "buy_count" in recs_df.columns:
            recs_df = recs_df.rename(columns={"buy_count": "analyst_count"})
        else:
            recs_df["analyst_count"] = 1

        return self._compute_score(recs_df)

    def score_us(self, recs_df: pd.DataFrame) -> pd.DataFrame:
        if recs_df.empty:
            return pd.DataFrame()

        keep = ["ticker", "name", "market", "analyst_win_rate",
                "analyst_excess_return", "buy_count", "upside_pct", "sector"]
        recs_df = recs_df[[c for c in keep if c in recs_df.columns]].copy()

        if "buy_count" in recs_df.columns:
            recs_df = recs_df.rename(columns={"buy_count": "analyst_count"})
        else:
            recs_df["analyst_count"] = 1

        return self._compute_score(recs_df)

    def _compute_score(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        for col in ("analyst_win_rate", "analyst_excess_return", "analyst_count"):
            if col not in df.columns:
                df[col] = 0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Normalise each dimension to [0, 1]
        def norm(s: pd.Series) -> pd.Series:
            mn, mx = s.min(), s.max()
            return (s - mn) / (mx - mn + 1e-9)

        df["score_win_rate"] = norm(df["analyst_win_rate"])
        df["score_analyst_count"] = norm(df["analyst_count"])
        df["score_excess_return"] = norm(df["analyst_excess_return"])
        df["score_recency"] = 0.5  # placeholder; set to 1 if date is available

        df["composite_score"] = (
            WEIGHT_WIN_RATE       * df["score_win_rate"]
            + WEIGHT_ANALYST_COUNT  * df["score_analyst_count"]
            + WEIGHT_EXCESS_RETURN  * df["score_excess_return"]
            + WEIGHT_RECENCY        * df["score_recency"]
        )

        return df.sort_values("composite_score", ascending=False).reset_index(drop=True)

    def select_top_stocks(
        self,
        cn_scored: pd.DataFrame,
        hk_scored: pd.DataFrame,
        us_scored: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge all markets and pick the global top N stocks."""
        parts = []
        for df, label in [(cn_scored, "CN"), (hk_scored, "HK"), (us_scored, "US")]:
            if df.empty:
                continue
            top = df.head(15).copy()
            top["market_label"] = label
            parts.append(top)

        if not parts:
            return pd.DataFrame()

        combined = pd.concat(parts, ignore_index=True)

        # Re-normalise composite_score globally so markets are comparable
        mn = combined["composite_score"].min()
        mx = combined["composite_score"].max()
        combined["global_score"] = (combined["composite_score"] - mn) / (mx - mn + 1e-9)

        combined = combined.sort_values("global_score", ascending=False)
        combined = combined.drop_duplicates(subset=["ticker"]).head(TOP_STOCKS_FINAL)
        return combined.reset_index(drop=True)
