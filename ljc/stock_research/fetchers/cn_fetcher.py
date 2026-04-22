"""A-share market analyst data fetcher via akshare (Eastmoney source)."""

import time
import pandas as pd
import akshare as ak
from tqdm import tqdm
from config import TOP_ANALYSTS_PER_MARKET, ANALYST_RANK_YEAR


class CNAnalystFetcher:
    MARKET = "CN"

    def get_top_analysts(self) -> pd.DataFrame:
        """
        Fetch top analysts ranked by annual return from Eastmoney.
        akshare columns: 分析师名称, 分析师单位, 年度指数, 2024年收益率,
                         成分股个数, 分析师ID, 行业
        """
        print(f"\n[CN] Fetching top {TOP_ANALYSTS_PER_MARKET} A-share analysts...")
        try:
            df = ak.stock_analyst_rank_em(year=ANALYST_RANK_YEAR)
        except Exception as e:
            print(f"  [CN] akshare rank fetch failed: {e}")
            return pd.DataFrame()

        # Map to internal column names
        year_col = f"{ANALYST_RANK_YEAR}年收益率"
        rename_map = {
            "分析师名称": "analyst_name",
            "分析师单位": "broker",
            "年度指数":   "annual_index",
            year_col:     "win_rate",       # use annual return as the "win" metric
            "12个月收益率": "excess_return",
            "成分股个数":  "stock_count",
            "分析师ID":   "analyst_id",
            "行业":       "industry",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        for col in ("win_rate", "excess_return", "annual_index"):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if "win_rate" not in df.columns:
            if "annual_index" in df.columns:
                df["win_rate"] = df["annual_index"]
            else:
                print("  [CN] No return metric found, returning empty")
                return pd.DataFrame()

        if "excess_return" not in df.columns:
            df["excess_return"] = df.get("annual_index", 0)

        # Only keep analysts with actual stock coverage
        if "stock_count" in df.columns:
            df = df[df["stock_count"] > 0]

        df = df.sort_values("win_rate", ascending=False).head(TOP_ANALYSTS_PER_MARKET)
        df["market"] = self.MARKET
        if not df.empty:
            print(f"  [CN] Got {len(df)} analysts, top return = {df['win_rate'].iloc[0]:.1f}%")
        return df.reset_index(drop=True)

    def get_analyst_stocks(self, analyst_id: str, analyst_name: str) -> pd.DataFrame:
        """
        Get current buy-rated stocks tracked by this analyst.
        akshare columns: 股票代码, 股票名称, 调入日期, 最新评级日期,
                         当前评级名称, 成交价格(前复权), 最新价格, 阶段涨跌幅
        """
        try:
            df = ak.stock_analyst_detail_em(
                analyst_id=str(analyst_id),
                indicator="最新跟踪成分股",
            )
        except Exception:
            return pd.DataFrame()

        rename_map = {
            "股票代码":       "ticker",
            "股票名称":       "name",
            "当前评级名称":   "rating",
            "最新价格":       "current_price",
            "成交价格(前复权)": "entry_price",
            "阶段涨跌幅":     "return_pct",
            "最新评级日期":   "rating_date",
        }
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        if "rating" in df.columns:
            df = df[df["rating"].str.contains("买入|增持|强烈推荐", na=False)]

        df["analyst"] = analyst_name
        df["market"] = self.MARKET
        return df

    def collect_recommendations(self, analysts_df: pd.DataFrame) -> pd.DataFrame:
        """Collect buy recommendations from all top analysts."""
        all_recs = []
        for _, row in tqdm(analysts_df.iterrows(), total=len(analysts_df),
                           desc="[CN] collecting recs"):
            analyst_id = row.get("analyst_id", "")
            name = row.get("analyst_name", "")
            if not analyst_id:
                continue
            recs = self.get_analyst_stocks(analyst_id, name)
            if not recs.empty:
                recs["analyst_win_rate"] = row.get("win_rate", 0)
                recs["analyst_excess_return"] = row.get("excess_return", 0)
                recs["sector"] = row.get("industry", "")
                all_recs.append(recs)
            time.sleep(0.3)

        if not all_recs:
            return pd.DataFrame()
        return pd.concat(all_recs, ignore_index=True)
