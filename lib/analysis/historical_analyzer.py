#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Historical Analyzer

Long-term trend analysis across multiple market cycles and decades.
Analyzes 18+ years of data (2007-2025) to identify long-term patterns,
crisis responses, and multi-decade conviction plays.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

import logging
import pandas as pd
from typing import Dict

from .base_analyzer import MultiAnalyzer

logger = logging.getLogger(__name__)
from ..data.data_loader import DataLoader


class HistoricalAnalyzer(MultiAnalyzer):
    """Analyzes historical patterns across 18+ years of investment data."""

    def __init__(self, data_loader: DataLoader) -> None:
        """Initialize with historical data capabilities."""
        super().__init__(data_loader)
        self.crisis_periods = {
            "2008_financial": ["Q3 2008", "Q4 2008", "Q1 2009", "Q2 2009"],
            "2020_covid": ["Q1 2020", "Q2 2020"],
            "2022_inflation": ["Q1 2022", "Q2 2022", "Q3 2022"],
        }

    def analyze_all(self) -> Dict[str, pd.DataFrame]:
        """Run all historical analyses."""
        results = {}

        results["manager_track_records"] = self.analyze_manager_track_records()
        results["stock_life_cycles"] = self.analyze_stock_life_cycles()
        results["sector_rotation_patterns"] = self.analyze_sector_rotation()
        results["crisis_response_analysis"] = self.analyze_crisis_responses()
        results["multi_decade_conviction"] = self.analyze_multi_decade_conviction()
        results["quarterly_activity_timeline"] = self.analyze_quarterly_timeline()
        results["long_term_winners"] = self.analyze_long_term_winners()

        for name, df in results.items():
            self.log_analysis_summary(df, name)

        return self.format_all_outputs(results)

    def analyze_manager_track_records(self) -> pd.DataFrame:
        """
        Analyze manager performance over decades.

        Track records include:
        - Years active
        - Crisis navigation success
        - Long-term performance patterns
        - Consistency across market cycles

        Action columns are mutually exclusive: sell_actions counts complete exits
        (Sell) only and reduce_actions counts position decreases (Reduce) only, so a
        sell-side total is sell_actions + reduce_actions.
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        history = self.data.history_df.copy()
        history["year"] = history["period"].str.extract(r"(\d{4})")
        history["year"] = pd.to_numeric(history["year"])

        manager_records = []

        for manager_id in history["manager_id"].unique():
            manager_data = history[history["manager_id"] == manager_id]

            first_year = manager_data["year"].min()
            last_year = manager_data["year"].max()
            # Use inclusive calculation: years_active = last_year - first_year + 1
            years_active = last_year - first_year + 1
            total_actions = len(manager_data)

            crisis_actions = self._analyze_crisis_actions(manager_data)

            action_breakdown = {
                k: int(v) for k, v in manager_data["action_type"].value_counts().to_dict().items()
            }  # Convert numpy to Python int

            yearly_actions = manager_data.groupby("year").size()
            consistency_score = 1 - (yearly_actions.std() / yearly_actions.mean() if yearly_actions.mean() > 0 else 0)

            current_holdings = 0
            if self.data.holdings_df is not None:
                current_holdings = len(self.data.holdings_df[self.data.holdings_df["manager_id"] == manager_id])

            record = {
                "manager_id": manager_id,
                "manager_name": self.data.manager_names.get(manager_id, manager_id),
                "years_active": years_active,
                "first_year": first_year,
                "last_year": last_year,
                "total_actions": total_actions,
                "current_holdings": current_holdings,
                "buy_actions": action_breakdown.get("Buy", 0),
                "sell_actions": action_breakdown.get("Sell", 0),
                "add_actions": action_breakdown.get("Add", 0),
                "reduce_actions": action_breakdown.get("Reduce", 0),
                "consistency_score": consistency_score,
                **crisis_actions,
            }

            manager_records.append(record)

        df = pd.DataFrame(manager_records)

        if self.data.holdings_df is not None and not self.data.holdings_df.empty:
            current_portfolios = self.data.holdings_df.groupby("manager_id")["value"].sum()
            df["current_portfolio_value"] = df["manager_id"].map(current_portfolios).fillna(0)

            # NOTE: earlier versions fabricated estimated_initial_value /
            # total_return_pct / annualized_return_pct here by assuming a
            # constant 10%/yr growth rate. 13F filings contain no purchase or
            # sale prices, so no actual return can be computed from this data;
            # the derived numbers were a pure function of years_active and were
            # presented as measured performance. They have been removed.

        # Calculate track_record_score, handling potential NaN values
        df["track_record_score"] = (
            df["years_active"].fillna(0) * 0.3
            + df["consistency_score"].fillna(0) * 20
            + df["crisis_buying_ratio"].fillna(0) * 10
            + (df["current_holdings"] > 0).astype(int) * 5
        )

        # Ensure track_record_score is never missing
        df["track_record_score"] = df["track_record_score"].fillna(0)

        # Log warning for any rows where score could not be calculated properly
        missing_score_count = (df["track_record_score"] == 0).sum()
        if missing_score_count > 0:
            managers_with_zero = df[df["track_record_score"] == 0]["manager_name"].tolist()
            if managers_with_zero:
                logger.warning(
                    f"track_record_score defaulted to 0 for {missing_score_count} managers: "
                    f"{managers_with_zero[:5]}{'...' if len(managers_with_zero) > 5 else ''}"
                )

        # Validate years_active calculation
        years_mismatch = df[df["years_active"] != (df["last_year"] - df["first_year"] + 1)]
        if not years_mismatch.empty:
            logger.error(
                f"years_active validation failed for {len(years_mismatch)} managers. "
                f"years_active must equal last_year - first_year + 1"
            )

        result = df.sort_values("track_record_score", ascending=False)
        return self.add_metadata_columns(result, window_quarters=72, analysis_type="manager_track_records")

    def analyze_stock_life_cycles(self) -> pd.DataFrame:
        """
        Analyze complete life cycles of stocks.

        Includes:
        - Entry timing for successful stocks
        - When smart money bought today's winners
        - Exit patterns before major declines

        total_sells counts complete exits (Sell) only and total_reduces counts
        position decreases (Reduce) only; a sell-side total is their sum.
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        history = self.data.history_df.copy()
        history["year"] = history["period"].str.extract(r"(\d{4})")

        # Chronological quarter number (year*4 + quarter). A string min/max on
        # "Qn YYYY" is lexicographic (all Q1s before any Q2) and yields wrong
        # first/last periods for tickers whose earliest activity is in Q2-Q4.
        history["_qnum"] = (
            history["period"].str.extract(r"(\d{4})").astype(int) * 4
            + history["period"].str.extract(r"Q(\d)").astype(int)
        )

        stock_actions = history.groupby("ticker").agg(
            {
                "period": "count",
                "_qnum": ["min", "max"],
                "action_type": lambda x: {
                    k: int(v) for k, v in x.value_counts().to_dict().items()
                },  # Convert numpy to Python int
                "manager_id": "nunique",
            }
        )

        stock_actions.columns = ["total_actions", "_first_qnum", "_last_qnum", "action_breakdown", "unique_managers"]

        stock_actions["first_year"] = (stock_actions["_first_qnum"] - 1) // 4
        stock_actions["last_year"] = (stock_actions["_last_qnum"] - 1) // 4
        stock_actions["years_tracked"] = stock_actions["last_year"] - stock_actions["first_year"] + 1
        stock_actions = stock_actions.drop(columns=["_first_qnum", "_last_qnum"])

        current_tickers = set()
        if self.data.holdings_df is not None:
            current_tickers = set(self.data.holdings_df["ticker"].unique())

        stock_actions["currently_held"] = stock_actions.index.isin(current_tickers)

        life_cycles = []

        # Chronologically earliest Buy per ticker (string .min() would be wrong)
        buys = history[history["action_type"] == "Buy"]
        first_buy_by_ticker = (
            buys.loc[buys.groupby("ticker")["_qnum"].idxmin()][["ticker", "period"]].set_index("ticker")["period"]
            if not buys.empty
            else pd.Series(dtype=object)
        )

        for ticker, row in stock_actions.iterrows():
            actions = row["action_breakdown"]

            first_buys = first_buy_by_ticker.get(ticker)

            complete_exits = history[(history["ticker"] == ticker) & (history["action"] == "Sell 100.00%")]

            life_cycle = {
                "ticker": ticker,
                "first_year": row["first_year"],
                "last_year": row["last_year"],
                "years_tracked": row["years_tracked"],
                "total_actions": row["total_actions"],
                "unique_managers": row["unique_managers"],
                "currently_held": row["currently_held"],
                "total_buys": actions.get("Buy", 0),
                "total_sells": actions.get("Sell", 0),
                "total_adds": actions.get("Add", 0),
                "total_reduces": actions.get("Reduce", 0),
                "first_buy_period": first_buys if pd.notna(first_buys) else "",
                "complete_exit_count": len(complete_exits),
                # Net action count. Reduce carries its full weight (it was
                # half-weighted, which made this score contradict the
                # buy-vs-sell scatter that plots the same stocks).
                "accumulation_score": actions.get("Buy", 0)
                + actions.get("Add", 0)
                - actions.get("Sell", 0)
                - actions.get("Reduce", 0),
            }

            life_cycles.append(life_cycle)

        df = pd.DataFrame(life_cycles)

        if not df.empty and self.data.holdings_df is not None and "stock" in self.data.holdings_df.columns:
            company_names = self.data.holdings_df.groupby("ticker")["stock"].first()
            df = df.set_index("ticker").join(company_names.rename("company_name"), how="left").reset_index()

        # Fall back to names from the activity history so EXITED stocks keep
        # their names. Without this fallback, company_name was non-empty iff
        # currently_held, and the "delisted" filter below deleted every exited
        # stock — making exit-pattern analysis impossible.
        if not df.empty and "stock" in history.columns:
            history_names = history.groupby("ticker")["stock"].first()
            df["company_name"] = (
                df.get("company_name", pd.Series(index=df.index, dtype=object))
                .replace("", pd.NA)
                .fillna(df["ticker"].map(history_names))
            )

        # Filter out delisted/unidentifiable tickers (no name from any source
        # and not currently held)
        if "company_name" in df.columns:
            df = df[
                ~(
                    (df["company_name"].isna() | (df["company_name"] == ""))
                    & (~df["currently_held"])
                )
            ]

        df["life_cycle_score"] = (
            df["years_tracked"] * 0.5
            + df["unique_managers"] * 2
            + df["accumulation_score"] * 0.1
            + df["currently_held"].astype(int) * 5
        )

        result = df.sort_values("life_cycle_score", ascending=False)
        n_quarters = int(history["period"].nunique())
        qmin, qmax = history.loc[history["_qnum"].idxmin(), "period"], history.loc[history["_qnum"].idxmax(), "period"]
        return self.add_metadata_columns(
            result,
            window_quarters=n_quarters,
            periods=[f"{qmin} to {qmax}"],
            analysis_type="stock_life_cycles",
        )

    def analyze_sector_rotation(self) -> pd.DataFrame:
        """
        Analyze sector rotation patterns over time.

        Note: Since we don't have sector data from Dataroma,
        we'll infer sectors from stock symbols and patterns.

        Output columns:
        - buy_actions: Count of new position initiations (Buy)
        - add_actions: Count of position increases (Add)
        - sell_actions: Count of complete exits (Sell)
        - reduce_actions: Count of position decreases (Reduce)
        - net_flow: Net activity = buy_actions - sell_actions
        - net_activity: Full net = buy_actions + add_actions - sell_actions - reduce_actions
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        history = self.data.history_df.copy()
        history["year"] = history["period"].str.extract(r"(\d{4})").astype(int)
        history["quarter"] = history["period"].str.extract(r"(Q\d)")

        tech_keywords = ["GOOGL", "GOOG", "AAPL", "MSFT", "META", "AMZN", "NVDA", "CRM", "ORCL", "IBM"]
        finance_keywords = ["BAC", "JPM", "WFC", "C", "GS", "MS", "BRK", "AXP", "V", "MA"]
        energy_keywords = ["XOM", "CVX", "COP", "OXY", "SLB", "EOG", "PXD", "VLO", "PSX"]
        healthcare_keywords = ["JNJ", "UNH", "PFE", "ABBV", "TMO", "DHR", "ABT", "CVS", "LLY"]

        def classify_sector(ticker):
            if ticker in tech_keywords:
                return "Technology"
            elif ticker in finance_keywords:
                return "Financials"
            elif ticker in energy_keywords:
                return "Energy"
            elif ticker in healthcare_keywords:
                return "Healthcare"
            else:
                return "Other"

        history["sector"] = history["ticker"].apply(classify_sector)

        sector_flows = history.groupby(["period", "sector", "action_type"]).size().unstack(fill_value=0)

        rotation_summary = []

        for period in history["period"].unique():
            period_data = sector_flows.loc[period] if period in sector_flows.index else pd.DataFrame()

            if not period_data.empty:
                for sector in period_data.index:
                    buy_count = period_data.loc[sector].get("Buy", 0)
                    add_count = period_data.loc[sector].get("Add", 0)
                    sell_count = period_data.loc[sector].get("Sell", 0)
                    reduce_count = period_data.loc[sector].get("Reduce", 0)

                    rotation_summary.append(
                        {
                            "period": period,
                            "sector": sector,
                            "buy_actions": buy_count,
                            "add_actions": add_count,
                            "sell_actions": sell_count,
                            "reduce_actions": reduce_count,
                            # net_flow = buy_actions - sell_actions (reconcilable)
                            "net_flow": buy_count - sell_count,
                            # net_activity includes all action types
                            "net_activity": buy_count + add_count - sell_count - reduce_count,
                        }
                    )

        result = pd.DataFrame(rotation_summary)
        return self.add_metadata_columns(result, window_quarters=72, analysis_type="sector_rotation_patterns")

    def analyze_crisis_responses(self) -> pd.DataFrame:
        """
        Compare behavior during different crisis periods.

        Analyzes:
        - 2008 Financial Crisis
        - 2020 COVID Crash
        - 2022 Inflation/Rate Hike Period

        sell_actions counts complete exits (Sell) only and reduce_actions counts
        position decreases (Reduce) only. sell_ratio is the combined sell-side
        share, (Sell + Reduce) / total_actions.
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        crisis_analysis = []

        for crisis_name, crisis_quarters in self.crisis_periods.items():
            crisis_data = self.data.history_df[self.data.history_df["period"].isin(crisis_quarters)]

            if crisis_data.empty:
                continue

            total_actions = len(crisis_data)
            action_breakdown = crisis_data["action_type"].value_counts()

            buy_ratio = (
                (action_breakdown.get("Buy", 0) + action_breakdown.get("Add", 0)) / total_actions
                if total_actions > 0
                else 0
            )
            sell_ratio = (
                (action_breakdown.get("Sell", 0) + action_breakdown.get("Reduce", 0)) / total_actions
                if total_actions > 0
                else 0
            )

            crisis_buyers = (
                crisis_data[crisis_data["action_type"].isin(["Buy", "Add"])]["manager_id"].value_counts().head(5)
            )

            crisis_buys = crisis_data[crisis_data["action_type"].isin(["Buy", "Add"])]["ticker"].value_counts().head(10)

            crisis_sells = (
                crisis_data[crisis_data["action_type"].isin(["Sell", "Reduce"])]["ticker"].value_counts().head(10)
            )

            crisis_summary = {
                "crisis": crisis_name,
                "period": ", ".join(crisis_quarters),
                "total_actions": total_actions,
                "buy_actions": action_breakdown.get("Buy", 0),
                "add_actions": action_breakdown.get("Add", 0),
                "sell_actions": action_breakdown.get("Sell", 0),
                "reduce_actions": action_breakdown.get("Reduce", 0),
                "buy_ratio": buy_ratio,
                "sell_ratio": sell_ratio,
                "top_buyers": ", ".join([f"{self.data.manager_names.get(m, m)}" for m in crisis_buyers.index[:3]]),
                "most_bought": ", ".join(crisis_buys.index[:5]),
                "most_sold": ", ".join(crisis_sells.index[:5]),
                "unique_managers": crisis_data["manager_id"].nunique(),
                "unique_stocks": crisis_data["ticker"].nunique(),
            }

            crisis_analysis.append(crisis_summary)

        result = pd.DataFrame(crisis_analysis)
        return self.add_metadata_columns(result, window_quarters=72, analysis_type="crisis_response_analysis")

    def analyze_multi_decade_conviction(self) -> pd.DataFrame:
        """
        Identify stocks held through multiple market cycles.

        Finds:
        - Stocks held for 10+ years
        - Concentration changes over time
        - True long-term conviction plays
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        history = self.data.history_df.copy()
        history["year"] = history["period"].str.extract(r"(\d{4})").astype(int)

        stock_spans = history.groupby("ticker")["year"].agg(["min", "max", "nunique"])
        long_term_stocks = stock_spans[stock_spans["max"] - stock_spans["min"] >= 10]

        conviction_plays = []

        for ticker in long_term_stocks.index:
            ticker_history = history[history["ticker"] == ticker]

            managers_by_year = ticker_history.groupby("year")["manager_id"].nunique()
            consistent_managers = []

            for manager in ticker_history["manager_id"].unique():
                manager_years = ticker_history[ticker_history["manager_id"] == manager]["year"].nunique()
                if manager_years >= 5:
                    consistent_managers.append(manager)

            accumulation = 0
            for _, row in ticker_history.iterrows():
                if row["action_type"] == "Buy":
                    accumulation += 1
                elif row["action_type"] == "Add":
                    accumulation += 0.5
                elif row["action_type"] == "Reduce":
                    accumulation -= 0.5
                elif row["action_type"] == "Sell":
                    accumulation -= 1

                currently_held = False
            current_holders = 0
            if self.data.holdings_df is not None:
                current_data = self.data.holdings_df[self.data.holdings_df["ticker"] == ticker]
                currently_held = len(current_data) > 0
                current_holders = current_data["manager_id"].nunique()

            conviction_play = {
                "ticker": ticker,
                "years_tracked": long_term_stocks.loc[ticker, "nunique"],
                "first_year": long_term_stocks.loc[ticker, "min"],
                "last_year": long_term_stocks.loc[ticker, "max"],
                "total_actions": len(ticker_history),
                "unique_managers_all_time": ticker_history["manager_id"].nunique(),
                "consistent_long_term_holders": len(consistent_managers),
                "consistent_holders_list": ", ".join(
                    [self.data.manager_names.get(m, m) for m in consistent_managers[:3]]
                ),
                "managers_shown": min(3, len(consistent_managers)),  # Actual count shown (max 3)
                "net_accumulation_score": accumulation,
                "currently_held": currently_held,
                "current_holders": current_holders,
                "avg_managers_per_year": managers_by_year.mean(),
                "max_managers_in_year": managers_by_year.max(),
                "conviction_score": (
                    len(consistent_managers) * 3 + accumulation * 0.5 + current_holders * 2 + (currently_held * 5)
                ),
            }

            conviction_plays.append(conviction_play)

        df = pd.DataFrame(conviction_plays)

        if not df.empty and self.data.holdings_df is not None and "stock" in self.data.holdings_df.columns:
            company_names = self.data.holdings_df.groupby("ticker")["stock"].first()
            df = df.set_index("ticker").join(company_names.rename("company_name"), how="left").reset_index()

        result = df.sort_values("conviction_score", ascending=False)
        return self.add_metadata_columns(result, window_quarters=40, analysis_type="multi_decade_conviction")

    def analyze_quarterly_timeline(self) -> pd.DataFrame:
        """
        Create quarterly activity timeline showing market dynamics.

        sell_actions counts complete exits (Sell) only and reduce_actions counts
        position decreases (Reduce) only, so buy_actions + add_actions +
        sell_actions + reduce_actions reconciles to total_actions.
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        timeline = (
            self.data.history_df.groupby("period")
            .agg(
                {
                    "action_type": lambda x: {
                        k: int(v) for k, v in x.value_counts().to_dict().items()
                    },  # Convert numpy to Python int
                    "manager_id": "nunique",
                    "ticker": "nunique",
                    "period": "count",
                }
            )
            .rename(columns={"period": "total_actions"})
        )

        timeline_data = []
        for period, row in timeline.iterrows():
            actions = row["action_type"]
            timeline_data.append(
                {
                    "period": period,
                    "total_actions": row["total_actions"],
                    "unique_managers": row["manager_id"],
                    "unique_stocks": row["ticker"],
                    "buy_actions": actions.get("Buy", 0),
                    "sell_actions": actions.get("Sell", 0),
                    "add_actions": actions.get("Add", 0),
                    "reduce_actions": actions.get("Reduce", 0),
                    "net_activity": actions.get("Buy", 0)
                    + actions.get("Add", 0)
                    - actions.get("Sell", 0)
                    - actions.get("Reduce", 0),
                }
            )

        df = pd.DataFrame(timeline_data)

        df["year"] = df["period"].str.extract(r"(\d{4})").astype(int)
        df["quarter"] = df["period"].str.extract(r"Q(\d)").astype(int)

        # Sort chronologically AND reset the index: the groupby above iterated
        # periods in lexicographic order, so without reset_index(drop=True) the
        # index labels stay scrambled and any later .index[] lookup (e.g. the
        # crisis-period shading in historical_visualizer) lands on wrong rows.
        result = df.sort_values(["year", "quarter"]).reset_index(drop=True)
        span = [f"{result['period'].iloc[0]} to {result['period'].iloc[-1]}"] if len(result) else []
        return self.add_metadata_columns(
            result, window_quarters=len(result), periods=span, analysis_type="quarterly_activity_timeline"
        )

    def analyze_long_term_winners(self) -> pd.DataFrame:
        """
        Identify stocks that have been long-term winners based on manager actions.
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        history = self.data.history_df.copy()
        history["year"] = history["period"].str.extract(r"(\d{4})").astype(int)

        # Dynamically determine current year from data
        current_year = history["year"].max()

        early_buys = history[(history["year"] <= 2010) & (history["action_type"] == "Buy")]["ticker"].unique()

        current_holdings = set()
        if self.data.holdings_df is not None:
            current_holdings = set(self.data.holdings_df["ticker"].unique())

        potential_winners = list(set(early_buys) & current_holdings)

        winners = []
        for ticker in potential_winners:
            ticker_history = history[history["ticker"] == ticker]

            first_buy = ticker_history[ticker_history["action_type"] == "Buy"]["period"].min()
            first_observed_year = int(ticker_history[ticker_history["action_type"] == "Buy"]["year"].min())

            net_actions = 0
            for _, row in ticker_history.iterrows():
                if row["action_type"] in ["Buy", "Add"]:
                    net_actions += 1
                elif row["action_type"] in ["Sell", "Reduce"]:
                    net_actions -= 1

                current_data = (
                    self.data.holdings_df[self.data.holdings_df["ticker"] == ticker]
                    if self.data.holdings_df is not None
                    else pd.DataFrame()
                )
            current_holders = len(current_data["manager_id"].unique()) if not current_data.empty else 0
            total_value = (
                current_data["value"].sum() if not current_data.empty and "value" in current_data.columns else 0
            )

            winner = {
                "ticker": ticker,
                "first_buy_period": first_buy,
                "first_observed_year": first_observed_year,
                "years_held": current_year - first_observed_year,
                "total_historical_actions": len(ticker_history),
                "net_accumulation": net_actions,
                "current_holders": current_holders,
                "current_total_value": total_value,
                "unique_managers_all_time": ticker_history["manager_id"].nunique(),
                "winner_score": (current_year - first_observed_year) * current_holders,
            }

            winners.append(winner)

        df = pd.DataFrame(winners)

        if not df.empty and self.data.holdings_df is not None and "stock" in self.data.holdings_df.columns:
            company_names = self.data.holdings_df.groupby("ticker")["stock"].first()
            df = df.set_index("ticker").join(company_names.rename("company_name"), how="left").reset_index()

        result = df.sort_values("winner_score", ascending=False) if not df.empty else df
        return self.add_metadata_columns(result, window_quarters=72, analysis_type="long_term_winners")

    def _analyze_crisis_actions(self, manager_data: pd.DataFrame) -> Dict[str, float]:
        """Analyze a manager's actions during crisis periods."""
        crisis_metrics = {}

        total_crisis_actions = 0
        total_crisis_buys = 0

        for crisis_name, quarters in self.crisis_periods.items():
            crisis_data = manager_data[manager_data["period"].isin(quarters)]
            if not crisis_data.empty:
                crisis_actions = len(crisis_data)
                crisis_buys = len(crisis_data[crisis_data["action_type"].isin(["Buy", "Add"])])

                crisis_metrics[f"{crisis_name}_actions"] = crisis_actions
                crisis_metrics[f"{crisis_name}_buy_ratio"] = crisis_buys / crisis_actions if crisis_actions > 0 else 0

                total_crisis_actions += crisis_actions
                total_crisis_buys += crisis_buys

        crisis_metrics["total_crisis_actions"] = total_crisis_actions
        crisis_metrics["crisis_buying_ratio"] = (
            total_crisis_buys / total_crisis_actions if total_crisis_actions > 0 else 0
        )

        return crisis_metrics
