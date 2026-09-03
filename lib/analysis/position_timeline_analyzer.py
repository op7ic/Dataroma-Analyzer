#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Position Timeline Analyzer

Tracks position building and reduction over time for manager-stock combinations.
Shows accumulation vs distribution phases, position sizing progression,
and identifies when managers flip from building to reducing positions.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

import pandas as pd
from typing import Dict
import re

from .base_analyzer import MultiAnalyzer
from ..data.data_loader import DataLoader


class PositionTimelineAnalyzer(MultiAnalyzer):
    """Analyzes position building and reduction patterns over time."""

    def __init__(self, data_loader: DataLoader) -> None:
        """Initialize with data loader."""
        super().__init__(data_loader)

    def analyze_all(self) -> Dict[str, pd.DataFrame]:
        """Run all position timeline analyses."""
        results = {}

        results["position_building_timeline"] = self.analyze_position_building_timeline()
        results["accumulation_vs_distribution"] = self.analyze_accumulation_distribution_phases()
        results["position_flip_points"] = self.analyze_position_flip_points()
        results["stock_timelines"] = self.analyze_stock_timelines_summary()

        for name, df in results.items():
            self.log_analysis_summary(df, name)

        return self.format_all_outputs(results)

    def analyze_position_building_timeline(self) -> pd.DataFrame:
        """
        Create detailed timeline showing how managers build/reduce positions over time.

        Shows quarter-by-quarter progression of positions with cumulative tracking.

        Column definitions:
        - shares_changed: Absolute magnitude of shares changed (always positive)
        - shares_delta: Signed delta (+positive for Buy/Add, -negative for Sell/Reduce)
        - cumulative_shares: Running total of shares held per ticker+manager

        Returns:
            DataFrame with position timeline for significant manager-stock combinations
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        print("Analyzing Position Building Timelines...")

        # Get all manager-stock combinations with 5+ activities (significant positions)
        position_counts = self.data.history_df.groupby(["ticker", "manager_id"]).size()
        significant_positions = position_counts[position_counts >= 5].index.tolist()

        timeline_data = []

        for ticker, manager in significant_positions:
            # Get all activities for this manager-stock combination
            activities = self.data.history_df[
                (self.data.history_df["ticker"] == ticker) & (self.data.history_df["manager_id"] == manager)
            ].copy()

            if activities.empty:
                continue

            # Sort by period chronologically (not alphabetically)
            # Period format: "Q1 2024", "Q2 2024", etc.
            def period_sort_key(period: str) -> tuple:
                """Convert period string to sortable (year, quarter) tuple."""
                match = re.match(r"Q(\d)\s+(\d{4})", str(period))
                if match:
                    return (int(match.group(2)), int(match.group(1)))
                return (0, 0)

            activities = activities.copy()
            activities["_sort_key"] = activities["period"].apply(period_sort_key)
            activities = activities.sort_values("_sort_key").drop(columns=["_sort_key"])

            # Check if we have complete history (first action should be Buy)
            # 13F EDGE CASE: If first action is Add/Reduce, manager had pre-existing
            # position before data tracking began. Our cumulative tracking will be
            # inaccurate because we don't know their initial position size.
            first_action_type = activities.iloc[0]["action_type"] if len(activities) > 0 else "Unknown"
            has_complete_history = first_action_type == "Buy"

            # Build timeline entries for this ticker+manager
            entries = []
            for _, act in activities.iterrows():
                action_type = act.get("action_type", "Hold")
                shares = act.get("shares", 0)
                action_str = act.get("action", "")
                quarter = act.get("period", "")

                # shares_changed is always positive (the magnitude)
                shares_changed = abs(shares) if shares else 0

                # Calculate signed delta based on action type
                # Buy/Add: positive delta (acquiring shares)
                # Sell/Reduce: negative delta (disposing shares)
                if action_type in ["Buy", "Add"]:
                    shares_delta = shares_changed
                elif action_type in ["Sell", "Reduce"]:
                    shares_delta = -shares_changed
                else:
                    # Hold or unknown - no change
                    shares_delta = 0

                entries.append({
                    "ticker": ticker,
                    "manager_id": manager,
                    "manager_name": self.data.manager_names.get(manager, manager),
                    "quarter": quarter,
                    "action_type": action_type,
                    "action": action_str,
                    "shares_changed": shares_changed,
                    "shares_delta": shares_delta,
                    "has_complete_history": has_complete_history,
                    "_sort_key": period_sort_key(quarter),
                })

            # Convert to DataFrame for cumulative calculation
            entries_df = pd.DataFrame(entries)

            # Sort by period to ensure correct cumulative calculation
            entries_df = entries_df.sort_values("_sort_key")

            # Calculate cumulative_shares as running total of shares_delta
            entries_df["cumulative_shares"] = entries_df["shares_delta"].cumsum()

            # Clip to ensure no negative cumulative (can't hold negative shares)
            entries_df["cumulative_shares"] = entries_df["cumulative_shares"].clip(lower=0)

            # Determine phase for each entry (based on last 3 actions)
            phases = []
            for i in range(len(entries_df)):
                # Get last 3 actions up to and including this row
                start_idx = max(0, i - 2)
                recent_actions = entries_df.iloc[start_idx:i + 1]["action_type"].tolist()

                if len(recent_actions) >= 2:
                    buy_add_count = sum(1 for a in recent_actions if a in ["Buy", "Add"])
                    sell_reduce_count = sum(1 for a in recent_actions if a in ["Sell", "Reduce"])

                    if buy_add_count > sell_reduce_count:
                        phase = "Accumulating"
                    elif sell_reduce_count > buy_add_count:
                        phase = "Distributing"
                    else:
                        phase = "Transitioning"
                else:
                    phase = "Initial"
                phases.append(phase)

            entries_df["phase"] = phases

            # Drop the sort key column
            entries_df = entries_df.drop(columns=["_sort_key"])

            timeline_data.append(entries_df)

        if not timeline_data:
            return pd.DataFrame()

        timeline_df = pd.concat(timeline_data, ignore_index=True)

        # Add company names
        if self.data.holdings_df is not None and "stock" in self.data.holdings_df.columns:
            company_names = self.data.holdings_df.groupby("ticker")["stock"].first()
            timeline_df = timeline_df.merge(
                company_names.to_frame("company_name"), left_on="ticker", right_index=True, how="left"
            )

        # Final sort by ticker, manager, and period (chronologically)
        def period_sort_key_series(period: str) -> tuple:
            """Convert period string to sortable (year, quarter) tuple."""
            match = re.match(r"Q(\d)\s+(\d{4})", str(period))
            if match:
                return (int(match.group(2)), int(match.group(1)))
            return (0, 0)

        timeline_df["_sort_key"] = timeline_df["quarter"].apply(period_sort_key_series)
        timeline_df = timeline_df.sort_values(["ticker", "manager_id", "_sort_key"])
        timeline_df = timeline_df.drop(columns=["_sort_key"])

        result = self.format_output(timeline_df)
        return self.add_metadata_columns(result, window_quarters=20, analysis_type="position_building_timeline")

    def analyze_accumulation_distribution_phases(self) -> pd.DataFrame:
        """
        Identify current phase (accumulating vs distributing) aggregated by STOCK across all managers.

        Shows which stocks are seeing net accumulation vs distribution across the manager universe.

        Returns only the 100 stocks with the largest |net activity| over the last
        4 quarters among stocks touched by >=2 managers - a deliberately
        directional slice, not a census of all stocks.

        Returns:
            DataFrame showing which stocks are being built up vs reduced
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        print("🔄 Analyzing Accumulation vs Distribution Phases...")

        # Get recent 4 quarters to determine current trend
        recent_quarters = self.get_recent_quarters(4)

        # Get recent activities for ALL stocks
        recent_activities = self.data.history_df[self.data.history_df["period"].isin(recent_quarters)]

        if recent_activities.empty:
            return pd.DataFrame()

        # Aggregate by STOCK across all managers
        stock_activity = (
            recent_activities.groupby("ticker")
            .agg({"action_type": lambda x: x.value_counts().to_dict(), "manager_id": "nunique", "period": "nunique"})
            .reset_index()
        )

        stock_activity.columns = ["ticker", "action_breakdown", "unique_managers", "quarters_active"]

        # Extract buy/sell counts
        stock_activity["buy_add_actions"] = stock_activity["action_breakdown"].apply(
            lambda x: x.get("Buy", 0) + x.get("Add", 0)
        )
        stock_activity["sell_reduce_actions"] = stock_activity["action_breakdown"].apply(
            lambda x: x.get("Sell", 0) + x.get("Reduce", 0)
        )
        stock_activity["net_activity"] = stock_activity["buy_add_actions"] - stock_activity["sell_reduce_actions"]

        # Determine phase based on net activity
        stock_activity["phase"] = "Mixed"
        stock_activity.loc[stock_activity["net_activity"] > 0, "phase"] = "Accumulating"
        stock_activity.loc[stock_activity["net_activity"] < 0, "phase"] = "Distributing"

        # Add current holdings information
        if self.data.holdings_df is not None and not self.data.holdings_df.empty:
            current_holdings = (
                self.data.holdings_df.groupby("ticker")
                .agg({"value": "sum", "shares": "sum", "manager_id": "count", "stock": "first"})
                .reset_index()
            )
            current_holdings.columns = ["ticker", "current_value", "current_shares", "current_holders", "company_name"]

            stock_activity = stock_activity.merge(current_holdings, on="ticker", how="left")
            stock_activity["current_value"] = stock_activity["current_value"].fillna(0)
            stock_activity["current_shares"] = stock_activity["current_shares"].fillna(0)
            stock_activity["current_holders"] = stock_activity["current_holders"].fillna(0)
        else:
            stock_activity["current_value"] = 0
            stock_activity["current_shares"] = 0
            stock_activity["current_holders"] = 0
            stock_activity["company_name"] = ""

        # Filter for stocks with at least 2 managers active
        significant_stocks = stock_activity[stock_activity["unique_managers"] >= 2].copy()

        if significant_stocks.empty:
            return pd.DataFrame()

        # Sort by absolute net activity (most active first).
        # NOTE: this ranking is directional by construction - a "Mixed" stock
        # (net_activity == 0) sorts last and therefore never survives the
        # head(100) cut. The phase mix of the returned rows is a property of
        # this selection, not of the stock universe; anything presenting it
        # (e.g. the accumulation/distribution pie) must say so.
        significant_stocks["abs_net_activity"] = significant_stocks["net_activity"].abs()
        significant_stocks = significant_stocks.sort_values("abs_net_activity", ascending=False)

        # Return top 100 most active (mix of accumulating and distributing)
        result = self.format_output(significant_stocks.drop(columns=["abs_net_activity"])).head(100)
        return self.add_metadata_columns(result, window_quarters=4, analysis_type="accumulation_vs_distribution")

    def analyze_position_flip_points(self) -> pd.DataFrame:
        """
        Identify when managers switched from accumulation to distribution.

        These "flip points" can signal important changes in conviction.
        Includes flip points from recent 8 quarters for historical perspective.

        Returns:
            DataFrame with position flip points including traceability status
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        print("Identifying Position Flip Points...")

        # Get current quarter for quarters_after_flip calculation
        recent_quarters_list = self.get_recent_quarters(1)
        current_quarter = recent_quarters_list[0] if recent_quarters_list else "Q4 2024"

        # Get recent 8 quarters for analysis scope (2 years of data)
        recent_quarters = self.get_recent_quarters(8)

        # Build set of tickers in position_building_timeline for traceability
        timeline_tickers = set()
        if self.data.history_df is not None:
            # Get significant positions (5+ activities) - same criteria as timeline
            position_counts = self.data.history_df.groupby(["ticker", "manager_id"]).size()
            significant_positions = position_counts[position_counts >= 5].index.tolist()
            timeline_tickers = set(t for t, _ in significant_positions)

        flip_points = []

        # Group by ticker and manager
        for (ticker, manager), group in self.data.history_df.groupby(["ticker", "manager_id"]):
            if len(group) < 4:
                continue

            # Sort chronologically using period_sort_key
            def period_sort_key(period: str) -> tuple:
                """Convert period string to sortable (year, quarter) tuple."""
                match = re.match(r"Q(\d)\s+(\d{4})", str(period))
                if match:
                    return (int(match.group(2)), int(match.group(1)))
                return (0, 0)

            group = group.copy()
            group["_sort_key"] = group["period"].apply(period_sort_key)
            group = group.sort_values("_sort_key").drop(columns=["_sort_key"])

            # Look for transitions from accumulation to distribution
            actions = group["action_type"].tolist()
            periods = group["period"].tolist()

            for i in range(len(actions) - 2):
                # Get 3-action window
                window = actions[i : i + 3]

                # Check for flip: 2+ accumulation actions -> distribution action
                early_accum = sum(1 for a in window[:2] if a in ["Buy", "Add"])
                late_distrib = sum(1 for a in window[1:] if a in ["Sell", "Reduce"])

                if early_accum >= 2 and late_distrib >= 1 and window[-1] in ["Sell", "Reduce"]:
                    # Found a flip point
                    flip_quarter = periods[i + 2]

                    # Only include flips from recent 8 quarters for manageable output
                    if flip_quarter not in recent_quarters:
                        continue

                    # Get shares at flip point
                    flip_action = group.iloc[i + 2]

                    # Calculate quarters between flip and current quarter
                    quarters_after = self._calculate_quarters_between(flip_quarter, current_quarter)

                    # Determine traceability status
                    if ticker in timeline_tickers:
                        traceability_status = "matched"
                    elif "-OLD" in ticker or ticker.endswith("Q"):
                        traceability_status = "delisted"
                    else:
                        traceability_status = "no_timeline"

                    flip_points.append(
                        {
                            "ticker": ticker,
                            "manager_id": manager,
                            "manager_name": self.data.manager_names.get(manager, manager),
                            "flip_quarter": flip_quarter,
                            "flip_action": flip_action.get("action", ""),
                            "shares_at_flip": flip_action.get("shares", 0),
                            "action_sequence": " -> ".join(window),
                            "quarters_before_flip": i + 1,
                            "quarters_after_flip": quarters_after,
                            "traceability_status": traceability_status,
                        }
                    )

        if not flip_points:
            return pd.DataFrame()

        flip_df = pd.DataFrame(flip_points)

        # Add company names from holdings_df
        if self.data.holdings_df is not None and "stock" in self.data.holdings_df.columns:
            company_names = self.data.holdings_df.groupby("ticker")["stock"].first()
            flip_df = flip_df.merge(
                company_names.to_frame("company_name"), left_on="ticker", right_index=True, how="left"
            )
        else:
            flip_df["company_name"] = ""

        # Try to get company name from history_df for missing values
        if "stock" in self.data.history_df.columns:
            history_names = self.data.history_df.groupby("ticker")["stock"].first()
            for idx in flip_df.index:
                if pd.isna(flip_df.at[idx, "company_name"]) or flip_df.at[idx, "company_name"] == "":
                    ticker = flip_df.at[idx, "ticker"]
                    if ticker in history_names.index:
                        flip_df.at[idx, "company_name"] = history_names[ticker]

        # Add current status
        if self.data.holdings_df is not None:
            current_holdings = self.data.holdings_df.groupby(["ticker", "manager_id"]).agg(
                {"shares": "first", "value": "first"}
            )
            flip_df = flip_df.merge(
                current_holdings.add_prefix("current_"), left_on=["ticker", "manager_id"], right_index=True, how="left"
            )

            # Fill missing current_shares/current_value with 0 for sold positions
            # Check if flip action indicates full sale
            def fill_current_shares(row):
                if pd.isna(row.get("current_shares")):
                    flip_action = str(row.get("flip_action", ""))
                    if "100%" in flip_action or "Sell" in flip_action:
                        return 0
                    return 0  # Position likely closed if not in current holdings
                return row.get("current_shares")

            def fill_current_value(row):
                if pd.isna(row.get("current_value")):
                    if row.get("current_shares") == 0:
                        return 0
                    flip_action = str(row.get("flip_action", ""))
                    if "100%" in flip_action:
                        return 0
                    return 0  # Position likely closed
                return row.get("current_value")

            flip_df["current_shares"] = flip_df.apply(fill_current_shares, axis=1)
            flip_df["current_value"] = flip_df.apply(fill_current_value, axis=1)

            flip_df["still_held"] = flip_df["current_shares"] > 0
        else:
            flip_df["current_shares"] = 0
            flip_df["current_value"] = 0
            flip_df["still_held"] = False

        # Sample from each quarter to ensure variety in the output
        # Goal: Get approximately equal representation from each quarter
        max_results = 100
        quarters_with_flips = flip_df["flip_quarter"].unique()
        flips_per_quarter = max(1, max_results // len(quarters_with_flips)) if len(quarters_with_flips) > 0 else max_results

        # Sort by flip quarter (most recent first), then by ticker for consistency
        def quarter_sort_key(q: str) -> tuple:
            match = re.match(r"Q(\d)\s+(\d{4})", str(q))
            if match:
                return (int(match.group(2)), int(match.group(1)))
            return (0, 0)

        flip_df["_sort_key"] = flip_df["flip_quarter"].apply(quarter_sort_key)
        flip_df = flip_df.sort_values(["_sort_key", "ticker"], ascending=[False, True])

        # Sample proportionally from each quarter for variety
        sampled_dfs = []
        for quarter in sorted(quarters_with_flips, key=quarter_sort_key, reverse=True):
            quarter_df = flip_df[flip_df["flip_quarter"] == quarter].head(flips_per_quarter)
            sampled_dfs.append(quarter_df)

        if sampled_dfs:
            flip_df = pd.concat(sampled_dfs, ignore_index=True)
            # Re-sort the combined result
            flip_df["_sort_key"] = flip_df["flip_quarter"].apply(quarter_sort_key)
            flip_df = flip_df.sort_values(["_sort_key", "ticker"], ascending=[False, True])

        flip_df = flip_df.drop(columns=["_sort_key"])

        # Add metadata about analysis scope
        flip_df = self.add_metadata_columns(
            flip_df,
            window_quarters=8,
            periods=recent_quarters,
            analysis_type="position_flip_points"
        )

        return self.format_output(flip_df).head(max_results)

    def _calculate_quarters_between(self, from_quarter: str, to_quarter: str) -> int:
        """
        Calculate the number of quarters between two quarter strings.

        Args:
            from_quarter: Starting quarter (e.g., "Q1 2024")
            to_quarter: Ending quarter (e.g., "Q4 2024")

        Returns:
            Number of quarters between (0 if same quarter, negative if from > to)
        """
        def parse_quarter(q: str) -> tuple:
            match = re.match(r"Q(\d)\s+(\d{4})", str(q))
            if match:
                return (int(match.group(2)), int(match.group(1)))
            return (0, 0)

        from_year, from_q = parse_quarter(from_quarter)
        to_year, to_q = parse_quarter(to_quarter)

        if from_year == 0 or to_year == 0:
            return 0

        # Calculate total quarters from a reference point
        from_total = from_year * 4 + from_q
        to_total = to_year * 4 + to_q

        return to_total - from_total

    def analyze_stock_timelines_summary(self) -> pd.DataFrame:
        """
        Create a human-readable summary of stock accumulation/distribution trends.

        This provides a quick overview for analysts to identify trending stocks
        without having to parse through the detailed timeline data.

        Returns:
            DataFrame with stock-level summary of activity trends
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        print("📊 Creating Stock Timelines Summary...")

        summary_rows = []

        for ticker in self.data.history_df["ticker"].unique():
            stock_data = self.data.history_df[self.data.history_df["ticker"] == ticker].copy()

            # Get company name safely
            company_raw = stock_data["stock"].iloc[0] if "stock" in stock_data.columns else ""
            company = str(company_raw)[:40] if pd.notna(company_raw) else ""

            # Count actions by type
            action_counts = stock_data["action_type"].value_counts()
            buy_count = action_counts.get("Buy", 0)
            add_count = action_counts.get("Add", 0)
            reduce_count = action_counts.get("Reduce", 0)
            sell_count = action_counts.get("Sell", 0)

            total_actions = len(stock_data)
            accumulation = buy_count + add_count
            distribution = reduce_count + sell_count

            # Get unique managers (top 3 by activity)
            manager_counts = stock_data["manager_id"].value_counts()
            top_manager_ids = manager_counts.head(3).index.tolist()
            top_managers = [self.data.manager_names.get(m, m) for m in top_manager_ids]
            num_managers = len(manager_counts)

            # Determine trend
            if accumulation > distribution * 1.5:
                trend = "Accumulating"
            elif distribution > accumulation * 1.5:
                trend = "Distributing"
            elif accumulation > distribution:
                trend = "Slight Accumulate"
            elif distribution > accumulation:
                trend = "Slight Distribute"
            else:
                trend = "Neutral"

            # Recent activity (last 4 quarters)
            recent_quarters = self.get_recent_quarters(4)
            recent_data = stock_data[stock_data["period"].isin(recent_quarters)]
            recent_actions = len(recent_data)

            # Skip stocks with no recent activity and low total activity
            if recent_actions == 0 and total_actions < 5:
                continue

            # Momentum score (accumulation - distribution as %)
            momentum = round((accumulation - distribution) / max(total_actions, 1) * 100, 1)

            summary_rows.append({
                "ticker": ticker,
                "company": company,
                "trend": trend,
                "momentum": momentum,
                "buys": buy_count,
                "adds": add_count,
                "reduces": reduce_count,
                "sells": sell_count,
                "manager_count": num_managers,
                "top_managers": ", ".join(str(m) for m in top_managers[:3]),
                "managers_shown": min(3, len(top_managers)),  # Actual count shown (max 3)
                "recent_activity": recent_actions,
            })

        if not summary_rows:
            return pd.DataFrame()

        summary_df = pd.DataFrame(summary_rows)

        # Sort by recent activity and momentum
        summary_df = summary_df.sort_values(
            ["recent_activity", "momentum"], ascending=[False, False]
        )

        # Keep top 200 most active stocks
        result = summary_df.head(200)
        # window_quarters must match the recency window actually used above
        # (get_recent_quarters(4)); it was previously mislabeled as 3.
        return self.add_metadata_columns(result, window_quarters=4, analysis_type="stock_timelines")
