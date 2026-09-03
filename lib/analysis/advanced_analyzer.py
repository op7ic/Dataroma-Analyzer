#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Advanced Analyzer

Sophisticated analysis including manager performance, sector rotation, and pattern detection.
Focuses on multi-decade patterns, manager excellence, and predictive signals from 18+ years of data.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

import pandas as pd
import numpy as np
import re
from typing import Dict
from collections import defaultdict, Counter

from .base_analyzer import MultiAnalyzer
from ..data.data_loader import DataLoader


class AdvancedHistoricalAnalyzer(MultiAnalyzer):
    """Advanced analysis for multi-decade patterns and manager excellence."""

    def __init__(self, data_loader: DataLoader) -> None:
        """Initialize with data loader."""
        super().__init__(data_loader)

    @staticmethod
    def _period_sort_key(period: str) -> tuple:
        """Convert 'Qn YYYY' to a chronologically sortable (year, quarter) tuple."""
        match = re.match(r"Q(\d)\s+(\d{4})", str(period))
        if match:
            return (int(match.group(2)), int(match.group(1)))
        return (0, 0)

    @classmethod
    def _sort_chronologically(cls, df: pd.DataFrame, period_col: str = "period") -> pd.DataFrame:
        """Sort activity rows by true quarter chronology, not string order.

        A plain string sort on "Qn YYYY" groups all Q1s of every year before
        any Q2 — corrupting any sequence/adjacency logic that follows.
        """
        df = df.copy()
        df["_sort_key"] = df[period_col].apply(cls._period_sort_key)
        return df.sort_values("_sort_key").drop(columns=["_sort_key"])

    def analyze_all(self) -> Dict[str, pd.DataFrame]:
        """Run all advanced historical analyses."""
        results = {}

        results["multi_decade_conviction"] = self.analyze_multi_decade_conviction()
        results["crisis_alpha_generators"] = self.analyze_crisis_alpha_generators()
        results["position_sizing_mastery"] = self.analyze_position_sizing_mastery()
        results["sector_rotation_excellence"] = self.analyze_sector_rotation_excellence()
        results["manager_evolution_patterns"] = self.analyze_manager_evolution()

        results["action_sequence_patterns"] = self.analyze_action_sequences()
        results["catalyst_timing_masters"] = self.analyze_catalyst_timing()
        results["theme_emergence_detection"] = self.analyze_theme_emergence()

        for name, df in results.items():
            self.log_analysis_summary(df, name)

        return self.format_all_outputs(results)

    def analyze_multi_decade_conviction(self) -> pd.DataFrame:
        """
        Analyze stocks held 10+ years by the same managers.
        These represent ultimate conviction plays with compound growth potential.

        years_held is the number of DISTINCT CALENDAR YEARS in which the ticker
        appears in the activity history - not a continuous holding duration, and
        a lower bound, since Dataroma exposes at most ~1,000 activity rows per
        manager. Rows are the top 50 by conviction_score.
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        print("🎯 Analyzing Multi-Decade Conviction Plays...")

        long_term_analysis = {}

        for ticker in self.data.history_df["ticker"].unique():
            ticker_data = self.data.history_df[self.data.history_df["ticker"] == ticker]

            years_with_activity = set()
            for period in ticker_data["period"]:
                if "Q" in str(period):
                    parts = str(period).split()
                    if len(parts) > 1:
                        years_with_activity.add(parts[1])

            years_held = len(years_with_activity)

            if years_held >= 5:
                managers = ticker_data["manager_id"].unique().tolist()

                manager_consistency = {}
                for manager in managers:
                    manager_data = ticker_data[ticker_data["manager_id"] == manager]
                    manager_years = set()
                    for period in manager_data["period"]:
                        if "Q" in str(period):
                            parts = str(period).split()
                            if len(parts) > 1:
                                manager_years.add(parts[1])

                    consistency_score = len(manager_years) / years_held
                    if consistency_score >= 0.3:
                        manager_consistency[manager] = {
                            "consistency_score": consistency_score,
                            "years_involved": len(manager_years),
                            "total_activities": len(manager_data),
                        }

                if manager_consistency:
                    current_holders = []
                    total_value = 0
                    if self.data.holdings_df is not None:
                        current_holding = self.data.holdings_df[self.data.holdings_df["ticker"] == ticker]
                        if not current_holding.empty:
                            total_value = current_holding["value"].sum()
                            current_holders = current_holding["manager_id"].tolist()

                    buy_actions = len(ticker_data[ticker_data["action_type"] == "Buy"])
                    add_actions = len(ticker_data[ticker_data["action_type"] == "Add"])
                    reduce_actions = len(ticker_data[ticker_data["action_type"] == "Reduce"])

                    conviction_score = (buy_actions + add_actions * 0.7) / max(1, reduce_actions * 0.5)

                    long_term_analysis[ticker] = {
                        "years_held": years_held,
                        "consistent_managers": len(manager_consistency),
                        "total_managers": len(managers),
                        "manager_details": manager_consistency,
                        "current_holders": len(current_holders),
                        "total_value": total_value,
                        "conviction_score": conviction_score,
                        "total_activities": len(ticker_data),
                        "buy_actions": buy_actions,
                        "periods_active": len(ticker_data["period"].unique()),
                    }

        if not long_term_analysis:
            return pd.DataFrame()

        conviction_df = pd.DataFrame.from_dict(long_term_analysis, orient="index")
        # Rank by conviction first: sorting by years_held first turned the
        # head(50) below into "the 50 longest-tracked tickers", so any
        # high-conviction stock with fewer distinct active years could never
        # reach the CSV (or the chart built from it).
        conviction_df = conviction_df.sort_values(by=["conviction_score", "years_held"], ascending=[False, False])

        if self.data.holdings_df is not None and "stock" in self.data.holdings_df.columns:
            company_names = self.data.holdings_df.groupby("ticker")["stock"].first()
            conviction_df = conviction_df.join(company_names.to_frame("company_name"))

        conviction_df["top_managers"] = conviction_df["manager_details"].apply(
            lambda x: (
                ", ".join(
                    [
                        self.data.manager_names.get(mgr_id, mgr_id)
                        for mgr_id, details in sorted(
                            x.items(), key=lambda item: item[1]["consistency_score"], reverse=True
                        )[:3]
                    ]
                )
                if x
                else ""
            )
        )
        # managers_shown = min(consistent_managers, 3) since top_managers shows max 3
        conviction_df["managers_shown"] = conviction_df["consistent_managers"].clip(upper=3)

        # Filter out delisted tickers (empty company_name with zero value)
        conviction_df = conviction_df[
            ~(
                (conviction_df["company_name"].isna() | (conviction_df["company_name"] == ""))
                & (conviction_df["total_value"] == 0)
            )
        ]

        # Flatten manager_details to human-readable columns
        def extract_top_manager_info(details: dict) -> tuple:
            """Extract top manager name, years involved, and consistency percentage."""
            if not details:
                return "", 0, 0.0
            # Sort by consistency_score descending and get the top manager
            sorted_managers = sorted(
                details.items(), key=lambda item: item[1]["consistency_score"], reverse=True
            )
            if sorted_managers:
                top_mgr_id, top_mgr_data = sorted_managers[0]
                top_mgr_name = self.data.manager_names.get(top_mgr_id, top_mgr_id)
                years_involved = top_mgr_data.get("years_involved", 0)
                consistency_pct = round(top_mgr_data.get("consistency_score", 0) * 100, 1)
                return top_mgr_name, years_involved, consistency_pct
            return "", 0, 0.0

        # Apply flattening
        flattened_info = conviction_df["manager_details"].apply(extract_top_manager_info)
        conviction_df["top_manager"] = flattened_info.apply(lambda x: x[0])
        conviction_df["top_manager_years"] = flattened_info.apply(lambda x: x[1])
        conviction_df["top_manager_consistency"] = flattened_info.apply(lambda x: f"{x[2]}%")

        # Drop the raw manager_details column
        conviction_df = conviction_df.drop(columns=["manager_details"])

        conviction_df["conviction_type"] = "Long-term Hold"
        conviction_df.loc[conviction_df["years_held"] >= 10, "conviction_type"] = "Decade+ Conviction"
        conviction_df.loc[conviction_df["years_held"] >= 15, "conviction_type"] = "Multi-Decade Champion"
        conviction_df.loc[
            (conviction_df["consistent_managers"] >= 3) & (conviction_df["years_held"] >= 8), "conviction_type"
        ] = "Consensus Champion"

        result = self.format_output(conviction_df.reset_index().rename(columns={"index": "ticker"})).head(50)
        return self.add_metadata_columns(result, window_quarters=40, analysis_type="multi_decade_conviction")

    def analyze_crisis_alpha_generators(self) -> pd.DataFrame:
        """
        Identify managers who consistently generate alpha during crisis periods.

        Output columns are structured for audit-friendly analysis:
        - Per-crisis columns: {crisis}_buy_actions, {crisis}_total_actions,
          {crisis}_unique_stocks, {crisis}_buy_ratio_pct
        - Best crisis columns: best_crisis_name, best_crisis_buy_ratio_pct,
          best_crisis_buy_actions, best_crisis_total_actions
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        print("Analyzing Crisis Alpha Generators...")

        # Define crisis periods with short keys for column naming
        crisis_periods = {
            "gfc_2008": ["Q1 2008", "Q2 2008", "Q3 2008", "Q4 2008"],
            "covid_2020": ["Q1 2020", "Q2 2020"],
            "inflation_2022": ["Q1 2022", "Q2 2022", "Q3 2022"],
        }

        # Human-readable names for display
        crisis_display_names = {
            "gfc_2008": "Financial Crisis 2008",
            "covid_2020": "COVID Crisis 2020",
            "inflation_2022": "Inflation Crisis 2022",
        }

        # Initialize manager performance tracking with flat structure
        manager_crisis_performance: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {
                "total_crisis_activities": 0,
                "buy_during_crisis": 0,
                "crisis_periods_active": 0,
            }
        )

        # Track per-crisis metrics separately for flattening
        manager_per_crisis_metrics: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)

        for crisis_key, periods in crisis_periods.items():
            crisis_activities = self.data.history_df[self.data.history_df["period"].isin(periods)]

            if not crisis_activities.empty:
                crisis_manager_actions = crisis_activities.groupby("manager_id").agg(
                    {
                        "action_type": lambda x: {
                            k: int(v) for k, v in dict(x.value_counts()).items()
                        },
                        "ticker": "nunique",
                        "period": "nunique",
                    }
                )

                for manager_id, data in crisis_manager_actions.iterrows():
                    manager_crisis_data = crisis_activities[crisis_activities["manager_id"] == manager_id]
                    action_counts = {
                        k: int(v) for k, v in manager_crisis_data["action_type"].value_counts().to_dict().items()
                    }

                    buy_actions = action_counts.get("Buy", 0) + action_counts.get("Add", 0)
                    total_actions = len(manager_crisis_data)
                    unique_stocks = int(data["ticker"])
                    buy_ratio = float(buy_actions / max(1, total_actions))

                    # Update aggregate metrics
                    manager_crisis_performance[manager_id]["total_crisis_activities"] += total_actions
                    manager_crisis_performance[manager_id]["buy_during_crisis"] += buy_actions
                    manager_crisis_performance[manager_id]["crisis_periods_active"] += 1

                    # Store per-crisis metrics for flattening
                    manager_per_crisis_metrics[manager_id][crisis_key] = {
                        "buy_actions": buy_actions,
                        "total_actions": total_actions,
                        "unique_stocks": unique_stocks,
                        "buy_ratio": buy_ratio,
                    }

        crisis_df = pd.DataFrame.from_dict(manager_crisis_performance, orient="index")

        if crisis_df.empty:
            return pd.DataFrame()

        # Filter BEFORE division to avoid division by zero or inf values
        crisis_df = crisis_df[(crisis_df["total_crisis_activities"] >= 5) & (crisis_df["crisis_periods_active"] >= 2)]

        if crisis_df.empty:
            return pd.DataFrame()

        # Add flattened per-crisis columns
        for crisis_key in crisis_periods.keys():
            crisis_df[f"{crisis_key}_buy_actions"] = crisis_df.index.map(
                lambda mgr, ck=crisis_key: manager_per_crisis_metrics.get(mgr, {}).get(ck, {}).get("buy_actions", 0)
            )
            crisis_df[f"{crisis_key}_total_actions"] = crisis_df.index.map(
                lambda mgr, ck=crisis_key: manager_per_crisis_metrics.get(mgr, {}).get(ck, {}).get("total_actions", 0)
            )
            crisis_df[f"{crisis_key}_unique_stocks"] = crisis_df.index.map(
                lambda mgr, ck=crisis_key: manager_per_crisis_metrics.get(mgr, {}).get(ck, {}).get("unique_stocks", 0)
            )
            crisis_df[f"{crisis_key}_buy_ratio_pct"] = crisis_df.index.map(
                lambda mgr, ck=crisis_key: round(
                    manager_per_crisis_metrics.get(mgr, {}).get(ck, {}).get("buy_ratio", 0.0) * 100, 1
                )
            )

        # Determine best crisis performance for each manager
        def get_best_crisis(manager_id: str) -> tuple:
            """Return (best_crisis_name, best_buy_ratio_pct, best_buy_actions, best_total_actions)."""
            manager_crises = manager_per_crisis_metrics.get(manager_id, {})
            if not manager_crises:
                return ("", 0.0, 0, 0)

            best_crisis = max(manager_crises.items(), key=lambda x: x[1].get("buy_ratio", 0))
            crisis_key, metrics = best_crisis
            return (
                crisis_display_names.get(crisis_key, crisis_key),
                round(metrics.get("buy_ratio", 0) * 100, 1),
                metrics.get("buy_actions", 0),
                metrics.get("total_actions", 0),
            )

        best_crisis_data = crisis_df.index.map(get_best_crisis)
        crisis_df["best_crisis_name"] = best_crisis_data.map(lambda x: x[0])
        crisis_df["best_crisis_buy_ratio_pct"] = best_crisis_data.map(lambda x: x[1])
        crisis_df["best_crisis_buy_actions"] = best_crisis_data.map(lambda x: x[2])
        crisis_df["best_crisis_total_actions"] = best_crisis_data.map(lambda x: x[3])

        # Safe to calculate now since total_crisis_activities >= 5
        crisis_df["crisis_alpha_score"] = (
            (crisis_df["buy_during_crisis"] / crisis_df["total_crisis_activities"])
            * crisis_df["crisis_periods_active"]
            * 10
        ).fillna(0)

        crisis_df["manager_name"] = crisis_df.index.map(lambda x: self.data.manager_names.get(x, x))

        if self.data.holdings_df is not None:
            portfolio_sizes = self.data.holdings_df.groupby("manager_id")["value"].sum()
            crisis_df = crisis_df.join(portfolio_sizes.to_frame("current_portfolio_value"))
            crisis_df["current_portfolio_value"] = crisis_df["current_portfolio_value"].fillna(0)

        crisis_df = crisis_df.sort_values(by="crisis_alpha_score", ascending=False)

        result = self.format_output(crisis_df.reset_index().rename(columns={"index": "manager_id"})).head(30)
        return self.add_metadata_columns(result, window_quarters=40, analysis_type="crisis_alpha_generators")

    def analyze_position_sizing_mastery(self) -> pd.DataFrame:
        """
        Analyze optimal position sizing patterns by manager.
        Identify who sizes positions optimally for maximum returns.
        """
        if (
            self.data.holdings_df is None
            or self.data.holdings_df.empty
            or "portfolio_percent" not in self.data.holdings_df.columns
        ):
            return pd.DataFrame()

        print("📊 Analyzing Position Sizing Mastery...")

        manager_sizing_analysis = {}

        for manager_id in self.data.holdings_df["manager_id"].unique():
            manager_holdings = self.data.holdings_df[self.data.holdings_df["manager_id"] == manager_id]

            if len(manager_holdings) < 5:
                continue

            position_sizes = manager_holdings["portfolio_percent"]

            avg_position = position_sizes.mean()
            max_position = position_sizes.max()
            position_concentration = (position_sizes > 5.0).sum() / len(position_sizes)
            position_variance = position_sizes.std()

            small_positions = (position_sizes <= 2.0).sum()
            medium_positions = ((position_sizes > 2.0) & (position_sizes <= 5.0)).sum()
            large_positions = (position_sizes > 5.0).sum()

            total_positions = len(position_sizes)

            efficiency_score = 0

            if 3.0 <= avg_position <= 7.0:
                efficiency_score += 30
            elif 2.0 <= avg_position <= 10.0:
                efficiency_score += 20
            else:
                efficiency_score += 5

            if 0.1 <= position_concentration <= 0.3:
                efficiency_score += 25
            elif position_concentration > 0.3:
                efficiency_score += 15

            if medium_positions > small_positions:
                efficiency_score += 20

            if position_variance < avg_position * 0.5:
                efficiency_score += 15

            manager_activities = 0
            if self.data.history_df is not None:
                manager_activities = len(self.data.history_df[self.data.history_df["manager_id"] == manager_id])

            manager_sizing_analysis[manager_id] = {
                "total_positions": total_positions,
                "avg_position_size": avg_position,
                "max_position_size": max_position,
                "position_concentration": position_concentration * 100,
                "position_variance": position_variance,
                "small_positions_pct": (small_positions / total_positions) * 100,
                "medium_positions_pct": (medium_positions / total_positions) * 100,
                "large_positions_pct": (large_positions / total_positions) * 100,
                "sizing_efficiency_score": efficiency_score,
                "total_portfolio_value": manager_holdings["value"].sum(),
                "historical_activities": manager_activities,
            }

        sizing_df = pd.DataFrame.from_dict(manager_sizing_analysis, orient="index")

        if sizing_df.empty:
            return pd.DataFrame()

        sizing_df = sizing_df[(sizing_df["total_positions"] >= 5) & (sizing_df["historical_activities"] >= 10)]

        if sizing_df.empty:
            return pd.DataFrame()

        sizing_df["manager_name"] = sizing_df.index.map(lambda x: self.data.manager_names.get(x, x))

        sizing_df["sizing_style"] = "Balanced"
        sizing_df.loc[sizing_df["position_concentration"] >= 30, "sizing_style"] = "High Conviction"
        sizing_df.loc[sizing_df["avg_position_size"] <= 3, "sizing_style"] = "Diversified"
        sizing_df.loc[sizing_df["large_positions_pct"] >= 40, "sizing_style"] = "Concentrated"
        # Guard against division by zero for avg_position_size
        sizing_df.loc[
            (sizing_df["avg_position_size"] > 0)
            & ((sizing_df["position_variance"] / sizing_df["avg_position_size"]) <= 0.3),
            "sizing_style",
        ] = "Systematic"

        sizing_df = sizing_df.sort_values(by="sizing_efficiency_score", ascending=False)

        result = self.format_output(sizing_df.reset_index().rename(columns={"index": "manager_id"})).head(40)
        return self.add_metadata_columns(result, window_quarters=1, analysis_type="position_sizing_mastery")

    def analyze_action_sequences(self) -> pd.DataFrame:
        """
        Identify predictive patterns in manager action sequences.
        Find what actions typically follow specific patterns.
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        print("🔄 Analyzing Action Sequence Patterns...")

        sequence_analysis = defaultdict(
            lambda: {
                "total_occurrences": 0,
                "next_action_outcomes": Counter(),
                "success_patterns": [],
                "tickers_involved": set(),
                "managers_involved": set(),
            }
        )

        for ticker in self.data.history_df["ticker"].unique():
            ticker_data = self._sort_chronologically(
                self.data.history_df[self.data.history_df["ticker"] == ticker]
            )

            if len(ticker_data) < 4:
                continue

            actions = ticker_data["action_type"].tolist()
            managers = ticker_data["manager_id"].tolist()

            for i in range(len(actions) - 3):
                sequence = " → ".join(actions[i : i + 3])
                next_action = actions[i + 3]

                seq_data = sequence_analysis[sequence]
                seq_data["total_occurrences"] += 1
                seq_data["next_action_outcomes"][next_action] += 1
                seq_data["tickers_involved"].add(ticker)
                seq_data["managers_involved"].update(managers[i : i + 4])

        sequences = []
        for sequence, data in sequence_analysis.items():
            total_occ = data["total_occurrences"]
            if total_occ >= 3:
                next_actions = data["next_action_outcomes"]
                if next_actions:
                    most_likely_next = next_actions.most_common(1)[0]
                    predictive_strength = most_likely_next[1] / total_occ
                else:
                    continue

                sequences.append(
                    {
                        "sequence_pattern": sequence,
                        "total_occurrences": data["total_occurrences"],
                        "most_likely_next_action": most_likely_next[0],
                        "predictive_strength": predictive_strength * 100,
                        "unique_tickers": len(data["tickers_involved"]),
                        "unique_managers": len(data["managers_involved"]),
                        "next_action_breakdown": dict(data["next_action_outcomes"]),
                    }
                )

        if not sequences:
            return pd.DataFrame()

        sequence_df = pd.DataFrame(sequences)

        sequence_df["pattern_score"] = (
            sequence_df["predictive_strength"]
            * np.log(sequence_df["total_occurrences"])
            * sequence_df["unique_managers"]
        )

        sequence_df = sequence_df.sort_values(by="pattern_score", ascending=False)

        result = self.format_output(sequence_df).head(30)
        return self.add_metadata_columns(result, window_quarters=40, analysis_type="action_sequence_patterns")

    def analyze_sector_rotation_excellence(self) -> pd.DataFrame:
        """
        Identify managers who excel at sector rotation timing.
        This requires sector classification which we'll approximate from company names.
        """
        if self.data.history_df is None or self.data.history_df.empty or self.data.holdings_df is None:
            return pd.DataFrame()

        print("🔄 Analyzing Sector Rotation Excellence...")

        sector_keywords = {
            "Technology": ["tech", "software", "microsoft", "apple", "google", "meta", "amazon", "nvidia"],
            "Finance": ["bank", "financial", "capital", "insurance", "credit"],
            "Healthcare": ["health", "pharma", "medical", "bio"],
            "Energy": ["energy", "oil", "gas", "petroleum"],
            "Consumer": ["retail", "consumer", "restaurant", "food"],
            "Industrial": ["industrial", "manufacturing", "aerospace"],
            "Real Estate": ["real estate", "reit", "property"],
        }

        ticker_sectors = {}
        if "stock" in self.data.holdings_df.columns:
            for _, row in self.data.holdings_df.iterrows():
                ticker = row["ticker"]
                company = str(row["stock"]).lower()

                assigned_sector = "Other"
                for sector, keywords in sector_keywords.items():
                    if any(keyword in company for keyword in keywords):
                        assigned_sector = sector
                        break

                ticker_sectors[ticker] = assigned_sector

        if not ticker_sectors:
            return pd.DataFrame()

        manager_sector_analysis = defaultdict(
            lambda: {
                "sectors_traded": set(),
                "sector_timing": defaultdict(list),
                "total_rotations": 0,
                "rotation_success_score": 0,
            }
        )

        # Group activities by manager and analyze sector changes over time
        for manager_id in self.data.history_df["manager_id"].unique():
            manager_activities = self.data.history_df[self.data.history_df["manager_id"] == manager_id].sort_values(
                by="period"
            )

            sector_activity_by_period = defaultdict(lambda: defaultdict(int))

            for _, activity in manager_activities.iterrows():
                ticker = activity["ticker"]
                if ticker in ticker_sectors:
                    sector = ticker_sectors[ticker]
                    period = activity["period"]
                    action = activity["action_type"]

                    sector_activity_by_period[(period, sector)][action] += 1
                    mgr_data = manager_sector_analysis[manager_id]
                    mgr_data["sectors_traded"].add(sector)

            periods = sorted(set([p for p, s in sector_activity_by_period.keys()]), key=self._period_sort_key)
            rotation_score = 0

            if len(periods) >= 4:
                for i in range(len(periods) - 1):
                    current_period = periods[i]
                    next_period = periods[i + 1]

                    current_sectors = {}
                    next_sectors = {}

                    for (period, sector), actions in sector_activity_by_period.items():
                        net_activity = actions["Buy"] + actions["Add"] - actions["Sell"] - actions["Reduce"]

                        if period == current_period:
                            current_sectors[sector] = net_activity
                        elif period == next_period:
                            next_sectors[sector] = net_activity

                    for sector in set(current_sectors.keys()) | set(next_sectors.keys()):
                        current_activity = current_sectors.get(sector, 0)
                        next_activity = next_sectors.get(sector, 0)

                        if current_activity < 0 and next_activity > 0:
                            rotation_score += 2
                        elif abs(current_activity - next_activity) >= 2:
                            rotation_score += 1

            manager_sector_analysis[manager_id]["total_rotations"] = len(
                manager_sector_analysis[manager_id]["sectors_traded"]
            )
            manager_sector_analysis[manager_id]["rotation_success_score"] = rotation_score

        rotation_data = []
        for manager_id, data in manager_sector_analysis.items():
            if len(data["sectors_traded"]) >= 3:
                rotation_data.append(
                    {
                        "manager_id": manager_id,
                        "manager_name": self.data.manager_names.get(manager_id, manager_id),
                        "sectors_traded": len(data["sectors_traded"]),
                        "rotation_success_score": data["rotation_success_score"],
                        "sectors_list": ", ".join(sorted(data["sectors_traded"])),
                    }
                )

        if not rotation_data:
            return pd.DataFrame()

        rotation_df = pd.DataFrame(rotation_data)
        rotation_df = rotation_df.sort_values(by="rotation_success_score", ascending=False)

        result = self.format_output(rotation_df).head(30)
        return self.add_metadata_columns(result, window_quarters=40, analysis_type="sector_rotation_excellence")

    def analyze_manager_evolution(self) -> pd.DataFrame:
        """
        Analyze how managers evolve their strategies over decades.
        Track changes in behavior, concentration, and sector focus.
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        print("📈 Analyzing Manager Evolution Patterns...")

        manager_evolution = {}

        for manager_id in self.data.history_df["manager_id"].unique():
            manager_data = self.data.history_df[self.data.history_df["manager_id"] == manager_id].sort_values(
                by="period"
            )

            if len(manager_data) < 20:
                continue

            years = sorted(set([p.split()[1] for p in manager_data["period"] if "Q" in p and len(p.split()) > 1]))

            if len(years) < 5:
                continue

            career_length = len(years)
            phase_size = max(2, career_length // 3)

            early_years = years[:phase_size]
            middle_years = years[phase_size : phase_size * 2]
            late_years = years[phase_size * 2 :]

            phases = {"Early Career": early_years, "Middle Career": middle_years, "Late Career": late_years}

            phase_analysis = {}
            for phase_name, phase_years in phases.items():
                phase_data = manager_data[
                    manager_data["period"].apply(lambda x: any(year in str(x) for year in phase_years))
                ]

                if not phase_data.empty:
                    action_types = phase_data["action_type"].value_counts()
                    unique_stocks = phase_data["ticker"].nunique()
                    total_activities = len(phase_data)

                    buy_ratio = (action_types.get("Buy", 0) + action_types.get("Add", 0)) / total_activities

                    phase_analysis[phase_name] = {
                        "unique_stocks": unique_stocks,
                        "total_activities": total_activities,
                        "buy_ratio": buy_ratio,
                        "years_span": len(phase_years),
                    }

            if len(phase_analysis) >= 2:
                early = phase_analysis.get("Early Career", {})
                late = phase_analysis.get("Late Career", {})

                if early and late:
                    diversification_change = late.get("unique_stocks", 0) - early.get("unique_stocks", 0)
                    activity_change = late.get("total_activities", 0) / max(1, late.get("years_span", 1)) - early.get(
                        "total_activities", 0
                    ) / max(1, early.get("years_span", 1))
                    style_change = abs(late.get("buy_ratio", 0) - early.get("buy_ratio", 0))

                    manager_evolution[manager_id] = {
                        "career_length_years": career_length,
                        "early_stocks": early.get("unique_stocks", 0),
                        "late_stocks": late.get("unique_stocks", 0),
                        "diversification_change": diversification_change,
                        "activity_per_year_change": activity_change,
                        "style_change_score": style_change * 100,
                        "early_buy_pct": early.get("buy_ratio", 0) * 100,
                        "late_buy_pct": late.get("buy_ratio", 0) * 100,
                        "total_activities": len(manager_data),
                    }

        if not manager_evolution:
            return pd.DataFrame()

        evolution_df = pd.DataFrame.from_dict(manager_evolution, orient="index")

        evolution_df["manager_name"] = evolution_df.index.map(lambda x: self.data.manager_names.get(x, x))

        evolution_df["evolution_type"] = "Stable"
        evolution_df.loc[evolution_df["diversification_change"] > 10, "evolution_type"] = "Diversifying"
        evolution_df.loc[evolution_df["diversification_change"] < -10, "evolution_type"] = "Concentrating"
        evolution_df.loc[evolution_df["style_change_score"] > 20, "evolution_type"] = "Style Shifter"
        evolution_df.loc[evolution_df["activity_per_year_change"] > 5, "evolution_type"] = "More Active"
        evolution_df.loc[evolution_df["activity_per_year_change"] < -5, "evolution_type"] = "Less Active"

        evolution_df["evolution_score"] = (
            abs(evolution_df["diversification_change"])
            + evolution_df["style_change_score"]
            + abs(evolution_df["activity_per_year_change"])
        )

        evolution_df = evolution_df.sort_values(by="evolution_score", ascending=False)

        result = self.format_output(evolution_df.reset_index().rename(columns={"index": "manager_id"})).head(30)
        return self.add_metadata_columns(result, window_quarters=40, analysis_type="manager_evolution_patterns")

    def analyze_catalyst_timing(self) -> pd.DataFrame:
        """
        Analyze managers who demonstrate exceptional timing in entries and exits.
        Look for patterns of buying before price rises and selling before declines.
        """
        if (
            self.data.history_df is None
            or self.data.history_df.empty
            or self.data.holdings_df is None
            or self.data.holdings_df.empty
        ):
            return pd.DataFrame()

        print("⏰ Analyzing Catalyst Timing Masters...")

        if "current_price" in self.data.holdings_df.columns:
            self.data.holdings_df.groupby("ticker")["current_price"].first().to_dict()

        manager_timing = {}

        for manager_id in self.data.history_df["manager_id"].unique():
            manager_actions = self.data.history_df[self.data.history_df["manager_id"] == manager_id].copy()

            if len(manager_actions) < 10:
                continue

            perfect_entries = 0
            perfect_exits = 0
            total_entries = 0
            total_exits = 0

            for ticker, ticker_actions in manager_actions.groupby("ticker"):
                ticker_actions = self._sort_chronologically(ticker_actions)

                entry_actions = ticker_actions[ticker_actions["action_type"].isin(["Buy", "Add"])]
                exit_actions = ticker_actions[ticker_actions["action_type"].isin(["Sell", "Reduce"])]

                if not entry_actions.empty:
                    total_entries += len(entry_actions)
                    first_entry_idx = ticker_actions.index[ticker_actions.index.get_loc(entry_actions.index[0])]
                    subsequent_actions = ticker_actions.loc[first_entry_idx:].iloc[1:5]
                    if not subsequent_actions.empty:
                        good_actions = subsequent_actions[
                            subsequent_actions["action_type"].isin(["Buy", "Add", "Hold"])
                        ]
                        if len(good_actions) >= len(subsequent_actions) * 0.6:
                            perfect_entries += 1

                if not exit_actions.empty:
                    total_exits += len(exit_actions)
                    for exit_idx in exit_actions.index:
                        exit_loc = ticker_actions.index.get_loc(exit_idx)
                        if exit_loc < len(ticker_actions) - 1:
                            subsequent = ticker_actions.iloc[exit_loc + 1 : exit_loc + 4]
                            if not subsequent[subsequent["action_type"] == "Buy"].empty:
                                pass
                            else:
                                perfect_exits += 1

            entry_success_rate = (perfect_entries / max(1, total_entries)) * 100
            exit_success_rate = (perfect_exits / max(1, total_exits)) * 100
            overall_timing_score = (entry_success_rate + exit_success_rate) / 2

            if total_entries + total_exits >= 20:
                manager_timing[manager_id] = {
                    "total_trades": total_entries + total_exits,
                    "entry_trades": total_entries,
                    "exit_trades": total_exits,
                    "perfect_entries": perfect_entries,
                    "perfect_exits": perfect_exits,
                    "entry_success_rate": round(entry_success_rate, 2),
                    "exit_success_rate": round(exit_success_rate, 2),
                    "timing_score": round(overall_timing_score, 2),
                    "years_active": len(
                        manager_actions["period"].apply(lambda x: str(x).split()[-1] if " " in str(x) else "").unique()
                    ),
                }

        if not manager_timing:
            return pd.DataFrame()

        timing_df = pd.DataFrame.from_dict(manager_timing, orient="index")
        timing_df.index.name = "manager_id"
        timing_df = timing_df.reset_index()

        timing_df["manager"] = timing_df["manager_id"].map(self.data.manager_names)

        timing_df = timing_df.sort_values(by="timing_score", ascending=False).head(30)

        result = self.format_output(timing_df)
        return self.add_metadata_columns(result, window_quarters=40, analysis_type="catalyst_timing_masters")

    def analyze_theme_emergence(self) -> pd.DataFrame:
        """
        Identify emerging investment themes by detecting early concentrations.
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()

        print("🎭 Analyzing Theme Emergence Patterns...")

        theme_analysis = {}
        recent_periods = self.get_recent_quarters(5)

        for ticker in self.data.history_df["ticker"].unique():
            ticker_data = self.data.history_df[self.data.history_df["ticker"] == ticker]

            recent_managers = set(ticker_data[ticker_data["period"].isin(recent_periods)]["manager_id"])

            historical_managers = set(ticker_data[~ticker_data["period"].isin(recent_periods)]["manager_id"])

            new_managers = recent_managers - historical_managers

            if len(new_managers) >= 2 and len(recent_managers) >= 3:
                recent_activities = ticker_data[ticker_data["period"].isin(recent_periods)]
                buy_activities = recent_activities[recent_activities["action_type"].isin(["Buy", "Add"])]

                if len(buy_activities) >= 2:
                    theme_analysis[ticker] = {
                        "total_recent_managers": len(recent_managers),
                        "new_managers_count": len(new_managers),
                        "recent_buy_activities": len(buy_activities),
                        "emergence_score": len(new_managers) * len(buy_activities),
                        "new_managers": list(new_managers),
                        "total_managers": len(recent_managers | historical_managers),
                    }

        if not theme_analysis:
            return pd.DataFrame()

        theme_df = pd.DataFrame.from_dict(theme_analysis, orient="index")

        if self.data.holdings_df is not None and "stock" in self.data.holdings_df.columns:
            company_names = self.data.holdings_df.groupby("ticker")["stock"].first()
            theme_df = theme_df.join(company_names.rename("company_name"))

        theme_df["new_manager_names"] = theme_df["new_managers"].apply(
            lambda managers: ", ".join([self.data.manager_names.get(mgr, mgr) for mgr in managers])
        )

        theme_df = theme_df.sort_values(by="emergence_score", ascending=False)

        result = self.format_output(theme_df.reset_index().rename(columns={"index": "ticker"})).head(25)
        return self.add_metadata_columns(result, window_quarters=5, analysis_type="theme_emergence_detection")
