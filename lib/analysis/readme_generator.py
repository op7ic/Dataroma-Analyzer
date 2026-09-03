#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - README Generator

Generates dynamic README documentation from analysis results.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

"""
README generator for analysis results.
Creates comprehensive documentation with embedded visualizations.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class ReadmeGenerator:
    """Generates comprehensive README for analysis results."""

    def __init__(self, analysis_dir: str = "analysis", data_loader: Optional[Any] = None) -> None:
        self.analysis_dir = Path(analysis_dir)
        self.data_loader = data_loader

    @staticmethod
    def _format_currency(value: float, precision: int = 2) -> str:
        """
        Format currency values consistently.

        Args:
            value: The numeric value to format
            precision: Number of decimal places for billions/millions

        Returns:
            Formatted string like "$1.23B", "$456M", or "$12,345"
        """
        if value >= 1e9:
            return f"${value/1e9:.{precision}f}B"
        elif value >= 1e6:
            return f"${value/1e6:.{precision}f}M"
        elif value >= 1e3:
            return f"${value:,.0f}"
        else:
            return f"${value:.2f}"

    @staticmethod
    def _format_manager_list(managers: str, max_display: int = 3) -> str:
        """
        Format manager list for display in tables.

        Args:
            managers: Comma-separated string of manager names
            max_display: Maximum number of managers to show

        Returns:
            Formatted string with truncation if needed
        """
        if not managers or managers == "N/A":
            return "N/A"

        manager_list = [m.strip() for m in str(managers).split(",")]
        if len(manager_list) <= max_display:
            return ", ".join(manager_list)

        shown = ", ".join(manager_list[:max_display])
        remaining = len(manager_list) - max_display
        return f"{shown} +{remaining} more"

    def _get_recent_quarters(self, num_quarters: int = 3) -> List[str]:
        """Get the most recent quarters from the data."""
        if self.data_loader is None or self.data_loader.history_df is None:
            return []

        df = self.data_loader.history_df
        if "period" not in df.columns or df.empty:
            return []

        # Get unique periods and sort chronologically
        periods = df["period"].unique()

        def period_sort_key(period: str) -> tuple:
            """Convert period string to sortable (year, quarter) tuple."""
            import re
            match = re.match(r"Q(\d)\s+(\d{4})", str(period))
            if match:
                quarter = int(match.group(1))
                year = int(match.group(2))
                return (year, quarter)
            return (0, 0)

        sorted_periods = sorted(periods, key=period_sort_key, reverse=True)
        return list(sorted_periods[:num_quarters])

    def _get_earliest_quarter(self) -> str:
        """Get the chronologically earliest quarter present in the data ('' if unknown)."""
        if self.data_loader is None or getattr(self.data_loader, "history_df", None) is None:
            return ""
        df = self.data_loader.history_df
        if df is None or df.empty or "period" not in df.columns:
            return ""
        import re

        def period_sort_key(period: str) -> tuple:
            match = re.match(r"Q(\d)\s+(\d{4})", str(period))
            if match:
                return (int(match.group(2)), int(match.group(1)))
            return (9999, 9)

        return min(df["period"].dropna().unique(), key=period_sort_key, default="")

    def generate_readme(self, results: Dict[str, pd.DataFrame], viz_paths: Dict[str, List[str]]) -> str:
        """Generate comprehensive README with all analysis results."""
        # Dynamically determine year range from data
        min_year, max_year = 2007, 2025  # Fallback values
        years_span = max_year - min_year

        # Try to get actual year range from manager track records or quarterly timeline
        if "manager_track_records" in results and not results["manager_track_records"].empty:
            df = results["manager_track_records"]
            if "first_year" in df.columns and "last_year" in df.columns:
                min_year = df["first_year"].min()
                max_year = df["last_year"].max()
                years_span = max_year - min_year
        elif "quarterly_activity_timeline" in results and not results["quarterly_activity_timeline"].empty:
            df = results["quarterly_activity_timeline"]
            if "year" in df.columns:
                min_year = df["year"].min()
                max_year = df["year"].max()
                years_span = max_year - min_year

        # Quick Stats are computed from the loaded data at generation time.
        # (They were previously hardcoded literals that silently went stale.)
        activities_count = holdings_count = managers_count = None
        if self.data_loader is not None:
            if getattr(self.data_loader, "activities_df", None) is not None:
                activities_count = len(self.data_loader.activities_df)
            if getattr(self.data_loader, "holdings_df", None) is not None:
                holdings_count = len(self.data_loader.holdings_df)
            if getattr(self.data_loader, "managers_df", None) is not None:
                managers_count = len(self.data_loader.managers_df)

        def _fmt_count(value) -> str:
            return f"{value:,}" if value is not None else "N/A"

        content = [
            "# 📊 Dataroma Investment Analysis",
            f"\n*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
            "\n## 🎯 Overview",
            f"\nThis analysis covers **{years_span}+ years** of investment data from top money managers, "
            "providing insights into current opportunities, manager performance patterns, "
            "and long-term investment trends.",
            "\n### 📈 Quick Stats",
            f"- **Total Activities Analyzed**: {_fmt_count(activities_count)}",
            f"- **Current Holdings**: {_fmt_count(holdings_count)}",
            f"- **Managers Tracked**: {_fmt_count(managers_count)}",
            f"- **Time Period**: {min_year}-{max_year}",
            "\n---\n",
        ]

        # Current Analysis Section
        content.extend(self._generate_current_section(results, viz_paths.get("current", [])))

        # Advanced Analysis Section
        content.extend(self._generate_advanced_section(results, viz_paths.get("advanced", [])))

        # Historical Analysis Section
        content.extend(self._generate_historical_section(results, viz_paths.get("historical", [])))

        # Methodology Section
        content.extend(self._generate_methodology_section())

        # Caveats and Data Understanding Section
        content.extend(self._generate_caveats_section(results))

        # Footer
        content.extend(
            [
                "\n---\n",
                "## 📅 Update Schedule",
                "\nThis analysis is refreshed monthly to capture the latest investment trends "
                "and manager activities.",
                "\n## 🔗 Data Source",
                "\nAll data is sourced from [Dataroma](https://www.dataroma.com), tracking "
                "portfolios of super investors.",
                "\n---",
                "\n*Analysis framework powered by modular Python architecture*",
            ]
        )

        return "\n".join(content)

    def _generate_current_section(self, results: Dict[str, pd.DataFrame], viz_paths: List[str]) -> List[str]:
        """Generate current analysis section."""
        # Sorting quarter labels as plain strings put "Q4 2023" ahead of
        # "Q2 2026", so this header disagreed with the Analysis Periods
        # section. Use the chronological sort both sections now share.
        recent_quarters = self._get_recent_quarters(3)

        quarter_range = (
            f"from {recent_quarters[-1]} to {recent_quarters[0]}" if len(recent_quarters) >= 3 else "recent quarters"
        )

        content = [
            "## 💡 Current Analysis (Last 3 Quarters)",
            f"\nImmediate opportunities and recent market activity {quarter_range}.",
            "\n### 📊 Visual Analysis\n",
        ]

        # Add visualizations - scan the current/visuals directory directly
        visuals_dir = self.analysis_dir / "current" / "visuals"
        if visuals_dir.exists():
            png_files = sorted(visuals_dir.glob("*.png"))
            if png_files:
                for png_file in png_files:
                    viz_name = png_file.stem.replace("_", " ").title()
                    relative_path = f"current/visuals/{png_file.name}"
                    content.append(f"#### {viz_name}")
                    content.append(f"![{viz_name}]({relative_path})")
                    content.append("")

        # Complete mapping for all possible current reports - only shown if files exist
        current_reports = {
            "52_week_high_sells": ("Profit taking patterns", "Strategic exits at peaks"),
            "52_week_low_buys": ("Value hunting activity", "Managers buying at market lows"),
            "concentration_changes": ("Portfolio shifts", "Major allocation adjustments"),
            "contrarian_opportunities": ("Against-the-trend plays", "Institutional contrarian bets"),
            "deep_value_plays": ("Deep value opportunities", "Undervalued institutional picks"),
            "hidden_gems": ("Under-the-radar opportunities", "5-factor scoring identifies stocks with high potential"),
            "high_conviction_low_price": ("Best value + conviction combo", "High conviction meets low price"),
            "highest_portfolio_concentration": ("Most focused positions", "Highest concentration institutional bets"),
            "momentum_stocks": ("Recent buying activity", "Tracks institutional accumulation patterns"),
            "most_sold_stocks": ("Recent exit activity", "Most divested institutional positions"),
            "new_positions": ("Fresh acquisitions", "Identifies emerging manager interests"),
            "stocks_under_$5": ("Ultra-low price opportunities", "Deep value plays under $5"),
            "stocks_under_$10": ("Sub-$10 opportunities", "Manager favorites under $10"),
            "stocks_under_$20": ("Affordable growth plays", "Quality stocks at accessible prices"),
            "stocks_under_$50": ("Mid-price value plays", "Institutional picks under $50"),
            "stocks_under_$100": ("Sub-$100 opportunities", "Value plays under $100"),
            "under_radar_picks": ("Hidden gem opportunities", "Under-the-radar institutional picks"),
            "value_price_opportunities": ("Multi-tier price analysis", "Comprehensive price-based screening"),
            "stock_timelines": ("Position timeline tracking", "Quarter-by-quarter position changes per stock"),
        }

        # Only add reports table if we have existing reports
        existing_reports = []
        for report_name, (desc, insight) in current_reports.items():
            if report_name in results and not results[report_name].empty:
                # Check if the actual CSV file exists
                csv_path = self.analysis_dir / "current" / f"{report_name}.csv"
                if csv_path.exists():
                    count = len(results[report_name])
                    existing_reports.append((report_name, desc, insight, count))

        if existing_reports:
            content.extend(
                [
                    "\n### 📋 Current Reports",
                    "\n| Report | Description | Key Insight |",
                    "| ------ | ----------- | ----------- |",
                ]
            )

            for report_name, desc, insight, count in existing_reports:
                content.append(
                    f"| [{report_name}.csv](current/{report_name}.csv) | " f"{desc} ({count} items) | {insight} |"
                )

            content.append("")

        # Add top opportunities - check for any high-value report that exists
        top_opportunity_reports = [
            "hidden_gems",
            "under_radar_picks",
            "deep_value_plays",
            "high_conviction_low_price",
            "momentum_stocks",
        ]

        for report_name in top_opportunity_reports:
            if report_name in results and not results[report_name].empty:
                csv_path = self.analysis_dir / "current" / f"{report_name}.csv"
                if csv_path.exists():
                    top_opportunities = results[report_name].head(5)
                    report_title = report_name.replace("_", " ").title()

                    # Special handling for different report types
                    if report_name == "hidden_gems":
                        content.extend(
                            [
                                "### 🌟 Top 5 Hidden Gems",
                                "\n| Ticker | Score | Price | Managers |",
                                "| ------ | ----- | ----- | -------- |",
                            ]
                        )

                        for _, gem in top_opportunities.iterrows():
                            managers_list = str(gem.get("managers", "")).split(",")[:3]
                            managers_str = ", ".join(m.strip() for m in managers_list)
                            content.append(
                                f"| **{gem.get('ticker', 'N/A')}** | "
                                f"{gem.get('hidden_gem_score', 0):.2f} | "
                                f"${gem.get('current_price', 0):.2f} | "
                                f"{managers_str} |"
                            )
                    elif report_name == "under_radar_picks":
                        # Use top 15 for under radar picks with proper analysis
                        top_opportunities = results[report_name].head(15)
                        content.extend(
                            [
                                "### 🔍 Top 15 Under Radar Picks",
                                "\n| Rank | Ticker | Score | Value | Why Under Radar |",
                                "| ---- | ------ | ----- | ----- | --------------- |",
                            ]
                        )

                        for rank, (_, pick) in enumerate(top_opportunities.iterrows(), 1):
                            ticker = pick.get("ticker", "N/A")
                            score = pick.get("under_radar_score", 0)
                            total_value = pick.get("total_value", 0)
                            total_shares = pick.get("total_shares", 1)
                            manager_count = pick.get("manager_count", 0)
                            pick_type = pick.get("pick_type", "Unknown")
                            avg_portfolio_pct = pick.get("avg_portfolio_pct", 0)
                            first_established = pick.get("first_established", "Unknown")

                            # Calculate approximate price per share
                            price_per_share = total_value / total_shares if total_shares > 0 else 0

                            # Format value using helper method
                            value_str = self._format_currency(total_value, precision=1)

                            # Generate "why under radar" explanation
                            reasons = []
                            if manager_count == 1:
                                reasons.append("Exclusive pick")
                            elif manager_count <= 2:
                                reasons.append("Limited recognition")

                            if avg_portfolio_pct > 10:
                                reasons.append(f"High conviction ({avg_portfolio_pct:.1f}%)")

                            if pick_type == "Growing Interest":
                                reasons.append("Growing institutional interest")
                            elif pick_type == "Exclusive Pick":
                                reasons.append("Single manager exclusive")

                            recent_qs = self._get_recent_quarters(3)
                            if recent_qs and any(q in str(first_established) for q in recent_qs):
                                reasons.append("Recent discovery")

                            if price_per_share < 50:
                                reasons.append("Low price entry")

                            why_under_radar = "; ".join(reasons) if reasons else "Institutional accumulation"

                            content.append(
                                f"| {rank} | **{ticker}** | " f"{score:.1f} | " f"{value_str} | " f"{why_under_radar} |"
                            )
                    else:
                        content.extend(
                            [
                                f"### 🌟 Top 5 {report_title}",
                                "\n| Ticker | Score | Price | Details |",
                                "| ------ | ----- | ----- | ------- |",
                            ]
                        )

                        for _, opp in top_opportunities.iterrows():
                            # Get the most relevant columns for each report type
                            score = opp.get("score", opp.get("momentum_score", opp.get("conviction_score", 0)))
                            price = opp.get("current_price", opp.get("price", 0))
                            details = str(opp.get("managers", opp.get("manager_name", opp.get("details", "N/A"))))

                            # Limit details to first 3 items if comma-separated
                            if "," in details:
                                details_list = details.split(",")[:3]
                                details = ", ".join(d.strip() for d in details_list)

                            content.append(
                                f"| **{opp.get('ticker', 'N/A')}** | "
                                f"{score:.2f} | "
                                f"${price:.2f} | "
                                f"{details} |"
                            )
                    break  # Only show one top opportunities table

        content.append("\n---\n")
        return content

    def _generate_advanced_section(self, results: Dict[str, pd.DataFrame], viz_paths: List[str]) -> List[str]:
        """Generate advanced analysis section."""
        content = [
            "## 🧠 Advanced Analysis (Manager Performance)",
            "\nDeep insights into manager strategies, performance patterns, and decision-making.",
            "\n### 📊 Visual Analysis\n",
        ]

        # Add visualizations - scan the advanced/visuals directory directly
        visuals_dir = self.analysis_dir / "advanced" / "visuals"
        if visuals_dir.exists():
            png_files = sorted(visuals_dir.glob("*.png"))
            if png_files:
                for png_file in png_files:
                    viz_name = png_file.stem.replace("_", " ").title()
                    relative_path = f"advanced/visuals/{png_file.name}"
                    content.append(f"#### {viz_name}")
                    content.append(f"![{viz_name}]({relative_path})")
                    content.append("")

        advanced_reports = {
            "position_building_timeline": (
                "📈 Position buildup/reduction over time",
                "Quarter-by-quarter view of how managers accumulate & distribute positions",
            ),
            "accumulation_vs_distribution": (
                "🔄 Current phase tracking",
                "Identifies which positions are being built vs reduced RIGHT NOW",
            ),
            "position_flip_points": (
                "🔀 Accumulation→Distribution transitions",
                "Pinpoints when managers switched from building to reducing",
            ),
            "action_sequence_patterns": ("Trading pattern analysis", "Institutional buy/sell sequence patterns"),
            "catalyst_timing_masters": ("Market timing excellence", "Managers with exceptional timing skills"),
            "crisis_alpha_generators": ("Crisis period outperformers", "Managers who buy during crashes"),
            "high_conviction_stocks": ("Highest conviction positions", "Stocks with strongest institutional backing"),
            "interesting_stocks_overview": ("Top-tier opportunities", "Multi-factor scoring of elite picks"),
            "long_term_winners": ("Sustained institutional interest", "Stocks with long-term institutional backing"),
            "manager_evolution_patterns": ("Strategy adaptation over time", "How managers evolve their approaches"),
            "manager_performance": ("Comprehensive manager evaluation", "Multi-dimensional performance metrics"),
            "manager_track_records": ("Multi-year activity history", "Comprehensive manager scoring with consistency"),
            "multi_manager_favorites": ("Consensus high-conviction picks", "Stocks held by multiple elite managers"),
            "position_sizing_mastery": ("Optimal allocation patterns", "Advanced portfolio construction analysis"),
            "sector_rotation_excellence": ("Elite sector allocation", "Superior sector rotation strategies"),
            "sector_rotation_patterns": ("Institutional sector flows", "Sector rotation trend analysis"),
            "theme_emergence_detection": ("Early theme identification", "Emerging investment theme detection"),
            "top_holdings": ("Largest institutional positions", "Deep dive into major institutional holdings"),
        }

        # Only add reports table if we have existing reports
        existing_advanced_reports = []
        for report_name, (desc, insight) in advanced_reports.items():
            if report_name in results and not results[report_name].empty:
                # Check if the actual CSV file exists
                csv_path = self.analysis_dir / "advanced" / f"{report_name}.csv"
                if csv_path.exists():
                    count = len(results[report_name])
                    existing_advanced_reports.append((report_name, desc, insight, count))

        if existing_advanced_reports:
            content.extend(
                [
                    "\n### 📋 Advanced Reports",
                    "\n| Report | Description | Key Insight |",
                    "| ------ | ----------- | ----------- |",
                ]
            )

            for report_name, desc, insight, count in existing_advanced_reports:
                content.append(
                    f"| [{report_name}.csv](advanced/{report_name}.csv) | " f"{desc} ({count} items) | {insight} |"
                )

            content.append("")

        # Add top managers - only if the CSV file actually exists
        if "manager_track_records" in results and not results["manager_track_records"].empty:
            csv_path = self.analysis_dir / "advanced" / "manager_track_records.csv"
            if csv_path.exists():
                # Get the data period from the CSV
                df = results["manager_track_records"]
                min_year = df["first_year"].min() if "first_year" in df.columns else 2007
                max_year = df["last_year"].max() if "last_year" in df.columns else 2025

                # Rank by track_record_score. (An earlier version ranked by a
                # fabricated "annualized return" that was a pure function of
                # years_active — 13F data cannot produce real returns.)
                top_managers = df.sort_values(
                    ["track_record_score", "years_active"], ascending=[False, False]
                ).head(15)

                content.extend(
                    [
                        f"### 🏆 Top 15 Managers by Track Record Score ({int(min_year)}-{int(max_year)})",
                        "\n| Rank | Manager | Score | Years Active | Total Actions |",
                        "| ---- | ------- | ----- | ------------ | ------------- |",
                    ]
                )

                for rank, (_, mgr) in enumerate(top_managers.iterrows(), 1):
                    manager_name = mgr.get("manager_name") or mgr.get("manager", "N/A")

                    track_score = mgr.get("track_record_score", 0)
                    years = mgr.get("years_active", 0)
                    total_actions = mgr.get("total_actions", 0)

                    content.append(
                        f"| {rank} | **{manager_name}** | "
                        f"{track_score:.2f} | "
                        f"{years} | "
                        f"{total_actions} |"
                    )

        content.append("\n---\n")
        return content

    def _generate_historical_section(self, results: Dict[str, pd.DataFrame], viz_paths: List[str]) -> List[str]:
        """Generate historical analysis section."""
        # Dynamically determine year range from data
        min_year, max_year = 2007, 2025  # Fallback values
        years_span = max_year - min_year

        # Try to get actual year range from data
        if "manager_track_records" in results and not results["manager_track_records"].empty:
            df = results["manager_track_records"]
            if "first_year" in df.columns and "last_year" in df.columns:
                min_year = df["first_year"].min()
                max_year = df["last_year"].max()
                years_span = max_year - min_year
        elif "quarterly_activity_timeline" in results and not results["quarterly_activity_timeline"].empty:
            df = results["quarterly_activity_timeline"]
            if "year" in df.columns:
                min_year = df["year"].min()
                max_year = df["year"].max()
                years_span = max_year - min_year

        content = [
            f"## 📚 Historical Analysis ({years_span}+ Years)",
            f"\nLong-term trends and patterns from {min_year} to {max_year}.",
            "\n### 📊 Visual Analysis\n",
        ]

        # Add visualizations - scan the historical/visuals directory directly
        visuals_dir = self.analysis_dir / "historical" / "visuals"
        if visuals_dir.exists():
            png_files = sorted(visuals_dir.glob("*.png"))
            if png_files:
                for png_file in png_files:
                    viz_name = png_file.stem.replace("_", " ").title()
                    relative_path = f"historical/visuals/{png_file.name}"
                    content.append(f"#### {viz_name}")
                    content.append(f"![{viz_name}]({relative_path})")
                    content.append("")

        n_quarters = (
            len(results["quarterly_activity_timeline"])
            if "quarterly_activity_timeline" in results and not results["quarterly_activity_timeline"].empty
            else None
        )
        timeline_insight = (
            f"{n_quarters} quarters of market timing insights" if n_quarters else "Market timing insights by quarter"
        )
        historical_reports = {
            "crisis_response_analysis": ("2008 vs 2020 comparison", "Crisis behavior patterns across decades"),
            "multi_decade_conviction": ("Stocks held 10+ years", "Ultimate long-term conviction plays"),
            "quarterly_activity_timeline": ("Full-history activity map", timeline_insight),
            "stock_life_cycles": ("Complete holding patterns", "Entry/exit patterns and optimal holding periods"),
        }

        # Only add reports table if we have existing reports
        existing_historical_reports = []
        for report_name, (desc, insight) in historical_reports.items():
            if report_name in results and not results[report_name].empty:
                # Check if the actual CSV file exists
                csv_path = self.analysis_dir / "historical" / f"{report_name}.csv"
                if csv_path.exists():
                    count = len(results[report_name])
                    existing_historical_reports.append((report_name, desc, insight, count))

        if existing_historical_reports:
            content.extend(
                [
                    "\n### 📋 Historical Reports",
                    "\n| Report | Description | Key Insight |",
                    "| ------ | ----------- | ----------- |",
                ]
            )

            for report_name, desc, insight, count in existing_historical_reports:
                content.append(
                    f"| [{report_name}.csv](historical/{report_name}.csv) | " f"{desc} ({count} items) | {insight} |"
                )

        content.append("\n---\n")
        return content

    def _generate_methodology_section(self) -> List[str]:
        """Generate methodology section."""
        # Build content list instead of returning immediately (fixes unreachable code bug)
        content = [
            "## 📐 Methodology",
            "\n### Scoring Algorithms",
            "\n#### Hidden Gem Score (0-10)",
            "- **Exclusivity Factor** (30%): Fewer managers = higher score",
            "- **Conviction Factor** (25%): Higher portfolio % = higher score",
            "- **Recent Activity** (20%): Recent buys boost score",
            "- **Momentum Factor** (15%): Multiple recent transactions",
            "- **Manager Quality** (10%): Premium for top-tier managers",
            "\n#### Track Record Score",
            "Computed as: years_active x 0.3 + consistency_score x 20 + crisis_buying_ratio x 10 "
            "+ 5 if the manager has current holdings.",
            "- **Consistency**: Stability of activity across observed years",
            "- **Crisis Buying**: Share of buy-side actions during the 2008 / 2020 / 2022 crisis windows",
            "- **Longevity**: Years of observed activity",
            "\n### Data Processing",
            "- **Quarters**: Parsed from Dataroma period labels; filing period taken from each page",
            "- **Price Data**: From Dataroma HTML at scrape time",
            "- **Manager Mapping**: Clean names without timestamps",
            "- **Activity Types**: Buy, Sell, Add, Reduce, Hold",
            "\n### Analysis Periods",
        ]

        # Add dynamic period information
        recent_quarters = self._get_recent_quarters(3)

        earliest = self._get_earliest_quarter()
        if recent_quarters and len(recent_quarters) >= 3:
            span_note = f"{earliest} - {recent_quarters[0]}" if earliest else f"Through {recent_quarters[0]}"
            content.extend(
                [
                    f"- **Current**: {recent_quarters[-1]} - {recent_quarters[0]} (last 3 quarters)",
                    f"- **Historical**: {span_note}",
                    "",
                ]
            )
        else:
            content.extend(["- **Current**: Last 3 quarters", "- **Historical**: Full cached history", ""])

        return content

    def _generate_caveats_section(self, results: Dict[str, pd.DataFrame]) -> List[str]:
        """
        Generate comprehensive edge case explanations and cross-file context.

        This section provides natural language explanations for data patterns
        that might otherwise be confusing to readers.

        Args:
            results: Dictionary of analysis DataFrames

        Returns:
            List of markdown-formatted strings
        """
        content = [
            "## Understanding the Data",
            "\nThis section provides context for interpreting the analysis results, "
            "including edge cases and cross-file relationships.",
        ]

        # a) Momentum Analysis Details
        content.extend(self._generate_momentum_details(results))

        # b) 52-Week Analysis Edge Cases
        content.extend(self._generate_52_week_edge_cases(results))

        # c) New Positions Context
        content.extend(self._generate_new_positions_context(results))

        # d) Cross-File Context
        content.extend(self._generate_cross_file_context(results))

        # e) Data Freshness Notes
        content.extend(self._generate_data_freshness_notes(results))

        return content

    def _generate_momentum_details(self, results: Dict[str, pd.DataFrame]) -> List[str]:
        """Generate detailed momentum analysis breakdown."""
        content = ["\n### Momentum Analysis Details"]

        if "momentum_stocks" not in results or results["momentum_stocks"].empty:
            content.append("\n*No momentum data available for the current period.*")
            return content

        momentum_df = results["momentum_stocks"].head(20)  # Top 20 for readability

        content.append(
            "\nThe momentum analysis tracks institutional accumulation and distribution patterns. "
            "Higher scores indicate stronger buying pressure from multiple managers."
        )

        # Handle actual column structure from momentum_stocks.csv
        if "ticker" in momentum_df.columns and "momentum_score" in momentum_df.columns:
            content.extend(
                [
                    "\n#### Top 20 Momentum Stocks",
                    "\n| Ticker | Company | Score | Buy Actions | Holders | Type |",
                    "| ------ | ------- | ----- | ----------- | ------- | ---- |",
                ]
            )

            for _, row in momentum_df.iterrows():
                ticker = row.get("ticker", "N/A")
                company = str(row.get("company_name", ""))[:25]
                if len(str(row.get("company_name", ""))) > 25:
                    company += "..."
                score = row.get("momentum_score", 0)
                buy_count = int(row.get("buy_count", 0))
                holders = int(row.get("current_holders", 0))
                momentum_type = row.get("momentum_type", "Unknown")

                content.append(
                    f"| **{ticker}** | {company} | {score:.1f} | {buy_count} | {holders} | {momentum_type} |"
                )

            # Add interpretation guide
            content.extend([
                "",
                "**Interpretation:**",
                "- **Recent Surge**: Strong recent accumulation across multiple quarters",
                "- **Consistent Buyer**: Steady accumulation pattern over time",
                "- **New Interest**: Fresh positions being established",
            ])

        # Fallback for simple format
        elif "ticker" in momentum_df.columns and "score" in momentum_df.columns:
            content.extend(
                [
                    "\n#### Momentum Scores",
                    "\n| Ticker | Score | Signal |",
                    "| ------ | ----- | ------ |",
                ]
            )

            for _, row in momentum_df.iterrows():
                ticker = row.get("ticker", "N/A")
                score = row.get("score", 0)
                signal = "Strong" if score > 50 else "Moderate" if score > 25 else "Mild"
                content.append(f"| **{ticker}** | {score:.1f} | {signal} |")

        content.append("")
        return content

    def _generate_52_week_edge_cases(self, results: Dict[str, pd.DataFrame]) -> List[str]:
        """Explain 52-week analysis filter criteria and edge cases."""
        content = ["\n### 52-Week Analysis Edge Cases"]

        content.append(
            "\nThe 52-week high/low analyses use these filter criteria:"
        )

        # 52-week low buys explanation
        content.append("\n#### 52-Week Low Buys (`52_week_low_buys.csv`)")
        content.append(
            "\nStocks bought while trading near their 52-week low (`near_52w_low=True`)."
        )

        if "52_week_low_buys" in results and not results["52_week_low_buys"].empty:
            low_buys_df = results["52_week_low_buys"]
            count = len(low_buys_df)
            content.append(f"\n- **{count} stocks** currently meet this criterion")

            # Show a few examples with context
            if "ticker" in low_buys_df.columns:
                examples = low_buys_df.head(3)
                example_tickers = ", ".join(examples["ticker"].tolist())
                content.append(f"- Examples: {example_tickers}")

            if "buying_managers" in low_buys_df.columns:
                content.append("- These are being accumulated by value-focused managers")

        # 52-week high sells explanation
        content.append("\n#### 52-Week High Sells (`52_week_high_sells.csv`)")
        content.append(
            "\nStocks sold while trading near their 52-week high (`near_52w_high=True`)."
        )

        if "52_week_high_sells" in results and not results["52_week_high_sells"].empty:
            high_sells_df = results["52_week_high_sells"]
            count = len(high_sells_df)
            content.append(f"\n- **{count} stocks** currently meet this criterion")

            if "ticker" in high_sells_df.columns:
                examples = high_sells_df.head(3)
                example_tickers = ", ".join(examples["ticker"].tolist())
                content.append(f"- Examples: {example_tickers}")

            if "selling_type" in high_sells_df.columns:
                heavy_dist = len(high_sells_df[high_sells_df["selling_type"] == "Heavy Distribution"])
                if heavy_dist > 0:
                    content.append(f"- {heavy_dist} stocks show \"Heavy Distribution\" patterns")

        content.append("")
        return content

    def _generate_new_positions_context(self, results: Dict[str, pd.DataFrame]) -> List[str]:
        """Provide context for new position entries."""
        content = ["\n### New Positions Context"]

        if "new_positions" not in results or results["new_positions"].empty:
            content.append("\n*No new positions identified in the current analysis window.*")
            return content

        new_pos_df = results["new_positions"]
        content.append(
            f"\nThe analysis identified **{len(new_pos_df)} new position entries** "
            "in the last 3 quarters. These represent fresh institutional interest."
        )

        # Group by ticker to show consolidated view
        try:
            # Check for manager column (could be 'manager' or 'manager_name')
            manager_col = "manager" if "manager" in new_pos_df.columns else "manager_name"
            if "ticker" in new_pos_df.columns and manager_col in new_pos_df.columns:
                # Ensure required columns exist
                agg_dict = {manager_col: lambda x: list(x)}
                # Handle different value column names
                if "total_value" in new_pos_df.columns:
                    agg_dict["total_value"] = "sum"
                elif "value" in new_pos_df.columns:
                    agg_dict["value"] = "sum"
                # Handle different portfolio % column names
                if "portfolio_pct" in new_pos_df.columns:
                    agg_dict["portfolio_pct"] = "mean"
                elif "portfolio_percent" in new_pos_df.columns:
                    agg_dict["portfolio_percent"] = "mean"

                ticker_managers = new_pos_df.groupby("ticker").agg(agg_dict)

                # Sort by value if available, otherwise by manager count
                if "total_value" in ticker_managers.columns:
                    ticker_managers = ticker_managers.sort_values("total_value", ascending=False)
                elif "value" in ticker_managers.columns:
                    ticker_managers = ticker_managers.sort_values("value", ascending=False)
                else:
                    ticker_managers["manager_count"] = ticker_managers[manager_col].apply(len)
                    ticker_managers = ticker_managers.sort_values("manager_count", ascending=False)

                content.extend(
                    [
                        "\n#### Top New Positions by Total Value",
                        "\n| Ticker | Managers Initiating | Total Value | Avg Portfolio % |",
                        "| ------ | ------------------- | ----------- | --------------- |",
                    ]
                )

                for ticker, row in ticker_managers.head(10).iterrows():
                    managers = row[manager_col]
                    manager_count = len(managers)
                    manager_str = ", ".join(str(m) for m in managers[:2])
                    if manager_count > 2:
                        manager_str += f" +{manager_count - 2} more"

                    # Handle value column which might be 'total_value' or 'value'
                    value_col = "total_value" if "total_value" in row else ("value" if "value" in row else None)
                    val_str = self._format_currency(row[value_col]) if value_col else "N/A"

                    # Handle portfolio_pct which might be different names
                    pct_col = "portfolio_pct" if "portfolio_pct" in row else ("portfolio_percent" if "portfolio_percent" in row else None)
                    avg_pct = row[pct_col] if pct_col else 0

                    content.append(f"| **{ticker}** | {manager_str} | {val_str} | {avg_pct:.2f}% |")
        except Exception as e:
            logger.warning(f"Error generating new positions table: {e}")
            content.append(f"\n*Error processing new positions data: {e}*")

        content.append("")
        return content

    def _generate_cross_file_context(self, results: Dict[str, pd.DataFrame]) -> List[str]:
        """Explain stocks appearing in multiple analysis files with context."""
        content = ["\n### Cross-File Context"]

        content.append(
            "\nSome stocks appear in multiple analysis files with opposite signals, because "
            "different managers traded them in opposite directions."
        )

        # Find overlaps between contrarian_opportunities and momentum_stocks
        contrarian_tickers: set = set()
        momentum_tickers: set = set()

        if "contrarian_opportunities" in results and not results["contrarian_opportunities"].empty:
            if "ticker" in results["contrarian_opportunities"].columns:
                contrarian_tickers = set(results["contrarian_opportunities"]["ticker"].tolist())

        if "momentum_stocks" in results and not results["momentum_stocks"].empty:
            if "ticker" in results["momentum_stocks"].columns:
                momentum_tickers = set(results["momentum_stocks"]["ticker"].tolist())

        overlap = contrarian_tickers & momentum_tickers

        if overlap:
            content.append("\n#### Stocks in Both Contrarian and Momentum Reports")
            content.append(
                "\nThese tickers appear in both `contrarian_opportunities.csv` and `momentum_stocks.csv`. "
                "This happens when different managers take opposite positions on the same stock:"
            )

            for ticker in list(overlap)[:5]:  # Limit to 5 examples
                explanation = self._explain_cross_file_ticker(ticker, results)
                content.append(f"\n- **{ticker}**: {explanation}")

        # Find overlaps between 52-week reports
        low_buy_tickers: set = set()
        high_sell_tickers: set = set()

        if "52_week_low_buys" in results and not results["52_week_low_buys"].empty:
            if "ticker" in results["52_week_low_buys"].columns:
                low_buy_tickers = set(results["52_week_low_buys"]["ticker"].tolist())

        if "52_week_high_sells" in results and not results["52_week_high_sells"].empty:
            if "ticker" in results["52_week_high_sells"].columns:
                high_sell_tickers = set(results["52_week_high_sells"]["ticker"].tolist())

        # Stocks being both bought at lows AND sold at highs (by different managers)
        # This would be unusual but worth noting
        fifty_two_overlap = low_buy_tickers & high_sell_tickers
        if fifty_two_overlap:
            content.append("\n#### Stocks with Divergent 52-Week Activity")
            content.append(
                "\nThese stocks appear in both 52-week low buys AND 52-week high sells, "
                "indicating strong disagreement among managers:"
            )
            for ticker in list(fifty_two_overlap)[:3]:
                content.append(f"- **{ticker}**: Different managers buying at lows while others sell at highs")

        # Check for new positions that are also in contrarian
        new_pos_tickers: set = set()
        if "new_positions" in results and not results["new_positions"].empty:
            if "ticker" in results["new_positions"].columns:
                new_pos_tickers = set(results["new_positions"]["ticker"].tolist())

        new_contrarian_overlap = new_pos_tickers & contrarian_tickers
        if new_contrarian_overlap and len(new_contrarian_overlap) > 0:
            content.append("\n#### New Positions with Contrarian Signals")
            content.append(
                "\nThese newly initiated positions also show contrarian patterns, "
                "suggesting managers are taking bold positions against the crowd:"
            )
            for ticker in list(new_contrarian_overlap)[:3]:
                content.append(f"- **{ticker}**: New position initiated amid contrarian activity")

        if not overlap and not fifty_two_overlap and not new_contrarian_overlap:
            content.append(
                "\n*No significant cross-file overlaps detected in the current analysis period.*"
            )

        content.append("")
        return content

    def _explain_cross_file_ticker(self, ticker: str, results: Dict[str, pd.DataFrame]) -> str:
        """Generate explanation for why a ticker appears in multiple files."""
        explanations = []

        # Check contrarian data
        if "contrarian_opportunities" in results and not results["contrarian_opportunities"].empty:
            contrarian_df = results["contrarian_opportunities"]
            if "ticker" in contrarian_df.columns:
                ticker_data = contrarian_df[contrarian_df["ticker"] == ticker]
                if not ticker_data.empty:
                    row = ticker_data.iloc[0]
                    signal = row.get("contrarian_signal", "Unknown")
                    buy_count = row.get("buy_count", 0)
                    sell_count = row.get("sell_count", 0)
                    explanations.append(f"Contrarian signal: {signal} (buys: {buy_count}, sells: {sell_count})")

        # Check momentum data
        if "momentum_stocks" in results and not results["momentum_stocks"].empty:
            momentum_df = results["momentum_stocks"]
            if "ticker" in momentum_df.columns:
                ticker_data = momentum_df[momentum_df["ticker"] == ticker]
                if not ticker_data.empty:
                    row = ticker_data.iloc[0]
                    score = row.get("score", row.get("momentum_score", 0))
                    explanations.append(f"Momentum score: {score}")

        if explanations:
            return " | ".join(explanations)
        return "Multiple managers taking different positions"

    def _generate_data_freshness_notes(self, results: Dict[str, pd.DataFrame]) -> List[str]:
        """Generate notes about data freshness and analysis window."""
        content = ["\n### Data Freshness Notes"]

        # Determine analysis window from data
        recent_quarters = self._get_recent_quarters(3)

        if recent_quarters:
            content.append(
                f"\n**Analysis Window**: {recent_quarters[-1]} to {recent_quarters[0]} (last 3 quarters)"
            )
        else:
            content.append("\n**Analysis Window**: Last 3 quarters")

        content.append("\nAll current analysis reports use manager filings from this window.")

        content.extend(
            [
                "\n1. **Filing Lag**: SEC 13F filings are reported quarterly with a 45-day delay. "
                "Positions may have changed since filing.",
                "\n2. **Historical Reference Columns**: Some reports include columns like `last_buy_period`, "
                "`first_established`, or `last_activity`. These carry dates from outside the "
                "3-quarter window.",
                "\n3. **Price Data**: Current prices are from the most recent scrape and may differ "
                "from the prices at which positions were established.",
                "\n4. **Manager Activity**: A single manager may have multiple entries for the same stock "
                "if they made multiple transactions (Buy, Add, Reduce) within the analysis window.",
            ]
        )

        # Add generated timestamp context
        content.append(
            f"\n**Report Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        content.append("")
        return content

    def _relative_path(self, viz_path: str) -> str:
        """Convert absolute path to relative for README."""
        try:
            return str(Path(viz_path).relative_to(self.analysis_dir))
        except ValueError:
            return viz_path

    def save_readme(self, content: str) -> str:
        """Save README to analysis directory."""
        readme_path = self.analysis_dir / "README.md"
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info(f"README saved to {readme_path}")
        return str(readme_path)
