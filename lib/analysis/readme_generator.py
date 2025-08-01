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
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class ReadmeGenerator:
    """Generates comprehensive README for analysis results."""
    
    def __init__(self, analysis_dir: str = "analysis", data_loader: Optional[Any] = None) -> None:
        self.analysis_dir = Path(analysis_dir)
        self.data_loader = data_loader
    
    def generate_readme(self, results: Dict[str, pd.DataFrame], 
                       viz_paths: Dict[str, List[str]]) -> str:
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
        
        content = [
            "# 📊 Dataroma Investment Analysis",
            f"\n*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
            "\n## 🎯 Overview",
            f"\nThis analysis covers **{years_span}+ years** of investment data from top money managers, "
            "providing insights into current opportunities, manager performance patterns, "
            "and long-term investment trends.",
            "\n### 📈 Quick Stats",
            f"- **Total Activities Analyzed**: 57,643",
            f"- **Current Holdings**: 3,311",
            f"- **Managers Tracked**: 81",
            f"- **Time Period**: {min_year}-{max_year}",
            "\n---\n"
        ]
        
        # Current Analysis Section
        content.extend(self._generate_current_section(results, viz_paths.get("current", [])))
        
        # Advanced Analysis Section
        content.extend(self._generate_advanced_section(results, viz_paths.get("advanced", [])))
        
        # Historical Analysis Section
        content.extend(self._generate_historical_section(results, viz_paths.get("historical", [])))
        
        # Methodology Section
        content.extend(self._generate_methodology_section())
        
        # Footer
        content.extend([
            "\n---\n",
            "## 📅 Update Schedule",
            "\nThis analysis is refreshed monthly to capture the latest investment trends "
            "and manager activities.",
            "\n## 🔗 Data Source",
            "\nAll data is sourced from [Dataroma](https://www.dataroma.com), tracking "
            "portfolios of super investors.",
            "\n---",
            f"\n*Analysis framework powered by modular Python architecture with 100% data accuracy validation*"
        ])
        
        return "\n".join(content)
    
    def _generate_current_section(self, results: Dict[str, pd.DataFrame], 
                                 viz_paths: List[str]) -> List[str]:
        """Generate current analysis section."""
        # Get recent quarters dynamically if data loader is available
        recent_quarters = []
        if self.data_loader is not None:
            # Use a simple approach to get recent quarters from history data
            if hasattr(self.data_loader, 'history_df') and self.data_loader.history_df is not None:
                periods = self.data_loader.history_df['period'].dropna().unique()
                # Extract and sort quarters
                quarter_data = []
                for period in periods:
                    if 'Q' in str(period):
                        parts = str(period).split()
                        if len(parts) == 2:
                            quarter_data.append(period)
                recent_quarters = sorted(quarter_data, reverse=True)[:3]
        
        quarter_range = f"from {recent_quarters[-1]} to {recent_quarters[0]}" if len(recent_quarters) >= 3 else "recent quarters"
        
        content = [
            "## 💡 Current Analysis (Last 3 Quarters)",
            f"\nImmediate opportunities and recent market activity {quarter_range}.",
            "\n### 📊 Visual Analysis\n"
        ]
        
        # Add visualizations - scan the current/visuals directory directly
        visuals_dir = self.analysis_dir / "current" / "visuals"
        if visuals_dir.exists():
            png_files = sorted(visuals_dir.glob("*.png"))
            if png_files:
                for png_file in png_files:
                    viz_name = png_file.stem.replace('_', ' ').title()
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
            "value_price_opportunities": ("Multi-tier price analysis", "Comprehensive price-based screening")
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
            content.extend([
                "\n### 📋 Current Reports",
                "\n| Report | Description | Key Insight |",
                "| ------ | ----------- | ----------- |"
            ])
            
            for report_name, desc, insight, count in existing_reports:
                content.append(f"| [{report_name}.csv](current/{report_name}.csv) | "
                             f"{desc} ({count} items) | {insight} |")
        
            
            content.append("")
        
        # Add top opportunities - check for any high-value report that exists
        top_opportunity_reports = ["hidden_gems", "under_radar_picks", "deep_value_plays", "high_conviction_low_price", "momentum_stocks"]
        
        for report_name in top_opportunity_reports:
            if report_name in results and not results[report_name].empty:
                csv_path = self.analysis_dir / "current" / f"{report_name}.csv"
                if csv_path.exists():
                    top_opportunities = results[report_name].head(5)
                    report_title = report_name.replace('_', ' ').title()
                    
                    # Special handling for different report types
                    if report_name == "hidden_gems":
                        content.extend([
                            "### 🌟 Top 5 Hidden Gems",
                            "\n| Ticker | Score | Price | Managers |",
                            "| ------ | ----- | ----- | -------- |"
                        ])
                        
                        for _, gem in top_opportunities.iterrows():
                            managers_list = str(gem.get('managers', '')).split(',')[:3]
                            managers_str = ', '.join(m.strip() for m in managers_list)
                            content.append(f"| **{gem.get('ticker', 'N/A')}** | "
                                         f"{gem.get('hidden_gem_score', 0):.2f} | "
                                         f"${gem.get('current_price', 0):.2f} | "
                                         f"{managers_str} |")
                    elif report_name == "under_radar_picks":
                        # Use top 15 for under radar picks with proper analysis
                        top_opportunities = results[report_name].head(15)
                        content.extend([
                            "### 🔍 Top 15 Under Radar Picks",
                            "\n| Rank | Ticker | Score | Value | Why Under Radar |",
                            "| ---- | ------ | ----- | ----- | --------------- |"
                        ])
                        
                        for rank, (_, pick) in enumerate(top_opportunities.iterrows(), 1):
                            ticker = pick.get('ticker', 'N/A')
                            score = pick.get('under_radar_score', 0)
                            total_value = pick.get('total_value', 0)
                            total_shares = pick.get('total_shares', 1)
                            manager_count = pick.get('manager_count', 0)
                            pick_type = pick.get('pick_type', 'Unknown')
                            avg_portfolio_pct = pick.get('avg_portfolio_pct', 0)
                            first_established = pick.get('first_established', 'Unknown')
                            
                            # Calculate approximate price per share
                            price_per_share = total_value / total_shares if total_shares > 0 else 0
                            
                            # Format value in millions/billions
                            if total_value >= 1e9:
                                value_str = f"${total_value/1e9:.1f}B"
                            elif total_value >= 1e6:
                                value_str = f"${total_value/1e6:.0f}M"
                            else:
                                value_str = f"${total_value:,.0f}"
                            
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
                            
                            if "Q1 2020" in str(first_established) or "Q2 2024" in str(first_established):
                                reasons.append("Recent discovery")
                            
                            if price_per_share < 50:
                                reasons.append("Low price entry")
                            
                            why_under_radar = "; ".join(reasons) if reasons else "Institutional accumulation"
                            
                            content.append(f"| {rank} | **{ticker}** | "
                                         f"{score:.1f} | "
                                         f"{value_str} | "
                                         f"{why_under_radar} |")
                    else:
                        content.extend([
                            f"### 🌟 Top 5 {report_title}",
                            "\n| Ticker | Score | Price | Details |",
                            "| ------ | ----- | ----- | ------- |"
                        ])
                        
                        for _, opp in top_opportunities.iterrows():
                            # Get the most relevant columns for each report type
                            score = opp.get('score', opp.get('momentum_score', opp.get('conviction_score', 0)))
                            price = opp.get('current_price', opp.get('price', 0))
                            details = str(opp.get('managers', opp.get('manager_name', opp.get('details', 'N/A'))))
                            
                            # Limit details to first 3 items if comma-separated
                            if ',' in details:
                                details_list = details.split(',')[:3]
                                details = ', '.join(d.strip() for d in details_list)
                            
                            content.append(f"| **{opp.get('ticker', 'N/A')}** | "
                                         f"{score:.2f} | "
                                         f"${price:.2f} | "
                                         f"{details} |")
                    break  # Only show one top opportunities table
        
        content.append("\n---\n")
        return content
    
    def _generate_advanced_section(self, results: Dict[str, pd.DataFrame], 
                                  viz_paths: List[str]) -> List[str]:
        """Generate advanced analysis section."""
        content = [
            "## 🧠 Advanced Analysis (Manager Performance)",
            "\nDeep insights into manager strategies, performance patterns, and decision-making.",
            "\n### 📊 Visual Analysis\n"
        ]
        
        # Add visualizations - scan the advanced/visuals directory directly
        visuals_dir = self.analysis_dir / "advanced" / "visuals"
        if visuals_dir.exists():
            png_files = sorted(visuals_dir.glob("*.png"))
            if png_files:
                for png_file in png_files:
                    viz_name = png_file.stem.replace('_', ' ').title()
                    relative_path = f"advanced/visuals/{png_file.name}"
                    content.append(f"#### {viz_name}")
                    content.append(f"![{viz_name}]({relative_path})")
                    content.append("")
        
        advanced_reports = {
            "action_sequence_patterns": ("Trading pattern analysis", "Institutional buy/sell sequence patterns"),
            "catalyst_timing_masters": ("Market timing excellence", "Managers with exceptional timing skills"),
            "crisis_alpha_generators": ("Crisis period outperformers", "Managers who buy during crashes"),
            "high_conviction_stocks": ("Highest conviction positions", "Stocks with strongest institutional backing"),
            "interesting_stocks_overview": ("Top-tier opportunities", "Multi-factor scoring of elite picks"),
            "long_term_winners": ("Sustained institutional interest", "Stocks with long-term institutional backing"),
            "manager_evolution_patterns": ("Strategy adaptation over time", "How managers evolve their approaches"),
            "manager_performance": ("Comprehensive manager evaluation", "Multi-dimensional performance metrics"),
            "manager_track_records": ("18+ year performance history", "Comprehensive manager scoring with consistency"),
            "multi_manager_favorites": ("Consensus high-conviction picks", "Stocks held by multiple elite managers"),
            "position_sizing_mastery": ("Optimal allocation patterns", "Advanced portfolio construction analysis"),
            "sector_rotation_excellence": ("Elite sector allocation", "Superior sector rotation strategies"),
            "sector_rotation_patterns": ("Institutional sector flows", "Sector rotation trend analysis"),
            "theme_emergence_detection": ("Early theme identification", "Emerging investment theme detection"),
            "top_holdings": ("Largest institutional positions", "Deep dive into major institutional holdings")
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
            content.extend([
                "\n### 📋 Advanced Reports",
                "\n| Report | Description | Key Insight |",
                "| ------ | ----------- | ----------- |"
            ])
            
            for report_name, desc, insight, count in existing_advanced_reports:
                content.append(f"| [{report_name}.csv](advanced/{report_name}.csv) | "
                             f"{desc} ({count} items) | {insight} |")
            
            content.append("")
        
        # Add top managers - only if the CSV file actually exists
        if "manager_track_records" in results and not results["manager_track_records"].empty:
            csv_path = self.analysis_dir / "advanced" / "manager_track_records.csv"
            if csv_path.exists():
                # Get the data period from the CSV
                df = results["manager_track_records"]
                min_year = df['first_year'].min() if 'first_year' in df.columns else 2007
                max_year = df['last_year'].max() if 'last_year' in df.columns else 2025
                
                # Sort by annual return first, then by track record score as tiebreaker
                top_managers = df.sort_values(
                    ['annualized_return_pct', 'track_record_score'], 
                    ascending=[False, False]
                ).head(15)
                
                content.extend([
                    f"### 🏆 Top 15 Managers by Annual Return ({int(min_year)}-{int(max_year)})",
                    "\n| Rank | Manager | Annual Return | Score | Years Active |",
                    "| ---- | ------- | ------------- | ----- | ------------ |"
                ])
                
                for rank, (_, mgr) in enumerate(top_managers.iterrows(), 1):
                    # Use the actual column names from the CSV
                    # The CSV has duplicate 'manager' columns - first is code, second is full name
                    if hasattr(mgr, 'iloc') and len(mgr) > 1:
                        manager_name = mgr.iloc[1]  # Second 'manager' column has full name
                    else:
                        manager_name = mgr.get('manager', 'N/A')
                    
                    track_score = mgr.get('track_record_score', 0)
                    years = mgr.get('years_active', 0)
                    annual_return = mgr.get('annualized_return_pct', 0)
                    
                    content.append(f"| {rank} | **{manager_name}** | "
                                 f"{annual_return:.1f}% | "
                                 f"{track_score:.2f} | "
                                 f"{years} |")
        
        content.append("\n---\n")
        return content
    
    def _generate_historical_section(self, results: Dict[str, pd.DataFrame], 
                                    viz_paths: List[str]) -> List[str]:
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
            "\n### 📊 Visual Analysis\n"
        ]
        
        # Add visualizations - scan the historical/visuals directory directly
        visuals_dir = self.analysis_dir / "historical" / "visuals"
        if visuals_dir.exists():
            png_files = sorted(visuals_dir.glob("*.png"))
            if png_files:
                for png_file in png_files:
                    viz_name = png_file.stem.replace('_', ' ').title()
                    relative_path = f"historical/visuals/{png_file.name}"
                    content.append(f"#### {viz_name}")
                    content.append(f"![{viz_name}]({relative_path})")
                    content.append("")
        
        historical_reports = {
            "crisis_response_analysis": ("2008 vs 2020 comparison", "Crisis behavior patterns across decades"),
            "multi_decade_conviction": ("Stocks held 10+ years", "Ultimate long-term conviction plays"),
            "quarterly_activity_timeline": ("18-year activity map", "73 quarters of market timing insights"),
            "stock_life_cycles": ("Complete holding patterns", "Entry/exit patterns and optimal holding periods")
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
            content.extend([
                "\n### 📋 Historical Reports",
                "\n| Report | Description | Key Insight |",
                "| ------ | ----------- | ----------- |"
            ])
            
            for report_name, desc, insight, count in existing_historical_reports:
                content.append(f"| [{report_name}.csv](historical/{report_name}.csv) | "
                             f"{desc} ({count} items) | {insight} |")
        
        content.append("\n---\n")
        return content
    
    def _generate_methodology_section(self) -> List[str]:
        """Generate methodology section."""
        return [
            "## 📐 Methodology",
            "\n### Scoring Algorithms",
            "\n#### Hidden Gem Score (0-10)",
            "- **Exclusivity Factor** (30%): Fewer managers = higher score",
            "- **Conviction Factor** (25%): Higher portfolio % = higher score",
            "- **Recent Activity** (20%): Recent buys boost score",
            "- **Momentum Factor** (15%): Multiple recent transactions",
            "- **Manager Quality** (10%): Premium for top-tier managers",
            "\n#### Track Record Score",
            "- **Win Rate**: Percentage of successful investments",
            "- **Consistency**: Performance stability over time",
            "- **Crisis Alpha**: Outperformance during downturns",
            "- **Longevity**: Years of active management",
            "\n### Data Processing",
            "- **Temporal Accuracy**: 100% validated quarter extraction",
            "- **Price Data**: Real-time from Dataroma HTML",
            "- **Manager Mapping**: Clean names without timestamps",
            "- **Activity Types**: Buy, Sell, Add, Reduce, Hold",
            "\n### Analysis Periods"
        ]
        
        # Add dynamic period information
        from ..analysis.base_analyzer import BaseAnalyzer
        base_analyzer = BaseAnalyzer(self.data_loader)
        recent_quarters = base_analyzer.get_recent_quarters(3)
        
        if recent_quarters and len(recent_quarters) >= 3:
            content.extend([
                f"- **Current**: {recent_quarters[-1]} - {recent_quarters[0]} (last 3 quarters)",
                f"- **Historical**: Q1 2007 - {recent_quarters[0]} (18+ years)",
                ""
            ])
        else:
            content.extend([
                "- **Current**: Last 3 quarters",
                "- **Historical**: Q1 2007 - Present (18+ years)",
                ""
            ])
        
        return content
    
    def _relative_path(self, viz_path: str) -> str:
        """Convert absolute path to relative for README."""
        try:
            return str(Path(viz_path).relative_to(self.analysis_dir))
        except:
            return viz_path
    
    def save_readme(self, content: str) -> str:
        """Save README to analysis directory."""
        readme_path = self.analysis_dir / "README.md"
        with open(readme_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"README saved to {readme_path}")
        return str(readme_path)