#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Analysis Orchestrator

Coordinates all analysis modules and generates comprehensive reports.
This is the main entry point for running comprehensive analysis across
all modules and generating reports and visualizations.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any
import json
from datetime import datetime

from ..data.data_loader import DataLoader
from .holdings_analyzer import HoldingsAnalyzer
from .gems_analyzer import GemsAnalyzer
from .momentum_analyzer import MomentumAnalyzer
from .price_analyzer import PriceAnalyzer
from .historical_analyzer import HistoricalAnalyzer
from .advanced_analyzer import AdvancedHistoricalAnalyzer
from .position_timeline_analyzer import PositionTimelineAnalyzer
from ..utils.data_validator import DataValidator
from ..utils.csv_formatter import CSVFormatter
from ..utils.formatters import DataFormatter
from ..utils.cross_file_validator import CrossFileValidator


class AnalysisOrchestrator:
    """
    Coordinates all analysis modules and generates complete reports.

    This is the main class that recreates all the analysis capabilities
    from the original analyze_holdings.py but with clean, modular architecture.
    """

    def __init__(self, cache_dir: str = "cache", output_dir: str = "analysis") -> None:
        """Initialize orchestrator with data loader and analyzers.

        Args:
            cache_dir: Directory containing the scraped cache.
            output_dir: Directory to write reports into. Parameterized so
                tests can redirect output instead of silently overwriting
                the real committed analysis/ files.
        """
        self.cache_dir = Path(cache_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.data_loader = DataLoader(cache_dir)

        self.analyzers: Dict[str, Any] = {}

        self.results: Dict[str, pd.DataFrame] = {}
        self.analysis_summary: Dict[str, Any] = {}

        # Initialize validator early to allow validate_data() to be called before load_data()
        self.validator = DataValidator(self.cache_dir)
        self.csv_formatter: Optional[CSVFormatter] = None

        logging.info(f"AnalysisOrchestrator initialized with cache_dir: {cache_dir}")

    def load_data(self) -> bool:
        """Load all data and initialize analyzers."""
        logging.info("Loading data for analysis...")

        success = self.data_loader.load_all_data()
        if not success:
            logging.error("Failed to load data")
            return False

        self._initialize_analyzers()

        logging.info("Data loaded successfully, analyzers initialized")
        return True

    def validate_data(self) -> bool:
        """Validate data integrity and consistency."""
        logging.info("Validating data integrity...")

        self.validator.validate_all_data()
        summary = self.validator.get_validation_summary()

        if summary["overall_status"] == "PASSED":
            logging.info(f"✅ Data validation passed ({summary['passed_checks']}/{summary['total_checks']} checks)")
            return True
        else:
            logging.error(f"❌ Data validation failed ({summary['failed_checks']}/{summary['total_checks']} checks)")
            for check, result in summary["details"].items():
                status = "✅" if result else "❌"
                logging.info(f"  {status} {check}")
            return False

    def _initialize_analyzers(self) -> None:
        """Initialize all analyzer modules."""
        self.analyzers = {
            "holdings": HoldingsAnalyzer(self.data_loader),
            "gems": GemsAnalyzer(self.data_loader),
            "momentum": MomentumAnalyzer(self.data_loader),
            "price": PriceAnalyzer(self.data_loader),
            "historical": HistoricalAnalyzer(self.data_loader),
            "advanced": AdvancedHistoricalAnalyzer(self.data_loader),
            "position_timeline": PositionTimelineAnalyzer(self.data_loader),
        }

        # csv_formatter is initialized here since it may need loaded data
        self.csv_formatter = CSVFormatter(self.cache_dir)

        logging.info("All analyzers initialized")

    def run_complete_analysis(self) -> Dict[str, pd.DataFrame]:
        """
        Run all analysis modules and generate comprehensive results.

        Returns:
            Dictionary mapping analysis names to DataFrames
        """
        if not self.data_loader.data_loaded:
            raise ValueError("Data must be loaded before running analysis")

        logging.info("Starting complete analysis...")
        start_time = datetime.now()

        self.results.clear()

        logging.info("Running holdings analyses...")
        holdings_results = self.analyzers["holdings"].analyze_all()
        self.results.update(holdings_results)

        logging.info("Running gems analyses...")
        gems_results = self.analyzers["gems"].analyze_all()
        self.results.update(gems_results)

        logging.info("Running momentum analyses...")
        momentum_results = self.analyzers["momentum"].analyze_all()
        self.results.update(momentum_results)

        logging.info("Running price analyses...")
        price_results = self.analyzers["price"].analyze_all()
        self.results.update(price_results)

        logging.info("Running historical analyses...")
        historical_results = self.analyzers["historical"].analyze_all()
        self.results.update(historical_results)

        logging.info("Running advanced historical analyses...")
        advanced_results = self.analyzers["advanced"].analyze_all()
        self.results.update(advanced_results)

        logging.info("Running position timeline analyses...")
        timeline_results = self.analyzers["position_timeline"].analyze_all()
        self.results.update(timeline_results)

        self._calculate_analysis_summary()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        logging.info(f"Complete analysis finished in {duration:.1f} seconds")
        logging.info(f"Generated {len(self.results)} analysis reports")

        return self.results

    def _calculate_analysis_summary(self) -> None:
        """Calculate summary statistics across all analyses."""
        summary = {
            "analysis_timestamp": datetime.now().isoformat(),
            "total_analyses": len(self.results),
            "data_summary": self.data_loader.get_data_summary(),
        }

        total_stocks_analyzed = set()
        total_managers_analyzed = set()
        analysis_stats = {}

        for name, df in self.results.items():
            if df.empty:
                continue

            stats = {
                "row_count": len(df),
                "has_data": not df.empty,
            }

            if "ticker" in df.columns:
                total_stocks_analyzed.update(df["ticker"].unique())
                stats["unique_tickers"] = len(df["ticker"].unique())

            if "managers" in df.columns:
                all_managers = df["managers"].dropna().str.split(", ").explode().unique()
                total_managers_analyzed.update(all_managers)
                stats["unique_managers"] = len(all_managers)

            analysis_stats[name] = stats

        summary.update(
            {
                "total_unique_stocks": len(total_stocks_analyzed),
                "total_unique_managers": len(total_managers_analyzed),
                "analysis_breakdown": analysis_stats,
            }
        )

        self.analysis_summary = summary
        logging.info(
            f"Analysis summary: {summary['total_unique_stocks']} stocks, {summary['total_unique_managers']} managers"
        )

    def save_all_reports(self, format_for_export: bool = True) -> Dict[str, List[str]]:
        """
        Save all analysis results as CSV files in appropriate directories.

        Args:
            format_for_export: Whether to apply formatting and clean column names

        Returns:
            Dictionary mapping folder names to list of saved files
        """
        if not self.results:
            logging.warning("No analysis results to save")
            return {}

        logging.info("Saving analysis reports...")

        current_dir = self.output_dir / "current"
        advanced_dir = self.output_dir / "advanced"
        historical_dir = self.output_dir / "historical"

        for dir_path in [current_dir, advanced_dir, historical_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
            (dir_path / "visuals").mkdir(exist_ok=True)

        saved_files = {"current": [], "advanced": [], "historical": []}

        current_analyses = [
            "hidden_gems",
            "deep_value_plays",
            "contrarian_opportunities",
            "under_radar_picks",
            "momentum_stocks",
            "new_positions",
            "stocks_under_$5",
            "stocks_under_$10",
            "stocks_under_$20",
            "stocks_under_$50",
            "stocks_under_$100",
            "high_conviction_low_price",
            "value_price_opportunities",
            "52_week_low_buys",
            "52_week_high_sells",
            "concentration_changes",
            "most_sold_stocks",
            "highest_portfolio_concentration",
            "stock_timelines",
        ]

        advanced_analyses = [
            "multi_manager_favorites",
            "top_holdings",
            "high_conviction_stocks",
            "interesting_stocks_overview",
            "manager_performance",
            "sector_rotation_patterns",
            "manager_track_records",
            "crisis_alpha_generators",
            "position_sizing_mastery",
            "manager_evolution_patterns",
            "action_sequence_patterns",
            "catalyst_timing_masters",
            "theme_emergence_detection",
        ]

        for name, df in self.results.items():
            # Save empty DataFrames too (they have metadata columns for validation)
            if df.empty:
                logging.warning(f"Empty analysis: {name} (saving with headers only)")

            try:
                if name in current_analyses:
                    output_dir = current_dir
                    category = "current"
                elif name in advanced_analyses:
                    output_dir = advanced_dir
                    category = "advanced"
                elif any(hist in name for hist in ["historical", "quarterly", "crisis", "life_cycle", "multi_decade"]):
                    output_dir = historical_dir
                    category = "historical"
                else:
                    output_dir = advanced_dir
                    category = "advanced"

                output_file = output_dir / f"{name}.csv"
                # Clean up duplicate columns and JSON before saving
                df_clean = DataFormatter.clean_dataframe_for_csv(df)
                df_clean.to_csv(output_file, index=False)
                saved_files[category].append(str(output_file))

                logging.debug(f"Saved {name}: {len(df)} rows -> {output_file}")

            except Exception as e:
                logging.error(f"Failed to save {name}: {e}")

        if format_for_export:
            logging.info("Formatting CSV files for better readability...")
            for dir_path in [current_dir, advanced_dir, historical_dir]:
                self.csv_formatter.format_all_csvs(dir_path)

        total_saved = sum(len(files) for files in saved_files.values())
        logging.info(f"Saved {total_saved} analysis reports")
        logging.info(f"  - Current: {len(saved_files['current'])} files")
        logging.info(f"  - Advanced: {len(saved_files['advanced'])} files")
        logging.info(f"  - Historical: {len(saved_files['historical'])} files")

        return saved_files

    def save_analysis_summary(self) -> None:
        """Save analysis summary as JSON."""
        if not self.analysis_summary:
            logging.warning("No analysis summary to save")
            return

        cache_json_dir = Path("cache/json")
        cache_json_dir.mkdir(parents=True, exist_ok=True)

        summary_file = cache_json_dir / "analysis_summary.json"
        with open(summary_file, "w") as f:
            json.dump(self.analysis_summary, f, indent=2, default=str)

        logging.info(f"Analysis summary saved to {summary_file}")

    def generate_readme(self) -> None:
        """Generate comprehensive README with analysis results."""
        if not self.results or not self.analysis_summary:
            logging.warning("No analysis data available for README generation")
            return

        readme_content = self._build_readme_content()

        readme_file = self.output_dir / "README.md"
        with open(readme_file, "w") as f:
            f.write(readme_content)

        logging.info(f"Analysis README generated: {readme_file}")

    def _build_readme_content(self) -> str:
        """Build comprehensive README content by dynamically scanning all files."""
        summary = self.analysis_summary
        min_year, max_year, years_span = self._get_year_range()

        content = [self._build_readme_header(summary, min_year, max_year, years_span)]

        csv_descriptions = self._get_csv_descriptions()
        chart_descriptions = self._get_chart_descriptions()
        categories = self._get_categories(years_span, min_year, max_year)

        for folder, title, description in categories:
            content.extend(self._build_category_section(
                folder, title, description, csv_descriptions, chart_descriptions
            ))

        content.extend(self._build_methodology_section())

        return "\n".join(content)

    def _get_year_range(self) -> tuple:
        """Determine year range from analysis results."""
        min_year, max_year = 2007, 2025  # Fallback values

        if "manager_track_records" in self.results and not self.results["manager_track_records"].empty:
            df = self.results["manager_track_records"]
            if "first_year" in df.columns and "last_year" in df.columns:
                min_year = df["first_year"].min()
                max_year = df["last_year"].max()
        elif "quarterly_activity_timeline" in self.results and not self.results["quarterly_activity_timeline"].empty:
            df = self.results["quarterly_activity_timeline"]
            if "year" in df.columns:
                min_year = df["year"].min()
                max_year = df["year"].max()

        return min_year, max_year, max_year - min_year

    def _build_readme_header(self, summary: Dict, min_year: int, max_year: int, years_span: int) -> str:
        """Build README header with overview stats."""
        return f"""# 📊 **Dataroma Investment Analysis**

*Generated: {summary.get('analysis_timestamp', 'Unknown')}*

## 🎯 **Overview**

This analysis covers **{years_span}+ years** of investment data from top money managers,
providing insights into current opportunities, manager performance patterns, and long-term investment trends.

### 📈 **Quick Stats**
- **Total Activities Analyzed**: {summary.get('data_summary', {}).get('activities_count', 0):,}
- **Current Holdings**: {summary.get('data_summary', {}).get('holdings_count', 0):,}
- **Managers Tracked**: {summary.get('total_unique_managers', 0)}
- **Stocks Analyzed**: {summary.get('total_unique_stocks', 0):,}
- **Analysis Reports**: {summary.get('total_analyses', 0)}
- **Time Period**: {min_year}-{max_year}

---
"""

    def _get_csv_descriptions(self) -> Dict[str, tuple]:
        """Get descriptions for CSV analysis files."""
        return {
            "hidden_gems": ("Under-the-radar opportunities", "5-factor scoring identifies high-potential stocks"),
            "momentum_stocks": ("Recent buying activity", "Tracks institutional accumulation patterns"),
            "new_positions": ("Fresh acquisitions", "Identifies emerging manager interests"),
            "52_week_low_buys": ("Value buying at 52-week lows", "Managers hunting for bargains"),
            "52_week_high_sells": ("Profit-taking at 52-week highs", "Strategic exits at peaks"),
            "concentration_changes": ("Portfolio allocation shifts", "Major position size changes"),
            "contrarian_opportunities": ("Mixed buy/sell signals", "Stocks with opposing manager views"),
            "deep_value_plays": ("Deep value opportunities", "Undervalued stocks with potential"),
            "high_conviction_low_price": ("Low-priced conviction plays", "Cheap stocks with high manager belief"),
            "highest_portfolio_concentration": ("Most concentrated positions", "Highest portfolio % allocations"),
            "most_sold_stocks": ("Recent institutional exits", "Stocks being abandoned"),
            "stocks_under_$5": ("Ultra-low price opportunities", "Deep value plays under $5"),
            "stocks_under_$10": ("Low-price institutional picks", "Quality under $10"),
            "stocks_under_$20": ("Affordable growth plays", "Accessible price points"),
            "stocks_under_$50": ("Mid-price value opportunities", "Balanced risk/reward"),
            "stocks_under_$100": ("Institutional favorites under $100", "Premium stocks at reasonable prices"),
            "under_radar_picks": ("Exclusive manager holdings", "Held by 1-2 premium managers only"),
            "value_price_opportunities": ("Value with momentum", "Cheap stocks gaining traction"),
            "stock_timelines": ("Stock accumulation/distribution trends", "Quick view of momentum direction"),
            "portfolio_concentration": ("Portfolio weight analysis", "Position sizing insights"),
            "price_opportunities": ("Price-based opportunities", "Multi-threshold value analysis"),
            "52_week_trading": ("52-week high/low activity", "Extreme price point trading"),
            "manager_track_records": ("18+ year performance history", "Comprehensive manager scoring"),
            "multi_manager_consensus": ("Consensus picks", "Stocks held by multiple gurus"),
            "crisis_alpha_generation": ("Crisis outperformers", "Managers who excel in downturns"),
            "manager_evolution": ("Strategy evolution", "How managers adapt over time"),
            "position_sizing": ("Allocation mastery", "Optimal position sizing patterns"),
            "top_holdings": ("Largest positions", "Where smart money concentrates"),
            "quarterly_activity_timeline": ("18-year activity map", "Market timing and sentiment"),
            "crisis_response_analysis": ("Crisis behavior comparison", "2008 vs COVID vs 2022"),
            "multi_decade_conviction": ("10+ year holdings", "Ultimate long-term plays"),
            "stock_life_cycles": ("Entry/exit patterns", "Stock lifecycle insights"),
        }

    def _get_chart_descriptions(self) -> Dict[str, str]:
        """Get descriptions for chart visualizations."""
        return {
            "52_week_analysis_current": "**52-Week High/Low Trading Analysis** - Identifies managers buying at lows and selling at highs",
            "hidden_gems_current": "**Top 20 Hidden Gems** - Under-the-radar opportunities with sophisticated 5-factor scoring",
            "momentum_analysis_current": "**Momentum Analysis** - Recent buying/selling activity patterns",
            "new_positions_current": "**New Position Analysis** - Fresh acquisitions by top managers",
            "portfolio_changes_current": "**Portfolio Concentration Changes** - Major allocation shifts",
            "price_opportunities_current": "**Price-Based Opportunities** - Value plays at different price points",
            "consensus_picks_advanced": "**Multi-Manager Consensus** - Stocks held by multiple top managers",
            "crisis_alpha_advanced": "**Crisis-Period Buying Behavior** - Buy-ratio and participation scores during market downturns (no returns)",
            "manager_evolution_advanced": "**Manager Evolution Patterns** - How strategies adapt over time",
            "position_sizing_advanced": "**Position Sizing Mastery** - Optimal allocation strategies",
            "top_holdings_advanced": "**Top Holdings Analysis** - Most valuable positions across all managers",
            "manager_performance_advanced": "**Manager Track Record Overview** - Activity, consistency and portfolio-size metrics (no returns)",
            "quarterly_activity_timeline": "**Investment Activity Timeline** - 18-year quarterly activity patterns with crisis periods",
            "crisis_response_comparison": "**Crisis Response Analysis** - Comparing behavior during major market crises",
            "multi_decade_conviction": "**Multi-Decade Conviction Plays** - Stocks held for 10+ years by top managers",
            "stock_life_cycles": "**Stock Life Cycle Analysis** - Entry, accumulation, and exit patterns over time",
            "manager_performance_historical": "**Historical Manager Performance** - Long-term track records and consistency",
        }

    def _get_categories(self, years_span: int, min_year: int, max_year: int) -> List[tuple]:
        """Get category definitions for README sections."""
        return [
            ("current", "💡 **Current Analysis** (Last 3 Quarters)", "Immediate opportunities and recent market activity."),
            ("advanced", "🧠 **Advanced Analysis** (Manager Performance)", "Deep insights into manager strategies, performance patterns, and decision-making."),
            ("historical", f"📚 **Historical Analysis** ({years_span}+ Years)", f"Long-term trends and patterns from {min_year} to {max_year}."),
        ]

    def _build_category_section(self, folder: str, title: str, description: str,
                                 csv_descriptions: Dict, chart_descriptions: Dict) -> List[str]:
        """Build content for a single category section."""
        content = [f"## {title}\n", f"{description}\n"]

        # Add visual analysis
        visuals_dir = self.output_dir / folder / "visuals"
        if visuals_dir.exists():
            png_files = sorted(visuals_dir.glob("*.png"))
            if png_files:
                content.append("### 📊 Visual Analysis\n")
                for png_file in png_files:
                    chart_name = png_file.stem
                    relative_path = f"{folder}/visuals/{png_file.name}"
                    desc = chart_descriptions.get(chart_name, f"**{chart_name.replace('_', ' ').title()}**")
                    chart_title = chart_name.replace("_", " ").title()
                    content.extend([f"#### {chart_title}", f"![{chart_title}]({relative_path})", f"*{desc}*", ""])
                content.append("")

        # Add data files table
        csv_dir = self.output_dir / folder
        if csv_dir.exists():
            csv_files = sorted(csv_dir.glob("*.csv"))
            if csv_files:
                content.extend(["### 📋 Data Files\n", "| Report | Description | Key Insights | Rows |",
                               "|--------|-------------|--------------|------|"])
                for csv_file in csv_files:
                    file_name = csv_file.stem
                    relative_path = f"{folder}/{csv_file.name}"
                    desc, insight = csv_descriptions.get(file_name, (file_name.replace("_", " ").title(), "Analysis results"))
                    row_count = str(len(self.results[file_name])) if file_name in self.results and not self.results[file_name].empty else "N/A"
                    content.append(f"| [{csv_file.name}]({relative_path}) | {desc} | {insight} | {row_count} |")
                content.append("")

        content.append("---\n")
        return content

    def _build_methodology_section(self) -> List[str]:
        """Build the methodology section of the README."""
        return [
            "## 📐 **Methodology**",
            "",
            "### Scoring Algorithms",
            "",
            "#### Hidden Gem Score (0-10)",
            "- **Exclusivity Factor** (30%): Fewer managers = higher score",
            "- **Conviction Factor** (25%): Higher portfolio % = higher score",
            "- **Recent Activity** (20%): Recent buys boost score",
            "- **Momentum Factor** (15%): Multiple recent transactions",
            "- **Manager Quality** (10%): Premium for top-tier managers",
            "",
            "#### Track Record Score",
            "- **Win Rate**: Percentage of successful investments",
            "- **Consistency**: Performance stability over time",
            "- **Crisis Alpha**: Outperformance during downturns",
            "- **Longevity**: Years of active management",
            "",
            "### Data Processing",
            "- **Temporal Accuracy**: 100% validated quarter extraction",
            "- **Price Data**: Real-time from Dataroma HTML",
            "- **Manager Mapping**: Clean names without timestamps",
            "- **Activity Types**: Buy, Sell, Add, Reduce, Hold",
            "",
            "---",
            "",
            "## 📅 **Update Schedule**",
            "",
            "This analysis is refreshed monthly to capture the latest investment trends and manager activities.",
            "",
            "## 🔗 **Data Source**",
            "",
            "All data is sourced from [Dataroma](https://www.dataroma.com), "
            "tracking portfolios of super investors.",
            "",
            "---",
            "",
            "*Analysis framework powered by modular Python architecture with 100% data accuracy validation*",
        ]

    def run_cross_file_validation(self) -> Dict[str, Any]:
        """
        Run cross-file validation to detect and document contradictions.

        This validates consistency across CSV output files and returns
        a summary documenting any apparent inconsistencies with explanations
        for why they are valid. Results are logged to console but not saved
        to CSV files.

        Returns:
            Dictionary with validation summary
        """
        logging.info("Running cross-file validation...")

        validator = CrossFileValidator(str(self.output_dir))
        issues = validator.validate_all()

        summary = validator.get_validation_summary()

        if summary['total_issues'] > 0:
            logging.info(f"Cross-file validation found {summary['total_issues']} documented items")
            for category, count in summary['categories'].items():
                if count > 0:
                    logging.info(f"  - {category}: {count} items")
        else:
            logging.info("Cross-file validation: No issues found")

        return summary

    def run_full_pipeline(self) -> bool:
        """
        Run the complete analysis pipeline: load data, analyze, save reports.

        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.load_data():
                return False

            results = self.run_complete_analysis()
            if not results:
                logging.error("No analysis results generated")
                return False

            self.save_all_reports(format_for_export=True)
            self.save_analysis_summary()
            self.generate_readme()

            # Run cross-file validation after all reports are saved
            self.run_cross_file_validation()

            logging.info("🎉 Full analysis pipeline completed successfully!")
            return True

        except Exception as e:
            logging.error(f"Analysis pipeline failed: {e}")
            import traceback

            traceback.print_exc()
            return False

    def get_analysis_by_name(self, name: str) -> Optional[pd.DataFrame]:
        """Get specific analysis result by name."""
        return self.results.get(name)

    def list_available_analyses(self) -> List[str]:
        """Get list of all available analysis names."""
        return list(self.results.keys())

    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics for all analyses."""
        if not self.results:
            return {}

        stats = {}
        for name, df in self.results.items():
            stats[name] = {
                "rows": len(df),
                "columns": len(df.columns),
                "has_data": not df.empty,
                "memory_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
            }

        return stats
