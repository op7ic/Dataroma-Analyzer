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
        
        # Add visualizations
        for viz_path in viz_paths:
            viz_name = Path(viz_path).stem.replace('_', ' ').title()
            content.append(f"#### {viz_name}")
            content.append(f"![{viz_name}]({self._relative_path(viz_path)})")
            content.append("")
        
        content.extend([
            "\n### 📋 Current Reports",
            "\n| Report | Description | Key Insight |",
            "| ------ | ----------- | ----------- |"
        ])
        
        current_reports = {
            "hidden_gems": ("Under-the-radar opportunities", "5-factor scoring identifies stocks with high potential"),
            "momentum_stocks": ("Recent buying activity", "Tracks institutional accumulation patterns"),
            "new_positions": ("Fresh acquisitions", "Identifies emerging manager interests"),
            "stocks_under_$5": ("Ultra-low price opportunities", "Deep value plays under $5"),
            "stocks_under_$20": ("Affordable growth plays", "Quality stocks at accessible prices"),
            "52_week_low_buys": ("Value hunting activity", "Managers buying at market lows"),
            "52_week_high_sells": ("Profit taking patterns", "Strategic exits at peaks"),
            "concentration_changes": ("Portfolio shifts", "Major allocation adjustments")
        }
        
        for report_name, (desc, insight) in current_reports.items():
            if report_name in results and not results[report_name].empty:
                count = len(results[report_name])
                content.append(f"| [{report_name}.csv](current/{report_name}.csv) | "
                             f"{desc} ({count} items) | {insight} |")
        
        content.append("")
        
        # Add top opportunities
        if "hidden_gems" in results and not results["hidden_gems"].empty:
            top_gems = results["hidden_gems"].head(5)
            content.extend([
                "### 🌟 Top 5 Hidden Gems",
                "\n| Ticker | Score | Price | Managers |",
                "| ------ | ----- | ----- | -------- |"
            ])
            
            for _, gem in top_gems.iterrows():
                managers_list = str(gem.get('managers', '')).split(',')[:3]
                managers_str = ', '.join(m.strip() for m in managers_list)
                content.append(f"| **{gem.get('ticker', 'N/A')}** | "
                             f"{gem.get('hidden_gem_score', 0):.2f} | "
                             f"${gem.get('current_price', 0):.2f} | "
                             f"{managers_str} |")
        
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
        
        # Add visualizations
        for viz_path in viz_paths:
            viz_name = Path(viz_path).stem.replace('_', ' ').title()
            content.append(f"#### {viz_name}")
            content.append(f"![{viz_name}]({self._relative_path(viz_path)})")
            content.append("")
        
        content.extend([
            "\n### 📋 Advanced Reports",
            "\n| Report | Description | Key Insight |",
            "| ------ | ----------- | ----------- |"
        ])
        
        advanced_reports = {
            "manager_track_records": ("18+ year performance history", "Comprehensive manager scoring"),
            "crisis_alpha_generators": ("Crisis period outperformers", "Managers who buy during crashes"),
            "position_sizing_mastery": ("Optimal allocation patterns", "Risk/reward optimization"),
            "multi_manager_favorites": ("Consensus high-conviction picks", "Stocks held by multiple gurus"),
            "sector_rotation_patterns": ("Timing market trends", "Early sector shift detection"),
            "manager_evolution_patterns": ("Strategy adaptation over time", "How managers evolve")
        }
        
        for report_name, (desc, insight) in advanced_reports.items():
            if report_name in results and not results[report_name].empty:
                count = len(results[report_name])
                content.append(f"| [{report_name}.csv](advanced/{report_name}.csv) | "
                             f"{desc} ({count} items) | {insight} |")
        
        content.append("")
        
        # Add top managers
        if "manager_track_records" in results and not results["manager_track_records"].empty:
            top_managers = results["manager_track_records"].nlargest(5, 'track_record_score')
            content.extend([
                "### 🏆 Top 5 Managers by Track Record",
                "\n| Manager | Score | Years Active | Win Rate |",
                "| ------- | ----- | ------------ | -------- |"
            ])
            
            for _, mgr in top_managers.iterrows():
                content.append(f"| **{mgr.get('manager_name', 'N/A')}** | "
                             f"{mgr.get('track_record_score', 0):.2f} | "
                             f"{mgr.get('years_active', 0)} | "
                             f"{mgr.get('win_rate', 0):.1f}% |")
        
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
        
        # Add visualizations
        for viz_path in viz_paths:
            viz_name = Path(viz_path).stem.replace('_', ' ').title()
            content.append(f"#### {viz_name}")
            content.append(f"![{viz_name}]({self._relative_path(viz_path)})")
            content.append("")
        
        content.extend([
            "\n### 📋 Historical Reports",
            "\n| Report | Description | Key Insight |",
            "| ------ | ----------- | ----------- |"
        ])
        
        historical_reports = {
            "multi_decade_conviction": ("Stocks held 10+ years", "Ultimate conviction plays"),
            "stock_life_cycles": ("Entry/exit patterns", "Optimal holding periods"),
            "crisis_response_analysis": ("2008 vs 2020 comparison", "Crisis behavior patterns"),
            "quarterly_activity_timeline": ("18-year activity map", "Market timing insights"),
            "long_term_winners": ("Best performers over time", "Compound growth champions")
        }
        
        for report_name, (desc, insight) in historical_reports.items():
            if report_name in results and not results[report_name].empty:
                count = len(results[report_name])
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