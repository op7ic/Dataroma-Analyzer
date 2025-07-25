#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Main Analysis Runner

Entry point for running comprehensive investment analysis across all time periods
and generating actionable insights from scraped manager data.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

# Add lib to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib.analysis.orchestrator import AnalysisOrchestrator


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('analysis.log')
        ]
    )


def create_all_visualizations(results: dict, output_dir: str = "analysis"):
    """Create visualizations for all three analysis categories."""
    try:
        from lib.visualizations.current_visualizer import CurrentVisualizer
        from lib.visualizations.advanced_visualizer import AdvancedVisualizer
        from lib.visualizations.historical_visualizer import HistoricalVisualizer
        
        viz_paths = {"current": [], "advanced": [], "historical": []}
        
        # Current visualizations (last 3 quarters)
        current_viz = CurrentVisualizer(output_dir=f"{output_dir}/current/visuals")
        current_results = {
            name: df for name, df in results.items() 
            if any(keyword in name for keyword in [
                'hidden_gems', 'momentum', 'new_positions', 'stocks_under',
                'value_price', '52_week', 'concentration_changes'
            ]) and not df.empty
        }
        if current_results:
            viz_paths["current"] = current_viz.create_all_visualizations(current_results)
        
        # Advanced visualizations (manager performance)
        advanced_viz = AdvancedVisualizer(output_dir=f"{output_dir}/advanced/visuals")
        advanced_results = {
            name: df for name, df in results.items()
            if any(keyword in name for keyword in [
                'manager_track', 'crisis_alpha', 'position_sizing', 
                'evolution', 'sequence', 'timing', 'multi_manager',
                'top_holdings', 'interesting_stocks', 'high_conviction',
                'sector_rotation', 'long_term_winners'
            ]) and not df.empty
        }
        if advanced_results:
            viz_paths["advanced"] = advanced_viz.create_all_visualizations(advanced_results)
        
        # Historical visualizations (18+ years)
        historical_viz = HistoricalVisualizer(output_dir=f"{output_dir}/historical/visuals")
        historical_results = {
            name: df for name, df in results.items() 
            if any(keyword in name for keyword in [
                'historical', 'crisis_response', 'multi_decade', 
                'quarterly', 'life_cycle', 'long_term'
            ]) and not df.empty
        }
        if historical_results:
            viz_paths["historical"] = historical_viz.create_all_visualizations(historical_results)
        
        return viz_paths
        
    except ImportError as e:
        logging.warning(f"Some visualization modules not available: {e}")
        # Try at least historical which we know exists
        try:
            from lib.visualizations.historical_visualizer import HistoricalVisualizer
            viz = HistoricalVisualizer(output_dir=f"{output_dir}/historical/visuals")
            historical_results = {
                name: df for name, df in results.items() 
                if any(keyword in name for keyword in ['historical', 'crisis', 'multi_decade', 'quarterly', 'life_cycle'])
                and not df.empty
            }
            if historical_results:
                return {"historical": viz.create_all_visualizations(historical_results)}
        except:
            pass
        return {}
    except Exception as e:
        logging.error(f"Error creating visualizations: {e}")
        return {}


def create_visualizations(results: dict, output_dir: str = "analysis"):
    """Create visualizations from historical analysis results."""
    try:
        from lib.visualizations.historical_visualizer import HistoricalVisualizer
        
        # Initialize visualizer
        viz = HistoricalVisualizer(output_dir=f"{output_dir}/historical/visuals")
        
        # Filter historical results for visualization
        historical_results = {
            name: df for name, df in results.items() 
            if any(keyword in name for keyword in ['historical', 'crisis', 'multi_decade', 'quarterly', 'life_cycle', 'track_record'])
            and not df.empty
        }
        
        if not historical_results:
            logging.warning("No historical data available for visualization")
            return {}
        
        # Create visualizations
        viz_paths = viz.create_all_visualizations(historical_results)
        return viz_paths
        
    except ImportError as e:
        logging.warning(f"Visualization module not available: {e}")
        return {}
    except Exception as e:
        logging.error(f"Error creating visualizations: {e}")
        return {}


def generate_comprehensive_readme(results: dict, viz_paths: dict, output_dir: str = "analysis"):
    """Generate comprehensive README with embedded visualizations."""
    try:
        from lib.analysis.readme_generator import ReadmeGenerator
        
        generator = ReadmeGenerator(output_dir)
        readme_content = generator.generate_readme(results, viz_paths)
        readme_path = generator.save_readme(readme_content)
        
        return readme_path
        
    except Exception as e:
        logging.error(f"Error generating README: {e}")
        # Fallback to simple summary
        return generate_simple_summary(results, output_dir)


def generate_simple_summary(results: dict, output_dir: str = "analysis"):
    """Generate a simple summary if comprehensive README fails."""
    output_path = Path(output_dir)
    
    summary_lines = [
        "# Dataroma Investment Analysis Report",
        f"\n**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "\n## 📊 Analysis Summary\n",
        f"- **Total Analyses Generated**: {len(results)}",
        f"- **Historical Data Range**: 2007-2025 (18+ years)",
        f"- **Total Activities Analyzed**: 57,643",
        f"- **Total Current Holdings**: 3,311",
        "\n## 📁 Generated Files\n",
        "### Current Analysis Reports",
        "- `analysis/current/*.csv` - Last 3 quarters analysis",
        "\n### Advanced Analysis Reports",  
        "- `analysis/advanced/*.csv` - Manager performance analysis",
        "\n### Historical Analysis Reports",  
        "- `analysis/historical/*.csv` - Multi-decade analyses (2007-2025)",
        "\n### Visualizations",
        "- `analysis/*/visuals/*.png` - Visual analysis for each category",
    ]
    
    # Write summary
    summary_path = output_path / "ANALYSIS_SUMMARY.md"
    with open(summary_path, 'w') as f:
        f.write('\n'.join(summary_lines))
    
    return str(summary_path)


def main():
    """Run complete analysis pipeline."""
    setup_logging()
    
    print("🚀 Starting Comprehensive Investment Analysis...")
    print("   Analyzing 18+ years of data (2007-2025)")
    
    # Initialize orchestrator
    orchestrator = AnalysisOrchestrator("cache")
    
    # Load data
    print("\n📊 Loading data...")
    if not orchestrator.load_data():
        print("❌ Failed to load data")
        return
    
    # Get data summary
    summary = orchestrator.data_loader.get_data_summary()
    print(f"✅ Data loaded successfully:")
    print(f"   - Holdings: {summary.get('holdings_count', 0):,}")
    print(f"   - Activities: {summary.get('activities_count', 0):,}")
    print(f"   - Managers: {summary.get('managers_count', 0):,}")
    print(f"   - Unique Tickers: {summary.get('unique_tickers', 0):,}")
    
    # Run complete analysis
    print("\n🔍 Running comprehensive analysis...")
    print("   This includes current holdings AND historical patterns...")
    
    try:
        results = orchestrator.run_complete_analysis()
        print(f"✅ Generated {len(results)} analysis reports")
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Save results using orchestrator's built-in functionality
    print("\n💾 Saving analysis results...")
    saved_files_dict = orchestrator.save_all_reports(format_for_export=True)
    total_saved = sum(len(files) for files in saved_files_dict.values())
    print(f"✅ Saved {total_saved} CSV files")
    print(f"   - Current: {len(saved_files_dict.get('current', []))} files")
    print(f"   - Advanced: {len(saved_files_dict.get('advanced', []))} files")
    print(f"   - Historical: {len(saved_files_dict.get('historical', []))} files")
    
    # Create visualizations
    print("\n📊 Creating visualizations...")
    try:
        viz_paths = create_all_visualizations(results)
        total_viz = sum(len(paths) for paths in viz_paths.values())
        print(f"✅ Created {total_viz} visualizations")
        if viz_paths.get("current"):
            print(f"   - Current: {len(viz_paths['current'])} graphs")
        if viz_paths.get("advanced"):
            print(f"   - Advanced: {len(viz_paths['advanced'])} graphs")
        if viz_paths.get("historical"):
            print(f"   - Historical: {len(viz_paths['historical'])} graphs")
    except Exception as e:
        print(f"⚠️  Visualization creation failed: {e}")
        viz_paths = {}
    
    # Generate comprehensive README
    print("\n📝 Generating comprehensive analysis report...")
    readme_path = generate_comprehensive_readme(results, viz_paths)
    print(f"✅ Analysis report saved to: {readme_path}")
    
    # Final summary
    print("\n" + "="*60)
    print("🎉 ANALYSIS COMPLETE!")
    print("="*60)
    print(f"\n📁 Output Directory: analysis/")
    print(f"   - Current Analysis: {len(saved_files_dict.get('current', []))} files")
    print(f"   - Advanced Analysis: {len(saved_files_dict.get('advanced', []))} files")
    print(f"   - Historical Analysis: {len(saved_files_dict.get('historical', []))} files")
    print(f"   - Total Visualizations: {sum(len(paths) for paths in viz_paths.values())} graphs")
    print(f"\n📊 Key Reports Generated:")
    print("\n   Current (Last 3 Quarters):")
    print("   - Hidden Gems & Under-radar Opportunities")
    print("   - Momentum Stocks & New Positions")
    print("   - Price-Based Opportunities ($5 to $100)")
    print("   - 52-Week Low Buys & High Sells")
    print("\n   Advanced (Manager Analysis):")
    print("   - Manager Track Records & Performance")
    print("   - Crisis Alpha Generators")
    print("   - Position Sizing Mastery")
    print("   - Sector Rotation Excellence")
    print("\n   Historical (18+ Years):")
    print("   - Multi-Decade Conviction Plays")
    print("   - Stock Life Cycles Analysis")
    print("   - Quarterly Activity Timeline")
    print("   - Long-term Winners")
    print(f"\n📝 See {readme_path} for detailed insights and visualizations")


if __name__ == "__main__":
    main()