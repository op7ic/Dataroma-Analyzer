"""
Visualization module for advanced manager analysis.
Creates graphs for manager performance, patterns, and strategic insights.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional
import logging
from matplotlib.ticker import MaxNLocator, FuncFormatter
from .manager_performance_overview import ManagerPerformanceOverview

logger = logging.getLogger(__name__)


class AdvancedVisualizer:
    """Creates visualizations for advanced manager analysis."""
    
    def __init__(self, output_dir: str = "analysis/advanced/visuals"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("deep")
        
        plt.rcParams['figure.dpi'] = 150
        plt.rcParams['savefig.dpi'] = 300
    
    def _extract_analysis_period(self, results: Dict[str, pd.DataFrame]) -> str:
        """Extract the analysis period from manager track records or other data."""
        if "manager_track_records" in results and not results["manager_track_records"].empty:
            df = results["manager_track_records"]
            if 'first_year' in df.columns and 'last_year' in df.columns:
                first_year = int(df['first_year'].min())
                last_year = int(df['last_year'].max())
                total_years = last_year - first_year + 1
                return f"{first_year}-{last_year} ({total_years} years)"
        
        for key, df in results.items():
            if df.empty:
                continue
            if 'years_active' in df.columns:
                avg_years = df['years_active'].mean()
                return f"Average {avg_years:.1f} year careers"
        
        return "Long-term Analysis"
    
    def _get_manager_name(self, row: pd.Series) -> str:
        """Get the full manager name, preferring the descriptive name over ID."""
        name_cols = ['manager_name', 'manager.1', 'manager_full_name']
        id_cols = ['manager', 'manager_id']
        
        for col in name_cols:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
                return str(row[col]).strip()
        
        for col in id_cols:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
                return str(row[col]).strip()
        
        return "Unknown Manager"
    
    def create_all_visualizations(self, results: Dict[str, pd.DataFrame]) -> List[str]:
        """Create all advanced analysis visualizations."""
        viz_paths = []
        
        analysis_period = self._extract_analysis_period(results)
        
        try:
            if "manager_track_records" in results and not results["manager_track_records"].empty:
                path = self.create_manager_performance_chart(results["manager_track_records"])
                if path:
                    viz_paths.append(path)
            
            if "crisis_alpha_generators" in results and not results["crisis_alpha_generators"].empty:
                path = self.create_crisis_alpha_chart(results["crisis_alpha_generators"])
                if path:
                    viz_paths.append(path)
            
            if "position_sizing_mastery" in results and not results["position_sizing_mastery"].empty:
                path = self.create_position_sizing_chart(results["position_sizing_mastery"])
                if path:
                    viz_paths.append(path)
            
            if "manager_evolution_patterns" in results and not results["manager_evolution_patterns"].empty:
                path = self.create_evolution_chart(results["manager_evolution_patterns"])
                if path:
                    viz_paths.append(path)
            
            if "multi_manager_favorites" in results and not results["multi_manager_favorites"].empty:
                path = self.create_consensus_picks_chart(results["multi_manager_favorites"])
                if path:
                    viz_paths.append(path)
            
            if "top_holdings" in results and not results["top_holdings"].empty:
                path = self.create_top_holdings_chart(results["top_holdings"])
                if path:
                    viz_paths.append(path)
            
            # Create comprehensive performance overview charts
            if "manager_track_records" in results and not results["manager_track_records"].empty:
                try:
                    perf_overview = ManagerPerformanceOverview(output_dir=str(self.output_dir))
                    perf_paths = perf_overview.create_all_performance_analyses(results["manager_track_records"])
                    viz_paths.extend(perf_paths)
                except Exception as e:
                    logger.error(f"Error creating performance overview charts: {e}")
            
        except Exception as e:
            logger.error(f"Error creating advanced visualizations: {e}")
        
        return viz_paths
    
    def create_manager_performance_chart(self, df: pd.DataFrame) -> str:
        """Create comprehensive manager performance visualization."""
        try:
            fig = plt.figure(figsize=(16, 12))
            
            gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
            ax1 = fig.add_subplot(gs[0, :2])
            ax2 = fig.add_subplot(gs[0, 2])
            ax3 = fig.add_subplot(gs[1, :2])
            ax4 = fig.add_subplot(gs[1, 2])
            ax5 = fig.add_subplot(gs[2, :])
            
            top_managers = df.nlargest(15, 'track_record_score')
            
            manager_col = 'manager_name' if 'manager_name' in top_managers.columns else 'manager'
            bars = ax1.barh(top_managers[manager_col], top_managers['track_record_score'], 
                           color='darkblue', alpha=0.7)
            ax1.set_xlabel('Track Record Score', fontweight='bold')
            ax1.set_title('Top 15 Managers by Overall Performance', fontsize=12, fontweight='bold')
            ax1.invert_yaxis()
            ax1.grid(True, alpha=0.3)
            
            max_score = top_managers['track_record_score'].max()
            ax1.set_xlim(0, max_score * 1.1)  # Add padding for labels
            for i, (_, row) in enumerate(top_managers.iterrows()):
                score = row['track_record_score']
                ax1.text(score + max_score * 0.02, i, f'{score:.1f}',
                        va='center', ha='left', fontsize=9, fontweight='bold')
            
            years_bins = pd.cut(df['years_active'], bins=[0, 5, 10, 15, 20], 
                              labels=['<5 years', '5-10 years', '10-15 years', '15+ years'])
            years_counts = years_bins.value_counts()
            ax2.pie(years_counts.values, labels=years_counts.index, autopct='%1.1f%%')
            ax2.set_title('Manager Experience Distribution')
            
            ax3.scatter(top_managers['total_actions'], top_managers['consistency_score'], 
                       s=100, alpha=0.6, c=top_managers['years_active'], cmap='viridis')
            for idx, row in top_managers.iterrows():
                manager_name = row[manager_col][:10] if len(str(row[manager_col])) > 10 else str(row[manager_col])
                ax3.annotate(manager_name, 
                           (row['total_actions'], row['consistency_score']),
                           fontsize=8, alpha=0.7)
            ax3.set_xlabel('Total Actions')
            ax3.set_ylabel('Consistency Score')
            ax3.set_title('Activity vs Consistency')
            
            if 'current_portfolio_value' in df.columns:
                top_by_value = df.nlargest(10, 'current_portfolio_value')
                values_billions = top_by_value['current_portfolio_value'] / 1e9
                bars = ax4.bar(range(len(top_by_value)), values_billions, color='green', alpha=0.7)
                ax4.set_xticks(range(len(top_by_value)))
                ax4.set_xticklabels(top_by_value[manager_col].str[:10], rotation=45, ha='right')
                ax4.set_ylabel('Portfolio Value ($B)', fontsize=11, fontweight='bold')
                ax4.set_title('Largest Portfolios', fontsize=12, fontweight='bold')
                ax4.grid(True, alpha=0.3)
                
                max_value = values_billions.max()
                ax4.set_ylim(0, max_value * 1.1)
                
                for i, value in enumerate(values_billions):
                    ax4.text(i, value + max_value * 0.02, f'${value:.0f}B',
                            ha='center', va='bottom', fontsize=9, fontweight='bold')
            
            if all(col in df.columns for col in ['first_year', 'last_year']):
                active_managers = df.dropna(subset=['first_year', 'last_year'])
                
                years_range = range(int(active_managers['first_year'].min()), 
                                  int(active_managers['last_year'].max()) + 1)
                active_count = []
                
                for year in years_range:
                    count = len(active_managers[
                        (active_managers['first_year'] <= year) & 
                        (active_managers['last_year'] >= year)
                    ])
                    active_count.append(count)
                
                ax5.plot(years_range, active_count, marker='o', linewidth=2, markersize=6)
                ax5.set_xlabel('Year')
                ax5.set_ylabel('Number of Active Managers')
                ax5.set_title('Manager Activity Timeline')
                ax5.grid(True, alpha=0.3)
                
                ax5.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=8))
                ax5.xaxis.set_major_formatter(FuncFormatter(lambda x, p: f'{int(x)}'))
            
            analysis_period = self._extract_analysis_period({'manager_track_records': df})
            plt.suptitle(f'Manager Performance Analysis ({analysis_period})', fontsize=18, fontweight='bold')
            
            output_path = self.output_dir / "manager_performance_advanced.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating manager performance chart: {e}")
            return None
    
    def create_crisis_alpha_chart(self, df: pd.DataFrame) -> str:
        """Create crisis alpha generators visualization."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()
            
            if 'crisis_alpha_score' in df.columns:
                top_crisis = df.nlargest(15, 'crisis_alpha_score')
                manager_name_col = 'manager_name' if 'manager_name' in top_crisis.columns else 'manager'
                bars = axes[0].barh(top_crisis[manager_name_col], top_crisis['crisis_alpha_score'],
                                  color='darkred', alpha=0.7)
                axes[0].set_xlabel('Crisis Alpha Score', fontweight='bold')
                axes[0].set_title('Top Crisis Alpha Generators', fontsize=12, fontweight='bold')
                axes[0].invert_yaxis()
                axes[0].grid(True, alpha=0.3)
                
                max_score = top_crisis['crisis_alpha_score'].max()
                axes[0].set_xlim(0, max_score * 1.1)  # Add padding for labels
                for i, (_, row) in enumerate(top_crisis.iterrows()):
                    score = row['crisis_alpha_score']
                    axes[0].text(score + max_score * 0.02, i, f'{score:.1f}',
                               va='center', ha='left', fontsize=9, fontweight='bold')
            
            if all(col in df.columns for col in ['total_crisis_activities', 'crisis_alpha_score']):
                axes[1].scatter(df['total_crisis_activities'], df['crisis_alpha_score'], 
                              s=100, alpha=0.6, color='red', edgecolors='black', linewidth=0.5)
                axes[1].set_xlabel('Total Crisis Activities', fontweight='bold')
                axes[1].set_ylabel('Crisis Alpha Score', fontweight='bold')
                axes[1].set_title('Activity vs Performance in Crises', fontsize=12, fontweight='bold')
                axes[1].grid(True, alpha=0.3)
                
                top_performers = df.nlargest(5, 'crisis_alpha_score')
                for i, (idx, row) in enumerate(top_performers.iterrows()):
                    manager_name_col = 'manager_name' if 'manager_name' in row else 'manager'
                    # Alternate offset positions to reduce overlap
                    offset = (5, 5) if i % 2 == 0 else (-5, -5)
                    ha = 'left' if i % 2 == 0 else 'right'
                    
                    axes[1].annotate(row[manager_name_col][:15],
                                   (row['total_crisis_activities'], row['crisis_alpha_score']),
                                   xytext=offset, textcoords='offset points',
                                   fontsize=8, fontweight='bold', ha=ha,
                                   bbox=dict(boxstyle='round,pad=0.2', facecolor='yellow', alpha=0.7))
                axes[1].invert_yaxis()
            
            if all(col in df.columns for col in ['buy_during_crisis', 'total_crisis_activities']):
                df['buy_ratio'] = df['buy_during_crisis'] / df['total_crisis_activities']
                axes[2].hist(df['buy_ratio'], bins=20, color='green', alpha=0.7)
                axes[2].set_xlabel('Crisis Buy Ratio')
                axes[2].set_ylabel('Number of Managers')
                axes[2].set_title('Buying Behavior During Crises')
                axes[2].axvline(x=df['buy_ratio'].mean(), color='red', 
                              linestyle='--', label='Average')
                axes[2].legend()
            
            if 'crisis_periods_active' in df.columns:
                period_counts = df['crisis_periods_active'].value_counts().sort_index()
                axes[3].bar(period_counts.index, period_counts.values, 
                          color=['red', 'orange', 'green'])
                axes[3].set_xlabel('Number of Crisis Periods Active')
                axes[3].set_ylabel('Number of Managers')
                axes[3].set_title('Crisis Participation Frequency')
                axes[3].set_xticks(period_counts.index)
            
            analysis_period = self._extract_analysis_period({'crisis_alpha_generators': df})
            plt.suptitle(f'Crisis Alpha Generation Analysis ({analysis_period})', fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            output_path = self.output_dir / "crisis_alpha_advanced.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating crisis alpha chart: {e}")
            return None
    
    def create_position_sizing_chart(self, df: pd.DataFrame) -> str:
        """Create position sizing mastery visualization."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()
            
            if all(col in df.columns for col in ['sizing_efficiency_score', 'avg_position_size']):
                scatter = axes[0].scatter(df['avg_position_size'], df['sizing_efficiency_score'],
                              s=100, alpha=0.6, c=df['position_concentration'], cmap='plasma')
                axes[0].set_xlabel('Average Position Size (%)')
                axes[0].set_ylabel('Sizing Efficiency Score')
                axes[0].set_title('Position Sizing Efficiency')
                cbar = axes[0].figure.colorbar(scatter, ax=axes[0])
                cbar.set_label('Position Concentration')
            
            if 'position_concentration' in df.columns:
                top_concentrated = df.nlargest(12, 'position_concentration')
                bars = axes[1].bar(range(len(top_concentrated)), 
                                 top_concentrated['position_concentration'],
                                 color='purple', alpha=0.7)
                axes[1].set_xticks(range(len(top_concentrated)))
                manager_name_col = 'manager_name' if 'manager_name' in top_concentrated.columns else 'manager'
                axes[1].set_xticklabels(top_concentrated[manager_name_col].str[:10], 
                                      rotation=45, ha='right')
                axes[1].set_ylabel('Position Concentration (%)', fontweight='bold')
                axes[1].set_title('Most Concentrated Portfolios', fontsize=12, fontweight='bold')
                axes[1].grid(True, alpha=0.3, axis='y')
            
            if all(col in df.columns for col in ['small_positions_pct', 'medium_positions_pct', 'large_positions_pct']):
                manager_name_col = 'manager_name' if 'manager_name' in df.columns else 'manager'
                top_10_managers = df.nlargest(10, 'sizing_efficiency_score')
                
                x = range(len(top_10_managers))
                axes[2].bar(x, top_10_managers['small_positions_pct'], 
                          label='Small Positions', color='lightcoral')
                axes[2].bar(x, top_10_managers['medium_positions_pct'], 
                          bottom=top_10_managers['small_positions_pct'],
                          label='Medium Positions', color='gold')
                axes[2].bar(x, top_10_managers['large_positions_pct'], 
                          bottom=top_10_managers['small_positions_pct'] + top_10_managers['medium_positions_pct'],
                          label='Large Positions', color='green')
                
                axes[2].set_xticks(x)
                axes[2].set_xticklabels(top_10_managers[manager_name_col].str[:10], rotation=45, ha='right')
                axes[2].set_ylabel('Position Percentage (%)', fontweight='bold')
                axes[2].set_title('Position Size Distribution (Top 10 Managers)', fontsize=12, fontweight='bold')
                axes[2].grid(True, alpha=0.3, axis='y')
                
                axes[2].legend(['Small (<2%)', 'Medium (2-5%)', 'Large (>5%)'], 
                             bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
            
            if 'sizing_style' in df.columns:
                style_counts = df['sizing_style'].value_counts()
                axes[3].pie(style_counts.values, labels=style_counts.index,
                          autopct='%1.1f%%', startangle=90)
                axes[3].set_title('Distribution of Sizing Styles')
            
            plt.suptitle('Position Sizing Mastery Analysis', fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            output_path = self.output_dir / "position_sizing_advanced.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating position sizing chart: {e}")
            return None
    
    def create_evolution_chart(self, df: pd.DataFrame) -> str:
        """Create manager evolution patterns visualization."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()
            
            if 'evolution_type' in df.columns:
                evolution_counts = df['evolution_type'].value_counts()
                axes[0].pie(evolution_counts.values, labels=evolution_counts.index,
                          autopct='%1.1f%%', startangle=90)
                axes[0].set_title('Manager Evolution Types')
            
            if 'evolution_score' in df.columns:
                top_evolving = df.nlargest(15, 'evolution_score')
                manager_name_col = 'manager_name' if 'manager_name' in top_evolving.columns else 'manager'
                bars = axes[1].barh(top_evolving[manager_name_col], 
                                  top_evolving['evolution_score'],
                                  color='teal', alpha=0.7)
                axes[1].set_xlabel('Evolution Score', fontweight='bold')
                axes[1].set_title('Highest Evolution Scores', fontsize=12, fontweight='bold')
                axes[1].invert_yaxis()
                axes[1].grid(True, alpha=0.3)
                
                max_score = top_evolving['evolution_score'].max()
                axes[1].set_xlim(0, max_score * 1.1)  # Add padding for labels
                for i, (_, row) in enumerate(top_evolving.iterrows()):
                    score = row['evolution_score']
                    axes[1].text(score + max_score * 0.02, i, f'{score:.1f}',
                               va='center', ha='left', fontsize=9, fontweight='bold')
            
            if all(col in df.columns for col in ['career_length_years', 'style_change_score']):
                axes[2].scatter(df['career_length_years'], df['style_change_score'],
                              s=80, alpha=0.6, color='coral')
                axes[2].set_xlabel('Career Length (Years)')
                axes[2].set_ylabel('Style Change Score')
                axes[2].set_title('Experience vs Style Evolution')
                
            
            if all(col in df.columns for col in ['early_buy_ratio', 'late_buy_ratio']):
                axes[3].scatter(df['early_buy_ratio'], df['late_buy_ratio'], 
                              s=80, alpha=0.6, color='purple', edgecolors='black', linewidth=0.5)
                
                axes[3].plot([0, 100], [0, 100], 'r--', alpha=0.7, linewidth=2.5, 
                           label='No Change', zorder=1)
                
                axes[3].set_xlabel('Early Career Buy Ratio (%)', fontweight='bold')
                axes[3].set_ylabel('Late Career Buy Ratio (%)', fontweight='bold')
                axes[3].set_title('Buy Behavior Evolution', fontsize=12, fontweight='bold')
                axes[3].grid(True, alpha=0.3)
                axes[3].legend(loc='upper left', fontsize=10, frameon=True, fancybox=True)
                
                df['buy_change'] = abs(df['late_buy_ratio'] - df['early_buy_ratio'])
                top_changers = df.nlargest(3, 'buy_change')
                for _, row in top_changers.iterrows():
                    manager_name_col = 'manager_name' if 'manager_name' in row else 'manager'
                    axes[3].annotate(row[manager_name_col][:10],
                                   (row['early_buy_ratio'], row['late_buy_ratio']),
                                   fontsize=8)
            
            plt.suptitle('Manager Evolution Patterns', fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            output_path = self.output_dir / "manager_evolution_advanced.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating evolution chart: {e}")
            return None
    
    def create_consensus_picks_chart(self, df: pd.DataFrame) -> str:
        """Create multi-manager consensus picks visualization."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()
            
            top_consensus = df.head(20)
            
            bars = axes[0].barh(top_consensus['ticker'], top_consensus['manager_count'],
                              color='darkblue', alpha=0.7)
            axes[0].set_xlabel('Number of Managers', fontweight='bold')
            axes[0].set_title('Top 20 Consensus Picks', fontsize=12, fontweight='bold')
            axes[0].invert_yaxis()
            axes[0].grid(True, alpha=0.3)
            
            max_count = top_consensus['manager_count'].max()
            axes[0].set_xlim(0, max_count * 1.1)  # Add padding for labels
            for i, (ticker, count) in enumerate(zip(top_consensus['ticker'], top_consensus['manager_count'])):
                axes[0].text(count + max_count * 0.02, i, f'{count}',
                           va='center', ha='left', fontsize=9, fontweight='bold')
            
            if 'avg_portfolio_pct' in df.columns:
                axes[1].scatter(top_consensus['manager_count'], 
                              top_consensus['avg_portfolio_pct'],
                              s=100, alpha=0.6, color='green')
                for idx, row in top_consensus.iterrows():
                    axes[1].annotate(row['ticker'], 
                                   (row['manager_count'], row['avg_portfolio_pct']),
                                   fontsize=8, alpha=0.7)
                axes[1].set_xlabel('Number of Managers')
                axes[1].set_ylabel('Average Portfolio %')
                axes[1].set_title('Consensus vs Concentration')
            
            axes[2].hist(df['manager_count'], bins=20, color='purple', alpha=0.7, edgecolor='black', linewidth=0.5)
            axes[2].set_xlabel('Number of Managers Holding', fontweight='bold')
            axes[2].set_ylabel('Number of Stocks', fontweight='bold')
            axes[2].set_title('Distribution of Manager Consensus', fontsize=12, fontweight='bold')
            axes[2].grid(True, alpha=0.3)
            
            mean_value = df['manager_count'].mean()
            axes[2].axvline(x=mean_value, color='red', linestyle='--', linewidth=2.5, 
                          label=f'Mean: {mean_value:.1f}', alpha=0.8)
            axes[2].legend(loc='upper right', fontsize=10, frameon=True, fancybox=True)
            
            if 'managers' in df.columns:
                manager_appearances = {}
                for managers_str in top_consensus['managers']:
                    if pd.notna(managers_str):
                        for mgr in str(managers_str).split(','):
                            mgr = mgr.strip()
                            manager_appearances[mgr] = manager_appearances.get(mgr, 0) + 1
                
                top_consensus_mgrs = sorted(manager_appearances.items(), 
                                          key=lambda x: x[1], reverse=True)[:10]
                axes[3].bar([m[0][:15] for m in top_consensus_mgrs],
                          [m[1] for m in top_consensus_mgrs],
                          color='darkgreen')
                axes[3].set_xlabel('Manager')
                axes[3].set_ylabel('Consensus Picks Count')
                axes[3].set_title('Managers Most Aligned with Consensus')
                axes[3].tick_params(axis='x', rotation=45)
            
            plt.suptitle('Multi-Manager Consensus Analysis', fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            output_path = self.output_dir / "consensus_picks_advanced.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating consensus picks chart: {e}")
            return None
    
    def create_top_holdings_chart(self, df: pd.DataFrame) -> str:
        """Create top holdings analysis visualization."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()
            
            top_by_value = df.nlargest(15, 'total_value')
            
            values_billions = top_by_value['total_value'] / 1e9
            bars = axes[0].barh(top_by_value['ticker'], values_billions,
                              color='gold', alpha=0.7)
            axes[0].set_xlabel('Total Value ($B)', fontweight='bold')
            axes[0].set_title('Top 15 Holdings by Total Value', fontsize=12, fontweight='bold')
            axes[0].invert_yaxis()
            axes[0].grid(True, alpha=0.3)
            
            max_value = values_billions.max()
            axes[0].set_xlim(0, max_value * 1.1)  # Add padding for labels
            for i, (ticker, value) in enumerate(zip(top_by_value['ticker'], values_billions)):
                axes[0].text(value + max_value * 0.02, i, f'${value:.1f}B',
                           va='center', ha='left', fontsize=9, fontweight='bold')
            
            if 'manager_count' in df.columns:
                axes[1].scatter(df['total_value'] / 1e9, df['manager_count'],
                              s=60, alpha=0.5, color='navy', edgecolors='black', linewidth=0.5)
                axes[1].set_xlabel('Total Value ($B) - Log Scale', fontweight='bold')
                axes[1].set_ylabel('Number of Managers', fontweight='bold')
                axes[1].set_title('Value vs Manager Interest', fontsize=12, fontweight='bold')
                axes[1].set_xscale('log')
                axes[1].grid(True, alpha=0.3)
                
                from matplotlib.ticker import FuncFormatter
                axes[1].xaxis.set_major_formatter(FuncFormatter(lambda x, p: f'${int(x)}B' if x >= 1 else f'${x:.1f}B'))
            
            if 'max_portfolio_pct' in df.columns and 'avg_portfolio_pct' in df.columns:
                axes[2].scatter(df['avg_portfolio_pct'], df['max_portfolio_pct'],
                              s=60, alpha=0.6, color='red')
                axes[2].set_xlabel('Average Portfolio Percentage (%)')
                axes[2].set_ylabel('Maximum Portfolio Percentage (%)')
                axes[2].set_title('Position Concentration Patterns')
                
                max_val = max(df['max_portfolio_pct'].max(), df['avg_portfolio_pct'].max())
                axes[2].plot([0, max_val], [0, max_val], 'k--', alpha=0.3, label='Equal avg/max')
                axes[2].legend()
            
            if 'avg_portfolio_pct' in top_by_value.columns:
                axes[3].bar(range(len(top_by_value)), 
                          top_by_value['avg_portfolio_pct'],
                          color='darkgreen')
                axes[3].set_xticks(range(len(top_by_value)))
                axes[3].set_xticklabels(top_by_value['ticker'], rotation=45)
                axes[3].set_ylabel('Average Portfolio Allocation (%)')
                axes[3].set_title('Average Portfolio Weight of Top Holdings')
            
            plt.suptitle('Top Holdings Analysis', fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            output_path = self.output_dir / "top_holdings_advanced.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating top holdings chart: {e}")
            return None