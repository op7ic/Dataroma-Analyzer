#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Manager Performance Overview

Comprehensive performance visualization across multiple time periods.
Shows portfolio value changes and performance metrics across all managers
in a single-page analysis.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple
import logging
from matplotlib.ticker import FuncFormatter, MaxNLocator

logger = logging.getLogger(__name__)


def billions_formatter(x, pos):
    """Format numbers as billions, millions, etc."""
    if x >= 1e12:
        return f'{x/1e12:.0f}T'
    elif x >= 1e9:
        return f'{x/1e9:.0f}B'
    elif x >= 1e6:
        return f'{x/1e6:.0f}M'
    elif x >= 1e3:
        return f'{x/1e3:.0f}K'
    else:
        return f'{x:.0f}'


class ManagerPerformanceOverview:
    """Creates time-based manager performance analyses (3yr, 5yr, 10yr, comprehensive)."""
    
    def __init__(self, output_dir: str = "analysis/advanced/visuals"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("deep")
        plt.rcParams['figure.dpi'] = 150
        plt.rcParams['savefig.dpi'] = 300
    
    def create_all_performance_analyses(self, track_records_df: pd.DataFrame) -> List[str]:
        """Create all time-based performance analyses."""
        viz_paths = []
        
        analyses = [
            (3, "3_year_performance"),
            (5, "5_year_performance"), 
            (10, "10_year_performance"),
            (None, "comprehensive_performance")
        ]
        
        for years, filename in analyses:
            try:
                path = self.create_performance_analysis(track_records_df, years, filename)
                if path:
                    viz_paths.append(path)
            except Exception as e:
                logger.error(f"Error creating {filename}: {e}")
        
        return viz_paths
    
    def create_performance_analysis(self, track_records_df: pd.DataFrame, 
                                  years_window: int = None, 
                                  filename: str = "performance_analysis") -> str:
        """Create a performance analysis for a specific time window."""
        
        if years_window:
            # Dynamically determine current year from data
            current_year = track_records_df['last_year'].max()
            cutoff_year = current_year - years_window
            
            # Filter managers based on performance period (last X years)
            if years_window == 3:
                # 3-year performance: Managers active in last 3 years with sufficient track record
                period_start = current_year - 3
                filtered_df = track_records_df[
                    (track_records_df['last_year'] >= period_start) & 
                    (track_records_df['years_active'] >= 3) &
                    (track_records_df['total_actions'] >= 10)  # Active managers
                ].copy()
                analysis_period = f"3-Year Performance Analysis ({period_start}-{current_year})"
                
            elif years_window == 5:
                # 5-year performance: Managers active over 5-year period with substantial experience
                period_start = current_year - 5
                filtered_df = track_records_df[
                    (track_records_df['last_year'] >= period_start) & 
                    (track_records_df['years_active'] >= 5) &
                    (track_records_df['first_year'] <= period_start)  # Started before or during period
                ].copy()
                analysis_period = f"5-Year Performance Analysis ({period_start}-{current_year})"
                
            elif years_window == 10:
                # 10-year performance: Managers with full 10-year track record
                period_start = current_year - 10
                filtered_df = track_records_df[
                    (track_records_df['years_active'] >= 10) &
                    (track_records_df['first_year'] <= period_start)  # Started 10+ years ago
                ].copy()
                analysis_period = f"10-Year Performance Analysis ({period_start}-{current_year})"
            else:
                # Default filtering for other periods
                filtered_df = track_records_df[
                    (track_records_df['last_year'] >= cutoff_year) & 
                    (track_records_df['years_active'] >= min(3, years_window))
                ].copy()
                analysis_period = f"Last {years_window} Years ({cutoff_year}-{current_year})"
            
            if filtered_df.empty:
                logger.warning(f"No data available for {years_window}-year window")
                return None
                
        else:
            filtered_df = track_records_df.copy()
            analysis_period = self._extract_analysis_period(filtered_df)
        
        return self._create_performance_chart(filtered_df, analysis_period, filename)
    
    def _create_performance_chart(self, track_records_df: pd.DataFrame, 
                                analysis_period: str, filename: str) -> str:
        """Create a performance analysis chart for the given data and time period."""
        try:
            is_simplified = any(period in filename for period in ['3_year', '5_year', '10_year'])
            
            if is_simplified:
                # Use same improved layout as comprehensive chart
                fig = plt.figure(figsize=(20, 14))
                gs = fig.add_gridspec(3, 3, hspace=0.45, wspace=0.4,
                                    top=0.88, bottom=0.1, left=0.08, right=0.95)
                
                # 1. Risk-Adjusted Returns (Top Left - Main Focus)
                ax1 = fig.add_subplot(gs[0:2, 0:2])
                self._create_risk_adjusted_returns(ax1, track_records_df)
                
                # 2. Cohort Analysis (Top Right)
                ax2 = fig.add_subplot(gs[0, 2])
                self._create_cohort_analysis(ax2, track_records_df)
                
                # 3. Drawdown Analysis (Middle Right)
                ax3 = fig.add_subplot(gs[1, 2])
                self._create_drawdown_analysis(ax3, track_records_df)
                
                # 4. Top Performers Summary Table (Bottom Left)
                ax4 = fig.add_subplot(gs[2, 0])
                self._create_top_performers_summary(ax4, track_records_df)
                
                # 5. Performance Evolution Timeline (Bottom Center-Right)
                ax5 = fig.add_subplot(gs[2, 1:3])
                self._create_performance_timeline(ax5, track_records_df)
                
            else:
                # Comprehensive chart - simplified to 4-5 key visualizations
                fig = plt.figure(figsize=(20, 14))
                gs = fig.add_gridspec(3, 3, hspace=0.45, wspace=0.4,
                                    top=0.88, bottom=0.1, left=0.08, right=0.95)
                
                # 1. Risk-Adjusted Returns (Top Left - Main Focus)
                ax1 = fig.add_subplot(gs[0:2, 0:2])
                self._create_risk_adjusted_returns(ax1, track_records_df)
                
                # 2. Cohort Analysis (Top Right)
                ax2 = fig.add_subplot(gs[0, 2])
                self._create_cohort_analysis(ax2, track_records_df)
                
                # 3. Drawdown Analysis (Middle Right)
                ax3 = fig.add_subplot(gs[1, 2])
                self._create_drawdown_analysis(ax3, track_records_df)
                
                # 4. Top Performers Summary Table (Bottom Left)
                ax4 = fig.add_subplot(gs[2, 0])
                self._create_top_performers_summary(ax4, track_records_df)
                
                # 5. Performance Evolution Timeline (Bottom Center-Right)
                ax5 = fig.add_subplot(gs[2, 1:3])
                self._create_performance_timeline(ax5, track_records_df)
            
            manager_count = len(track_records_df)
            title = f'Manager Performance Analysis: {manager_count} Managers ({analysis_period})'
            
            if is_simplified:
                fig.suptitle(title, fontsize=18, fontweight='bold', y=0.95, ha='center')
            else:
                fig.suptitle(title, fontsize=20, fontweight='bold', y=0.94, ha='center')
            
            plt.tight_layout()
            
            output_path = self.output_dir / f"{filename}.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', pad_inches=0.1)
            plt.close()
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating {filename} chart: {e}")
            return None
    
    def _extract_analysis_period(self, df: pd.DataFrame) -> str:
        """Extract analysis period from track records."""
        if 'first_year' in df.columns and 'last_year' in df.columns:
            first_year = int(df['first_year'].min())
            last_year = int(df['last_year'].max())
            return f"{first_year}-{last_year}"
        return "Long-term Analysis"
    
    def _create_portfolio_value_chart(self, ax, df: pd.DataFrame):
        """Create portfolio value growth analysis with performance indicators."""
        if 'current_portfolio_value' in df.columns:
            if 'estimated_initial_value' in df.columns and 'annualized_return_pct' in df.columns:
                top_performers = df.nlargest(15, 'annualized_return_pct')
                manager_names = self._get_manager_names(top_performers)
                returns = top_performers['annualized_return_pct']
                
                colors = []
                for ret in returns:
                    if ret >= 25:
                        colors.append('darkgreen')
                    elif ret >= 15:
                        colors.append('green')
                    elif ret >= 10:
                        colors.append('lightgreen')
                    elif ret >= 5:
                        colors.append('gold')
                    elif ret >= 0:
                        colors.append('orange')
                    else:
                        colors.append('red')
                
                bars = ax.barh(range(len(top_performers)), returns, color=colors)
                ax.set_yticks(range(len(top_performers)))
                ax.set_yticklabels(manager_names, fontsize=9)
                ax.set_xlabel('Annualized Return (%)', fontsize=11, fontweight='bold')
                ax.set_title('Top Performers by Annualized Returns', fontsize=12, fontweight='bold')
                ax.invert_yaxis()
                
                ax.axvline(x=10, color='gray', linestyle='--', alpha=0.5, label='10% Benchmark')
                ax.axvline(x=15, color='darkgray', linestyle='--', alpha=0.5, label='15% Excellent')
                ax.legend(loc='lower right', fontsize=8)
                
                max_return = returns.max()
                ax.set_xlim(0, max_return * 1.15)
                
                for i, (_, row) in enumerate(top_performers.iterrows()):
                    return_pct = row['annualized_return_pct']
                    value_b = row['current_portfolio_value'] / 1e9 if pd.notna(row['current_portfolio_value']) else 0
                    
                    ax.text(return_pct + max_return * 0.02, i, f'{return_pct:.1f}%',
                           va='center', ha='left', fontsize=9, fontweight='bold')
                    
                    ax.text(max_return * 1.12, i, f'${value_b:.1f}B',
                           va='center', ha='right', fontsize=8, alpha=0.7)
                           
            elif 'estimated_initial_value' in df.columns:
                df['total_return_pct'] = ((df['current_portfolio_value'] - df['estimated_initial_value']) 
                                        / df['estimated_initial_value'] * 100)
                
                top_performers = df.nlargest(15, 'total_return_pct')
                manager_names = self._get_manager_names(top_performers)
                
                bars = ax.barh(range(len(top_performers)), top_performers['total_return_pct']) 
                ax.set_yticks(range(len(top_performers)))
                ax.set_yticklabels(manager_names, fontsize=10)
                ax.set_xlabel('Total Return (%)', fontsize=12)
                ax.set_title('Top Portfolio Growth (Total Returns)', fontsize=14, fontweight='bold')
                ax.invert_yaxis()
                
            else:
                top_by_value = df.nlargest(15, 'current_portfolio_value')
                manager_names = self._get_manager_names(top_by_value)
                values_billions = top_by_value['current_portfolio_value'] / 1e9
                
                colors = ['darkblue' if v >= 100 else 'blue' if v >= 50 else 'steelblue' if v >= 10 else 'lightblue' 
                         for v in values_billions]
                
                ax.barh(range(len(top_by_value)), values_billions, color=colors)
                ax.set_yticks(range(len(top_by_value)))
                ax.set_yticklabels(manager_names, fontsize=10)
                ax.set_xlabel('Portfolio Value ($B)', fontsize=12)
                ax.set_title('Largest Portfolio Values', fontsize=14, fontweight='bold')
                ax.invert_yaxis()
    
    def _create_performance_vs_experience(self, ax, df: pd.DataFrame):
        """Create performance vs experience scatter plot."""
        if all(col in df.columns for col in ['years_active', 'track_record_score']):
            scatter = ax.scatter(df['years_active'], df['track_record_score'], 
                               s=50, alpha=0.7, c=df['current_portfolio_value'], 
                               cmap='viridis', edgecolors='black', linewidth=0.5)
            ax.set_xlabel('Years Active', fontsize=10, fontweight='bold')
            ax.set_ylabel('Track Record Score', fontsize=10, fontweight='bold', 
                         rotation=0, ha='right', labelpad=50)
            ax.set_title('Performance vs Experience', fontsize=11, fontweight='bold')
            ax.grid(True, alpha=0.3)
            
            cbar = plt.colorbar(scatter, ax=ax)
            cbar.ax.yaxis.set_major_formatter(FuncFormatter(billions_formatter))
            cbar.set_label('Portfolio Value', fontsize=8)
    
    def _create_returns_distribution(self, ax, df: pd.DataFrame):
        """Create annual returns distribution."""
        if 'annualized_return_pct' in df.columns:
            returns = df['annualized_return_pct'].dropna()
            ax.hist(returns, bins=15, color='skyblue', alpha=0.7, edgecolor='black', linewidth=0.5)
            ax.axvline(returns.mean(), color='red', linestyle='--', linewidth=2,
                      label=f'Mean: {returns.mean():.1f}%')
            ax.set_xlabel('Annualized Return (%)', fontsize=10, fontweight='bold')
            ax.set_ylabel('Count', fontsize=10, fontweight='bold')
            ax.set_title('Returns Distribution', fontsize=11, fontweight='bold')
            ax.legend(fontsize=9)
            ax.grid(True, alpha=0.3)
    
    def _create_crisis_performance(self, ax, df: pd.DataFrame):
        """Create crisis performance analysis."""
        crisis_cols = [col for col in df.columns if 'crisis' in col.lower() and 'ratio' in col.lower()]
        if crisis_cols:
            crisis_data = df[crisis_cols].mean()
            ax.bar(range(len(crisis_data)), crisis_data.values, color='coral')
            ax.set_xticks(range(len(crisis_data))) 
            ax.set_xticklabels([col.replace('_', ' ').title() for col in crisis_data.index], 
                              rotation=45, ha='right')
            ax.set_ylabel('Average Buy Ratio')
            ax.set_title('Crisis Buying Behavior')
    
    def _create_consistency_analysis(self, ax, df: pd.DataFrame):
        """Create consistency score analysis."""
        if 'consistency_score' in df.columns:
            bins = [0, 0.5, 0.7, 0.8, 1.0]
            labels = ['Low', 'Medium', 'High', 'Very High']
            df['consistency_category'] = pd.cut(df['consistency_score'], bins=bins, labels=labels)
            
            counts = df['consistency_category'].value_counts()
            ax.pie(counts.values, labels=counts.index, autopct='%1.1f%%', 
                  colors=['red', 'orange', 'lightgreen', 'darkgreen'])
            ax.set_title('Consistency Distribution')
    
    def _create_top_performers_table(self, ax, df: pd.DataFrame):
        """Create a table of top performers."""
        ax.axis('off')
        
        top_10 = df.nlargest(10, 'track_record_score')
        
        table_data = []
        for _, row in top_10.iterrows():
            manager_name = self._get_manager_name(row)
            if len(manager_name) > 18:
                manager_name = manager_name[:15] + "..."
            
            portfolio_val = f"${row['current_portfolio_value']/1e9:.1f}B" if pd.notna(row['current_portfolio_value']) else "N/A"
            annual_return = f"{row['annualized_return_pct']:.1f}%" if pd.notna(row['annualized_return_pct']) else "N/A"
            years_active = f"{row['years_active']:.0f}" if pd.notna(row['years_active']) else "N/A"
            
            table_data.append([manager_name, portfolio_val, annual_return, years_active])
        
        table = ax.table(cellText=table_data,
                        colLabels=['Manager', 'Portfolio Value', 'Annual Return', 'Years Active'],
                        cellLoc='center',
                        loc='center',
                        bbox=[0, 0, 1, 1])
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.5)
        
        for (row, col), cell in table.get_celld().items():
            if row == 0:
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor('#4472C4')
            else:
                cell.set_facecolor('#F2F2F2' if row % 2 == 0 else 'white')
            
            if col == 0:
                cell.set_width(0.35)
            elif col == 1:
                cell.set_width(0.25)
            elif col == 2:
                cell.set_width(0.25)
            else:
                cell.set_width(0.15)
        
        ax.set_title('Top 10 Performers by Track Record', fontsize=12, fontweight='bold', pad=15)
    
    def _create_size_vs_performance(self, ax, df: pd.DataFrame):
        """Create portfolio size vs performance scatter."""
        if all(col in df.columns for col in ['current_portfolio_value', 'annualized_return_pct']):
            valid_data = df.dropna(subset=['current_portfolio_value', 'annualized_return_pct'])
            portfolio_billions = valid_data['current_portfolio_value'] / 1e9
            
            ax.scatter(portfolio_billions, valid_data['annualized_return_pct'],
                      s=60, alpha=0.6, color='purple', edgecolors='black', linewidth=0.5)
            ax.set_xlabel('Portfolio Size ($B)', fontsize=10, fontweight='bold')
            ax.set_ylabel('Annual Return (%)', fontsize=10, fontweight='bold')
            ax.set_title('Size vs Performance', fontsize=11, fontweight='bold')
            
            ax.set_xscale('log')
            ax.xaxis.set_major_formatter(FuncFormatter(lambda x, p: f'${int(x)}B' if x >= 1 else f'${x:.1f}B'))
            ax.grid(True, alpha=0.3)
    
    def _create_activity_vs_returns(self, ax, df: pd.DataFrame):
        """Create activity vs returns analysis."""
        if all(col in df.columns for col in ['total_actions', 'annualized_return_pct']):
            valid_data = df.dropna(subset=['total_actions', 'annualized_return_pct'])
            ax.scatter(valid_data['total_actions'], valid_data['annualized_return_pct'],
                      s=60, alpha=0.6, color='brown')
            ax.set_xlabel('Total Actions')
            ax.set_ylabel('Annual Return (%)')
            ax.set_title('Activity vs Returns')
    
    def _create_performance_timeline(self, ax, df: pd.DataFrame):
        """Create performance timeline showing market participation trends."""
        if all(col in df.columns for col in ['first_year', 'last_year', 'track_record_score']):
            years = range(int(df['first_year'].min()), int(df['last_year'].max()) + 1)
            
            active_counts = []
            avg_performance = []
            top_performer_scores = []
            
            for year in years:
                active_managers = df[(df['first_year'] <= year) & (df['last_year'] >= year)]
                active_counts.append(len(active_managers))
                
                if len(active_managers) > 0:
                    avg_perf = active_managers['track_record_score'].mean()
                    top_perf = active_managers['track_record_score'].quantile(0.9)
                else:
                    avg_perf = 0
                    top_perf = 0
                    
                avg_performance.append(avg_perf)
                top_performer_scores.append(top_perf)
            
            ax2 = ax.twinx()
            
            bars = ax.bar(years, active_counts, alpha=0.3, color='steelblue', 
                         label='Active Managers', width=0.8)
            ax.set_ylabel('Number of Active Managers', color='steelblue', fontweight='bold')
            ax.tick_params(axis='y', labelcolor='steelblue')
            
            line1 = ax2.plot(years, avg_performance, 'red', linewidth=3, marker='o', 
                           markersize=4, label='Average Performance', alpha=0.8)
            line2 = ax2.plot(years, top_performer_scores, 'darkgreen', linewidth=2, 
                           linestyle='--', marker='s', markersize=3, 
                           label='Top 10% Performance', alpha=0.8)
            
            ax2.set_ylabel('Track Record Score', color='darkred', fontweight='bold')
            ax2.tick_params(axis='y', labelcolor='darkred')
            
            ax.set_xlabel('Year', fontweight='bold')
            ax.set_title('Market Participation & Performance Evolution\n' +
                        '(Bars = Active Managers, Lines = Performance Levels)', 
                        fontsize=12, fontweight='bold', pad=15)
            ax.grid(True, alpha=0.3)
            
            ax.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=10))
            ax.xaxis.set_major_formatter(FuncFormatter(lambda x, p: f'{int(x)}'))
            
            # Create compact legend in upper left to avoid overlap with data
            lines1, labels1 = ax.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels() 
            
            legend = ax.legend(lines1 + lines2, labels1 + labels2, 
                             loc='upper left', 
                             frameon=True, fontsize=7, framealpha=0.95,
                             edgecolor='gray', fancybox=True)
    
    def _get_manager_names(self, df: pd.DataFrame) -> List[str]:
        """Get list of manager names, preferring full names over IDs."""
        names = []
        for _, row in df.iterrows():
            names.append(self._get_manager_name(row))
        return names
    
    def _get_manager_name(self, row: pd.Series) -> str:
        """Get manager name, preferring full name over ID."""
        name_cols = ['manager_name', 'manager.1', 'manager_full_name']
        id_cols = ['manager', 'manager_id']
        
        for col in name_cols:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
                return str(row[col]).strip()
        
        for col in id_cols:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
                return str(row[col]).strip()
        
        return "Unknown Manager"
    
    def _create_risk_adjusted_returns(self, ax, df: pd.DataFrame):
        """Create risk-adjusted returns analysis with Sharpe ratios."""
        if 'annualized_return_pct' in df.columns and 'consistency_score' in df.columns:
            # Calculate proxy Sharpe ratio using consistency as inverse of volatility
            df['sharpe_proxy'] = df['annualized_return_pct'] * df['consistency_score']
            
            # Filter for managers with positive returns and good data
            valid_df = df[(df['annualized_return_pct'] > 0) & (df['consistency_score'] > 0)].copy()
            
            if len(valid_df) > 0:
                # Create scatter plot with size based on portfolio value
                sizes = []
                for _, row in valid_df.iterrows():
                    if pd.notna(row.get('current_portfolio_value')):
                        size = min(500, max(50, row['current_portfolio_value'] / 1e9 * 2))
                    else:
                        size = 100
                    sizes.append(size)
                
                scatter = ax.scatter(valid_df['annualized_return_pct'], 
                                   valid_df['consistency_score'],
                                   s=sizes, 
                                   c=valid_df['sharpe_proxy'],
                                   cmap='RdYlGn', 
                                   alpha=0.7,
                                   edgecolors='black',
                                   linewidth=0.5)
                
                # Add colorbar
                cbar = plt.colorbar(scatter, ax=ax)
                cbar.set_label('Risk-Adjusted Score', fontsize=10)
                
                # Smart labeling - only label top 3, bottom 2, and 2 outliers
                top_sharpe = valid_df.nlargest(3, 'sharpe_proxy')
                bottom_sharpe = valid_df.nsmallest(2, 'sharpe_proxy')
                outliers = valid_df.nlargest(2, 'annualized_return_pct')
                
                # Track labeled positions to avoid overlaps
                labeled_positions = []
                
                def get_offset(x, y, labeled_positions):
                    """Calculate offset to avoid overlaps."""
                    base_offsets = [(10, 10), (-10, 10), (10, -10), (-10, -10), 
                                   (15, 0), (-15, 0), (0, 15), (0, -15)]
                    for offset in base_offsets:
                        overlapping = False
                        for lx, ly in labeled_positions:
                            if abs(x - lx) < 0.1 and abs(y - ly) < 0.05:
                                overlapping = True
                                break
                        if not overlapping:
                            return offset
                    return (20, 20)  # Default if all positions taken
                
                # Label top performers with smart positioning
                for df_subset, color in [(top_sharpe, 'yellow'), (bottom_sharpe, 'lightcoral'), 
                                        (outliers, 'lightblue')]:
                    for _, row in df_subset.iterrows():
                        x, y = row['annualized_return_pct'], row['consistency_score']
                        if (x, y) not in labeled_positions:
                            manager_name = self._get_manager_name(row)
                            if len(manager_name) > 15:
                                manager_name = manager_name[:12] + "..."
                            
                            offset = get_offset(x, y, labeled_positions)
                            ha = 'left' if offset[0] > 0 else 'right'
                            
                            ax.annotate(manager_name,
                                      (x, y),
                                      xytext=offset, textcoords='offset points',
                                      fontsize=8, fontweight='bold', ha=ha,
                                      bbox=dict(boxstyle='round,pad=0.2', facecolor=color, alpha=0.7),
                                      arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.2',
                                                    alpha=0.5, lw=0.5))
                            labeled_positions.append((x, y))
                
                # Add quadrant lines
                median_return = valid_df['annualized_return_pct'].median()
                median_consistency = valid_df['consistency_score'].median()
                ax.axhline(y=median_consistency, color='gray', linestyle='--', alpha=0.5)
                ax.axvline(x=median_return, color='gray', linestyle='--', alpha=0.5)
                
                # Labels
                ax.set_xlabel('Annualized Return (%)', fontsize=12, fontweight='bold')
                ax.set_ylabel('Consistency Score', fontsize=12, fontweight='bold')
                ax.set_title('Risk-Adjusted Performance Analysis\n(Size = Portfolio Value, Color = Risk-Adjusted Score)', 
                           fontsize=14, fontweight='bold', pad=15)
                ax.grid(True, alpha=0.3)
                
                # Add quadrant labels inside plot area with better positioning
                ax.text(0.85, 0.85, 'High Return\nHigh Consistency', transform=ax.transAxes,
                       fontsize=8, ha='center', va='center',
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='lightgreen', alpha=0.5))
                ax.text(0.15, 0.85, 'Low Return\nHigh Consistency', transform=ax.transAxes,
                       fontsize=8, ha='center', va='center',
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='lightyellow', alpha=0.5))
                ax.text(0.85, 0.15, 'High Return\nLow Consistency', transform=ax.transAxes,
                       fontsize=8, ha='center', va='center',
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='orange', alpha=0.5))
                ax.text(0.15, 0.15, 'Low Return\nLow Consistency', transform=ax.transAxes,
                       fontsize=8, ha='center', va='center',
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='lightcoral', alpha=0.5))
    
    def _create_cohort_analysis(self, ax, df: pd.DataFrame):
        """Create cohort-based performance comparison."""
        if 'first_year' in df.columns and 'annualized_return_pct' in df.columns:
            # Define cohorts based on when managers started - use dynamic current year
            current_year = df['last_year'].max() if 'last_year' in df.columns else df['first_year'].max()
            df['cohort'] = pd.cut(df['first_year'], 
                                 bins=[2000, 2008, 2012, 2016, 2020, current_year + 1],
                                 labels=['Pre-2008', '2008-2012', '2012-2016', '2016-2020', f'2020-{current_year}'])
            
            cohort_stats = df.groupby('cohort').agg({
                'annualized_return_pct': ['mean', 'median', 'std', 'count'],
                'track_record_score': 'mean'
            }).round(2)
            
            # Create box plot
            cohort_data = []
            cohort_labels = []
            for cohort in ['Pre-2008', '2008-2012', '2012-2016', '2016-2020', '2020+']:
                data = df[df['cohort'] == cohort]['annualized_return_pct'].dropna()
                if len(data) > 0:
                    cohort_data.append(data)
                    cohort_labels.append(f'{cohort}\n(n={len(data)})')
            
            if cohort_data:
                bp = ax.boxplot(cohort_data, patch_artist=True)
                
                # Color boxes by median performance
                colors = ['darkgreen', 'green', 'gold', 'orange', 'lightcoral']
                for patch, color in zip(bp['boxes'], colors[:len(bp['boxes'])]):
                    patch.set_facecolor(color)
                    patch.set_alpha(0.7)
                
                # Custom x-axis labels without sample size
                ax.set_xticklabels(['Pre-2008', '2008-2012', '2012-2016', '2016-2020', '2020+'][:len(cohort_data)])
                
                # Add sample sizes inside boxes
                for i, data in enumerate(cohort_data):
                    y_pos = data.quantile(0.75)  # Position at 75th percentile
                    ax.text(i + 1, y_pos, f'n={len(data)}', 
                           ha='center', va='bottom', fontsize=9, fontweight='bold',
                           bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.8))
                
                ax.set_ylabel('Annualized Return (%)', fontsize=11, fontweight='bold')
                ax.set_title('Performance by Manager Cohort\n(When They Started Managing)', 
                           fontsize=12, fontweight='bold')
                ax.grid(True, alpha=0.3, axis='y')
                
                # Add median line
                medians = [data.median() for data in cohort_data]
                ax.plot(range(1, len(medians) + 1), medians, 'r--', linewidth=2, 
                       label='Median Trend')
                ax.legend(loc='upper right', fontsize=9)
    
    def _create_drawdown_analysis(self, ax, df: pd.DataFrame):
        """Create drawdown and crisis performance analysis."""
        crisis_cols = []
        for col in df.columns:
            if 'crisis' in col.lower() and 'ratio' in col.lower():
                crisis_cols.append(col)
        
        if crisis_cols and 'annualized_return_pct' in df.columns:
            # Calculate average crisis buy ratio
            df['avg_crisis_buy_ratio'] = df[crisis_cols].mean(axis=1)
            
            # Create scatter plot
            valid_df = df.dropna(subset=['avg_crisis_buy_ratio', 'annualized_return_pct'])
            
            scatter = ax.scatter(valid_df['avg_crisis_buy_ratio'] * 100,
                               valid_df['annualized_return_pct'],
                               s=80, alpha=0.6, 
                               c=valid_df['track_record_score'],
                               cmap='viridis',
                               edgecolors='black',
                               linewidth=0.5)
            
            # Add trend line
            if len(valid_df) > 5:
                z = np.polyfit(valid_df['avg_crisis_buy_ratio'] * 100, 
                             valid_df['annualized_return_pct'], 1)
                p = np.poly1d(z)
                x_trend = np.linspace(valid_df['avg_crisis_buy_ratio'].min() * 100,
                                    valid_df['avg_crisis_buy_ratio'].max() * 100, 100)
                ax.plot(x_trend, p(x_trend), "r--", alpha=0.8, linewidth=2)
                
                # Place trend equation in clear space
                trend_text = f'Trend: {"+" if z[0] > 0 else ""}{z[0]:.2f}x'
                ax.text(0.05, 0.95, trend_text, transform=ax.transAxes,
                       fontsize=10, fontweight='bold', va='top',
                       bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
            
            ax.set_xlabel('Average Crisis Buy Ratio (%)', fontsize=11, fontweight='bold')
            ax.set_ylabel('Annualized Return (%)', fontsize=11, fontweight='bold')
            ax.set_title('Crisis Behavior vs Long-term Returns', fontsize=12, fontweight='bold')
            ax.grid(True, alpha=0.3)
            
            # Add colorbar
            cbar = plt.colorbar(scatter, ax=ax)
            cbar.set_label('Track Record Score', fontsize=9)
            
            # Smart labeling for crisis performers to avoid overlaps
            # Only label top 2 and most interesting outliers
            top_crisis = valid_df.nlargest(2, 'avg_crisis_buy_ratio')
            high_return_crisis = valid_df[valid_df['avg_crisis_buy_ratio'] > 0.5].nlargest(1, 'annualized_return_pct')
            
            labeled_points = []
            for df_subset in [top_crisis, high_return_crisis]:
                for _, row in df_subset.iterrows():
                    x, y = row['avg_crisis_buy_ratio'] * 100, row['annualized_return_pct']
                    
                    # Skip if too close to already labeled point
                    skip = False
                    for lx, ly in labeled_points:
                        if abs(x - lx) < 5 and abs(y - ly) < 2:
                            skip = True
                            break
                    
                    if not skip:
                        manager_name = self._get_manager_name(row)
                        if len(manager_name) > 12:
                            manager_name = manager_name[:10] + ".."
                        
                        # Dynamic offset based on position
                        if x > 70:
                            offset = (-10, 0)
                            ha = 'right'
                        else:
                            offset = (10, 0)
                            ha = 'left'
                        
                        ax.annotate(manager_name,
                                  (x, y),
                                  xytext=offset, textcoords='offset points',
                                  fontsize=8, fontweight='bold', ha=ha,
                                  bbox=dict(boxstyle='round,pad=0.2', facecolor='yellow', alpha=0.7),
                                  arrowprops=dict(arrowstyle='->', alpha=0.5, lw=0.5))
                        labeled_points.append((x, y))
    
    def _create_top_performers_summary(self, ax, df: pd.DataFrame):
        """Create a simplified top performers summary table."""
        ax.axis('off')
        
        # Calculate composite score
        if 'annualized_return_pct' in df.columns and 'consistency_score' in df.columns:
            df['composite_score'] = (
                df['annualized_return_pct'] * 0.4 +
                df['consistency_score'] * 100 * 0.3 +
                df['track_record_score'] * 0.3
            )
            top_5 = df.nlargest(5, 'composite_score')
        else:
            top_5 = df.nlargest(5, 'track_record_score')
        
        table_data = []
        for i, (_, row) in enumerate(top_5.iterrows(), 1):
            manager_name = self._get_manager_name(row)
            # Ensure manager name fits in column with stricter limit
            if len(manager_name) > 15:
                manager_name = manager_name[:12] + "."
            
            annual_return = f"{row['annualized_return_pct']:.1f}%" if pd.notna(row.get('annualized_return_pct')) else "N/A"
            years = f"{row['years_active']:.0f}y" if pd.notna(row.get('years_active')) else "N/A"
            consistency = f"{row['consistency_score']:.2f}" if pd.notna(row.get('consistency_score')) else "N/A"
            
            table_data.append([f"#{i}", manager_name, annual_return, consistency, years])
        
        table = ax.table(cellText=table_data,
                        colLabels=['Rank', 'Manager', 'Return', 'Consistency', 'Years'],
                        cellLoc='center',
                        loc='center',
                        bbox=[0, 0, 1, 1])
        
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.8)
        
        # Style the table with proper column widths
        for (row, col), cell in table.get_celld().items():
            if row == 0:
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor('#2C5282')
            else:
                cell.set_facecolor('#F7FAFC' if row % 2 == 0 else 'white')
                if col == 2:  # Return column
                    text = cell.get_text().get_text()
                    if text != 'N/A' and '%' in text:
                        try:
                            if float(text.rstrip('%')) >= 15:
                                cell.set_text_props(weight='bold', color='darkgreen')
                        except ValueError:
                            pass
            
            # Set column widths to prevent overflow
            if col == 0:  # Rank
                cell.set_width(0.15)
            elif col == 1:  # Manager (narrower to prevent overflow)
                cell.set_width(0.35)
                cell.set_text_props(fontsize=8)  # Smaller font for manager names
            elif col == 2:  # Return
                cell.set_width(0.2)
            elif col == 3:  # Consistency
                cell.set_width(0.2)
            else:  # Years
                cell.set_width(0.1)
        
        ax.set_title('Top 5 Performers\n(Composite Score: Return + Consistency + Track Record)', 
                    fontsize=12, fontweight='bold', pad=20)