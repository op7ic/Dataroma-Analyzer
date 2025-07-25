#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Current Visualizer

Generates visualizations for recent market activity and opportunities.
Visualization module for current analysis (last 3 quarters) that creates 
graphs for immediate opportunities and recent market activity.

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
from typing import Dict, List, Set
import logging
from datetime import datetime, timedelta
import re

logger = logging.getLogger(__name__)


class CurrentVisualizer:
    """Creates visualizations for current market opportunities."""
    
    def __init__(self, output_dir: str = "analysis/current/visuals"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")
        
        plt.rcParams['figure.dpi'] = 150
        plt.rcParams['savefig.dpi'] = 300
    
    def _extract_time_periods(self, results: Dict[str, pd.DataFrame]) -> str:
        """Extract and format time periods from the data."""
        all_periods = set()
        
        for key, df in results.items():
            if df.empty:
                continue
                
            period_cols = ['period', 'periods', 'quarter', 'quarters']
            for col in period_cols:
                if col in df.columns:
                    periods_data = df[col].dropna()
                    for period_entry in periods_data:
                        if pd.isna(period_entry):
                            continue
                        # Handle comma-separated periods
                        if isinstance(period_entry, str):
                            quarters = re.findall(r'Q[1-4]\s+\d{4}', str(period_entry))
                            all_periods.update(quarters)
        
        if not all_periods:
            return "Last 3 Quarters"
        
        sorted_periods = sorted(list(all_periods), key=self._sort_quarter_key)
        
        if len(sorted_periods) <= 3:
            return f"{sorted_periods[0]} - {sorted_periods[-1]}"
        else:
            # Take first and last of recent periods
            return f"{sorted_periods[-3]} - {sorted_periods[-1]}"
    
    def _sort_quarter_key(self, quarter_str: str) -> tuple:
        """Create a sort key for quarter strings like 'Q1 2025'."""
        try:
            match = re.match(r'Q(\d)\s+(\d{4})', quarter_str)
            if match:
                q_num, year = match.groups()
                return (int(year), int(q_num))
            return (0, 0)
        except:
            return (0, 0)
    
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
        """Create all current analysis visualizations."""
        viz_paths = []
        
        time_period = self._extract_time_periods(results)
        
        try:
            if "hidden_gems" in results and not results["hidden_gems"].empty:
                path = self.create_hidden_gems_chart(results["hidden_gems"], time_period)
                if path:
                    viz_paths.append(path)
            
            if "momentum_stocks" in results and not results["momentum_stocks"].empty:
                path = self.create_momentum_chart(results["momentum_stocks"], time_period)
                if path:
                    viz_paths.append(path)
            
            price_dfs = self._collect_price_dfs(results)
            if price_dfs:
                path = self.create_price_opportunities_chart(price_dfs, time_period)
                if path:
                    viz_paths.append(path)
            
            if "52_week_low_buys" in results or "52_week_high_sells" in results:
                path = self.create_52_week_chart(
                    results.get("52_week_low_buys"),
                    results.get("52_week_high_sells"),
                    time_period
                )
                if path:
                    viz_paths.append(path)
            
            if "new_positions" in results and not results["new_positions"].empty:
                path = self.create_new_positions_analysis_chart(results["new_positions"], time_period)
                if path:
                    viz_paths.append(path)
            
            if "concentration_changes" in results and not results["concentration_changes"].empty:
                path = self.create_portfolio_changes_chart(results["concentration_changes"], time_period)
                if path:
                    viz_paths.append(path)
            
            # Create low-price accumulation chart using price-based data
            if price_dfs:
                # Combine all low-price stocks (under $20) for accumulation analysis
                low_price_stocks = []
                for key, df in price_dfs.items():
                    if 'stocks_under_$20' in key or 'stocks_under_$10' in key or 'stocks_under_$5' in key:
                        low_price_stocks.append(df)
                
                if low_price_stocks:
                    combined_low_price = pd.concat(low_price_stocks, ignore_index=True).drop_duplicates(subset=['ticker'])
                    path = self.create_low_price_accumulation_chart(combined_low_price, time_period)
                    if path:
                        viz_paths.append(path)
            
        except Exception as e:
            logger.error(f"Error creating current visualizations: {e}")
        
        return viz_paths
    
    def create_hidden_gems_chart(self, df: pd.DataFrame, time_period: str = "Last 3 Quarters") -> str:
        """Create hidden gems opportunity chart."""
        try:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
            
            top_gems = df.nlargest(20, 'hidden_gem_score')
            
            bars = ax1.barh(top_gems['ticker'], top_gems['hidden_gem_score'], 
                          color='purple', alpha=0.7)
            ax1.set_xlabel('Hidden Gem Score', fontweight='bold')
            ax1.set_title(f'Top 20 Hidden Gems ({time_period})', fontsize=14, fontweight='bold')
            ax1.invert_yaxis()
            ax1.grid(True, alpha=0.3)
            
            max_score = top_gems['hidden_gem_score'].max()
            for i, (ticker, score) in enumerate(zip(top_gems['ticker'], top_gems['hidden_gem_score'])):
                ax1.text(score + max_score * 0.02, i, f'{score:.2f}', 
                        va='center', ha='left', fontsize=9, fontweight='bold')
            
            ax2.scatter(top_gems['manager_count'], top_gems['hidden_gem_score'], 
                       s=100, alpha=0.6, c='purple', edgecolors='black', linewidth=0.5)
            
            # Improved label positioning to avoid overlap
            texts = []
            for idx, row in top_gems.iterrows():
                # Offset labels slightly to reduce overlap
                x_offset = 0.1
                y_offset = 0.02
                texts.append(ax2.annotate(row['ticker'], 
                           (row['manager_count'] + x_offset, row['hidden_gem_score'] + y_offset),
                           fontsize=8, alpha=0.8, fontweight='bold'))
            
            ax2.set_xlabel('Number of Managers', fontweight='bold')
            ax2.set_ylabel('Hidden Gem Score', fontweight='bold')
            ax2.set_title('Hidden Gems: Manager Count vs Score Analysis', fontsize=12, fontweight='bold')
            ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            output_path = self.output_dir / "hidden_gems_current.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating hidden gems chart: {e}")
            return None
    
    def create_momentum_chart(self, df: pd.DataFrame, time_period: str = "Last 3 Quarters") -> str:
        """Create momentum stocks visualization."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(15, 12))
            axes = axes.flatten()
            
            top_buys = df.nlargest(15, 'buy_count')
            bars = axes[0].barh(top_buys['ticker'], top_buys['buy_count'], 
                              color='green', alpha=0.7)
            axes[0].set_xlabel('Number of Buys', fontweight='bold')
            axes[0].set_title(f'Most Bought Stocks ({time_period})', fontsize=12, fontweight='bold')
            axes[0].invert_yaxis()
            axes[0].grid(True, alpha=0.3)
            
            max_buys = top_buys['buy_count'].max()
            for i, (ticker, count) in enumerate(zip(top_buys['ticker'], top_buys['buy_count'])):
                axes[0].text(count + max_buys * 0.02, i, f'{count}', 
                           va='center', ha='left', fontsize=9, fontweight='bold')
            
            top_momentum = df.nlargest(15, 'momentum_score')
            bars = axes[1].barh(top_momentum['ticker'], top_momentum['momentum_score'], 
                              color='blue', alpha=0.7)
            axes[1].set_xlabel('Momentum Score', fontweight='bold')
            axes[1].set_title(f'Highest Momentum Stocks ({time_period})', fontsize=12, fontweight='bold')
            axes[1].invert_yaxis()
            axes[1].grid(True, alpha=0.3)
            
            max_momentum = top_momentum['momentum_score'].max()
            for i, (ticker, score) in enumerate(zip(top_momentum['ticker'], top_momentum['momentum_score'])):
                axes[1].text(score + max_momentum * 0.02, i, f'{score:.0f}', 
                           va='center', ha='left', fontsize=9, fontweight='bold')
            
            # Momentum-Price Quadrant Analysis (improved readability)
            if 'current_price' in df.columns:
                # Filter out extreme price outliers for better visualization
                price_q99 = df['current_price'].quantile(0.99)
                filtered_df = df[df['current_price'] <= price_q99].copy()
                
                # Create quadrant chart with better spacing
                scatter = axes[2].scatter(filtered_df['momentum_score'], filtered_df['current_price'], 
                                        alpha=0.7, s=50, c='steelblue', edgecolors='white', linewidth=1)
                
                # Add quadrant lines using filtered data
                momentum_median = filtered_df['momentum_score'].median()
                price_median = filtered_df['current_price'].median()
                axes[2].axvline(x=momentum_median, color='red', linestyle='--', alpha=0.8, linewidth=2)
                axes[2].axhline(y=price_median, color='red', linestyle='--', alpha=0.8, linewidth=2)
                
                # Identify and highlight cheap momentum plays
                cheap_momentum = filtered_df[(filtered_df['momentum_score'] > momentum_median) & 
                                            (filtered_df['current_price'] < price_median)]
                if not cheap_momentum.empty:
                    axes[2].scatter(cheap_momentum['momentum_score'], cheap_momentum['current_price'], 
                                  color='gold', s=100, edgecolors='black', linewidth=2, 
                                  label=f'Cheap Momentum ({len(cheap_momentum)})', alpha=0.9, zorder=5)
                    
                    # Annotate only top 5 to avoid crowding
                    top_cheap_momentum = cheap_momentum.nlargest(5, 'momentum_score')
                    for _, row in top_cheap_momentum.iterrows():
                        axes[2].annotate(row['ticker'], 
                                       (row['momentum_score'], row['current_price']),
                                       xytext=(8, 8), textcoords='offset points',
                                       fontsize=9, fontweight='bold',
                                       bbox=dict(boxstyle='round,pad=0.3', facecolor='yellow', alpha=0.9, edgecolor='black'))
                
                axes[2].set_xlabel('Momentum Score', fontweight='bold', fontsize=11)
                axes[2].set_ylabel('Stock Price ($)', fontweight='bold', fontsize=11)
                axes[2].set_title('Momentum vs Price Analysis', fontsize=12, fontweight='bold')
                axes[2].grid(True, alpha=0.4, linestyle=':')
                
                # Better positioned quadrant labels
                axes[2].text(0.02, 0.98, 'Low Mom.\nHigh Price', transform=axes[2].transAxes,
                           ha='left', va='top', fontsize=8, fontweight='bold',
                           bbox=dict(boxstyle='round,pad=0.3', facecolor='lightcoral', alpha=0.8))
                axes[2].text(0.98, 0.02, 'High Mom.\nLow Price', transform=axes[2].transAxes,
                           ha='right', va='bottom', fontsize=8, fontweight='bold',
                           bbox=dict(boxstyle='round,pad=0.3', facecolor='lightgreen', alpha=0.8))
                
                if not cheap_momentum.empty:
                    axes[2].legend(loc='upper left', fontsize=9)
                    
                # Add note about outliers if any were filtered
                if len(filtered_df) < len(df):
                    outliers_count = len(df) - len(filtered_df)
                    axes[2].text(0.98, 0.98, f'Note: {outliers_count} outlier(s) filtered', 
                               transform=axes[2].transAxes, ha='right', va='top', 
                               fontsize=8, style='italic', alpha=0.7)
            else:
                # Fallback to momentum distribution if no price data
                axes[2].hist(df['momentum_score'], bins=20, color='purple', alpha=0.7, 
                            edgecolor='black', linewidth=0.5)
                axes[2].set_xlabel('Momentum Score', fontweight='bold')
                axes[2].set_ylabel('Number of Stocks', fontweight='bold')
                axes[2].set_title('Momentum Score Distribution', fontsize=12, fontweight='bold')
                axes[2].grid(True, alpha=0.3)
            
            # Create actionable opportunities table based on scatter plot analysis
            axes[3].axis('off')
            
            # Calculate medians for quadrant analysis (same as scatter plot)
            if 'momentum_score' in df.columns and 'current_price' in df.columns and len(df) > 0:
                # Filter data same way as scatter plot
                momentum_q99 = df['momentum_score'].quantile(0.99)
                price_q99 = df['current_price'].quantile(0.99)
                filtered_df = df[(df['momentum_score'] <= momentum_q99) & 
                               (df['current_price'] <= price_q99)].copy()
                
                if not filtered_df.empty:
                    momentum_median = filtered_df['momentum_score'].median()
                    price_median = filtered_df['current_price'].median()
                    
                    # Identify cheap momentum opportunities (High Mom + Low Price quadrant)
                    cheap_momentum = filtered_df[(filtered_df['momentum_score'] > momentum_median) & 
                                                (filtered_df['current_price'] < price_median)]
                    
                    # Build actionable opportunities table
                    table_data = []
                    headers = ['Rank', 'Ticker', 'Price', 'Momentum Score', 'Key Details']
                    
                    if not cheap_momentum.empty:
                        # Get top 10 cheap momentum opportunities
                        top_opportunities = cheap_momentum.nlargest(10, 'momentum_score')
                        
                        for idx, (_, row) in enumerate(top_opportunities.iterrows(), 1):
                            # Get key details
                            details = []
                            if 'buy_count' in row and pd.notna(row['buy_count']):
                                details.append(f"{int(row['buy_count'])} buys")
                            if 'current_holders' in row and pd.notna(row['current_holders']):
                                details.append(f"{int(row['current_holders'])} mgrs")
                            if 'managers' in row and pd.notna(row['managers']):
                                mgr_names = str(row['managers']).split(',')[:2]  # Top 2 managers
                                details.extend([mgr.strip()[:10] for mgr in mgr_names])
                            
                            table_data.append([
                                f"#{idx}",
                                row['ticker'],
                                f"${row['current_price']:.2f}",
                                f"{row['momentum_score']:.1f}",
                                ', '.join(details[:3])  # Limit to 3 key details
                            ])
                        
                        # Add summary row
                        table_data.append(['—', '—————', '————————', '—————————', '—————————————'])
                        avg_price = top_opportunities['current_price'].mean()
                        avg_momentum = top_opportunities['momentum_score'].mean()
                        table_data.append([
                            'AVG',
                            f'{len(cheap_momentum)} total',
                            f"${avg_price:.2f}",
                            f"{avg_momentum:.1f}",
                            f"Sweet spot stocks"
                        ])
                    else:
                        # No cheap momentum opportunities found
                        table_data = [
                            ['1', 'N/A', 'N/A', 'N/A', 'No cheap momentum stocks found'],
                            ['—', '—————', '————————', '—————————', '—————————————'],
                            ['INFO', f'{len(df)} total', 'See scatter plot', 'for analysis', 'All stocks analyzed']
                        ]
                else:
                    # Fallback when filtering removes all data
                    table_data = [
                        ['1', 'N/A', 'N/A', 'N/A', 'Data filtering issue'],
                        ['—', '—————', '————————', '—————————', '—————————————'],
                        ['INFO', f'{len(df)} stocks', 'Price/momentum', 'data available', 'Check raw data']
                    ]
            else:
                # Fallback when required columns missing
                table_data = [
                    ['1', 'N/A', 'N/A', 'N/A', 'Missing price/momentum data'],
                    ['—', '—————', '————————', '—————————', '—————————————'],
                    ['INFO', f'{len(df)} stocks', 'Limited analysis', 'possible', 'Check data source']
                ]
            
            # Create full-sized table that takes up the entire panel space
            table = axes[3].table(cellText=table_data, colLabels=headers,
                                 loc='center', cellLoc='left')
            table.auto_set_font_size(False)
            table.set_fontsize(10)  # Reasonable font for better readability
            table.scale(1.0, 2.0)  # Reasonable scale to fill the panel space
            
            # Style the header row with blue background and white text
            for i in range(len(headers)):
                table[(0, i)].set_facecolor('#4472C4')
                table[(0, i)].set_text_props(weight='bold', color='white')
            
            # Format columns appropriately for opportunities table
            for row in range(1, len(table_data) + 1):
                if row <= len(table_data):  # Safety check
                    table[(row, 0)].set_text_props(ha='center')  # Rank column (center)
                    table[(row, 1)].set_text_props(ha='center')  # Ticker column (center)
                    table[(row, 2)].set_text_props(ha='right')   # Price column (right-aligned)
                    table[(row, 3)].set_text_props(ha='center')  # Momentum Score (center)
                    table[(row, 4)].set_text_props(ha='left')    # Key Details (left)
            
            # Make table fill the available space better
            table.auto_set_column_width(col=list(range(len(headers))))
            
            axes[3].set_title('Momentum Analysis Summary', fontsize=12, fontweight='bold')
            
            plt.suptitle(f'Momentum Analysis ({time_period})', fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            output_path = self.output_dir / "momentum_analysis_current.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating momentum chart: {e}")
            return None
    
    def create_price_opportunities_chart(self, price_dfs: Dict[str, pd.DataFrame], time_period: str = "Last 3 Quarters") -> str:
        """Create price-based opportunities visualization."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(15, 10))
            axes = axes.flatten()
            
            # Create a stacked bar chart showing manager distribution across price ranges
            manager_distribution = {}
            price_range_order = ['<$5', '<$10', '<$20', '<$50', '<$100']
            
            for label, df in price_dfs.items():
                if not df.empty:
                    clean_label = label.replace('stocks_under_', '<')
                    manager_distribution[clean_label] = {}
                    
                    # Count unique managers per price range
                    if 'managers' in df.columns:
                        all_managers = set()
                        for managers in df['managers'].dropna():
                            if isinstance(managers, str):
                                for mgr in managers.split('\n'):
                                    mgr = mgr.strip()
                                    if mgr:
                                        all_managers.add(mgr)
                        
                        # Get top value stocks by total_value
                        if 'total_value' in df.columns:
                            top_value = df.nlargest(5, 'total_value')['total_value'].sum() / 1e9
                        else:
                            top_value = len(df) * 0.1  # Fallback
                        
                        manager_distribution[clean_label] = {
                            'stocks': len(df),
                            'managers': len(all_managers),
                            'top_value': top_value
                        }
            
            # Create informative bar chart with multiple metrics
            if manager_distribution:
                labels = []
                stock_counts = []
                manager_counts = []
                top_values = []
                
                for pr in price_range_order:
                    if pr in manager_distribution:
                        labels.append(pr)
                        stock_counts.append(manager_distribution[pr]['stocks'])
                        manager_counts.append(manager_distribution[pr]['managers'])
                        top_values.append(manager_distribution[pr]['top_value'])
                
                x = np.arange(len(labels))
                width = 0.35
                
                # Create grouped bar chart
                bars1 = axes[0].bar(x - width/2, stock_counts, width, label='Stock Count', color='teal', alpha=0.7)
                bars2 = axes[0].bar(x + width/2, manager_counts, width, label='Active Managers', color='navy', alpha=0.7)
                
                axes[0].set_xlabel('Price Range', fontweight='bold')
                axes[0].set_ylabel('Count', fontweight='bold')
                axes[0].set_title('Opportunities by Price Range: Stocks vs Manager Interest', fontsize=12, fontweight='bold')
                axes[0].set_xticks(x)
                axes[0].set_xticklabels(labels)
                axes[0].legend()
                axes[0].grid(True, alpha=0.3, axis='y')
                
                # Add value labels
                for bars in [bars1, bars2]:
                    for bar in bars:
                        height = bar.get_height()
                        axes[0].text(bar.get_x() + bar.get_width()/2., height + 1,
                                   f'{int(height)}', ha='center', va='bottom', fontsize=9)
            else:
                axes[0].text(0.5, 0.5, 'No price range data available', 
                           transform=axes[0].transAxes, ha='center', va='center')
            
            # Best under $20 opportunities with value formatting
            if "stocks_under_$20" in price_dfs and not price_dfs["stocks_under_$20"].empty:
                under_20 = price_dfs["stocks_under_$20"].head(10)
                values_billions = under_20['total_value'] / 1e9
                bars = axes[1].barh(under_20['ticker'], values_billions, color='coral', alpha=0.7)
                axes[1].set_xlabel('Total Value ($B)', fontweight='bold')
                axes[1].set_title('Top 10 Opportunities Under $20', fontsize=12, fontweight='bold')
                axes[1].invert_yaxis()
                axes[1].grid(True, alpha=0.3)
                
                # Add value labels with proper formatting
                max_value = values_billions.max()
                axes[1].set_xlim(0, max_value * 1.1)  # Add padding
                for i, (ticker, value) in enumerate(zip(under_20['ticker'], values_billions)):
                    axes[1].text(value + max_value * 0.02, i, f'${value:.2f}B', 
                               va='center', ha='left', fontsize=9, fontweight='bold')
            
            all_prices = []
            for df in price_dfs.values():
                if not df.empty and 'current_price' in df.columns:
                    all_prices.extend(df['current_price'].tolist())
            
            if all_prices:
                # Use better bins and add statistics
                axes[2].hist(all_prices, bins=30, color='navy', alpha=0.7, edgecolor='black', linewidth=0.5)
                axes[2].set_xlabel('Price ($)', fontweight='bold')
                axes[2].set_ylabel('Number of Stocks', fontweight='bold')
                axes[2].set_title('Price Distribution of All Opportunities', fontsize=12, fontweight='bold')
                axes[2].set_xlim(0, 100)
                axes[2].grid(True, alpha=0.3)
                
                # Add mean and median lines
                mean_price = np.mean(all_prices)
                median_price = np.median(all_prices)
                axes[2].axvline(mean_price, color='red', linestyle='--', 
                              label=f'Mean: ${mean_price:.2f}', linewidth=2)
                axes[2].axvline(median_price, color='orange', linestyle='--', 
                              label=f'Median: ${median_price:.2f}', linewidth=2)
                axes[2].legend()
            
            # Manager concentration in low-price stocks with count labels
            low_price_managers = {}
            for df in price_dfs.values():
                if not df.empty and 'managers' in df.columns:
                    for managers in df['managers']:
                        if pd.notna(managers):
                            for mgr in str(managers).split(','):
                                mgr = mgr.strip()
                                low_price_managers[mgr] = low_price_managers.get(mgr, 0) + 1
            
            if low_price_managers:
                top_managers = sorted(low_price_managers.items(), 
                                    key=lambda x: x[1], reverse=True)[:10]
                mgr_names = [m[0][:20] for m in top_managers]  # Truncate long names
                mgr_counts = [m[1] for m in top_managers]
                
                bars = axes[3].barh(mgr_names, mgr_counts, color='darkgreen', alpha=0.7)
                axes[3].set_xlabel('Number of Low-Price Positions', fontweight='bold')
                axes[3].set_title('Managers with Most Low-Price Bets', fontsize=12, fontweight='bold')
                axes[3].invert_yaxis()
                axes[3].grid(True, alpha=0.3)
                
                # Add count labels at bar ends
                max_count = max(mgr_counts)
                axes[3].set_xlim(0, max_count * 1.1)  # Add padding
                for i, (name, count) in enumerate(top_managers):
                    axes[3].text(count + max_count * 0.02, i, f'{count}', 
                               va='center', ha='left', fontsize=9, fontweight='bold')
            
            plt.suptitle(f'Price-Based Opportunities Analysis ({time_period})', fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            output_path = self.output_dir / "price_opportunities_current.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating price opportunities chart: {e}")
            return None
    
    def create_52_week_chart(self, low_buys_df: pd.DataFrame, high_sells_df: pd.DataFrame, time_period: str = "Last 3 Quarters") -> str:
        """Create 52-week high/low analysis chart."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()
            
            if low_buys_df is not None and not low_buys_df.empty:
                top_low_buys = low_buys_df.head(15)
                # Use buy_count as proxy for activity level
                bars = axes[0].barh(top_low_buys['ticker'], 
                                  top_low_buys['buy_count'], 
                                  color='green', alpha=0.7)
                axes[0].set_xlabel('Number of Buy Transactions', fontweight='bold')
                axes[0].set_title('Stocks Bought Near 52-Week Lows', fontsize=12, fontweight='bold')
                axes[0].invert_yaxis()
                axes[0].grid(True, alpha=0.3)
                
                max_value = top_low_buys['buy_count'].max()
                for i, (ticker, count) in enumerate(zip(top_low_buys['ticker'], top_low_buys['buy_count'])):
                    axes[0].text(count + max_value * 0.02, i, f'{count}', 
                               va='center', ha='left', fontsize=9, fontweight='bold')
                
                low_managers = {}
                if 'buying_managers' in low_buys_df.columns:
                    for managers in low_buys_df['buying_managers']:
                        if pd.notna(managers):
                            for mgr in str(managers).split(','):
                                mgr = mgr.strip()
                                low_managers[mgr] = low_managers.get(mgr, 0) + 1
                
                if low_managers:
                    top_low_mgrs = sorted(low_managers.items(), 
                                        key=lambda x: x[1], reverse=True)[:8]  # Reduced from 10 to 8
                    bars = axes[2].bar([m[0][:15] for m in top_low_mgrs], 
                                     [m[1] for m in top_low_mgrs], 
                                     color='darkgreen', alpha=0.7)
                    axes[2].set_xlabel('Manager', fontweight='bold')
                    axes[2].set_ylabel('Low-Buy Count', fontweight='bold')
                    axes[2].set_title('Managers Buying at Lows', fontsize=12, fontweight='bold')
                    axes[2].grid(True, alpha=0.3)
                    
                    # Improve label rotation and alignment
                    plt.setp(axes[2].get_xticklabels(), rotation=45, ha='right')
                    axes[2].tick_params(axis='x', labelsize=9)
                    
                    for i, (name, count) in enumerate(top_low_mgrs):
                        axes[2].text(i, count + 0.1, f'{count}', 
                                   ha='center', va='bottom', fontsize=9, fontweight='bold')
            
            if high_sells_df is not None and not high_sells_df.empty:
                top_high_sells = high_sells_df.head(15)
                # Use sell_count as proxy for activity level
                bars = axes[1].barh(top_high_sells['ticker'], 
                                  top_high_sells['sell_count'], 
                                  color='red', alpha=0.7)
                axes[1].set_xlabel('Number of Sell Transactions', fontweight='bold')
                axes[1].set_title('Stocks Sold Near 52-Week Highs', fontsize=12, fontweight='bold')
                axes[1].invert_yaxis()
                axes[1].grid(True, alpha=0.3)
                
                max_value = top_high_sells['sell_count'].max()
                axes[1].set_xlim(0, max_value * 1.1)  # Add 10% padding
                for i, (ticker, count) in enumerate(zip(top_high_sells['ticker'], top_high_sells['sell_count'])):
                    axes[1].text(count + max_value * 0.02, i, f'{count}', 
                               va='center', ha='left', fontsize=9, fontweight='bold')
                
                high_managers = {}
                if 'selling_managers' in high_sells_df.columns:
                    for managers in high_sells_df['selling_managers']:
                        if pd.notna(managers):
                            for mgr in str(managers).split(','):
                                mgr = mgr.strip()
                                high_managers[mgr] = high_managers.get(mgr, 0) + 1
                
                if high_managers and not low_managers:
                    # Use axes[2] for high managers if no low managers
                    top_high_mgrs = sorted(high_managers.items(), 
                                         key=lambda x: x[1], reverse=True)[:10]
                    axes[2].bar([m[0][:15] for m in top_high_mgrs], 
                              [m[1] for m in top_high_mgrs], 
                              color='darkred')
                    axes[2].set_xlabel('Manager')
                    axes[2].set_ylabel('High-Sell Count')
                    axes[2].set_title('Managers Selling at Highs')
                    axes[2].tick_params(axis='x', rotation=45)
                
                # Create actionable 52-week trading opportunities table
                axes[3].axis('off')
                
                # Combine and analyze both buy and sell opportunities
                all_opportunities = []
                
                # Process 52-week low buy opportunities
                if low_buys_df is not None and not low_buys_df.empty:
                    for _, row in low_buys_df.iterrows():
                        opportunity = {
                            'ticker': row['ticker'],
                            'action': 'BUY',
                            'current_price': row.get('current_price', 0),
                            'buy_count': row.get('buy_count', 0),
                            'managers': row.get('buying_managers', ''),
                            '52_week_low': row.get('52_week_low', 0),
                            '52_week_high': row.get('52_week_high', 0),
                            '52_week_position_pct': row.get('52_week_position_pct', 0)
                        }
                        all_opportunities.append(opportunity)
                
                # Process 52-week high sell opportunities  
                if high_sells_df is not None and not high_sells_df.empty:
                    for _, row in high_sells_df.iterrows():
                        opportunity = {
                            'ticker': row['ticker'],
                            'action': 'SELL',
                            'current_price': row.get('current_price', 0),
                            'sell_count': row.get('sell_count', 0),
                            'managers': row.get('selling_managers', ''),
                            '52_week_low': row.get('52_week_low', 0),
                            '52_week_high': row.get('52_week_high', 0),
                            '52_week_position_pct': row.get('52_week_position_pct', 100)
                        }
                        all_opportunities.append(opportunity)
                
                table_data = []
                headers = ['Action', 'Ticker', 'Price', 'From 52W Low/High', 'Activity']
                
                if all_opportunities:
                    # Sort buy opportunities by proximity to 52-week low (lowest % first)
                    buy_opps = [opp for opp in all_opportunities if opp['action'] == 'BUY']
                    sell_opps = [opp for opp in all_opportunities if opp['action'] == 'SELL']
                    
                    buy_opps.sort(key=lambda x: x['52_week_position_pct'])
                    sell_opps.sort(key=lambda x: x['52_week_position_pct'], reverse=True)  # Highest % first (near highs)
                    
                    # Show top 5 buy opportunities
                    for opp in buy_opps[:5]:
                        if opp['current_price'] > 0 and opp['52_week_low'] > 0:
                            distance_from_low = ((opp['current_price'] - opp['52_week_low']) / opp['52_week_low'] * 100)
                            managers_short = str(opp['managers']).split(',')[0][:12] if opp['managers'] else 'Unknown'
                            table_data.append([
                                'BUY',
                                opp['ticker'],
                                f"${opp['current_price']:.2f}",
                                f"+{distance_from_low:.1f}% from low",
                                f"{int(opp['buy_count'])} buys, {managers_short}"
                            ])
                    
                    # Add separator
                    if buy_opps and sell_opps:
                        table_data.append(['—', '————', '—————————', '——————————————', '————————————————'])
                    
                    # Show top 5 sell opportunities
                    for opp in sell_opps[:5]:
                        if opp['current_price'] > 0 and opp['52_week_high'] > 0:
                            distance_from_high = ((opp['52_week_high'] - opp['current_price']) / opp['52_week_high'] * 100)
                            managers_short = str(opp['managers']).split(',')[0][:12] if opp['managers'] else 'Unknown'
                            table_data.append([
                                'SELL',
                                opp['ticker'],
                                f"${opp['current_price']:.2f}",
                                f"-{distance_from_high:.1f}% from high",
                                f"{int(opp['sell_count'])} sells, {managers_short}"
                            ])
                    
                    # Add summary row
                    table_data.append(['—', '————', '—————————', '——————————————', '————————————————'])
                    buy_count = len(buy_opps)
                    sell_count = len(sell_opps)
                    table_data.append([
                        'TOTAL',
                        f'{buy_count}B/{sell_count}S',
                        time_period,
                        f'{buy_count + sell_count} opportunities',
                        'Value hunt vs Profit take'
                    ])
                else:
                    # No data available
                    table_data = [
                        ['N/A', 'No data', 'N/A', 'No 52-week analysis', 'Check data source'],
                        ['—', '————', '—————————', '——————————————', '————————————————'],
                        ['INFO', time_period, 'Period', 'Limited 52-week data', 'available']
                    ]
                
                # Create full-sized table that fills the entire panel
                table = axes[3].table(cellText=table_data, colLabels=headers,
                                     loc='center', cellLoc='left')
                table.auto_set_font_size(False)
                table.set_fontsize(10)  # Reasonable font for better readability
                table.scale(1.0, 2.5)  # Taller rows for better readability
                
                # Style the header row
                for i in range(len(headers)):
                    table[(0, i)].set_facecolor('#4472C4')
                    table[(0, i)].set_text_props(weight='bold', color='white')
                
                # Format columns for trading table
                for row in range(1, len(table_data) + 1):
                    if row <= len(table_data):  # Safety check
                        table[(row, 0)].set_text_props(ha='center', weight='bold')  # Action (BUY/SELL)
                        table[(row, 1)].set_text_props(ha='center')  # Ticker
                        table[(row, 2)].set_text_props(ha='right')   # Price (right-aligned)
                        table[(row, 3)].set_text_props(ha='center')  # Distance from 52W
                        table[(row, 4)].set_text_props(ha='left')    # Activity details
                        
                        # Color-code action column
                        if row < len(table_data) and len(table_data[row-1]) > 0:
                            action = table_data[row-1][0]
                            if action == 'BUY':
                                table[(row, 0)].set_facecolor('#90EE90')  # Light green
                            elif action == 'SELL':
                                table[(row, 0)].set_facecolor('#FFB6C1')  # Light red
                
                # Make table fill the available space better
                table.auto_set_column_width(col=list(range(len(headers))))
            
            plt.suptitle('52-Week High/Low Trading Analysis', fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            output_path = self.output_dir / "52_week_analysis_current.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating 52-week chart: {e}")
            return None
    
    def create_new_positions_analysis_chart(self, df: pd.DataFrame, time_period: str = "Last 3 Quarters") -> str:
        """Create comprehensive new positions analysis focused on price vs portfolio weight."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(20, 14))
            axes = axes.flatten()
            
            # Define thresholds clearly
            HIGH_CONVICTION_THRESHOLD = df['portfolio_percent'].quantile(0.9) if 'portfolio_percent' in df.columns else 5.0
            LOW_PRICE_THRESHOLD = 50.0
            
            # Main chart: Price vs Portfolio Weight scatter with quadrant analysis
            if 'current_price' in df.columns and 'portfolio_percent' in df.columns:
                # Filter out extreme outliers for better visualization
                price_q99 = df['current_price'].quantile(0.99)
                filtered_df = df[df['current_price'] <= price_q99].copy()
                
                # Calculate medians for quadrant lines
                price_median = filtered_df['current_price'].median()
                weight_median = filtered_df['portfolio_percent'].median()
                
                # Create base scatter plot
                scatter = axes[0].scatter(filtered_df['current_price'], filtered_df['portfolio_percent'], 
                                        alpha=0.6, s=60, c='steelblue', edgecolors='white', linewidth=1)
                
                # Add quadrant lines with labels
                axes[0].axvline(x=price_median, color='gray', linestyle='--', alpha=0.7, linewidth=1, 
                              label=f'Median Price: ${price_median:.0f}')
                axes[0].axhline(y=weight_median, color='gray', linestyle='--', alpha=0.7, linewidth=1,
                              label=f'Median Weight: {weight_median:.1f}%')
                
                # Highlight high-conviction positions (>90th percentile)
                high_conviction = filtered_df[filtered_df['portfolio_percent'] > HIGH_CONVICTION_THRESHOLD]
                if not high_conviction.empty:
                    axes[0].scatter(high_conviction['current_price'], high_conviction['portfolio_percent'], 
                                  color='gold', s=100, edgecolors='black', linewidth=2, 
                                  label=f'High Conviction (>{HIGH_CONVICTION_THRESHOLD:.1f}%)', alpha=0.9, zorder=5)
                    
                    # Annotate with ticker and price for high conviction plays
                    for _, row in high_conviction.iterrows():
                        axes[0].annotate(f"{row['ticker']}\n${row['current_price']:.0f}", 
                                       (row['current_price'], row['portfolio_percent']),
                                       xytext=(8, 8), textcoords='offset points',
                                       fontsize=8, fontweight='bold',
                                       bbox=dict(boxstyle='round,pad=0.2', facecolor='yellow', alpha=0.8, edgecolor='black'))
                
                # Calculate and prominently display correlation
                correlation = filtered_df['current_price'].corr(filtered_df['portfolio_percent'])
                correlation_text = f'Correlation: {correlation:.3f}\n(Practically Zero - No Price/Weight Relationship)'
                axes[0].text(0.02, 0.98, correlation_text, 
                           transform=axes[0].transAxes, ha='left', va='top', 
                           fontsize=11, fontweight='bold',
                           bbox=dict(boxstyle='round,pad=0.4', facecolor='lightyellow', alpha=0.9, edgecolor='orange'))
                
                # Add quadrant labels
                axes[0].text(0.75, 0.95, 'High Price\nHigh Conviction', transform=axes[0].transAxes,
                           ha='center', va='top', fontsize=9, fontweight='bold',
                           bbox=dict(boxstyle='round,pad=0.3', facecolor='lightcoral', alpha=0.7))
                axes[0].text(0.25, 0.05, 'Low Price\nLow Conviction', transform=axes[0].transAxes,
                           ha='center', va='bottom', fontsize=9, fontweight='bold',
                           bbox=dict(boxstyle='round,pad=0.3', facecolor='lightgray', alpha=0.7))
                axes[0].text(0.25, 0.95, 'Low Price\nHigh Conviction', transform=axes[0].transAxes,
                           ha='center', va='top', fontsize=9, fontweight='bold',
                           bbox=dict(boxstyle='round,pad=0.3', facecolor='lightgreen', alpha=0.7))
                
                axes[0].set_xlabel('Stock Price ($)', fontweight='bold', fontsize=12)
                axes[0].set_ylabel('Portfolio Weight (%)', fontweight='bold', fontsize=12)
                axes[0].set_title('Price vs Portfolio Weight Quadrant Analysis', fontsize=14, fontweight='bold')
                axes[0].grid(True, alpha=0.3, linestyle=':')
                axes[0].legend(loc='upper right', fontsize=9)
            else:
                axes[0].text(0.5, 0.5, 'Price or portfolio weight data not available', 
                           transform=axes[0].transAxes, ha='center', va='center')
            
            # Portfolio Concentration Analysis (sorted bar chart)
            if 'portfolio_percent' in df.columns:
                # Sort by portfolio weight descending for easier reading
                top_positions = df.nlargest(15, 'portfolio_percent')
                
                # Create horizontal bar chart with consistent green theme
                bars = axes[1].barh(range(len(top_positions)), top_positions['portfolio_percent'], 
                                  color='darkgreen', alpha=0.8, edgecolor='black', linewidth=0.5)
                axes[1].set_yticks(range(len(top_positions)))
                axes[1].set_yticklabels([f"{row['ticker']} (${row['current_price']:.0f})" 
                                       for _, row in top_positions.iterrows()], fontsize=10)
                axes[1].set_xlabel('Portfolio Weight (%)', fontweight='bold', fontsize=11)
                axes[1].set_title(f'Top 15 New Positions by Portfolio Weight', fontsize=13, fontweight='bold')
                axes[1].invert_yaxis()
                axes[1].grid(True, alpha=0.4, axis='x', linestyle=':')
                
                # Add high conviction threshold line
                axes[1].axvline(x=HIGH_CONVICTION_THRESHOLD, color='gold', linestyle='--', alpha=0.8, linewidth=2,
                              label=f'High Conviction ({HIGH_CONVICTION_THRESHOLD:.1f}%+)')
                axes[1].legend(loc='lower right', fontsize=9)
                
                # Add value labels for top 10 only to avoid clutter
                for i, (_, row) in enumerate(top_positions.head(10).iterrows()):
                    axes[1].text(row['portfolio_percent'] + 0.1, i, f"{row['portfolio_percent']:.1f}%", 
                               va='center', ha='left', fontsize=9, fontweight='bold')
            else:
                axes[1].text(0.5, 0.5, 'Portfolio weight data not available', transform=axes[1].transAxes, ha='center')
                axes[1].set_title('Portfolio Concentration Analysis', fontsize=13, fontweight='bold')
            
            return self._complete_new_positions_chart(axes, df, time_period, fig, HIGH_CONVICTION_THRESHOLD, LOW_PRICE_THRESHOLD)
            
        except Exception as e:
            logger.error(f"Error creating new positions analysis chart: {e}")
            return None
    
    def _complete_new_positions_chart(self, axes, df: pd.DataFrame, time_period: str, fig, high_conviction_threshold: float, low_price_threshold: float) -> str:
        """Complete the new positions chart with remaining panels."""
        
        # Portfolio weight distribution with better spacing and consistent colors
        if 'portfolio_percent' in df.columns:
            weights = df['portfolio_percent'].dropna()
            axes[2].hist(weights, bins=15, color='steelblue', alpha=0.7, edgecolor='black', linewidth=0.5)
            axes[2].set_xlabel('Portfolio Weight (%)', fontweight='bold', fontsize=11)
            axes[2].set_ylabel('Number of Positions', fontweight='bold', fontsize=11)
            axes[2].set_title('Portfolio Weight Distribution', fontsize=13, fontweight='bold')
            axes[2].grid(True, alpha=0.4, linestyle=':')
            
            # Add statistics with clear labels
            mean_weight = weights.mean()
            median_weight = weights.median()
            axes[2].axvline(x=mean_weight, color='red', linestyle='-', alpha=0.8, linewidth=2, label=f'Mean: {mean_weight:.1f}%')
            axes[2].axvline(x=median_weight, color='orange', linestyle='--', alpha=0.8, linewidth=2, label=f'Median: {median_weight:.1f}%')
            axes[2].axvline(x=high_conviction_threshold, color='gold', linestyle=':', alpha=0.8, linewidth=2, label=f'High Conviction: {high_conviction_threshold:.1f}%')
            axes[2].legend(fontsize=9, loc='upper right')
        else:
            axes[2].text(0.5, 0.5, 'Portfolio weight data not available', transform=axes[2].transAxes, ha='center')
            axes[2].set_title('Portfolio Weight Distribution', fontsize=13, fontweight='bold')
        
        # Create full-sized comprehensive analysis table
        axes[3].axis('off')
        if not df.empty:
            # Calculate metrics
            total_positions = len(df)
            high_conviction_threshold = 5.0  # More reasonable threshold
            low_price_threshold = 50
            
            # Build comprehensive table data
            table_data = []
            headers = ['Metric', 'Value', 'Top Tickers']
            
            # High Conviction positions (>5.0% portfolio weight)
            if 'portfolio_percent' in df.columns:
                high_conv_df = df[df['portfolio_percent'].fillna(0) > high_conviction_threshold]
                high_conviction_count = len(high_conv_df)
                if high_conviction_count > 0:
                    high_conv_top3 = high_conv_df.nlargest(3, 'portfolio_percent')
                    high_conv_tickers = [f"{row['ticker']} ({row['portfolio_percent']:.1f}%)" 
                                       for _, row in high_conv_top3.iterrows() if pd.notna(row['portfolio_percent'])]
                    table_data.append(['High Conviction (>5%)', f'{high_conviction_count} positions', ', '.join(high_conv_tickers[:3])])
                else:
                    table_data.append(['High Conviction (>5%)', '0 positions', 'None found'])
            else:
                table_data.append(['High Conviction (>5%)', 'N/A', 'Portfolio % data not available'])
            
            # Low Price positions (<$50)
            if 'current_price' in df.columns:
                price_data = df[df['current_price'].fillna(float('inf')) < low_price_threshold]
                low_price_count = len(price_data)
                if low_price_count > 0:
                    low_price_top3 = price_data.nsmallest(3, 'current_price')
                    low_price_tickers = [f"{row['ticker']} (${row['current_price']:.2f})" 
                                       for _, row in low_price_top3.iterrows() if pd.notna(row['current_price'])]
                    table_data.append(['Low Price (<$50)', f'{low_price_count} positions', ', '.join(low_price_tickers[:3])])
                else:
                    table_data.append(['Low Price (<$50)', '0 positions', 'None found'])
            else:
                table_data.append(['Low Price (<$50)', 'N/A', 'Price data not available'])
            
            # Sweet Spot positions (both low price AND high conviction)
            if 'current_price' in df.columns and 'portfolio_percent' in df.columns:
                sweet_spot_df = df[(df['current_price'].fillna(float('inf')) < low_price_threshold) & 
                                 (df['portfolio_percent'].fillna(0) > high_conviction_threshold)]
                sweet_spot_count = len(sweet_spot_df)
                if sweet_spot_count > 0:
                    sweet_spot_tickers = [f"{row['ticker']}" for _, row in sweet_spot_df.head(5).iterrows()]
                    table_data.append(['Sweet Spot (Low + High)', f'{sweet_spot_count} positions', ', '.join(sweet_spot_tickers)])
                else:
                    table_data.append(['Sweet Spot (Low + High)', '0 positions', 'None found'])
            else:
                table_data.append(['Sweet Spot (Low + High)', 'N/A', 'Insufficient data'])
            
            # Position value statistics (using 'value' column which should exist)
            if 'value' in df.columns:
                valid_values = df['value'].dropna()
                if len(valid_values) > 0:
                    avg_value = valid_values.mean()
                    median_value = valid_values.median()
                    table_data.append(['Position Values', f'Avg: ${avg_value/1e6:.1f}M', f'Median: ${median_value/1e6:.1f}M'])
                else:
                    table_data.append(['Position Values', 'N/A', 'No value data'])
            else:
                table_data.append(['Position Values', 'N/A', 'Value data not available'])
            
            # Recent activity summary (using 'period' column)
            if 'period' in df.columns:
                recent_periods = df['period'].value_counts().head(3)
                period_summary = ', '.join([f"{period} ({count})" for period, count in recent_periods.items()])
                table_data.append(['Recent Activity', f'{len(df)} total positions', period_summary])
            else:
                table_data.append(['Recent Activity', f'{total_positions} positions', time_period])
            
            # Create full-sized table that fills the panel
            table = axes[3].table(cellText=table_data, colLabels=headers, 
                                 loc='center', cellLoc='left')
            table.auto_set_font_size(False)
            table.set_fontsize(10)  # Reasonable font size
            table.scale(1.0, 2.0)  # Reasonable scaling - 2x row height
            
            # Style the header row
            for i in range(len(headers)):
                table[(0, i)].set_facecolor('#4472C4')
                table[(0, i)].set_text_props(weight='bold', color='white')
            
            # Left-align text columns, center-align numeric values
            for i in range(1, len(table_data) + 1):
                if i <= len(table_data):  # Safety check
                    table[(i, 0)].set_text_props(ha='left')  # Metric column
                    table[(i, 1)].set_text_props(ha='center')  # Value column
                    table[(i, 2)].set_text_props(ha='left')  # Top Tickers column
                
            # Make table fill more of the available space
            table.auto_set_font_size(False)
            table.auto_set_column_width(col=list(range(len(headers))))
        
        # Single title to avoid duplication
        plt.suptitle(f'New Positions Analysis: Price vs Portfolio Weight ({time_period})', 
                    fontsize=16, fontweight='bold', y=0.95)
        plt.tight_layout(rect=[0, 0.03, 1, 0.93])  # Adjust layout to accommodate single title
        
        output_path = self.output_dir / "new_positions_current.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(output_path)

    def create_low_price_accumulation_chart(self, df: pd.DataFrame, time_period: str = "Last 3 Quarters") -> str:
        """Create low-price stock accumulation analysis chart."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()
            
            # Filter for low-price stocks (under $20)
            price_col = 'current_price' if 'current_price' in df.columns else 'initial_price'
            if price_col in df.columns:
                low_price_df = df[df[price_col] <= 20].copy()
            else:
                low_price_df = df.copy()
            
            if low_price_df.empty:
                axes[0].text(0.5, 0.5, 'No low-price stocks found', transform=axes[0].transAxes, ha='center')
                axes[0].set_title('Low-Price Stock Accumulation (Under $20)', fontsize=12, fontweight='bold')
            else:
                # Calculate total buying activity - check for different manager column names
                manager_col = None
                for col in ['managers', 'manager', 'manager_list']:
                    if col in low_price_df.columns:
                        manager_col = col
                        break
                
                if manager_col:
                    if manager_col == 'managers':
                        low_price_df['manager_count'] = low_price_df['managers'].str.count(',') + 1
                    else:
                        # For single manager per row, count unique managers per stock
                        manager_counts = low_price_df.groupby('ticker')[manager_col].nunique()
                        low_price_df = low_price_df.merge(manager_counts.rename('manager_count'), 
                                                        left_on='ticker', right_index=True, how='left')
                else:
                    low_price_df['manager_count'] = 1
                
                # Sort by total accumulation (manager count * portfolio weight)
                if 'portfolio_percent' in low_price_df.columns:
                    low_price_df['accumulation_score'] = low_price_df['manager_count'] * low_price_df['portfolio_percent']
                    sort_col = 'accumulation_score'
                    xlabel = 'Accumulation Score (Managers × Portfolio %)'
                else:
                    sort_col = 'manager_count'
                    xlabel = 'Number of Managers Accumulating'
                
                top_accumulation = low_price_df.nlargest(15, sort_col)
                
                # Use green for accumulation (consistent color coding)
                bars = axes[0].barh(top_accumulation['ticker'], top_accumulation[sort_col], 
                                  color='darkgreen', alpha=0.7)
                axes[0].set_xlabel(xlabel, fontweight='bold')
                axes[0].set_title('Low-Price Stock Accumulation (Under $20)', fontsize=12, fontweight='bold')
                axes[0].invert_yaxis()
                axes[0].grid(True, alpha=0.3)
                
                # Add data labels for top 10 only
                max_val = top_accumulation[sort_col].max()
                for i, (ticker, val) in enumerate(zip(top_accumulation['ticker'][:10], top_accumulation[sort_col][:10])):
                    axes[0].text(val + max_val * 0.02, i, f'{val:.1f}', 
                               va='center', ha='left', fontsize=9, fontweight='bold')
            
            # Price distribution histogram for low-price stocks
            if price_col in low_price_df.columns and not low_price_df.empty:
                prices = low_price_df[price_col].dropna()
                axes[1].hist(prices, bins=20, color='darkgreen', alpha=0.7, edgecolor='black', linewidth=0.5)
                axes[1].set_xlabel('Stock Price ($)', fontweight='bold')
                axes[1].set_xlim(0, 20)  # Focus on under $20 range
                axes[1].axvline(x=10, color='red', linestyle='--', alpha=0.7, label='$10 threshold')
                axes[1].legend()
            else:
                axes[1].text(0.5, 0.5, 'Price data not available', transform=axes[1].transAxes, ha='center')
            axes[1].set_ylabel('Count', fontweight='bold')
            axes[1].set_title('Price Distribution (Under $20 Stocks)', fontsize=12, fontweight='bold')
            axes[1].grid(True, alpha=0.3)
            
            # Manager vs Low-Price Stock Heatmap
            if not low_price_df.empty and manager_col:
                # Create heatmap data: Manager × Stock accumulation
                manager_stock_data = []
                
                if manager_col == 'managers':
                    # Multiple managers per row
                    for _, row in low_price_df.head(10).iterrows():  # Top 10 stocks
                        if pd.notna(row['managers']):
                            managers = [m.strip() for m in str(row['managers']).split(',')][:5]  # Top 5 managers
                            for manager in managers:
                                manager_stock_data.append({
                                    'manager': manager[:15],  # Truncate names
                                    'ticker': row['ticker'],
                                    'value': row.get('portfolio_percent', 1)
                                })
                else:
                    # Single manager per row - aggregate by stock
                    top_stocks = low_price_df.nlargest(10, 'accumulation_score' if 'accumulation_score' in low_price_df.columns else 'manager_count')
                    for ticker in top_stocks['ticker'].unique()[:10]:
                        stock_data = low_price_df[low_price_df['ticker'] == ticker]
                        for _, row in stock_data.head(5).iterrows():  # Top 5 managers per stock
                            if pd.notna(row[manager_col]):
                                manager_stock_data.append({
                                    'manager': str(row[manager_col])[:15],
                                    'ticker': ticker,
                                    'value': row.get('portfolio_percent', 1)
                                })
                
                if manager_stock_data:
                    heatmap_df = pd.DataFrame(manager_stock_data)
                    pivot_df = heatmap_df.pivot_table(values='value', index='manager', columns='ticker', fill_value=0)
                    
                    im = axes[2].imshow(pivot_df.values, cmap='Greens', aspect='auto', interpolation='nearest')
                    axes[2].set_xticks(range(len(pivot_df.columns)))
                    axes[2].set_xticklabels(pivot_df.columns, rotation=45, ha='right', fontsize=9)
                    axes[2].set_yticks(range(len(pivot_df.index)))
                    axes[2].set_yticklabels(pivot_df.index, fontsize=9)
                    axes[2].set_title('Manager × Low-Price Stock Accumulation', fontsize=12, fontweight='bold')
                    
                    # Add colorbar
                    plt.colorbar(im, ax=axes[2], label='Portfolio %', shrink=0.8)
                else:
                    axes[2].text(0.5, 0.5, 'No accumulation data available', transform=axes[2].transAxes, ha='center')
            else:
                axes[2].text(0.5, 0.5, 'No accumulation data available', transform=axes[2].transAxes, ha='center')
            
            # Create full-sized comprehensive opportunities table
            axes[3].axis('off')
            if not low_price_df.empty:
                # Calculate required metrics
                avg_price = low_price_df[price_col].mean() if price_col in low_price_df.columns else 0
                total_positions = len(low_price_df)
                unique_managers = low_price_df[manager_col].nunique() if manager_col else 0
                
                # Build comprehensive opportunities table data
                table_data = []
                headers = ['Ticker', 'Price', 'Managers', 'Score', 'Top Manager']
                
                # Get top 10 opportunities with full details
                sort_col = 'accumulation_score' if 'accumulation_score' in low_price_df.columns else 'manager_count'
                top_10 = low_price_df.nlargest(10, sort_col)
                
                # Add individual stock rows
                for _, stock in top_10.iterrows():
                    # Find top manager for this stock
                    stock_managers = []
                    if manager_col and pd.notna(stock.get(manager_col)):
                        if manager_col == 'managers':
                            stock_managers = [m.strip() for m in str(stock[manager_col]).split(',') if m.strip()]
                        else:
                            stock_managers = [str(stock[manager_col])]
                    top_manager = stock_managers[0][:12] if stock_managers else 'N/A'
                    
                    table_data.append([
                        stock['ticker'],
                        f"${stock.get(price_col, 0):.2f}",
                        str(stock.get('manager_count', 1)),
                        f"{stock.get(sort_col, 0):.1f}",
                        top_manager
                    ])
                
                # Add separator row of dashes
                table_data.append(['—————', '—————', '—————', '—————', '—————'])
                
                # Add comprehensive summary row
                table_data.append([
                    'TOTAL',
                    f"${avg_price:.2f}",
                    f"{unique_managers}",
                    f"{total_positions}",
                    time_period[:12]
                ])
                
                # Create full-sized table that fills the entire panel
                table = axes[3].table(cellText=table_data, colLabels=headers,
                                     loc='center', cellLoc='center')
                table.auto_set_font_size(False)
                table.set_fontsize(10)  # Reasonable font size
                table.scale(1.0, 1.8)  # Reasonable scaling with taller rows
                
                # Style the header row
                for i in range(len(headers)):
                    table[(0, i)].set_facecolor('#4472C4')
                    table[(0, i)].set_text_props(weight='bold', color='white')
                
                # Style separator row (second to last data row)
                sep_row_idx = len(table_data) - 1  # Separator is second to last in table_data
                if sep_row_idx >= 0:
                    for i in range(len(headers)):
                        table[(sep_row_idx + 1, i)].set_facecolor('#E0E0E0')  # +1 for header offset
                
                # Style summary row (last data row)
                summary_row_idx = len(table_data)  # Last row in table_data
                if summary_row_idx >= 0:
                    for i in range(len(headers)):
                        table[(summary_row_idx, i)].set_facecolor('#F0F0F0')  # Header already at 0
                        table[(summary_row_idx, i)].set_text_props(weight='bold')
                
                # Center-align all columns as requested (only for data rows, not header)
                for row in range(1, len(table_data) + 1):
                    for col in range(len(headers)):
                        if row <= len(table_data):  # Safety check
                            table[(row, col)].set_text_props(ha='center')
                
                # Make table fill more of the available space
                table.auto_set_column_width(col=list(range(len(headers))))
            else:
                # No data available
                summary_text = f"No Low-Price Positions Found\nAnalysis Period: {time_period}"
                axes[3].text(0.5, 0.5, summary_text, transform=axes[3].transAxes,
                           ha='center', va='center', fontsize=14, fontweight='bold',
                           bbox=dict(boxstyle='round,pad=0.5', facecolor='lightcoral', alpha=0.8))
            
            plt.suptitle(f'Low-Price Stock Accumulation Analysis ({time_period})', 
                        fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            output_path = self.output_dir / "low_price_accumulation_current.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating new positions chart: {e}")
            return None
    
    def create_portfolio_changes_chart(self, df: pd.DataFrame, time_period: str = "Last 3 Quarters") -> str:
        """Create portfolio concentration changes visualization."""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            axes = axes.flatten()
            
            # Use concentration_score as the metric
            score_col = 'concentration_score' if 'concentration_score' in df.columns else 'change_pct'
            
            if score_col in df.columns:
                top_increases = df.nlargest(10, score_col)
                bars = axes[0].barh(top_increases['ticker'], top_increases[score_col], 
                                  color='green', alpha=0.7)
                axes[0].set_xlabel('Concentration Score', fontweight='bold')
                axes[0].set_title('Highest Concentration Scores', fontsize=12, fontweight='bold')
                axes[0].invert_yaxis()
                axes[0].grid(True, alpha=0.3)
                
                max_score = top_increases[score_col].max()
                axes[0].set_xlim(0, max_score * 1.1)  # Add 10% padding
                for i, (ticker, score) in enumerate(zip(top_increases['ticker'], top_increases[score_col])):
                    axes[0].text(score + max_score * 0.02, i, f'{score:.1f}', 
                               va='center', ha='left', fontsize=9, fontweight='bold')
                
                top_decreases = df.nsmallest(10, score_col)
                bars = axes[1].barh(top_decreases['ticker'], top_decreases[score_col], 
                                  color='red', alpha=0.7)
                axes[1].set_xlabel('Concentration Score', fontweight='bold')
                axes[1].set_title('Lowest Concentration Scores', fontsize=12, fontweight='bold')
                axes[1].invert_yaxis()
                axes[1].grid(True, alpha=0.3)
                
                min_score = top_decreases[score_col].min()
                axes[1].set_xlim(min_score * 1.1, 0)  # Adjust limits for negative values
                for i, (ticker, score) in enumerate(zip(top_decreases['ticker'], top_decreases[score_col])):
                    # Position labels correctly for negative values
                    label_x = score + (abs(min_score) * 0.02 if score < 0 else min_score * 0.02)
                    axes[1].text(label_x, i, f'{score:.1f}', 
                               va='center', ha='left', fontsize=9, fontweight='bold')
                
                axes[2].hist(df[score_col], bins=30, color='blue', alpha=0.7, edgecolor='black', linewidth=0.5)
                axes[2].set_xlabel('Concentration Score', fontweight='bold')
                axes[2].set_ylabel('Number of Positions', fontweight='bold')
                axes[2].set_title('Distribution of Concentration Scores', fontsize=12, fontweight='bold')
                axes[2].grid(True, alpha=0.3)
                
                # Focus on where the data actually is
                score_95th = df[score_col].quantile(0.95)
                axes[2].set_xlim(df[score_col].min() - 5, score_95th + 5)
                
                mean_score = df[score_col].mean()
                axes[2].axvline(mean_score, color='red', linestyle='--', 
                              label=f'Mean: {mean_score:.1f}', linewidth=2)
                axes[2].legend()
                
                if 'change_type' in df.columns:
                    change_counts = df['change_type'].value_counts()
                    colors = ['darkgreen', 'orange', 'red', 'purple', 'brown'][:len(change_counts)]
                    wedges, texts, autotexts = axes[3].pie(change_counts.values, labels=change_counts.index, 
                                                         autopct='%1.1f%%', startangle=90, colors=colors,
                                                         textprops={'fontsize': 10, 'fontweight': 'bold'})
                    axes[3].set_title('Portfolio Action Summary', fontsize=12, fontweight='bold')
                    
                    # Improve text visibility
                    for autotext in autotexts:
                        autotext.set_color('white')
                        autotext.set_fontweight('bold')
                else:
                    # Fallback: show score ranges with better formatting
                    high_score = len(df[df[score_col] >= 50])
                    med_score = len(df[(df[score_col] >= 25) & (df[score_col] < 50)])
                    low_score = len(df[df[score_col] < 25])
                    
                    labels = ['High Concentration', 'Medium Concentration', 'Low Concentration']
                    sizes = [high_score, med_score, low_score]
                    colors = ['darkgreen', 'orange', 'lightcoral']
                    
                    # Only show non-zero segments
                    non_zero_data = [(label, size, color) for label, size, color in zip(labels, sizes, colors) if size > 0]
                    if non_zero_data:
                        labels_nz, sizes_nz, colors_nz = zip(*non_zero_data)
                        wedges, texts, autotexts = axes[3].pie(sizes_nz, labels=labels_nz, colors=colors_nz, 
                                                             autopct='%1.1f%%', startangle=90,
                                                             textprops={'fontsize': 10, 'fontweight': 'bold'})
                        axes[3].set_title('Concentration Score Ranges', fontsize=12, fontweight='bold')
                        
                        # Improve text visibility
                        for autotext in autotexts:
                            autotext.set_color('white')
                            autotext.set_fontweight('bold')
            
            plt.suptitle(f'Portfolio Concentration Changes ({time_period})', 
                        fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            output_path = self.output_dir / "portfolio_changes_current.png"
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"Error creating portfolio changes chart: {e}")
            return None
    
    def _collect_price_dfs(self, results: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Collect all price-based DataFrames."""
        price_dfs = {}
        price_keywords = ['stocks_under_$5', 'stocks_under_$10', 'stocks_under_$20', 
                         'stocks_under_$50', 'stocks_under_$100']
        
        for key, df in results.items():
            if any(keyword in key for keyword in price_keywords) and not df.empty:
                price_dfs[key] = df
        
        return price_dfs
    
    def _create_empty_chart(self, message: str) -> str:
        """Create an empty chart with a message."""
        fig, ax = plt.subplots(1, 1, figsize=(10, 6))
        ax.text(0.5, 0.5, message, transform=ax.transAxes, 
                ha='center', va='center', fontsize=16, fontweight='bold')
        ax.axis('off')
        
        output_path = self.output_dir / "empty_chart.png"
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(output_path)