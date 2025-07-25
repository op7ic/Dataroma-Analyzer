#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Historical Visualizer

Creates long-term trend visualizations across market cycles.
Historical data visualization module that creates beautiful graphs 
and charts for multi-decade investment analysis.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import matplotlib.dates as mdates
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


class HistoricalVisualizer:
    """Creates visualizations for historical investment data."""
    
    def __init__(self, output_dir: str = "analysis/historical/visuals"):
        """Initialize visualizer with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.crisis_colors = {
            "2008_financial": "#FF6B6B",
            "2020_covid": "#4ECDC4", 
            "2022_inflation": "#FFE66D"
        }
        
    def create_all_visualizations(self, data: Dict[str, pd.DataFrame]) -> Dict[str, str]:
        """Create all historical visualizations."""
        viz_paths = {}
        
        if "quarterly_activity_timeline" in data:
            viz_paths["timeline"] = self.plot_activity_timeline(data["quarterly_activity_timeline"])
        
        if "manager_track_records" in data:
            viz_paths["manager_performance"] = self.plot_manager_performance(data["manager_track_records"])
        
        if "crisis_response_analysis" in data:
            viz_paths["crisis_comparison"] = self.plot_crisis_comparison(data["crisis_response_analysis"])
        
        if "multi_decade_conviction" in data:
            viz_paths["conviction_plays"] = self.plot_conviction_plays(data["multi_decade_conviction"])
        
        if "stock_life_cycles" in data:
            viz_paths["life_cycles"] = self.plot_stock_life_cycles(data["stock_life_cycles"])
        
        if "sector_rotation_patterns" in data:
            viz_paths["sector_rotation"] = self.plot_sector_rotation(data["sector_rotation_patterns"])
        
        return viz_paths
    
    def plot_activity_timeline(self, df: pd.DataFrame) -> str:
        """Create comprehensive activity timeline visualization."""
        fig = plt.figure(figsize=(18, 14))
        gs = fig.add_gridspec(3, 1, height_ratios=[1.2, 1, 1], hspace=0.25)
        axes = [fig.add_subplot(gs[i]) for i in range(3)]
        
        df = df.sort_values(['year', 'quarter'])
        
        crisis_periods = {
            "2008 Financial Crisis": ["Q3 2008", "Q4 2008", "Q1 2009", "Q2 2009"],
            "COVID-19 Pandemic": ["Q1 2020", "Q2 2020"],
            "2022 Inflation Crisis": ["Q1 2022", "Q2 2022", "Q3 2022"]
        }
        
        ax1 = axes[0]
        ax1.fill_between(range(len(df)), df['total_actions'], alpha=0.3, label='Total Actions', color='steelblue')
        ax1.plot(range(len(df)), df['total_actions'], linewidth=2, color='darkblue')
        
        crisis_colors = ['#FF6B6B', '#4ECDC4', '#FFE66D']
        for (crisis, periods), color in zip(crisis_periods.items(), crisis_colors):
            crisis_indices = []
            for period in periods:
                if period in df['period'].values:
                    idx = df[df['period'] == period].index[0]
                    crisis_indices.append(idx)
                    ax1.axvspan(idx-0.5, idx+0.5, alpha=0.3, color=color)
            
            if crisis_indices:
                mid_idx = crisis_indices[len(crisis_indices)//2]
                ax1.text(mid_idx, ax1.get_ylim()[1] * 0.95, crisis, 
                        ha='center', va='top', fontsize=10, fontweight='bold',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor=color, alpha=0.7))
        
        # Dynamic date range from data
        min_year = df['year'].min()
        max_year = df['year'].max()
        ax1.set_title(f'Investment Activity Volume Over Time ({min_year}-{max_year})', fontsize=14, fontweight='bold')
        ax1.set_ylabel('Total Actions', fontweight='bold')
        ax1.grid(True, alpha=0.3)
        
        ax2 = axes[1]
        buy_data = df['buy_actions'] + df['add_actions']
        sell_data = df['sell_actions'] + df['reduce_actions']
        
        width = 0.8
        x_positions = np.arange(len(df))
        
        ax2.bar(x_positions, buy_data, width=width,
                label='Buy/Add', color='green', alpha=0.7)
        ax2.bar(x_positions, -sell_data, width=width,
                label='Sell/Reduce', color='red', alpha=0.7)
        ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.8)
        
        ax2.set_title('Buy vs Sell Activity by Quarter', fontsize=14, fontweight='bold')
        ax2.set_ylabel('Number of Actions', fontweight='bold')
        ax2.legend(loc='upper left', bbox_to_anchor=(1.02, 1))
        ax2.grid(True, alpha=0.3)
        
        ax3 = axes[2]
        cumulative_net = df['net_activity'].cumsum()
        ax3.plot(range(len(df)), cumulative_net, 
                linewidth=3, color='purple', label='Cumulative Net Activity')
        ax3.fill_between(range(len(df)), 0, cumulative_net, 
                        where=(cumulative_net > 0), 
                        color='green', alpha=0.3, label='Net Buying')
        ax3.fill_between(range(len(df)), 0, cumulative_net, 
                        where=(cumulative_net <= 0), 
                        color='red', alpha=0.3, label='Net Selling')
        
        ax3.set_title('Cumulative Net Market Sentiment', fontsize=14, fontweight='bold')
        ax3.set_ylabel('Cumulative Net Actions', fontweight='bold')
        ax3.set_xlabel('Quarter', fontweight='bold')
        ax3.legend(loc='best')
        ax3.grid(True, alpha=0.3)
        
        for ax in axes:
            tick_spacing = 8
            ax.set_xticks(range(0, len(df), tick_spacing))
            ax.set_xticklabels(df['period'].iloc[::tick_spacing], rotation=45, ha='right', fontsize=9)
            
            ax.set_xticks(range(0, len(df), 4), minor=True)
            ax.tick_params(axis='x', which='minor', length=2)
        
        plt.suptitle('Quarterly Investment Activity Timeline Analysis', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        path = self.output_dir / "quarterly_activity_timeline.png"
        plt.savefig(path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(path)
    
    def plot_manager_performance(self, df: pd.DataFrame) -> str:
        """Create manager track record visualization."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        top_managers = df.nlargest(20, 'track_record_score')
        
        ax1 = axes[0, 0]
        managers = top_managers.sort_values('first_year')
        y_positions = range(len(managers))
        
        for i, (_, manager) in enumerate(managers.iterrows()):
            ax1.barh(i, manager['last_year'] - manager['first_year'], 
                    left=manager['first_year'], height=0.8,
                    alpha=0.7, label=str(manager['manager_name'])[:20])
            
        ax1.set_yticks(y_positions)
        ax1.set_yticklabels([m[:25] for m in managers['manager_name']], fontsize=9)
        ax1.set_xlabel('Year')
        ax1.set_title('Manager Activity Timelines', fontsize=14, fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='x')
        
        ax2 = axes[0, 1]
        crisis_data = top_managers[['manager_name', 'crisis_buying_ratio', 'total_crisis_actions']].copy()
        crisis_data = crisis_data[crisis_data['total_crisis_actions'] > 0].nlargest(15, 'crisis_buying_ratio')
        
        if not crisis_data.empty:
            bars = ax2.barh(range(len(crisis_data)), crisis_data['crisis_buying_ratio'])
            
            for i, (bar, ratio) in enumerate(zip(bars, crisis_data['crisis_buying_ratio'])):
                if ratio > 0.5:
                    bar.set_color('green')
                elif ratio > 0.3:
                    bar.set_color('yellow')
                else:
                    bar.set_color('red')
            
            ax2.set_yticks(range(len(crisis_data)))
            ax2.set_yticklabels([m[:25] for m in crisis_data['manager_name']], fontsize=9)
            ax2.set_xlabel('Crisis Buying Ratio')
            ax2.set_title('Crisis Response: Buying vs Selling Ratio', fontsize=14, fontweight='bold')
            ax2.grid(True, alpha=0.3, axis='x')
        
        ax3 = axes[1, 0]
        consistency_data = top_managers[['manager_name', 'consistency_score', 'years_active']].copy()
        consistency_data = consistency_data[consistency_data['years_active'] >= 5].nlargest(15, 'consistency_score')
        
        if not consistency_data.empty:
            scatter = ax3.scatter(consistency_data['years_active'], 
                                consistency_data['consistency_score'],
                                s=200, alpha=0.6, c=consistency_data.index, cmap='viridis')
            
            for _, row in consistency_data.iterrows():
                ax3.annotate(row['manager_name'].split('-')[0][:10], 
                           (row['years_active'], row['consistency_score']),
                           fontsize=8, alpha=0.8)
            
            ax3.set_xlabel('Years Active')
            ax3.set_ylabel('Consistency Score')
            ax3.set_title('Manager Consistency Over Time', fontsize=14, fontweight='bold')
            ax3.grid(True, alpha=0.3)
        
        ax4 = axes[1, 1]
        action_cols = ['buy_actions', 'add_actions', 'reduce_actions', 'sell_actions']
        action_data = top_managers[action_cols].head(10)
        
        action_data.plot(kind='bar', stacked=True, ax=ax4, 
                        color=['green', 'lightgreen', 'orange', 'red'])
        ax4.set_xticklabels([m[:15] for m in top_managers['manager_name'].head(10)], 
                           rotation=45, ha='right')
        ax4.set_ylabel('Number of Actions')
        ax4.set_title('Action Distribution by Top Managers', fontsize=14, fontweight='bold')
        ax4.legend(['Buy', 'Add', 'Reduce', 'Sell'], loc='upper right')
        ax4.grid(True, alpha=0.3, axis='y')
        
        # Dynamic date range from manager data
        min_year = df['first_year'].min()
        max_year = df['last_year'].max()
        plt.suptitle(f'Manager Track Records Analysis ({min_year}-{max_year})', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        path = self.output_dir / "manager_performance_historical.png"
        plt.savefig(path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(path)
    
    def plot_crisis_comparison(self, df: pd.DataFrame) -> str:
        """Create crisis response comparison visualization."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 10))
        
        ax1 = axes[0, 0]
        action_types = ['buy_actions', 'add_actions', 'reduce_actions', 'sell_actions']
        crisis_names = df['crisis'].values
        
        x = np.arange(len(crisis_names))
        width = 0.2
        
        colors = ['darkgreen', 'lightgreen', 'orange', 'red']
        for i, action in enumerate(action_types):
            ax1.bar(x + i*width, df[action], width, 
                   label=action.replace('_', ' ').title(), color=colors[i])
        
        ax1.set_xlabel('Crisis Period', fontweight='bold')
        ax1.set_ylabel('Number of Actions', fontweight='bold')
        ax1.set_title('Investment Actions During Crisis Periods', fontsize=14, fontweight='bold')
        ax1.set_xticks(x + width * 1.5)
        ax1.set_xticklabels([c.replace('_', ' ').title() for c in crisis_names], rotation=15)
        ax1.legend(loc='upper left', bbox_to_anchor=(1.02, 1), 
                  labels=['Buy', 'Add', 'Reduce', 'Sell'])
        ax1.grid(True, alpha=0.3, axis='y')
        
        ax2 = axes[0, 1]
        ax2.bar(range(len(df)), df['buy_ratio'], color='green', alpha=0.7, label='Buy Ratio')
        ax2.bar(range(len(df)), df['sell_ratio'], bottom=df['buy_ratio'], 
               color='red', alpha=0.7, label='Sell Ratio')
        
        from matplotlib.ticker import PercentFormatter
        ax2.set_ylabel('Percentage', fontweight='bold')
        ax2.set_ylim(0, 1)
        ax2.yaxis.set_major_formatter(PercentFormatter(1.0))
        
        ax2.set_title('Buy vs Sell Ratios During Crises', fontsize=14, fontweight='bold')
        ax2.set_xticks(range(len(df)))
        ax2.set_xticklabels([c.replace('_', ' ').title() for c in df['crisis']], rotation=15)
        ax2.legend()
        ax2.grid(True, alpha=0.3, axis='y')
        
        ax3 = axes[1, 0]
        bars = ax3.bar(range(len(df)), df['unique_managers'], color='purple', alpha=0.7)
        ax3.set_ylabel('Number of Active Managers', fontweight='bold')
        ax3.set_title('Manager Participation During Crises', fontsize=14, fontweight='bold')
        ax3.set_xticks(range(len(df)))
        ax3.set_xticklabels([c.replace('_', ' ').title() for c in df['crisis']], rotation=15)
        ax3.grid(True, alpha=0.3, axis='y')
        
        for i, (crisis, count) in enumerate(zip(df['crisis'], df['unique_managers'])):
            ax3.text(i, count + 1, f'{count}', ha='center', va='bottom', fontweight='bold')
        
        ax4 = axes[1, 1]
        ax4.axis('off')
        
        table_data = []
        for _, crisis in df.iterrows():
            crisis_name = crisis['crisis'].replace('_', ' ').title()
            bought_stocks = crisis['most_bought'] if pd.notna(crisis['most_bought']) else 'N/A'
            sold_stocks = crisis['most_sold'] if pd.notna(crisis['most_sold']) else 'N/A'
            
            if len(bought_stocks) > 30:
                bought_stocks = bought_stocks[:27] + '...'
            if len(sold_stocks) > 30:
                sold_stocks = sold_stocks[:27] + '...'
            
            table_data.append([crisis_name, bought_stocks, sold_stocks])
        
        table = ax4.table(cellText=table_data, 
                         colLabels=['Crisis Period', 'Most Bought', 'Most Sold'],
                         cellLoc='left', loc='center',
                         bbox=[0, 0, 1, 1])
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 2)
        
        for (row, col), cell in table.get_celld().items():
            if row == 0:
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor('#4472C4')
            else:
                cell.set_facecolor('#F2F2F2' if row % 2 == 0 else 'white')
        
        ax4.set_title('Most Traded Stocks During Each Crisis', fontsize=14, fontweight='bold')
        
        plt.suptitle('Crisis Response Analysis: Comparing Market Behaviors', 
                    fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        path = self.output_dir / "crisis_response_comparison.png"
        plt.savefig(path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(path)
    
    def plot_conviction_plays(self, df: pd.DataFrame) -> str:
        """Create multi-decade conviction visualization."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        top_conviction = df.nlargest(20, 'conviction_score')
        
        ax1 = axes[0, 0]
        for i, (_, stock) in enumerate(top_conviction.head(10).iterrows()):
            years = stock['years_held']
            bar = ax1.barh(i, years, height=0.8, alpha=0.7, color='darkblue')
            label = f"{stock['ticker']}"
            if 'company_name' in stock and pd.notna(stock['company_name']):
                label += f" ({stock['company_name'][:15]})"
            ax1.text(years/2, i, label, ha='center', va='center', fontsize=8, fontweight='bold')
            
            ax1.text(years + 0.2, i, f'{years:.1f}y', va='center', ha='left', fontsize=9, fontweight='bold')
        
        ax1.set_yticks(range(10))
        ax1.set_yticklabels([''] * 10)
        ax1.set_xlabel('Years Held', fontweight='bold')
        ax1.set_title('Top 10 Multi-Decade Conviction Plays', fontsize=14, fontweight='bold')
        ax1.grid(True, alpha=0.3, axis='x')
        ax1.set_xlim(0, ax1.get_xlim()[1] * 1.1)  # Add padding for labels
        
        ax2 = axes[0, 1]
        scatter = ax2.scatter(top_conviction['years_held'], 
                            top_conviction['conviction_score'],
                            s=top_conviction['total_managers']*20,
                            alpha=0.6, c=top_conviction['current_holders'],
                            cmap='RdYlGn', edgecolors='black', linewidth=0.5)
        
        for idx, stock in top_conviction.head(10).iterrows():
            bubble_size = stock['total_managers'] * 20
            offset = max(5, bubble_size / 40)  # Proportional to bubble size
            ax2.annotate(stock['ticker'], 
                        (stock['years_held'], stock['conviction_score']),
                        xytext=(offset, offset), textcoords='offset points',
                        fontsize=8, fontweight='bold',
                        bbox=dict(boxstyle='round,pad=0.2', facecolor='yellow', alpha=0.7))
        
        ax2.set_xlabel('Years Held')
        ax2.set_ylabel('Conviction Score')
        ax2.set_title('Long-Term Accumulation Patterns', fontsize=14, fontweight='bold')
        ax2.grid(True, alpha=0.3)
        
        ax3 = axes[1, 0]
        holder_data = top_conviction[['ticker', 'consistent_managers', 'current_holders']].head(15)
        
        x = np.arange(len(holder_data))
        width = 0.35
        
        ax3.bar(x - width/2, holder_data['consistent_managers'], 
               width, label='Long-term Holders', color='blue', alpha=0.7)
        ax3.bar(x + width/2, holder_data['current_holders'], 
               width, label='Current Holders', color='green', alpha=0.7)
        
        ax3.set_xlabel('Stock')
        ax3.set_ylabel('Number of Managers')
        ax3.set_title('Consistent vs Current Holdings', fontsize=14, fontweight='bold')
        ax3.set_xticks(x)
        ax3.set_xticklabels(holder_data['ticker'], rotation=45, ha='right')
        ax3.legend()
        ax3.grid(True, alpha=0.3, axis='y')
        
        ax4 = axes[1, 1]
        ax4.hist(df['conviction_score'], bins=30, alpha=0.7, color='purple', edgecolor='black')
        
        mean_val = df['conviction_score'].mean()
        ax4.axvline(mean_val, color='red', linestyle='--', linewidth=2)
        
        ax4.text(mean_val + 0.05, ax4.get_ylim()[1]*0.9, f'Mean: {mean_val:.1f}', 
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8, edgecolor='red'),
                fontweight='bold', fontsize=10)
        
        ax4.set_xlabel('Conviction Score', fontweight='bold')
        ax4.set_ylabel('Number of Stocks', fontweight='bold')
        ax4.set_title('Distribution of Conviction Scores', fontsize=14, fontweight='bold')
        ax4.grid(True, alpha=0.3, axis='y')
        
        plt.suptitle('Multi-Decade Conviction Analysis', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        path = self.output_dir / "multi_decade_conviction.png"
        plt.savefig(path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(path)
    
    def plot_stock_life_cycles(self, df: pd.DataFrame) -> str:
        """Create stock life cycle visualization."""
        fig = plt.figure(figsize=(18, 14))
        gs = fig.add_gridspec(2, 2, hspace=0.3, wspace=0.3)
        axes = [fig.add_subplot(gs[i//2, i%2]) for i in range(4)]
        
        top_stocks = df.nlargest(20, 'life_cycle_score')
        
        ax1 = axes[0]
        
        decade_groups = {
            '2000s': top_stocks[top_stocks['first_year'] < 2010],
            '2010s': top_stocks[(top_stocks['first_year'] >= 2010) & (top_stocks['first_year'] < 2020)],
            '2020s': top_stocks[top_stocks['first_year'] >= 2020]
        }
        
        colors = ['#3498db', '#2ecc71', '#e74c3c']
        y_offset = 0
        decade_labels_added = set()
        
        for (decade, group), color in zip(decade_groups.items(), colors):
            for idx, (_, stock) in enumerate(group.head(5).iterrows()):
                label = decade if decade not in decade_labels_added else ""
                if label:
                    decade_labels_added.add(decade)
                
                ax1.barh(y_offset, stock['years_tracked'], 
                        left=stock['first_year'], height=0.8,
                        alpha=0.7, color=color, label=label)
                
                ax1.text(stock['first_year'] + stock['years_tracked']/2, 
                        y_offset, stock['ticker'], ha='center', va='center', 
                        fontsize=9, fontweight='bold',
                        bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.8))
                
                ax1.text(stock['first_year'] + stock['years_tracked'] + 0.5, 
                        y_offset, f"{stock['years_tracked']:.0f}y", 
                        ha='left', va='center', fontsize=8)
                
                y_offset += 1
        
        ax1.set_xlabel('Year', fontweight='bold')
        ax1.set_ylabel('Stocks (grouped by entry decade)', fontweight='bold')
        ax1.set_title('Stock Life Cycles by Entry Decade', fontsize=14, fontweight='bold')
        ax1.legend(loc='upper left', bbox_to_anchor=(1.02, 1))
        ax1.grid(True, alpha=0.3, axis='x')
        ax1.set_yticks([])
        
        ax2 = axes[1]
        
        entry_years = df.groupby('first_year').size()
        currently_held_by_year = df[df['currently_held']].groupby('first_year').size()
        
        width = 0.8
        ax2.bar(entry_years.index, entry_years.values, width=width,
               alpha=0.5, label='Total Stock Entries', color='blue')
        ax2.bar(currently_held_by_year.index, currently_held_by_year.values, width=width,
               alpha=0.8, label='Still Held Today', color='green')
        
        ax2.set_xlabel('Entry Year', fontweight='bold')
        ax2.set_ylabel('Number of Stocks', fontweight='bold')
        ax2.set_title('Stock Entry Patterns Over Time', fontsize=14, fontweight='bold')
        ax2.legend()
        ax2.grid(True, alpha=0.3, axis='y')
        
        ax3 = axes[2]
        
        buy_actions = top_stocks['total_buys'] + top_stocks['total_adds']
        sell_actions = top_stocks['total_sells'] + top_stocks['total_reduces']
        
        scatter = ax3.scatter(buy_actions, sell_actions,
                            s=top_stocks['unique_managers']*10,
                            c=top_stocks['accumulation_score'],
                            cmap='RdYlGn', alpha=0.6, edgecolors='black', linewidth=0.5)
        
        max_val = max(ax3.get_xlim()[1], ax3.get_ylim()[1])
        ax3.plot([0, max_val], [0, max_val], 'k--', alpha=0.3, label='Neutral Line')
        
        for _, stock in top_stocks.iterrows():
            if stock['accumulation_score'] > 70 or stock['accumulation_score'] < -70:
                ax3.annotate(stock['ticker'], 
                           (stock['total_buys'] + stock['total_adds'],
                            stock['total_sells'] + stock['total_reduces']),
                           xytext=(5, 5), textcoords='offset points',
                           fontsize=8, fontweight='bold',
                           bbox=dict(boxstyle='round,pad=0.2', 
                                   facecolor='yellow' if stock['accumulation_score'] > 0 else 'lightcoral', 
                                   alpha=0.8))
        
        ax3.set_xlabel('Buy + Add Actions', fontweight='bold')
        ax3.set_ylabel('Sell + Reduce Actions', fontweight='bold')
        ax3.set_title('Accumulation vs Distribution Patterns', fontsize=14, fontweight='bold')
        cbar = plt.colorbar(scatter, ax=ax3, label='Accumulation Score')
        ax3.grid(True, alpha=0.3)
        
        ax3.text(0.02, 0.98, 'Above line = Net Distribution\nBelow line = Net Accumulation', 
                transform=ax3.transAxes, va='top', fontsize=9,
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.8))
        
        ax4 = axes[3]
        
        interesting_stocks = top_stocks.nlargest(8, 'unique_managers')
        
        bars = ax4.bar(range(len(interesting_stocks)), interesting_stocks['unique_managers'], 
                       color='purple', alpha=0.7)
        
        for i, (_, stock) in enumerate(interesting_stocks.iterrows()):
            height = stock['unique_managers']
            ax4.text(i, height + 0.5, f'{int(height)}', 
                    ha='center', va='bottom', fontsize=9, fontweight='bold')
            
            ax4.text(i, height/2, f"{stock['years_tracked']:.0f}y", 
                    ha='center', va='center', fontsize=8, color='white', fontweight='bold')
        
        ax4.set_xticks(range(len(interesting_stocks)))
        ax4.set_xticklabels(interesting_stocks['ticker'], rotation=45, ha='right')
        ax4.set_ylabel('Unique Managers (All Time)', fontweight='bold')
        ax4.set_title('Stocks with Highest Manager Interest', fontsize=14, fontweight='bold')
        ax4.grid(True, alpha=0.3, axis='y')
        
        plt.suptitle('Stock Life Cycle Analysis', fontsize=16, fontweight='bold')
        plt.tight_layout()
        
        path = self.output_dir / "stock_life_cycles.png"
        plt.savefig(path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(path)
    
    def plot_sector_rotation(self, df: pd.DataFrame) -> str:
        """Create sector rotation visualization."""
        if df.empty:
            return ""
        
        fig, axes = plt.subplots(2, 1, figsize=(16, 10))
        
        pivot_data = df.pivot_table(values='net_flow', index='period', columns='sector', fill_value=0)
        
        df['year'] = df['period'].str.extract(r'(\d{4})').astype(int)
        
        ax1 = axes[0]
        
        sampled_data = pivot_data.iloc[::4, :]
        
        sns.heatmap(sampled_data.T, cmap='RdYlGn', center=0, 
                   cbar_kws={'label': 'Net Flow'}, ax=ax1,
                   xticklabels=True, yticklabels=True)
        
        ax1.set_title('Sector Rotation Heatmap (Net Flow by Quarter)', fontsize=14, fontweight='bold')
        ax1.set_xlabel('Quarter')
        ax1.set_ylabel('Sector')
        
        ax2 = axes[1]
        
        for sector in pivot_data.columns:
            if sector != 'Other':
                cumsum = pivot_data[sector].cumsum()
                ax2.plot(range(len(cumsum)), cumsum, linewidth=2, label=sector)
        
        ax2.set_title('Cumulative Sector Flows Over Time', fontsize=14, fontweight='bold')
        ax2.set_xlabel('Quarter')
        ax2.set_ylabel('Cumulative Net Flow')
        ax2.legend(loc='upper left')
        ax2.grid(True, alpha=0.3)
        
        step = max(1, len(pivot_data) // 20)
        ax2.set_xticks(range(0, len(pivot_data), step))
        ax2.set_xticklabels(pivot_data.index[::step], rotation=45, ha='right')
        
        plt.tight_layout()
        
        path = self.output_dir / "sector_rotation.png"
        plt.savefig(path, dpi=300, bbox_inches='tight')
        plt.close()
        
        return str(path)