"""
Advanced historical analysis for deep insights from 18+ years of data.
Focus on multi-decade patterns, manager excellence, and predictive signals.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from collections import defaultdict, Counter
from datetime import datetime

from .base_analyzer import BaseAnalyzer, MultiAnalyzer
from ..utils.calculations import TextAnalysisUtils
from ..data.data_loader import DataLoader


class AdvancedHistoricalAnalyzer(MultiAnalyzer):
    """Advanced analysis for multi-decade patterns and manager excellence."""
    
    def __init__(self, data_loader: DataLoader) -> None:
        """Initialize with data loader."""
        super().__init__(data_loader)
    
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
        """
        if (self.data.history_df is None or self.data.history_df.empty):
            return pd.DataFrame()
        
        print("🎯 Analyzing Multi-Decade Conviction Plays...")
        
        long_term_analysis = {}
        
        for ticker in self.data.history_df['ticker'].unique():
            ticker_data = self.data.history_df[self.data.history_df['ticker'] == ticker]
            
            years_with_activity = set()
            for period in ticker_data['period']:
                if 'Q' in str(period):
                    year = str(period).split()[1]
                    years_with_activity.add(year)
            
            years_held = len(years_with_activity)
            
            if years_held >= 5:
                managers = ticker_data['manager_id'].unique().tolist()
                
                manager_consistency = {}
                for manager in managers:
                    manager_data = ticker_data[ticker_data['manager_id'] == manager]
                    manager_years = set()
                    for period in manager_data['period']:
                        if 'Q' in str(period):
                            year = str(period).split()[1]
                            manager_years.add(year)
                    
                    consistency_score = len(manager_years) / years_held
                    if consistency_score >= 0.3:
                        manager_consistency[manager] = {
                            'consistency_score': consistency_score,
                            'years_involved': len(manager_years),
                            'total_activities': len(manager_data)
                        }
                
                if manager_consistency:
                    current_holders = []
                    total_value = 0
                    if self.data.holdings_df is not None:
                        current_holding = self.data.holdings_df[
                            self.data.holdings_df['ticker'] == ticker
                        ]
                        if not current_holding.empty:
                            total_value = current_holding['value'].sum()
                            current_holders = current_holding['manager_id'].tolist()
                    
                    buy_actions = len(ticker_data[ticker_data['action_type'] == 'Buy'])
                    add_actions = len(ticker_data[ticker_data['action_type'] == 'Add'])
                    reduce_actions = len(ticker_data[ticker_data['action_type'] == 'Reduce'])
                    
                    conviction_score = (buy_actions + add_actions * 0.7) / max(1, reduce_actions * 0.5)
                    
                    long_term_analysis[ticker] = {
                        'years_held': years_held,
                        'consistent_managers': len(manager_consistency),
                        'total_managers': len(managers),
                        'manager_details': manager_consistency,
                        'current_holders': len(current_holders),
                        'total_value': total_value,
                        'conviction_score': conviction_score,
                        'total_activities': len(ticker_data),
                        'buy_actions': buy_actions,
                        'periods_active': len(ticker_data['period'].unique())
                    }
        
        if not long_term_analysis:
            return pd.DataFrame()
        
        conviction_df = pd.DataFrame.from_dict(long_term_analysis, orient='index')
        conviction_df = conviction_df.sort_values(by=['years_held', 'conviction_score'], ascending=[False, False])
        
        if self.data.holdings_df is not None and 'stock' in self.data.holdings_df.columns:
            company_names = self.data.holdings_df.groupby('ticker')['stock'].first()
            conviction_df = conviction_df.join(company_names.to_frame('company_name'))
        
        conviction_df['top_managers'] = conviction_df['manager_details'].apply(
            lambda x: ', '.join([
                self.data.manager_names.get(mgr_id, mgr_id) 
                for mgr_id, details in sorted(x.items(), 
                key=lambda item: item[1]['consistency_score'], reverse=True)[:3]
            ]) if x else ''
        )
        
        conviction_df['conviction_type'] = 'Long-term Hold'
        conviction_df.loc[conviction_df['years_held'] >= 10, 'conviction_type'] = 'Decade+ Conviction'
        conviction_df.loc[conviction_df['years_held'] >= 15, 'conviction_type'] = 'Multi-Decade Champion'
        conviction_df.loc[
            (conviction_df['consistent_managers'] >= 3) & (conviction_df['years_held'] >= 8),
            'conviction_type'
        ] = 'Consensus Champion'
        
        return self.format_output(conviction_df.reset_index().rename(columns={'index': 'ticker'})).head(50)
    
    def analyze_crisis_alpha_generators(self) -> pd.DataFrame:
        """
        Identify managers who consistently generate alpha during crisis periods.
        """
        if (self.data.history_df is None or self.data.history_df.empty):
            return pd.DataFrame()
        
        print("⚠️ Analyzing Crisis Alpha Generators...")
        
        crisis_periods = {
            'Financial Crisis 2008': ['Q1 2008', 'Q2 2008', 'Q3 2008', 'Q4 2008'],
            'COVID Crisis 2020': ['Q1 2020', 'Q2 2020'],
            'Inflation Crisis 2022': ['Q1 2022', 'Q2 2022', 'Q3 2022']
        }
        
        manager_crisis_performance = defaultdict(lambda: {
            'total_crisis_activities': 0,
            'buy_during_crisis': 0,
            'crisis_periods_active': 0,
            'crisis_details': {}
        })
        
        for crisis_name, periods in crisis_periods.items():
            crisis_activities = self.data.history_df[
                self.data.history_df['period'].isin(periods)
            ]
            
            if not crisis_activities.empty:
                crisis_manager_actions = crisis_activities.groupby('manager_id').agg({
                    'action_type': lambda x: dict(x.value_counts()),
                    'ticker': 'nunique',
                    'period': 'nunique'
                })
                
                for manager_id, data in crisis_manager_actions.iterrows():
                    # Get action type counts for this manager during crisis
                    manager_crisis_data = crisis_activities[crisis_activities['manager_id'] == manager_id]
                    action_counts = manager_crisis_data['action_type'].value_counts().to_dict()
                    
                    buy_actions = action_counts.get('Buy', 0) + action_counts.get('Add', 0)
                    total_actions = len(manager_crisis_data)
                    
                    manager_crisis_performance[manager_id]['total_crisis_activities'] += total_actions
                    manager_crisis_performance[manager_id]['buy_during_crisis'] += buy_actions
                    manager_crisis_performance[manager_id]['crisis_periods_active'] += 1
                    manager_crisis_performance[manager_id]['crisis_details'][crisis_name] = {
                        'actions': action_counts,
                        'unique_stocks': int(data['ticker']),
                        'buy_ratio': float(buy_actions / max(1, total_actions))
                    }
        
        crisis_df = pd.DataFrame.from_dict(manager_crisis_performance, orient='index')
        
        if crisis_df.empty:
            return pd.DataFrame()
        
        crisis_df['crisis_alpha_score'] = (
            (crisis_df['buy_during_crisis'] / crisis_df['total_crisis_activities']) * 
            crisis_df['crisis_periods_active'] * 10
        ).fillna(0)
        
        crisis_df = crisis_df[
            (crisis_df['total_crisis_activities'] >= 5) &
            (crisis_df['crisis_periods_active'] >= 2)
        ]
        
        if crisis_df.empty:
            return pd.DataFrame()
        
        crisis_df['manager_name'] = crisis_df.index.map(
            lambda x: self.data.manager_names.get(x, x)
        )
        
        if self.data.holdings_df is not None:
            portfolio_sizes = self.data.holdings_df.groupby('manager_id')['value'].sum()
            crisis_df = crisis_df.join(portfolio_sizes.to_frame('current_portfolio_value'))
            crisis_df['current_portfolio_value'] = crisis_df['current_portfolio_value'].fillna(0)
        
        crisis_df = crisis_df.sort_values(by='crisis_alpha_score', ascending=False)
        
        return self.format_output(crisis_df.reset_index().rename(columns={'index': 'manager_id'})).head(30)
    
    def analyze_position_sizing_mastery(self) -> pd.DataFrame:
        """
        Analyze optimal position sizing patterns by manager.
        Identify who sizes positions optimally for maximum returns.
        """
        if (self.data.holdings_df is None or 
            self.data.holdings_df.empty or 
            'portfolio_percent' not in self.data.holdings_df.columns):
            return pd.DataFrame()
        
        print("📊 Analyzing Position Sizing Mastery...")
        
        manager_sizing_analysis = {}
        
        for manager_id in self.data.holdings_df['manager_id'].unique():
            manager_holdings = self.data.holdings_df[
                self.data.holdings_df['manager_id'] == manager_id
            ]
            
            if len(manager_holdings) < 5:
                continue
            
            position_sizes = manager_holdings['portfolio_percent']
            
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
                manager_activities = len(
                    self.data.history_df[self.data.history_df['manager_id'] == manager_id]
                )
            
            manager_sizing_analysis[manager_id] = {
                'total_positions': total_positions,
                'avg_position_size': avg_position,
                'max_position_size': max_position,
                'position_concentration': position_concentration * 100,
                'position_variance': position_variance,
                'small_positions_pct': (small_positions / total_positions) * 100,
                'medium_positions_pct': (medium_positions / total_positions) * 100,
                'large_positions_pct': (large_positions / total_positions) * 100,
                'sizing_efficiency_score': efficiency_score,
                'total_portfolio_value': manager_holdings['value'].sum(),
                'historical_activities': manager_activities
            }
        
        sizing_df = pd.DataFrame.from_dict(manager_sizing_analysis, orient='index')
        
        if sizing_df.empty:
            return pd.DataFrame()
        
        sizing_df = sizing_df[
            (sizing_df['total_positions'] >= 5) &
            (sizing_df['historical_activities'] >= 10)
        ]
        
        if sizing_df.empty:
            return pd.DataFrame()
        
        sizing_df['manager_name'] = sizing_df.index.map(
            lambda x: self.data.manager_names.get(x, x)
        )
        
        sizing_df['sizing_style'] = 'Balanced'
        sizing_df.loc[sizing_df['position_concentration'] >= 30, 'sizing_style'] = 'High Conviction'
        sizing_df.loc[sizing_df['avg_position_size'] <= 3, 'sizing_style'] = 'Diversified'
        sizing_df.loc[sizing_df['large_positions_pct'] >= 40, 'sizing_style'] = 'Concentrated'
        sizing_df.loc[
            (sizing_df['position_variance'] / sizing_df['avg_position_size']) <= 0.3,
            'sizing_style'
        ] = 'Systematic'
        
        sizing_df = sizing_df.sort_values(by='sizing_efficiency_score', ascending=False)
        
        return self.format_output(sizing_df.reset_index().rename(columns={'index': 'manager_id'})).head(40)
    
    def analyze_action_sequences(self) -> pd.DataFrame:
        """
        Identify predictive patterns in manager action sequences.
        Find what actions typically follow specific patterns.
        """
        if (self.data.history_df is None or self.data.history_df.empty):
            return pd.DataFrame()
        
        print("🔄 Analyzing Action Sequence Patterns...")
        
        sequence_analysis = defaultdict(lambda: {
            'total_occurrences': 0,
            'next_action_outcomes': Counter(),
            'success_patterns': [],
            'tickers_involved': set(),
            'managers_involved': set()
        })
        
        for ticker in self.data.history_df['ticker'].unique():
            ticker_data = self.data.history_df[
                self.data.history_df['ticker'] == ticker
            ].sort_values(by='period')
            
            if len(ticker_data) < 4:
                continue
            
            actions = ticker_data['action_type'].tolist()
            managers = ticker_data['manager_id'].tolist()
            
            for i in range(len(actions) - 3):
                sequence = ' → '.join(actions[i:i+3])
                next_action = actions[i+3]
                
                seq_data = sequence_analysis[sequence]
                seq_data['total_occurrences'] += 1
                seq_data['next_action_outcomes'][next_action] += 1
                seq_data['tickers_involved'].add(ticker)
                seq_data['managers_involved'].update(managers[i:i+4])
        
        sequences = []
        for sequence, data in sequence_analysis.items():
            total_occ = data['total_occurrences']
            if total_occ >= 3:
                next_actions = data['next_action_outcomes']
                if next_actions:
                    most_likely_next = next_actions.most_common(1)[0]
                    predictive_strength = most_likely_next[1] / total_occ
                else:
                    continue
                
                sequences.append({
                    'sequence_pattern': sequence,
                    'total_occurrences': data['total_occurrences'],
                    'most_likely_next_action': most_likely_next[0],
                    'predictive_strength': predictive_strength * 100,
                    'unique_tickers': len(data['tickers_involved']),
                    'unique_managers': len(data['managers_involved']),
                    'next_action_breakdown': dict(data['next_action_outcomes'])
                })
        
        if not sequences:
            return pd.DataFrame()
        
        sequence_df = pd.DataFrame(sequences)
        
        sequence_df['pattern_score'] = (
            sequence_df['predictive_strength'] * 
            np.log(sequence_df['total_occurrences']) *
            sequence_df['unique_managers']
        )
        
        sequence_df = sequence_df.sort_values(by='pattern_score', ascending=False)
        
        return self.format_output(sequence_df).head(30)
    
    def analyze_sector_rotation_excellence(self) -> pd.DataFrame:
        """
        Identify managers who excel at sector rotation timing.
        This requires sector classification which we'll approximate from company names.
        """
        if (self.data.history_df is None or 
            self.data.history_df.empty or
            self.data.holdings_df is None):
            return pd.DataFrame()
        
        print("🔄 Analyzing Sector Rotation Excellence...")
        
        sector_keywords = {
            'Technology': ['tech', 'software', 'microsoft', 'apple', 'google', 'meta', 'amazon', 'nvidia'],
            'Finance': ['bank', 'financial', 'capital', 'insurance', 'credit'],
            'Healthcare': ['health', 'pharma', 'medical', 'bio'],
            'Energy': ['energy', 'oil', 'gas', 'petroleum'],
            'Consumer': ['retail', 'consumer', 'restaurant', 'food'],
            'Industrial': ['industrial', 'manufacturing', 'aerospace'],
            'Real Estate': ['real estate', 'reit', 'property']
        }
        
        ticker_sectors = {}
        if 'stock' in self.data.holdings_df.columns:
            for _, row in self.data.holdings_df.iterrows():
                ticker = row['ticker']
                company = str(row['stock']).lower()
                
                assigned_sector = 'Other'
                for sector, keywords in sector_keywords.items():
                    if any(keyword in company for keyword in keywords):
                        assigned_sector = sector
                        break
                
                ticker_sectors[ticker] = assigned_sector
        
        if not ticker_sectors:
            return pd.DataFrame()
        
        manager_sector_analysis = defaultdict(lambda: {
            'sectors_traded': set(),
            'sector_timing': defaultdict(list),
            'total_rotations': 0,
            'rotation_success_score': 0
        })
        
        # Group activities by manager and analyze sector changes over time
        for manager_id in self.data.history_df['manager_id'].unique():
            manager_activities = self.data.history_df[
                self.data.history_df['manager_id'] == manager_id
            ].sort_values(by='period')
            
            sector_activity_by_period = defaultdict(lambda: defaultdict(int))
            
            for _, activity in manager_activities.iterrows():
                ticker = activity['ticker']
                if ticker in ticker_sectors:
                    sector = ticker_sectors[ticker]
                    period = activity['period']
                    action = activity['action_type']
                    
                    sector_activity_by_period[(period, sector)][action] += 1
                    mgr_data = manager_sector_analysis[manager_id]
                    mgr_data['sectors_traded'].add(sector)
            
            periods = sorted(set([p for p, s in sector_activity_by_period.keys()]))
            rotation_score = 0
            
            if len(periods) >= 4:
                for i in range(len(periods) - 1):
                    current_period = periods[i]
                    next_period = periods[i + 1]
                    
                    current_sectors = {}
                    next_sectors = {}
                    
                    for (period, sector), actions in sector_activity_by_period.items():
                        net_activity = actions['Buy'] + actions['Add'] - actions['Sell'] - actions['Reduce']
                        
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
            
            manager_sector_analysis[manager_id]['total_rotations'] = len(manager_sector_analysis[manager_id]['sectors_traded'])
            manager_sector_analysis[manager_id]['rotation_success_score'] = rotation_score
        
        rotation_data = []
        for manager_id, data in manager_sector_analysis.items():
            if len(data['sectors_traded']) >= 3:
                rotation_data.append({
                    'manager_id': manager_id,
                    'manager_name': self.data.manager_names.get(manager_id, manager_id),
                    'sectors_traded': len(data['sectors_traded']),
                    'rotation_success_score': data['rotation_success_score'],
                    'sectors_list': ', '.join(sorted(data['sectors_traded']))
                })
        
        if not rotation_data:
            return pd.DataFrame()
        
        rotation_df = pd.DataFrame(rotation_data)
        rotation_df = rotation_df.sort_values(by='rotation_success_score', ascending=False)
        
        return self.format_output(rotation_df).head(30)
    
    def analyze_manager_evolution(self) -> pd.DataFrame:
        """
        Analyze how managers evolve their strategies over decades.
        Track changes in behavior, concentration, and sector focus.
        """
        if (self.data.history_df is None or self.data.history_df.empty):
            return pd.DataFrame()
        
        print("📈 Analyzing Manager Evolution Patterns...")
        
        manager_evolution = {}
        
        for manager_id in self.data.history_df['manager_id'].unique():
            manager_data = self.data.history_df[
                self.data.history_df['manager_id'] == manager_id
            ].sort_values(by='period')
            
            if len(manager_data) < 20:
                continue
            
            years = sorted(set([
                p.split()[1] for p in manager_data['period'] 
                if 'Q' in p and len(p.split()) > 1
            ]))
            
            if len(years) < 5:
                continue
            
            career_length = len(years)
            phase_size = max(2, career_length // 3)
            
            early_years = years[:phase_size]
            middle_years = years[phase_size:phase_size*2]
            late_years = years[phase_size*2:]
            
            phases = {
                'Early Career': early_years,
                'Middle Career': middle_years,
                'Late Career': late_years
            }
            
            phase_analysis = {}
            for phase_name, phase_years in phases.items():
                phase_data = manager_data[
                    manager_data['period'].apply(
                        lambda x: any(year in str(x) for year in phase_years)
                    )
                ]
                
                if not phase_data.empty:
                    action_types = phase_data['action_type'].value_counts()
                    unique_stocks = phase_data['ticker'].nunique()
                    total_activities = len(phase_data)
                    
                    buy_ratio = (action_types.get('Buy', 0) + action_types.get('Add', 0)) / total_activities
                    
                    phase_analysis[phase_name] = {
                        'unique_stocks': unique_stocks,
                        'total_activities': total_activities,
                        'buy_ratio': buy_ratio,
                        'years_span': len(phase_years)
                    }
            
            if len(phase_analysis) >= 2:
                early = phase_analysis.get('Early Career', {})
                late = phase_analysis.get('Late Career', {})
                
                if early and late:
                    diversification_change = late.get('unique_stocks', 0) - early.get('unique_stocks', 0)
                    activity_change = late.get('total_activities', 0) / max(1, late.get('years_span', 1)) - \
                                    early.get('total_activities', 0) / max(1, early.get('years_span', 1))
                    style_change = abs(late.get('buy_ratio', 0) - early.get('buy_ratio', 0))
                    
                    manager_evolution[manager_id] = {
                        'career_length_years': career_length,
                        'early_stocks': early.get('unique_stocks', 0),
                        'late_stocks': late.get('unique_stocks', 0),
                        'diversification_change': diversification_change,
                        'activity_per_year_change': activity_change,
                        'style_change_score': style_change * 100,
                        'early_buy_ratio': early.get('buy_ratio', 0) * 100,
                        'late_buy_ratio': late.get('buy_ratio', 0) * 100,
                        'total_activities': len(manager_data)
                    }
        
        if not manager_evolution:
            return pd.DataFrame()
        
        evolution_df = pd.DataFrame.from_dict(manager_evolution, orient='index')
        
        evolution_df['manager_name'] = evolution_df.index.map(
            lambda x: self.data.manager_names.get(x, x)
        )
        
        evolution_df['evolution_type'] = 'Stable'
        evolution_df.loc[evolution_df['diversification_change'] > 10, 'evolution_type'] = 'Diversifying'
        evolution_df.loc[evolution_df['diversification_change'] < -10, 'evolution_type'] = 'Concentrating'
        evolution_df.loc[evolution_df['style_change_score'] > 20, 'evolution_type'] = 'Style Shifter'
        evolution_df.loc[evolution_df['activity_per_year_change'] > 5, 'evolution_type'] = 'More Active'
        evolution_df.loc[evolution_df['activity_per_year_change'] < -5, 'evolution_type'] = 'Less Active'
        
        evolution_df['evolution_score'] = (
            abs(evolution_df['diversification_change']) +
            evolution_df['style_change_score'] +
            abs(evolution_df['activity_per_year_change'])
        )
        
        evolution_df = evolution_df.sort_values(by='evolution_score', ascending=False)
        
        return self.format_output(evolution_df.reset_index().rename(columns={'index': 'manager_id'})).head(30)
    
    def analyze_catalyst_timing(self) -> pd.DataFrame:
        """
        Analyze managers who demonstrate exceptional timing in entries and exits.
        Look for patterns of buying before price rises and selling before declines.
        """
        if (self.data.history_df is None or self.data.history_df.empty or 
            self.data.holdings_df is None or self.data.holdings_df.empty):
            return pd.DataFrame()
        
        print("⏰ Analyzing Catalyst Timing Masters...")
        
        price_data = {}
        if 'current_price' in self.data.holdings_df.columns:
            price_data = self.data.holdings_df.groupby('ticker')['current_price'].first().to_dict()
        
        manager_timing = {}
        
        for manager_id in self.data.history_df['manager_id'].unique():
            manager_actions = self.data.history_df[
                self.data.history_df['manager_id'] == manager_id
            ].copy()
            
            if len(manager_actions) < 10:
                continue
            
            timing_scores = []
            perfect_entries = 0
            perfect_exits = 0
            total_entries = 0
            total_exits = 0
            
            for ticker, ticker_actions in manager_actions.groupby('ticker'):
                ticker_actions = ticker_actions.sort_values(by='period')
                
                entry_actions = ticker_actions[
                    ticker_actions['action_type'].isin(['Buy', 'Add'])
                ]
                exit_actions = ticker_actions[
                    ticker_actions['action_type'].isin(['Sell', 'Reduce'])
                ]
                
                if not entry_actions.empty:
                    total_entries += len(entry_actions)
                    first_entry_idx = ticker_actions.index[ticker_actions.index.get_loc(entry_actions.index[0])]
                    subsequent_actions = ticker_actions.loc[first_entry_idx:].iloc[1:5]
                    if not subsequent_actions.empty:
                        good_actions = subsequent_actions[
                            subsequent_actions['action_type'].isin(['Buy', 'Add', 'Hold'])
                        ]
                        if len(good_actions) >= len(subsequent_actions) * 0.6:
                            perfect_entries += 1
                
                if not exit_actions.empty:
                    total_exits += len(exit_actions)
                    for exit_idx in exit_actions.index:
                        exit_loc = ticker_actions.index.get_loc(exit_idx)
                        if exit_loc < len(ticker_actions) - 1:
                            subsequent = ticker_actions.iloc[exit_loc + 1:exit_loc + 4]
                            if not subsequent[subsequent['action_type'] == 'Buy'].empty:
                                pass
                            else:
                                perfect_exits += 1
            
            entry_success_rate = (perfect_entries / max(1, total_entries)) * 100
            exit_success_rate = (perfect_exits / max(1, total_exits)) * 100
            overall_timing_score = (entry_success_rate + exit_success_rate) / 2
            
            if total_entries + total_exits >= 20:
                manager_timing[manager_id] = {
                    'total_trades': total_entries + total_exits,
                    'entry_trades': total_entries,
                    'exit_trades': total_exits,
                    'perfect_entries': perfect_entries,
                    'perfect_exits': perfect_exits,
                    'entry_success_rate': round(entry_success_rate, 2),
                    'exit_success_rate': round(exit_success_rate, 2),
                    'timing_score': round(overall_timing_score, 2),
                    'years_active': len(manager_actions['period'].apply(lambda x: str(x).split()[-1] if ' ' in str(x) else '').unique())
                }
        
        if not manager_timing:
            return pd.DataFrame()
        
        timing_df = pd.DataFrame.from_dict(manager_timing, orient='index')
        timing_df.index.name = 'manager_id'
        timing_df = timing_df.reset_index()
        
        timing_df['manager'] = timing_df['manager_id'].map(self.data.manager_names)
        
        timing_df = timing_df.sort_values(by='timing_score', ascending=False).head(30)
        
        return self.format_output(timing_df)
    
    def analyze_theme_emergence(self) -> pd.DataFrame:
        """
        Identify emerging investment themes by detecting early concentrations.
        """
        if (self.data.history_df is None or self.data.history_df.empty):
            return pd.DataFrame()
        
        print("🎭 Analyzing Theme Emergence Patterns...")
        
        theme_analysis = {}
        recent_periods = self.get_recent_quarters(5)
        
        for ticker in self.data.history_df['ticker'].unique():
            ticker_data = self.data.history_df[self.data.history_df['ticker'] == ticker]
            
            recent_managers = set(
                ticker_data[ticker_data['period'].isin(recent_periods)]['manager_id']
            )
            
            historical_managers = set(
                ticker_data[~ticker_data['period'].isin(recent_periods)]['manager_id']
            )
            
            new_managers = recent_managers - historical_managers
            
            if len(new_managers) >= 2 and len(recent_managers) >= 3:
                recent_activities = ticker_data[
                    ticker_data['period'].isin(recent_periods)
                ]
                buy_activities = recent_activities[
                    recent_activities['action_type'].isin(['Buy', 'Add'])
                ]
                
                if len(buy_activities) >= 2:
                    theme_analysis[ticker] = {
                        'total_recent_managers': len(recent_managers),
                        'new_managers_count': len(new_managers),
                        'recent_buy_activities': len(buy_activities),
                        'emergence_score': len(new_managers) * len(buy_activities),
                        'new_managers': list(new_managers),
                        'total_managers': len(recent_managers | historical_managers)
                    }
        
        if not theme_analysis:
            return pd.DataFrame()
        
        theme_df = pd.DataFrame.from_dict(theme_analysis, orient='index')
        
        if self.data.holdings_df is not None and 'stock' in self.data.holdings_df.columns:
            company_names = self.data.holdings_df.groupby('ticker')['stock'].first()
            theme_df = theme_df.join(company_names.rename('company_name'))
        
        theme_df['new_manager_names'] = theme_df['new_managers'].apply(
            lambda managers: ', '.join([
                self.data.manager_names.get(mgr, mgr) for mgr in managers
            ])
        )
        
        theme_df = theme_df.sort_values(by='emergence_score', ascending=False)
        
        return self.format_output(theme_df.reset_index().rename(columns={'index': 'ticker'})).head(25)