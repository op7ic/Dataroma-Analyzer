"""
Historical analysis module for multi-decade investment patterns.

Analyzes 18+ years of data (2007-2025) to identify long-term patterns,
crisis responses, and multi-decade conviction plays.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

from .base_analyzer import BaseAnalyzer, MultiAnalyzer
from ..data.data_loader import DataLoader


class HistoricalAnalyzer(MultiAnalyzer):
    """Analyzes historical patterns across 18+ years of investment data."""
    
    def __init__(self, data_loader: DataLoader) -> None:
        """Initialize with historical data capabilities."""
        super().__init__(data_loader)
        self.crisis_periods = {
            "2008_financial": ["Q3 2008", "Q4 2008", "Q1 2009", "Q2 2009"],
            "2020_covid": ["Q1 2020", "Q2 2020"],
            "2022_inflation": ["Q1 2022", "Q2 2022", "Q3 2022"],
        }
        
    def analyze_all(self) -> Dict[str, pd.DataFrame]:
        """Run all historical analyses."""
        results = {}
        
        results["manager_track_records"] = self.analyze_manager_track_records()
        results["stock_life_cycles"] = self.analyze_stock_life_cycles()
        results["sector_rotation_patterns"] = self.analyze_sector_rotation()
        results["crisis_response_analysis"] = self.analyze_crisis_responses()
        results["multi_decade_conviction"] = self.analyze_multi_decade_conviction()
        results["quarterly_activity_timeline"] = self.analyze_quarterly_timeline()
        results["long_term_winners"] = self.analyze_long_term_winners()
        
        for name, df in results.items():
            self.log_analysis_summary(df, name)
        
        return self.format_all_outputs(results)
    
    def analyze_manager_track_records(self) -> pd.DataFrame:
        """
        Analyze manager performance over decades.
        
        Track records include:
        - Years active
        - Crisis navigation success
        - Long-term performance patterns
        - Consistency across market cycles
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()
        
        history = self.data.history_df.copy()
        history['year'] = history['period'].str.extract(r'(\d{4})')
        history['year'] = pd.to_numeric(history['year'])
        
        manager_records = []
        
        for manager_id in history['manager_id'].unique():
            manager_data = history[history['manager_id'] == manager_id]
            
            years_active = manager_data['year'].nunique()
            first_year = manager_data['year'].min()
            last_year = manager_data['year'].max()
            total_actions = len(manager_data)
            
            crisis_actions = self._analyze_crisis_actions(manager_data)
            
            action_breakdown = manager_data['action_type'].value_counts().to_dict()
            
            yearly_actions = manager_data.groupby('year').size()
            consistency_score = 1 - (yearly_actions.std() / yearly_actions.mean() if yearly_actions.mean() > 0 else 0)
            
            current_holdings = 0
            if self.data.holdings_df is not None:
                current_holdings = len(self.data.holdings_df[self.data.holdings_df['manager_id'] == manager_id])
            
            record = {
                'manager_id': manager_id,
                'manager_name': self.data.manager_names.get(manager_id, manager_id),
                'years_active': years_active,
                'first_year': first_year,
                'last_year': last_year,
                'total_actions': total_actions,
                'current_holdings': current_holdings,
                'buy_actions': action_breakdown.get('Buy', 0),
                'sell_actions': action_breakdown.get('Sell', 0),
                'add_actions': action_breakdown.get('Add', 0),
                'reduce_actions': action_breakdown.get('Reduce', 0),
                'consistency_score': consistency_score,
                **crisis_actions
            }
            
            manager_records.append(record)
        
        df = pd.DataFrame(manager_records)
        
        if self.data.holdings_df is not None and not self.data.holdings_df.empty:
            current_portfolios = self.data.holdings_df.groupby('manager_id')['value'].sum()
            df['current_portfolio_value'] = df['manager_id'].map(current_portfolios).fillna(0)
            
            df['estimated_initial_value'] = df.apply(
                lambda row: row['current_portfolio_value'] / (1.1 ** row['years_active']) 
                if row['years_active'] > 0 and row['current_portfolio_value'] > 0 else 0, 
                axis=1
            )
            
            df['total_return_pct'] = df.apply(
                lambda row: ((row['current_portfolio_value'] - row['estimated_initial_value']) 
                           / row['estimated_initial_value'] * 100) 
                if row['estimated_initial_value'] > 0 else 0, 
                axis=1
            ).round(2)
            
            df['annualized_return_pct'] = df.apply(
                lambda row: (row['total_return_pct'] / row['years_active']) 
                if row['years_active'] > 0 else 0, 
                axis=1
            ).round(2)
        
        df['track_record_score'] = (
            df['years_active'] * 0.3 +
            df['consistency_score'] * 20 +
            df['crisis_buying_ratio'] * 10 +
            (df['current_holdings'] > 0).astype(int) * 5
        )
        
        return df.sort_values('track_record_score', ascending=False)
    
    def analyze_stock_life_cycles(self) -> pd.DataFrame:
        """
        Analyze complete life cycles of stocks.
        
        Includes:
        - Entry timing for successful stocks
        - When smart money bought today's winners
        - Exit patterns before major declines
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()
        
        history = self.data.history_df.copy()
        history['year'] = history['period'].str.extract(r'(\d{4})')
        
        stock_actions = history.groupby('ticker').agg({
            'period': ['min', 'max', 'count'],
            'action_type': lambda x: x.value_counts().to_dict(),
            'manager_id': 'nunique'
        })
        
        stock_actions.columns = ['first_action', 'last_action', 'total_actions', 'action_breakdown', 'unique_managers']
        
        stock_actions['first_year'] = stock_actions['first_action'].str.extract(r'(\d{4})').astype(int)
        stock_actions['last_year'] = stock_actions['last_action'].str.extract(r'(\d{4})').astype(int)
        stock_actions['years_tracked'] = stock_actions['last_year'] - stock_actions['first_year'] + 1
        
        current_tickers = set()
        if self.data.holdings_df is not None:
            current_tickers = set(self.data.holdings_df['ticker'].unique())
        
        stock_actions['currently_held'] = stock_actions.index.isin(current_tickers)
        
        life_cycles = []
        
        for ticker, row in stock_actions.iterrows():
            actions = row['action_breakdown']
            
            first_buys = history[(history['ticker'] == ticker) & (history['action_type'] == 'Buy')]['period'].min()
            
            complete_exits = history[(history['ticker'] == ticker) & (history['action'] == 'Sell 100.00%')]
            
            life_cycle = {
                'ticker': ticker,
                'first_year': row['first_year'],
                'last_year': row['last_year'],
                'years_tracked': row['years_tracked'],
                'total_actions': row['total_actions'],
                'unique_managers': row['unique_managers'],
                'currently_held': row['currently_held'],
                'total_buys': actions.get('Buy', 0),
                'total_sells': actions.get('Sell', 0),
                'total_adds': actions.get('Add', 0),
                'total_reduces': actions.get('Reduce', 0),
                'first_buy_period': first_buys if pd.notna(first_buys) else '',
                'complete_exit_count': len(complete_exits),
                'accumulation_score': actions.get('Buy', 0) + actions.get('Add', 0) - actions.get('Sell', 0) - actions.get('Reduce', 0) * 0.5
            }
            
            life_cycles.append(life_cycle)
        
        df = pd.DataFrame(life_cycles)
        
        if not df.empty and self.data.holdings_df is not None and 'stock' in self.data.holdings_df.columns:
            company_names = self.data.holdings_df.groupby('ticker')['stock'].first()
            df = df.set_index('ticker').join(company_names.rename('company_name'), how='left').reset_index()
        
        df['life_cycle_score'] = (
            df['years_tracked'] * 0.5 +
            df['unique_managers'] * 2 +
            df['accumulation_score'] * 0.1 +
            df['currently_held'].astype(int) * 5
        )
        
        return df.sort_values('life_cycle_score', ascending=False)
    
    def analyze_sector_rotation(self) -> pd.DataFrame:
        """
        Analyze sector rotation patterns over time.
        
        Note: Since we don't have sector data from Dataroma,
        we'll infer sectors from stock symbols and patterns.
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()
        
        history = self.data.history_df.copy()
        history['year'] = history['period'].str.extract(r'(\d{4})').astype(int)
        history['quarter'] = history['period'].str.extract(r'(Q\d)')
        
        tech_keywords = ['GOOGL', 'GOOG', 'AAPL', 'MSFT', 'META', 'AMZN', 'NVDA', 'CRM', 'ORCL', 'IBM']
        finance_keywords = ['BAC', 'JPM', 'WFC', 'C', 'GS', 'MS', 'BRK', 'AXP', 'V', 'MA']
        energy_keywords = ['XOM', 'CVX', 'COP', 'OXY', 'SLB', 'EOG', 'PXD', 'VLO', 'PSX']
        healthcare_keywords = ['JNJ', 'UNH', 'PFE', 'ABBV', 'TMO', 'DHR', 'ABT', 'CVS', 'LLY']
        
        def classify_sector(ticker):
            if ticker in tech_keywords:
                return 'Technology'
            elif ticker in finance_keywords:
                return 'Financials'
            elif ticker in energy_keywords:
                return 'Energy'
            elif ticker in healthcare_keywords:
                return 'Healthcare'
            else:
                return 'Other'
        
        history['sector'] = history['ticker'].apply(classify_sector)
        
        sector_flows = history.groupby(['period', 'sector', 'action_type']).size().unstack(fill_value=0)
        sector_flows['net_flow'] = sector_flows.get('Buy', 0) + sector_flows.get('Add', 0) - sector_flows.get('Sell', 0) - sector_flows.get('Reduce', 0)
        
        rotation_summary = []
        
        for period in history['period'].unique():
            period_data = sector_flows.loc[period] if period in sector_flows.index else pd.DataFrame()
            
            if not period_data.empty and 'net_flow' in period_data.columns:
                for sector in period_data.index:
                    rotation_summary.append({
                        'period': period,
                        'sector': sector,
                        'net_flow': period_data.loc[sector, 'net_flow'],
                        'buy_actions': period_data.loc[sector].get('Buy', 0),
                        'sell_actions': period_data.loc[sector].get('Sell', 0)
                    })
        
        return pd.DataFrame(rotation_summary)
    
    def analyze_crisis_responses(self) -> pd.DataFrame:
        """
        Compare behavior during different crisis periods.
        
        Analyzes:
        - 2008 Financial Crisis
        - 2020 COVID Crash  
        - 2022 Inflation/Rate Hike Period
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()
        
        crisis_analysis = []
        
        for crisis_name, crisis_quarters in self.crisis_periods.items():
            crisis_data = self.data.history_df[
                self.data.history_df['period'].isin(crisis_quarters)
            ]
            
            if crisis_data.empty:
                continue
            
            total_actions = len(crisis_data)
            action_breakdown = crisis_data['action_type'].value_counts()
            
            buy_ratio = (action_breakdown.get('Buy', 0) + action_breakdown.get('Add', 0)) / total_actions if total_actions > 0 else 0
            sell_ratio = (action_breakdown.get('Sell', 0) + action_breakdown.get('Reduce', 0)) / total_actions if total_actions > 0 else 0
            
            crisis_buyers = crisis_data[crisis_data['action_type'].isin(['Buy', 'Add'])]['manager_id'].value_counts().head(5)
            
            crisis_buys = crisis_data[crisis_data['action_type'].isin(['Buy', 'Add'])]['ticker'].value_counts().head(10)
            
            crisis_sells = crisis_data[crisis_data['action_type'].isin(['Sell', 'Reduce'])]['ticker'].value_counts().head(10)
            
            crisis_summary = {
                'crisis': crisis_name,
                'period': ', '.join(crisis_quarters),
                'total_actions': total_actions,
                'buy_actions': action_breakdown.get('Buy', 0),
                'add_actions': action_breakdown.get('Add', 0),
                'sell_actions': action_breakdown.get('Sell', 0),
                'reduce_actions': action_breakdown.get('Reduce', 0),
                'buy_ratio': buy_ratio,
                'sell_ratio': sell_ratio,
                'top_buyers': ', '.join([f"{self.data.manager_names.get(m, m)}" for m in crisis_buyers.index[:3]]),
                'most_bought': ', '.join(crisis_buys.index[:5]),
                'most_sold': ', '.join(crisis_sells.index[:5]),
                'unique_managers': crisis_data['manager_id'].nunique(),
                'unique_stocks': crisis_data['ticker'].nunique()
            }
            
            crisis_analysis.append(crisis_summary)
        
        return pd.DataFrame(crisis_analysis)
    
    def analyze_multi_decade_conviction(self) -> pd.DataFrame:
        """
        Identify stocks held through multiple market cycles.
        
        Finds:
        - Stocks held for 10+ years
        - Concentration changes over time
        - True long-term conviction plays
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()
        
        history = self.data.history_df.copy()
        history['year'] = history['period'].str.extract(r'(\d{4})').astype(int)
        
        stock_spans = history.groupby('ticker')['year'].agg(['min', 'max', 'nunique'])
        long_term_stocks = stock_spans[stock_spans['max'] - stock_spans['min'] >= 10]
        
        conviction_plays = []
        
        for ticker in long_term_stocks.index:
            ticker_history = history[history['ticker'] == ticker]
            
            managers_by_year = ticker_history.groupby('year')['manager_id'].nunique()
            consistent_managers = []
            
            for manager in ticker_history['manager_id'].unique():
                manager_years = ticker_history[ticker_history['manager_id'] == manager]['year'].nunique()
                if manager_years >= 5:
                    consistent_managers.append(manager)
            
            accumulation = 0
            for _, row in ticker_history.iterrows():
                if row['action_type'] == 'Buy':
                    accumulation += 1
                elif row['action_type'] == 'Add':
                    accumulation += 0.5
                elif row['action_type'] == 'Reduce':
                    accumulation -= 0.5
                elif row['action_type'] == 'Sell':
                    accumulation -= 1
            
                currently_held = False
            current_holders = 0
            if self.data.holdings_df is not None:
                current_data = self.data.holdings_df[self.data.holdings_df['ticker'] == ticker]
                currently_held = len(current_data) > 0
                current_holders = current_data['manager_id'].nunique()
            
            conviction_play = {
                'ticker': ticker,
                'years_tracked': long_term_stocks.loc[ticker, 'nunique'],
                'first_year': long_term_stocks.loc[ticker, 'min'],
                'last_year': long_term_stocks.loc[ticker, 'max'],
                'total_actions': len(ticker_history),
                'unique_managers_all_time': ticker_history['manager_id'].nunique(),
                'consistent_long_term_holders': len(consistent_managers),
                'consistent_holders_list': ', '.join([self.data.manager_names.get(m, m) for m in consistent_managers[:3]]),
                'net_accumulation_score': accumulation,
                'currently_held': currently_held,
                'current_holders': current_holders,
                'avg_managers_per_year': managers_by_year.mean(),
                'max_managers_in_year': managers_by_year.max(),
                'conviction_score': (
                    len(consistent_managers) * 3 +
                    accumulation * 0.5 +
                    current_holders * 2 +
                    (currently_held * 5)
                )
            }
            
            conviction_plays.append(conviction_play)
        
        df = pd.DataFrame(conviction_plays)
        
        if not df.empty and self.data.holdings_df is not None and 'stock' in self.data.holdings_df.columns:
            company_names = self.data.holdings_df.groupby('ticker')['stock'].first()
            df = df.set_index('ticker').join(company_names.rename('company_name'), how='left').reset_index()
        
        return df.sort_values('conviction_score', ascending=False)
    
    def analyze_quarterly_timeline(self) -> pd.DataFrame:
        """
        Create quarterly activity timeline showing market dynamics.
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()
        
        timeline = self.data.history_df.groupby('period').agg({
            'action_type': lambda x: x.value_counts().to_dict(),
            'manager_id': 'nunique',
            'ticker': 'nunique',
            'period': 'count'
        }).rename(columns={'period': 'total_actions'})
        
        timeline_data = []
        for period, row in timeline.iterrows():
            actions = row['action_type']
            timeline_data.append({
                'period': period,
                'total_actions': row['total_actions'],
                'unique_managers': row['manager_id'],
                'unique_stocks': row['ticker'],
                'buy_actions': actions.get('Buy', 0),
                'sell_actions': actions.get('Sell', 0),
                'add_actions': actions.get('Add', 0),
                'reduce_actions': actions.get('Reduce', 0),
                'net_activity': actions.get('Buy', 0) + actions.get('Add', 0) - actions.get('Sell', 0) - actions.get('Reduce', 0)
            })
        
        df = pd.DataFrame(timeline_data)
        
        df['year'] = df['period'].str.extract(r'(\d{4})').astype(int)
        df['quarter'] = df['period'].str.extract(r'Q(\d)').astype(int)
        
        return df.sort_values(['year', 'quarter'])
    
    def analyze_long_term_winners(self) -> pd.DataFrame:
        """
        Identify stocks that have been long-term winners based on manager actions.
        """
        if self.data.history_df is None or self.data.history_df.empty:
            return pd.DataFrame()
        
        history = self.data.history_df.copy()
        history['year'] = history['period'].str.extract(r'(\d{4})').astype(int)
        
        # Dynamically determine current year from data
        current_year = history['year'].max()
        
        early_buys = history[
            (history['year'] <= 2010) & 
            (history['action_type'] == 'Buy')
        ]['ticker'].unique()
        
        current_holdings = set()
        if self.data.holdings_df is not None:
            current_holdings = set(self.data.holdings_df['ticker'].unique())
        
        potential_winners = list(set(early_buys) & current_holdings)
        
        winners = []
        for ticker in potential_winners:
            ticker_history = history[history['ticker'] == ticker]
            
            first_buy = ticker_history[ticker_history['action_type'] == 'Buy']['period'].min()
            first_buy_year = int(ticker_history[ticker_history['action_type'] == 'Buy']['year'].min())
            
            net_actions = 0
            for _, row in ticker_history.iterrows():
                if row['action_type'] in ['Buy', 'Add']:
                    net_actions += 1
                elif row['action_type'] in ['Sell', 'Reduce']:
                    net_actions -= 1
            
                current_data = self.data.holdings_df[self.data.holdings_df['ticker'] == ticker] if self.data.holdings_df is not None else pd.DataFrame()
            current_holders = len(current_data['manager_id'].unique()) if not current_data.empty else 0
            total_value = current_data['value'].sum() if not current_data.empty and 'value' in current_data.columns else 0
            
            winner = {
                'ticker': ticker,
                'first_buy_period': first_buy,
                'first_buy_year': first_buy_year,
                'years_held': current_year - first_buy_year,
                'total_historical_actions': len(ticker_history),
                'net_accumulation': net_actions,
                'current_holders': current_holders,
                'current_total_value': total_value,
                'unique_managers_all_time': ticker_history['manager_id'].nunique(),
                'winner_score': (current_year - first_buy_year) * current_holders
            }
            
            winners.append(winner)
        
        df = pd.DataFrame(winners)
        
        if not df.empty and self.data.holdings_df is not None and 'stock' in self.data.holdings_df.columns:
            company_names = self.data.holdings_df.groupby('ticker')['stock'].first()
            df = df.set_index('ticker').join(company_names.rename('company_name'), how='left').reset_index()
        
        return df.sort_values('winner_score', ascending=False) if not df.empty else df
    
    def _analyze_crisis_actions(self, manager_data: pd.DataFrame) -> Dict[str, float]:
        """Analyze a manager's actions during crisis periods."""
        crisis_metrics = {}
        
        total_crisis_actions = 0
        total_crisis_buys = 0
        
        for crisis_name, quarters in self.crisis_periods.items():
            crisis_data = manager_data[manager_data['period'].isin(quarters)]
            if not crisis_data.empty:
                crisis_actions = len(crisis_data)
                crisis_buys = len(crisis_data[crisis_data['action_type'].isin(['Buy', 'Add'])])
                
                crisis_metrics[f'{crisis_name}_actions'] = crisis_actions
                crisis_metrics[f'{crisis_name}_buy_ratio'] = crisis_buys / crisis_actions if crisis_actions > 0 else 0
                
                total_crisis_actions += crisis_actions
                total_crisis_buys += crisis_buys
        
        crisis_metrics['total_crisis_actions'] = total_crisis_actions
        crisis_metrics['crisis_buying_ratio'] = total_crisis_buys / total_crisis_actions if total_crisis_actions > 0 else 0
        
        return crisis_metrics