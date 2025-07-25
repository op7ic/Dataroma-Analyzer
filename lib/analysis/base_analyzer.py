#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Base Analyzer

Abstract base class providing common analysis functionality and patterns
for all analysis modules.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

from abc import ABC, abstractmethod
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any

from ..data.data_loader import DataLoader
from ..utils.formatters import DataFormatter
from ..utils.calculations import FinancialCalculations, ScoringUtils
from datetime import datetime
import re


class BaseAnalyzer(ABC):
    """
    Abstract base class for all analysis modules.
    
    Provides common functionality and enforces consistent interface
    across all analyzer implementations.
    """
    
    def __init__(self, data_loader: DataLoader):
        """Initialize analyzer with data loader."""
        self.data = data_loader
        self.formatter = DataFormatter()
        self.calc = FinancialCalculations()
        self.scoring = ScoringUtils()
        
        if not self.data.data_loaded:
            raise ValueError("Data must be loaded before creating analyzer")
        
        self._recent_quarters_cache = None
    
    @abstractmethod
    def analyze(self) -> pd.DataFrame:
        """
        Perform the core analysis.
        
        Returns:
            DataFrame with analysis results
        """
        pass
    
    def get_analysis_name(self) -> str:
        """Return the analysis name for file naming."""
        return self.__class__.__name__.replace('Analyzer', '').lower()
    
    def get_analysis_title(self) -> str:
        """Return the analysis title for display."""
        name = self.get_analysis_name()
        return name.replace('_', ' ').title()
    
    def get_recent_quarters(self, num_quarters: int = 3) -> List[str]:
        """
        Get the most recent quarters from the data.
        
        Args:
            num_quarters: Number of recent quarters to return (default: 3)
            
        Returns:
            List of quarter strings (e.g., ["Q1 2025", "Q4 2024", "Q3 2024"])
        """
        if self._recent_quarters_cache and len(self._recent_quarters_cache) >= num_quarters:
            return self._recent_quarters_cache[:num_quarters]
        
        if self.data.history_df is None or self.data.history_df.empty:
            logging.warning("No history data available to determine recent quarters")
            return []
        
        all_quarters = self.data.history_df['period'].dropna().unique()
        
        quarter_data = []
        for quarter in all_quarters:
            match = re.match(r'Q(\d) (\d{4})', quarter)
            if match:
                q_num = int(match.group(1))
                year = int(match.group(2))
                quarter_data.append((year, q_num, quarter))
        
        quarter_data.sort(key=lambda x: (x[0], x[1]), reverse=True)
        
        recent_quarters = [q[2] for q in quarter_data[:num_quarters]]
        
        self._recent_quarters_cache = recent_quarters
        
        logging.info(f"Determined recent {num_quarters} quarters: {recent_quarters}")
        return recent_quarters
    
    def filter_recent_activities(self, df: pd.DataFrame, num_quarters: int = 3) -> pd.DataFrame:
        """
        Filter DataFrame to only include activities from recent quarters.
        
        Args:
            df: DataFrame with 'period' column
            num_quarters: Number of recent quarters to include
            
        Returns:
            Filtered DataFrame
        """
        recent_quarters = self.get_recent_quarters(num_quarters)
        if not recent_quarters:
            return df
        
        return df[df['period'].isin(recent_quarters)]
    
    def format_output(self, df: pd.DataFrame, apply_precision: bool = True) -> pd.DataFrame:
        """Apply consistent formatting to output DataFrame."""
        if df.empty:
            return df
        
        if apply_precision:
            df = self.formatter.apply_precision_formatting(df)
        
        return df
    
    def prepare_for_export(self, df: pd.DataFrame, clean_names: bool = True) -> pd.DataFrame:
        """Prepare DataFrame for export (CSV/Excel)."""
        return self.formatter.prepare_for_export(df, clean_names=clean_names)
    
    def validate_required_columns(self, df: pd.DataFrame, required_columns: List[str]) -> bool:
        """
        Validate that DataFrame contains required columns.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            
        Returns:
            True if all columns exist, False otherwise
        """
        if df is None or df.empty:
            logging.warning(f"DataFrame is empty for {self.__class__.__name__}")
            return False
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.warning(f"Missing required columns in {self.__class__.__name__}: {missing_columns}")
            return False
        
        return True
    
    def filter_active_holdings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out inactive/historical holdings (those with 0 value)."""
        if df is None or df.empty:
            return df
        
        if "value" in df.columns:
            active_df = df[df["value"] > 0].copy()
            logging.debug(f"Filtered to {len(active_df)} active positions from {len(df)} total")
            return active_df
        
        return df
    
    def add_calculated_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add common calculated fields to DataFrame."""
        if df is None or df.empty:
            return df
        
        df = df.copy()
        
        required_52w_cols = ["current_price", "52_week_low", "52_week_high"]
        if all(col in df.columns for col in required_52w_cols):
            df["52_week_position_pct"] = df.apply(
                lambda row: self.calc.calculate_52_week_position(
                    row["current_price"], row["52_week_low"], row["52_week_high"]
                ), axis=1
            )
        
        if "current_price" in df.columns and "reported_price" in df.columns:
            df["price_change_pct"] = df.apply(
                lambda row: self.calc.calculate_price_change_percentage(
                    row["current_price"], row["reported_price"]
                ), axis=1
            )
        
        if "shares" in df.columns and "current_price" in df.columns:
            df["current_position_value"] = df.apply(
                lambda row: self.calc.calculate_position_value(
                    row["shares"], row["current_price"]
                ), axis=1
            )
        
        return df
    
    def get_manager_summary(self, manager_ids: pd.Series) -> str:
        """Get formatted summary of managers."""
        return self.data.get_manager_list(manager_ids)
    
    def get_activity_summary(self, activities: pd.Series) -> str:
        """Get formatted summary of activities."""
        return self.data.get_activity_summary(activities)
    
    def log_analysis_summary(self, df: pd.DataFrame, analysis_name: Optional[str] = None) -> None:
        """Log summary of analysis results."""
        name = analysis_name or self.get_analysis_name()
        
        if df.empty:
            logging.warning(f"{name} analysis returned no results")
        else:
            logging.info(f"{name} analysis completed: {len(df)} results")
            
            if "manager_count" in df.columns:
                avg_managers = df["manager_count"].mean()
                logging.info(f"  Average managers per stock: {avg_managers:.1f}")
            
            if "total_value" in df.columns:
                total_value = df["total_value"].sum()
                logging.info(f"  Total value analyzed: ${total_value:,.0f}")


class MultiAnalyzer(BaseAnalyzer):
    """
    Base class for analyzers that produce multiple related analyses.
    """
    
    @abstractmethod
    def analyze_all(self) -> Dict[str, pd.DataFrame]:
        """
        Perform all related analyses.
        
        Returns:
            Dictionary mapping analysis names to DataFrames
        """
        pass
    
    def analyze(self) -> pd.DataFrame:
        """Default implementation returns the first analysis."""
        results = self.analyze_all()
        if results:
            return list(results.values())[0]
        return pd.DataFrame()
    
    def format_all_outputs(self, results: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Apply formatting to all analysis results."""
        formatted_results = {}
        for name, df in results.items():
            formatted_results[name] = self.format_output(df)
        return formatted_results