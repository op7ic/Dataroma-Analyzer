"""
Data formatting utilities for analysis outputs.
"""

import logging
import pandas as pd
from typing import Dict


class DataFormatter:
    """Utility class for consistent data formatting across all analysis modules."""
    
    # Define precision rules for numeric formatting
    PRECISION_RULES = {
        # Price and percentage fields
        "price": 2,
        "current_price": 2,
        "reported_price": 2,
        "avg_portfolio_pct": 2,
        "max_portfolio_pct": 2,
        "portfolio_pct_std": 2,
        "gain_loss_pct": 2,
        "portfolio_percent": 2,
        "conviction_score": 2,
        "appeal_score": 2,
        "value_score": 1,
        "consensus_score": 2,
        
        # Financial ratios
        "pe_ratio": 2,
        "debt_to_equity": 2,
        "roe": 2,
        "current_ratio": 2,
        "gross_margin": 2,
        "beta": 2,
        "eps": 4,
        
        # Large numbers (no decimals)
        "market_cap": 0,
        "total_shares": 0,
        "shares": 0,
        "total_position_value": 2,
        "total_value": 2,
        "value": 2,
        
        # Count fields
        "manager_count": 0,
        "position_count": 0,
        "num_managers": 0,
        "buy_count": 0,
        "sell_count": 0,
    }
    
    @classmethod
    def apply_precision_formatting(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply consistent numeric formatting to DataFrame columns.
        
        Args:
            df: DataFrame to format
            
        Returns:
            Formatted DataFrame
        """
        if df.empty:
            return df
        
        df = df.copy()
        
        # Apply formatting based on precision rules
        for col, decimals in cls.PRECISION_RULES.items():
            if col in df.columns:
                try:
                    # Convert to numeric first
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                    # Round to specified decimals
                    df[col] = df[col].round(decimals)
                except Exception as e:
                    logging.warning(f"Could not format column {col}: {e}")
        
        return df
    
    @classmethod
    def format_market_cap(cls, market_cap: float) -> str:
        """Format market cap for display."""
        if pd.isna(market_cap) or market_cap <= 0:
            return "N/A"
        elif market_cap >= 1_000_000_000_000:  # Trillion
            return f"${market_cap / 1_000_000_000_000:.1f}T"
        elif market_cap >= 1_000_000_000:  # Billion
            return f"${market_cap / 1_000_000_000:.1f}B"
        elif market_cap >= 1_000_000:  # Million
            return f"${market_cap / 1_000_000:.1f}M"
        else:
            return f"${market_cap:,.0f}"
    
    @classmethod
    def categorize_market_cap(cls, market_cap: float) -> str:
        """Categorize market cap into standard buckets."""
        if pd.isna(market_cap) or market_cap <= 0:
            return "Unknown"
        elif market_cap < 300_000_000:  # < $300M
            return "Micro-Cap"
        elif market_cap < 2_000_000_000:  # < $2B
            return "Small-Cap"
        elif market_cap < 10_000_000_000:  # < $10B
            return "Mid-Cap"
        elif market_cap < 200_000_000_000:  # < $200B
            return "Large-Cap"
        else:
            return "Mega-Cap"
    
    @classmethod
    def format_percentage(cls, value: float, decimals: int = 2) -> str:
        """Format value as percentage."""
        if pd.isna(value):
            return "N/A"
        return f"{value:.{decimals}f}%"
    
    @classmethod
    def format_currency(cls, value: float, decimals: int = 2) -> str:
        """Format value as currency."""
        if pd.isna(value):
            return "N/A"
        if value >= 1_000_000:
            return f"${value / 1_000_000:.1f}M"
        elif value >= 1_000:
            return f"${value / 1_000:.1f}K"
        else:
            return f"${value:.{decimals}f}"
    
    @classmethod
    def clean_column_names(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Clean column names for better readability."""
        df = df.copy()
        
        # Column name replacements
        replacements = {
            "ticker": "Ticker",
            "stock": "Company",
            "manager_name": "Manager",
            "manager_names": "Managers",
            "total_value": "Total Value ($)",
            "avg_portfolio_pct": "Avg Portfolio %",
            "max_portfolio_pct": "Max Portfolio %",
            "manager_count": "# Managers",
            "conviction_score": "Conviction Score",
            "appeal_score": "Appeal Score",
            "value_score": "Value Score",
            "current_price": "Current Price ($)",
            "reported_price": "Reported Price ($)",
            "shares": "Shares",
            "total_shares": "Total Shares",
            "market_cap": "Market Cap",
            "pe_ratio": "P/E Ratio",
        }
        
        # Apply replacements
        df.columns = [replacements.get(col, col.replace("_", " ").title()) for col in df.columns]
        
        return df
    
    @classmethod
    def prepare_for_export(cls, df: pd.DataFrame, clean_names: bool = True) -> pd.DataFrame:
        """Prepare DataFrame for CSV export with formatting and cleaned names."""
        if df.empty:
            return df
        
        # Apply precision formatting
        df = cls.apply_precision_formatting(df)
        
        # Clean column names if requested
        if clean_names:
            df = cls.clean_column_names(df)
        
        return df