"""
Common financial calculations and utility functions.
"""

import pandas as pd
import numpy as np
import re
from typing import Optional, Union


class FinancialCalculations:
    """Common financial calculations for analysis modules."""
    
    @staticmethod
    def calculate_position_value(shares: Union[int, float], price: Union[int, float]) -> float:
        """Calculate position value from shares and price."""
        if pd.isna(shares) or pd.isna(price) or shares <= 0 or price <= 0:
            return 0.0
        return float(shares) * float(price)
    
    @staticmethod
    def calculate_portfolio_percentage(position_value: float, total_portfolio_value: float) -> float:
        """Calculate what percentage a position represents of total portfolio."""
        if pd.isna(position_value) or pd.isna(total_portfolio_value) or total_portfolio_value <= 0:
            return 0.0
        return (position_value / total_portfolio_value) * 100
    
    @staticmethod
    def calculate_price_change_percentage(current_price: float, original_price: float) -> float:
        """Calculate percentage change between two prices."""
        if pd.isna(current_price) or pd.isna(original_price) or original_price <= 0:
            return 0.0
        return ((current_price - original_price) / original_price) * 100
    
    @staticmethod
    def calculate_conviction_score(portfolio_pct: float, manager_count: int) -> float:
        """Calculate conviction score based on portfolio percentage and manager count."""
        if pd.isna(portfolio_pct) or manager_count <= 0:
            return 0.0
        return portfolio_pct * manager_count
    
    @staticmethod
    def calculate_52_week_position(current_price: float, week_52_low: float, week_52_high: float) -> float:
        """Calculate where current price sits in 52-week range (0-100%)."""
        if any(pd.isna([current_price, week_52_low, week_52_high])) or week_52_high <= week_52_low:
            return 50.0  # Default to middle if data is invalid
        
        if current_price <= week_52_low:
            return 0.0
        elif current_price >= week_52_high:
            return 100.0
        else:
            return ((current_price - week_52_low) / (week_52_high - week_52_low)) * 100
    
    @staticmethod
    def is_near_52_week_low(current_price: float, week_52_low: float, threshold_pct: float = 10.0) -> bool:
        """Check if current price is near 52-week low (within threshold percentage)."""
        if pd.isna(current_price) or pd.isna(week_52_low) or week_52_low <= 0:
            return False
        
        threshold_price = week_52_low * (1 + threshold_pct / 100)
        return current_price <= threshold_price
    
    @staticmethod
    def is_near_52_week_high(current_price: float, week_52_high: float, threshold_pct: float = 10.0) -> bool:
        """Check if current price is near 52-week high (within threshold percentage)."""
        if pd.isna(current_price) or pd.isna(week_52_high) or week_52_high <= 0:
            return False
        
        threshold_price = week_52_high * (1 - threshold_pct / 100)
        return current_price >= threshold_price


class TextAnalysisUtils:
    """Utility functions for analyzing text data like activities."""
    
    @staticmethod
    def extract_percentage_change(action: str) -> Optional[float]:
        """Extract percentage change from action string."""
        if pd.isna(action):
            return None
        
        action_str = str(action)
        
        # Handle special cases
        if "Sold All" in action_str:
            return -100.0
        
        # Extract percentage using regex
        match = re.search(r"([+-]?\d+\.?\d*)\s*%", action_str)
        if match:
            return float(match.group(1))
        
        # Check if action contains Reduce (negative change)
        if "Reduce" in action_str:
            match = re.search(r"Reduce\s+([+-]?\d+\.?\d*)", action_str)
            if match:
                value = float(match.group(1))
                return -abs(value)  # Ensure it's negative for reductions
        
        return None
    
    @staticmethod
    def extract_action_type(action: str) -> str:
        """Extract action type from action string."""
        if pd.isna(action):
            return "Hold"
            
        action_lower = str(action).lower()
        
        if "sold all" in action_lower or "sold out" in action_lower or "exit" in action_lower:
            return "Sell"
        elif "new" in action_lower or "buy" in action_lower:
            return "Buy"
        elif "add" in action_lower or "+" in action:
            return "Add"
        elif "reduce" in action_lower or "-" in action:
            return "Reduce"
        else:
            return "Hold"
    
    @staticmethod
    def clean_company_name(company_name: str) -> str:
        """Clean and standardize company names."""
        if pd.isna(company_name):
            return ""
        
        name = str(company_name).strip()
        
        # Remove common prefixes/suffixes that might be inconsistent
        name = re.sub(r'^-\s*', '', name)  # Remove leading dash
        name = re.sub(r'\s+', ' ', name)   # Multiple spaces to single space
        
        return name.strip()


class ScoringUtils:
    """Utility functions for creating various scoring algorithms."""
    
    @staticmethod
    def calculate_hidden_gem_score(
        manager_count: int,
        max_portfolio_pct: float,
        avg_portfolio_pct: float,
        recent_activity_score: float = 0,
        price_momentum_score: float = 0,
        manager_quality_score: float = 1.0
    ) -> float:
        """
        Calculate sophisticated hidden gem score using multiple factors.
        
        Factors:
        1. Exclusivity (low manager count but high conviction)
        2. Conviction (high portfolio percentages)
        3. Recent activity (buying momentum)
        4. Price momentum (technical factors)
        5. Manager quality (track record weighting)
        """
        # Factor 1: Exclusivity score (inverse of manager count, but reward some managers)
        exclusivity_score = max(0, (5 - manager_count) / 4) if manager_count <= 5 else 0
        
        # Factor 2: Conviction score (based on portfolio allocations)
        conviction_score = min(max_portfolio_pct / 10, 1.0) + (avg_portfolio_pct / 20)
        
        # Factor 3: Recent activity score (0-1, passed in)
        activity_score = min(recent_activity_score, 1.0)
        
        # Factor 4: Price momentum score (0-1, passed in)
        momentum_score = min(price_momentum_score, 1.0)
        
        # Factor 5: Manager quality multiplier (1.0+ based on track record)
        quality_multiplier = max(manager_quality_score, 0.5)
        
        # Weighted combination
        base_score = (
            exclusivity_score * 0.3 +
            conviction_score * 0.4 +
            activity_score * 0.15 +
            momentum_score * 0.15
        )
        
        # Apply manager quality multiplier
        final_score = base_score * quality_multiplier
        
        return round(final_score, 3)
    
    @staticmethod
    def calculate_appeal_score(
        manager_count: int,
        avg_portfolio_pct: float,
        recent_buy_count: int = 0,
        value_factor: float = 0,
        max_score: float = 10.0
    ) -> float:
        """Calculate general stock appeal score (0-10)."""
        score = 0
        
        # Manager count factor (max 3 points)
        score += min((manager_count / 10) * 3, 3)
        
        # Portfolio percentage factor (max 2 points)
        score += min((avg_portfolio_pct / 5) * 2, 2)
        
        # Value factor (max 2 points)
        score += min(value_factor * 2, 2)
        
        # Recent activity factor (max 3 points)
        score += min((recent_buy_count / 20) * 3, 3)
        
        return round(min(score, max_score), 2)
    
    @staticmethod
    def calculate_manager_quality_score(
        manager_id: str,
        total_portfolio_value: float = 0,
        track_record_years: int = 0,
        performance_metrics: dict = None
    ) -> float:
        """Calculate manager quality score for weighting (1.0 = average, higher = better)."""
        # Default scores for well-known high-quality managers
        premium_managers = {
            "berkshire": 2.0,
            "bh": 2.0,
            "munger": 1.8,
            "cm": 1.8,
            "akre": 1.6,
            "value": 1.5,  # Li Lu
            "pershing": 1.4,  # Ackman
            "mohnish": 1.3,
        }
        
        base_score = premium_managers.get(manager_id, 1.0)
        
        # Adjust based on portfolio size (larger = more credible)
        if total_portfolio_value > 10_000_000_000:  # $10B+
            base_score *= 1.2
        elif total_portfolio_value > 1_000_000_000:  # $1B+
            base_score *= 1.1
        
        return min(base_score, 2.5)  # Cap at 2.5x