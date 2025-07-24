"""
Price-based analysis module.

Analyzes stocks by price ranges and thresholds to identify opportunities
at different investment levels (under $5, $10, $20, $50, $100).
"""

import logging
import pandas as pd
from typing import Dict, List, Optional, Any, Union

from .base_analyzer import BaseAnalyzer, MultiAnalyzer
from ..data.data_loader import DataLoader


class PriceAnalyzer(MultiAnalyzer):
    """Analyzes stocks by price ranges for different investment strategies."""
    
    def __init__(self, data_loader: DataLoader) -> None:
        """Initialize with data loader."""
        super().__init__(data_loader)
    
    def analyze_all(self) -> Dict[str, pd.DataFrame]:
        """Run all price-based analyses."""
        results = {}
        
        # Price threshold analyses
        price_thresholds = [5, 10, 20, 50, 100]
        for threshold in price_thresholds:
            results[f"stocks_under_${threshold}"] = self.analyze_stocks_under_price(threshold)
        
        # Additional price analyses
        results["high_conviction_low_price"] = self.analyze_high_conviction_low_price()
        results["value_price_opportunities"] = self.analyze_value_price_opportunities()
        
        # Log summaries
        for name, df in results.items():
            self.log_analysis_summary(df, name)
        
        return self.format_all_outputs(results)
    
    def analyze_stocks_under_price(self, max_price: float) -> pd.DataFrame:
        """
        Analyze stocks under a specific price threshold.
        
        Enhanced from original - uses Dataroma current_price data.
        
        Args:
            max_price: Maximum price threshold
            
        Returns:
            DataFrame with stocks under the price threshold
        """
        if not self.validate_required_columns(
            self.data.holdings_df, ["ticker", "manager_id", "value"]
        ):
            return pd.DataFrame()
        
        holdings = self.filter_active_holdings(self.data.holdings_df)
        if holdings.empty:
            return pd.DataFrame()
        
        # Get current price - try multiple sources
        analysis_df = holdings.copy()
        
        if "current_price" not in analysis_df.columns:
            # Estimate from value/shares if no direct price data
            if "shares" in analysis_df.columns and "value" in analysis_df.columns:
                analysis_df["current_price"] = (
                    analysis_df["value"] / analysis_df["shares"]
                ).fillna(0)
            else:
                logging.warning(f"No price data available for price analysis under ${max_price}")
                return pd.DataFrame()
        
        # Filter by price threshold and exclude zero-value positions
        price_filtered = analysis_df[
            (analysis_df["current_price"] > 0) &
            (analysis_df["current_price"] <= max_price) &
            (analysis_df["value"] > 0)  # Active positions only
        ].copy()
        
        if price_filtered.empty:
            return pd.DataFrame()
        
        # Group by ticker for analysis
        grouped = price_filtered.groupby("ticker").agg({
            "manager_id": ["count", self.get_manager_summary],
            "current_price": "first",
            "shares": "sum" if "shares" in price_filtered.columns else "count",
            "value": "sum",
            "portfolio_percent": ["mean", "max"] if "portfolio_percent" in price_filtered.columns else "mean",
            "stock": "first" if "stock" in price_filtered.columns else "count",
        })
        
        # Flatten columns
        if "portfolio_percent" in price_filtered.columns:
            grouped.columns = [
                "manager_count", "managers", "current_price", "total_shares",
                "total_value", "avg_portfolio_pct", "max_portfolio_pct", "company_name"
            ]
        else:
            grouped.columns = [
                "manager_count", "managers", "current_price", "total_shares", 
                "total_value", "avg_portfolio_pct", "company_name"
            ]
        
        # Reset index to make ticker a regular column and ensure we can modify the dataframe
        grouped = grouped.reset_index()
        
        # Add recent activity if available
        if (self.data.history_df is not None and 
            not self.data.history_df.empty and
            "action_type" in self.data.history_df.columns):
            
            # Get recent quarters for filtering
            recent_quarters = self.get_recent_quarters(3)
            
            recent_activity = (
                self.data.history_df[
                    self.data.history_df["action_type"].isin(["Buy", "Add"]) &
                    (self.data.history_df["period"].isin(recent_quarters))
                ]
                .groupby("ticker")
                .agg({"period": ["count", "max"]})
            )
            recent_activity.columns = ["buy_count", "last_buy_period"]
            grouped = grouped.join(recent_activity, how="left")
            grouped["buy_count"] = grouped["buy_count"].fillna(0)
        
        # Calculate price opportunity score
        grouped["price_opportunity_score"] = (
            grouped["manager_count"] * 2 +  # More managers = better
            grouped["avg_portfolio_pct"] +   # Higher conviction = better
            grouped.get("buy_count", 0)      # Recent activity = better
        )
        
        # Add price category
        if max_price <= 5:
            grouped["price_category"] = "Ultra-Low Price"
        elif max_price <= 10:
            grouped["price_category"] = "Low Price" 
        elif max_price <= 20:
            grouped["price_category"] = "Affordable"
        elif max_price <= 50:
            grouped["price_category"] = "Mid-Price"
        else:
            grouped["price_category"] = "Higher Price"
        
        # Filter out very small positions (less than $10k total value)
        meaningful_positions = grouped[grouped["total_value"] >= 10000].copy()
        
        # Sort by manager count and portfolio percentage
        meaningful_positions = meaningful_positions.sort_values(
            by=["manager_count", "avg_portfolio_pct"], ascending=[False, False]
        )
        
        return self.format_output(meaningful_positions.reset_index()).head(50)
    
    def analyze_high_conviction_low_price(self) -> pd.DataFrame:
        """
        Find low-priced stocks with high manager conviction (>5% positions).
        
        Returns:
            DataFrame with high-conviction low-price opportunities
        """
        if not self.validate_required_columns(
            self.data.holdings_df, ["ticker", "portfolio_percent"]
        ):
            return pd.DataFrame()
        
        holdings = self.filter_active_holdings(self.data.holdings_df)
        if holdings.empty:
            return pd.DataFrame()
        
        # Get current prices
        if "current_price" not in holdings.columns:
            if "shares" in holdings.columns and "value" in holdings.columns:
                holdings = holdings.copy()
                holdings["current_price"] = (holdings["value"] / holdings["shares"]).fillna(0)
            else:
                return pd.DataFrame()
        
        # Filter for high conviction (>5%) AND low price (<$25)
        high_conviction_low_price = holdings[
            (holdings["portfolio_percent"] > 5.0) &
            (holdings["current_price"] > 0) &
            (holdings["current_price"] < 25.0)
        ].copy()
        
        if high_conviction_low_price.empty:
            return pd.DataFrame()
        
        # Group by ticker
        grouped = high_conviction_low_price.groupby("ticker").agg({
            "manager_id": ["count", self.get_manager_summary],
            "current_price": "first",
            "portfolio_percent": ["mean", "max"],
            "value": "sum",
            "shares": "sum" if "shares" in high_conviction_low_price.columns else "count",
            "stock": "first" if "stock" in high_conviction_low_price.columns else "count",
        })
        
        grouped.columns = [
            "manager_count", "managers", "current_price", "avg_portfolio_pct", 
            "max_portfolio_pct", "total_value", "total_shares", "company_name"
        ]
        
        # Reset index to make ticker a regular column and ensure we can modify the dataframe
        grouped = grouped.reset_index()
        
        # Calculate conviction-price score
        grouped["conviction_price_score"] = (
            grouped["max_portfolio_pct"] * 2 +  # Higher conviction = better
            (25 - grouped["current_price"]) +   # Lower price = better
            grouped["manager_count"]             # More managers = better
        )
        
        # Categorize opportunities
        grouped["opportunity_type"] = "High Conviction Low Price"
        grouped.loc[
            (grouped["max_portfolio_pct"] > 10) & (grouped["current_price"] < 10),
            "opportunity_type"
        ] = "Deep Conviction Bargain"
        grouped.loc[
            grouped["manager_count"] >= 3,
            "opportunity_type"
        ] = "Multi-Manager Conviction"
        
        # Sort by conviction-price score
        grouped = grouped.sort_values(by="conviction_price_score", ascending=False)
        
        return self.format_output(grouped.reset_index()).head(30)
    
    def analyze_value_price_opportunities(self) -> pd.DataFrame:
        """
        Find value opportunities using price momentum and manager activity.
        
        Returns:
            DataFrame with value opportunities based on price and activity analysis
        """
        if not self.validate_required_columns(
            self.data.holdings_df, ["ticker", "manager_id", "value"]
        ):
            return pd.DataFrame()
        
        holdings = self.filter_active_holdings(self.data.holdings_df)
        if holdings.empty:
            return pd.DataFrame()
        
        # Calculate price metrics
        price_cols = ["current_price", "reported_price"]
        available_price_cols = [col for col in price_cols if col in holdings.columns]
        
        if len(available_price_cols) < 2:
            # Estimate prices if not available
            if "shares" in holdings.columns and "value" in holdings.columns:
                holdings = holdings.copy()
                holdings["current_price"] = (holdings["value"] / holdings["shares"]).fillna(0)
                if "reported_price" not in holdings.columns:
                    holdings["reported_price"] = holdings["current_price"]  # Fallback
            else:
                return pd.DataFrame()
        
        # Group by ticker for value analysis
        value_analysis = holdings.groupby("ticker").agg({
            "manager_id": ["count", self.get_manager_summary],
            "current_price": "first",
            "reported_price": "mean" if "reported_price" in holdings.columns else "first",
            "value": "sum",
            "portfolio_percent": ["mean", "max"] if "portfolio_percent" in holdings.columns else "mean",
            "stock": "first" if "stock" in holdings.columns else "count",
        })
        
        # Flatten columns
        if "portfolio_percent" in holdings.columns:
            value_analysis.columns = [
                "manager_count", "managers", "current_price", "avg_reported_price",
                "total_value", "avg_portfolio_pct", "max_portfolio_pct", "company_name"
            ]
        else:
            value_analysis.columns = [
                "manager_count", "managers", "current_price", "avg_reported_price",
                "total_value", "avg_portfolio_pct", "company_name"
            ]
        
        # Calculate price change
        value_analysis["price_change_pct"] = (
            (value_analysis["current_price"] - value_analysis["avg_reported_price"]) /
            value_analysis["avg_reported_price"] * 100
        ).fillna(0)
        
        # Add 52-week data if available
        if "52_week_low" in holdings.columns:
            week_52_data = holdings.groupby("ticker")["52_week_low"].first()
            value_analysis = value_analysis.join(week_52_data)
            
            # Calculate discount to 52-week low
            value_analysis["discount_to_52w_low_pct"] = (
                (value_analysis["current_price"] - value_analysis["52_week_low"]) /
                value_analysis["52_week_low"] * 100
            ).fillna(100)
        else:
            value_analysis["discount_to_52w_low_pct"] = 100  # Assume no discount
        
        # Add recent buying activity
        recent_buy_count = 0
        if (self.data.history_df is not None and 
            not self.data.history_df.empty and
            "action_type" in self.data.history_df.columns):
            
            # Get recent quarters for filtering
            recent_quarters = self.get_recent_quarters(3)
            
            recent_buys = (
                self.data.history_df[
                    (self.data.history_df["action_type"].isin(["Buy", "Add"])) &
                    (self.data.history_df["period"].isin(recent_quarters))
                ]
                .groupby("ticker")
                .size()
                .to_frame("recent_buy_count")
            )
            value_analysis = value_analysis.join(recent_buys, how="left")
            value_analysis["recent_buy_count"] = value_analysis["recent_buy_count"].fillna(0)
        
        # Calculate value opportunity score
        value_analysis["value_opportunity_score"] = 0
        
        # Factor 1: Price discount (negative price change = positive score)
        value_analysis["value_opportunity_score"] += (
            value_analysis["price_change_pct"].apply(lambda x: max(0, -x) / 5)
        )
        
        # Factor 2: Proximity to 52-week low (closer = better)
        value_analysis["value_opportunity_score"] += (
            (100 - value_analysis["discount_to_52w_low_pct"]) / 20
        )
        
        # Factor 3: Manager conviction
        value_analysis["value_opportunity_score"] += (
            value_analysis["avg_portfolio_pct"] / 2
        )
        
        # Factor 4: Recent activity
        if "recent_buy_count" in value_analysis.columns:
            value_analysis["value_opportunity_score"] += (
                value_analysis["recent_buy_count"]
            )
        
        # Factor 5: Manager count
        value_analysis["value_opportunity_score"] += (
            value_analysis["manager_count"]
        )
        
        # Filter for meaningful opportunities (score > 5)
        value_opportunities = value_analysis[
            value_analysis["value_opportunity_score"] > 5
        ].copy()
        
        if value_opportunities.empty:
            # If no high-scoring opportunities, show top scoring ones anyway
            value_opportunities = value_analysis.nlargest(20, "value_opportunity_score").copy()
        
        # Categorize value type
        value_opportunities["value_type"] = "Value Opportunity"
        value_opportunities.loc[
            value_opportunities["price_change_pct"] < -10, "value_type"
        ] = "Price Discount"
        value_opportunities.loc[
            value_opportunities["discount_to_52w_low_pct"] < 20, "value_type" 
        ] = "Near 52W Low"
        value_opportunities.loc[
            (value_opportunities["recent_buy_count"] > 3) & 
            (value_opportunities["avg_portfolio_pct"] > 3), "value_type"
        ] = "Active Accumulation"
        
        # Sort by value opportunity score
        value_opportunities = value_opportunities.sort_values(
            by="value_opportunity_score", ascending=False
        )
        
        return self.format_output(value_opportunities.reset_index()).head(40)


class StocksUnderPriceAnalyzer(BaseAnalyzer):
    """Focused analyzer for specific price thresholds."""
    
    def __init__(self, data_loader: DataLoader, max_price: float) -> None:
        """Initialize with specific price threshold."""
        super().__init__(data_loader)
        self.max_price = max_price
    
    def analyze(self) -> pd.DataFrame:
        """Analyze stocks under the specified price threshold."""
        price_analyzer = PriceAnalyzer(self.data)
        return price_analyzer.analyze_stocks_under_price(self.max_price)