"""
Momentum and activity analysis module.

Analyzes buying/selling momentum, new positions, and activity trends
to identify stocks with strong institutional interest.
"""

import logging
import pandas as pd
from typing import Dict, List, Optional, Any, Union, Callable

from .base_analyzer import BaseAnalyzer, MultiAnalyzer
from ..utils.calculations import TextAnalysisUtils
from ..data.data_loader import DataLoader


class MomentumAnalyzer(MultiAnalyzer):
    """Analyzes buying/selling momentum and activity trends."""
    
    def __init__(self, data_loader: DataLoader) -> None:
        """Initialize with data loader."""
        super().__init__(data_loader)
    
    def analyze_all(self) -> Dict[str, pd.DataFrame]:
        """Run all momentum analyses."""
        results = {}
        
        # Core momentum analyses
        results["momentum_stocks"] = self.analyze_momentum_stocks()
        results["new_positions"] = self.analyze_new_positions()
        results["52_week_low_buys"] = self.analyze_52_week_low_buys()
        results["52_week_high_sells"] = self.analyze_52_week_high_sells()
        results["most_sold_stocks"] = self.analyze_most_sold_stocks()
        results["concentration_changes"] = self.analyze_concentration_changes()
        
        # Log summaries
        for name, df in results.items():
            self.log_analysis_summary(df, name)
        
        return self.format_all_outputs(results)
    
    def analyze_momentum_stocks(self) -> pd.DataFrame:
        """
        Identify stocks with momentum based on recent buying activity.
        
        Returns:
            DataFrame with momentum stocks based on recent accumulation
        """
        if (self.data.history_df is None or 
            self.data.history_df.empty or
            "action_type" not in self.data.history_df.columns):
            logging.warning("No activity data available for momentum analysis")
            return pd.DataFrame()
        
        # Filter recent buying activities (last 3 quarters)
        recent_quarters = self.get_recent_quarters(3)
        recent_buys = self.data.history_df[
            (self.data.history_df["action_type"].isin(["Buy", "Add"])) &
            (self.data.history_df["period"].isin(recent_quarters))
        ].copy()
        
        if recent_buys.empty:
            return pd.DataFrame()
        
        # Group by ticker for momentum analysis
        momentum = recent_buys.groupby("ticker").agg({
            "manager_id": ["count", self.get_manager_summary],
            "action": self.get_activity_summary,
            "period": lambda x: ", ".join(x.unique()),
            "shares": "sum" if "shares" in recent_buys.columns else "count",
        })
        
        # Flatten columns
        if "shares" in recent_buys.columns:
            momentum.columns = ["buy_count", "managers", "activities", "periods", "shares_accumulated"]
        else:
            momentum.columns = ["buy_count", "managers", "activities", "periods"]
        
        # Add current holdings information
        if self.data.holdings_df is not None and not self.data.holdings_df.empty:
            current_holdings = self.data.holdings_df.groupby("ticker").agg({
                "value": "sum",
                "portfolio_percent": "mean" if "portfolio_percent" in self.data.holdings_df.columns else "count",
                "manager_id": "count",
                "stock": "first" if "stock" in self.data.holdings_df.columns else "count",
                "current_price": "first" if "current_price" in self.data.holdings_df.columns else "count",
            })
            
            # Rename columns
            rename_dict = {
                "value": "current_total_value",
                "portfolio_percent": "avg_portfolio_pct", 
                "manager_id": "current_holders",
            }
            if "stock" in self.data.holdings_df.columns:
                rename_dict["stock"] = "company_name"
            if "current_price" in self.data.holdings_df.columns:
                rename_dict["current_price"] = "current_price"
            
            current_holdings = current_holdings.rename(columns=rename_dict)
            
            momentum = momentum.join(current_holdings, how="left")
        
        # Calculate momentum score
        momentum["momentum_score"] = 0
        
        # Factor 1: Number of buying instances (more = better)
        momentum["momentum_score"] += momentum["buy_count"] * 2
        
        # Factor 2: Number of managers buying (more = better)
        if "current_holders" in momentum.columns:
            momentum["momentum_score"] += momentum["current_holders"]
        
        # Factor 3: Current position size (larger positions = more conviction)
        if "avg_portfolio_pct" in momentum.columns:
            momentum["momentum_score"] += momentum["avg_portfolio_pct"]
        
        # Factor 4: Recency (more recent activity = higher score)
        # Dynamic recency scoring based on recent quarters
        recent_quarters = self.get_recent_quarters(3)
        def score_recency(periods_str):
            if not recent_quarters:
                return 1
            # Check if most recent quarter is present
            if len(recent_quarters) > 0 and recent_quarters[0] in str(periods_str):
                return 5
            elif len(recent_quarters) > 1 and recent_quarters[1] in str(periods_str):
                return 4
            elif len(recent_quarters) > 2 and recent_quarters[2] in str(periods_str):
                return 3
            else:
                return 1
        
        momentum["recency_bonus"] = momentum["periods"].apply(score_recency)
        momentum["momentum_score"] += momentum["recency_bonus"]
        
        # Add momentum classification
        momentum["momentum_type"] = "Moderate"
        momentum.loc[momentum["buy_count"] >= 5, "momentum_type"] = "Strong"
        momentum.loc[momentum["buy_count"] >= 10, "momentum_type"] = "Very Strong"
        
        # Dynamic recent surge detection
        if recent_quarters and len(recent_quarters) >= 2:
            recent_pattern = "|".join(recent_quarters[:2])  # Last 2 quarters
            momentum.loc[
                (momentum["buy_count"] >= 3) & 
                (momentum["periods"].str.contains(recent_pattern, na=False)), 
                "momentum_type"
            ] = "Recent Surge"
        
        # Filter out low momentum (require at least 2 buy actions)
        momentum = momentum[momentum["buy_count"] >= 2].copy()
        
        # Sort by momentum score
        momentum = momentum.sort_values("momentum_score", ascending=False)
        
        return self.format_output(momentum.reset_index()).head(50)
    
    def analyze_new_positions(self) -> pd.DataFrame:
        """
        Identify new positions with their initiation dates.
        
        Returns:
            DataFrame with new positions and timing information
        """
        if (self.data.history_df is None or 
            self.data.history_df.empty or
            "action_type" not in self.data.history_df.columns):
            return pd.DataFrame()
        
        # Get recent quarters only
        recent_quarters = self.get_recent_quarters(3)
        
        # Filter for new positions (Buy actions) from recent quarters
        new_positions = self.data.history_df[
            (self.data.history_df["action_type"] == "Buy") &
            (self.data.history_df["period"].isin(recent_quarters))
        ].copy()
        
        if new_positions.empty:
            return pd.DataFrame()
        
        # Group by ticker and manager to get initiation details
        agg_dict = {
            "action": "first",
        }
        
        if "shares" in new_positions.columns:
            agg_dict["shares"] = "sum"
        if "value" in new_positions.columns:
            agg_dict["value"] = "sum"
            
        new_analysis = new_positions.groupby(["ticker", "manager_id", "period"]).agg(agg_dict).reset_index()
        
        # Add manager names
        new_analysis["manager_name"] = new_analysis["manager_id"].map(
            lambda x: self.data.manager_names.get(x, x)
        )
        
        # Add company names if available
        if (self.data.holdings_df is not None and 
            "stock" in self.data.holdings_df.columns):
            company_names = self.data.holdings_df.groupby("ticker")["stock"].first()
            new_analysis = new_analysis.set_index("ticker").join(
                company_names.rename("company_name")
            ).reset_index()
        
        # Add current position status
        if self.data.holdings_df is not None and not self.data.holdings_df.empty:
            current_status = self.data.holdings_df.groupby(["ticker", "manager_id"]).agg({
                "value": "sum",
                "portfolio_percent": "first" if "portfolio_percent" in self.data.holdings_df.columns else "count",
                "current_price": "first" if "current_price" in self.data.holdings_df.columns else "count",
            })
            
            # Merge current status
            new_analysis = new_analysis.set_index(["ticker", "manager_id"]).join(
                current_status, how="left", rsuffix="_current"
            ).reset_index()
            
            # Calculate position growth (if both values available)
            if "value" in new_analysis.columns and "value_current" in new_analysis.columns:
                new_analysis["position_growth"] = (
                    (new_analysis["value_current"] - new_analysis["value"]) / 
                    new_analysis["value"] * 100
                ).fillna(0)
        
        # Sort by period (most recent first) and value
        new_analysis = new_analysis.sort_values(
            ["period", "value"], ascending=[False, False]
        )
        
        return self.format_output(new_analysis).head(100)
    
    def analyze_52_week_low_buys(self) -> pd.DataFrame:
        """
        Find stocks being bought near 52-week lows using Dataroma data.
        
        FIXED: Now uses actual 52-week data from Dataroma HTML parsing.
        
        Returns:
            DataFrame with value buying opportunities
        """
        if (self.data.history_df is None or 
            self.data.history_df.empty or
            "action_type" not in self.data.history_df.columns):
            return pd.DataFrame()
        
        # Get recent buy activities
        recent_buys = self.data.history_df[
            (self.data.history_df["action_type"].isin(["Buy", "Add"])) &
            (self.data.history_df["period"].isin(self.get_recent_quarters(3)))
        ].copy()
        
        if recent_buys.empty:
            return pd.DataFrame()
        
        # Group buying activity by ticker
        buy_summary = recent_buys.groupby("ticker").agg({
            "manager_id": ["count", self.get_manager_summary],
            "shares": "sum" if "shares" in recent_buys.columns else "count",
            "period": lambda x: ", ".join(x.unique()),
            "action": self.get_activity_summary,
        })
        
        buy_summary.columns = ["buy_count", "buying_managers", "shares_bought", "periods", "activities"]
        
        # Get current holdings with 52-week data
        if (self.data.holdings_df is not None and 
            not self.data.holdings_df.empty):
            
            # Check if 52-week data is available
            required_cols = ["current_price", "52_week_low"]
            available_cols = [col for col in required_cols if col in self.data.holdings_df.columns]
            
            if len(available_cols) >= 2:
                # We have 52-week data from Dataroma!
                price_data = self.data.holdings_df.groupby("ticker").agg({
                    "current_price": "first",
                    "52_week_low": "first",
                    "52_week_high": "first" if "52_week_high" in self.data.holdings_df.columns else "first",
                    "value": "sum",
                    "portfolio_percent": "mean" if "portfolio_percent" in self.data.holdings_df.columns else "count",
                    "stock": "first" if "stock" in self.data.holdings_df.columns else "count",
                })
                
                # Calculate 52-week metrics
                if "52_week_high" in price_data.columns:
                    price_data["52_week_position_pct"] = price_data.apply(
                        lambda row: self.calc.calculate_52_week_position(
                            row["current_price"], row["52_week_low"], row["52_week_high"]
                        ), axis=1
                    )
                else:
                    # Just use low data
                    price_data["52_week_position_pct"] = (
                        (price_data["current_price"] - price_data["52_week_low"]) /
                        price_data["52_week_low"] * 100
                    ).clip(0, 100)
                
                # Identify near-low positions
                price_data["near_52w_low"] = price_data.apply(
                    lambda row: self.calc.is_near_52_week_low(
                        row["current_price"], row["52_week_low"], 15.0
                    ), axis=1
                )
                
                buy_summary = buy_summary.join(price_data, how="inner")
                
                # Filter for positions being bought near 52-week lows
                low_buys = buy_summary[
                    (buy_summary["near_52w_low"] == True) |
                    (buy_summary["52_week_position_pct"] < 25)  # Bottom 25% of range
                ].copy()
                
                if not low_buys.empty:
                    # Calculate value opportunity score
                    low_buys["value_opportunity_score"] = (
                        low_buys["buy_count"] * 2 +  # More buys = better
                        (100 - low_buys["52_week_position_pct"]) / 10 +  # Lower position = better
                        low_buys["value"] / 1000000  # Position size factor
                    )
                    
                    # Categorize opportunity type
                    low_buys["opportunity_type"] = "Value Buying"
                    low_buys.loc[
                        low_buys["52_week_position_pct"] < 10, "opportunity_type"
                    ] = "Deep Value"
                    low_buys.loc[
                        low_buys["buy_count"] >= 5, "opportunity_type"
                    ] = "Strong Accumulation"
                    
                    # Sort by value opportunity score
                    low_buys = low_buys.sort_values("value_opportunity_score", ascending=False)
                    
                    return self.format_output(low_buys.reset_index()).head(40)
        
        # Fallback: If no 52-week data, show recent accumulation
        buy_summary["buy_signal"] = "Recent Accumulation"
        buy_summary = buy_summary.sort_values("buy_count", ascending=False)
        
        return self.format_output(buy_summary.reset_index()).head(30)
    
    def analyze_52_week_high_sells(self) -> pd.DataFrame:
        """
        Find stocks being sold near 52-week highs using Dataroma data.
        
        FIXED: Now uses actual 52-week data from Dataroma HTML parsing.
        
        Returns:
            DataFrame with profit-taking activities  
        """
        if (self.data.history_df is None or 
            self.data.history_df.empty or
            "action_type" not in self.data.history_df.columns):
            return pd.DataFrame()
        
        # Get recent sell activities
        recent_sells = self.data.history_df[
            (self.data.history_df["action_type"].isin(["Sell", "Reduce"])) &
            (self.data.history_df["period"].isin(self.get_recent_quarters(3)))
        ].copy()
        
        if recent_sells.empty:
            return pd.DataFrame()
        
        # Group selling activity by ticker
        sell_summary = recent_sells.groupby("ticker").agg({
            "manager_id": ["count", self.get_manager_summary],
            "shares": "sum" if "shares" in recent_sells.columns else "count", 
            "period": lambda x: ", ".join(x.unique()),
            "action": self.get_activity_summary,
        })
        
        sell_summary.columns = ["sell_count", "selling_managers", "shares_sold", "periods", "activities"]
        
        # Get current holdings with 52-week data
        if (self.data.holdings_df is not None and 
            not self.data.holdings_df.empty):
            
            # Check for 52-week high data
            required_cols = ["current_price", "52_week_high"]
            available_cols = [col for col in required_cols if col in self.data.holdings_df.columns]
            
            if len(available_cols) >= 2:
                # We have 52-week data!
                price_data = self.data.holdings_df.groupby("ticker").agg({
                    "current_price": "first",
                    "52_week_low": "first" if "52_week_low" in self.data.holdings_df.columns else "first",
                    "52_week_high": "first",
                    "value": "sum",
                    "portfolio_percent": "mean" if "portfolio_percent" in self.data.holdings_df.columns else "count",
                    "stock": "first" if "stock" in self.data.holdings_df.columns else "count",
                })
                
                # Calculate 52-week position
                if "52_week_low" in price_data.columns:
                    price_data["52_week_position_pct"] = price_data.apply(
                        lambda row: self.calc.calculate_52_week_position(
                            row["current_price"], row["52_week_low"], row["52_week_high"]
                        ), axis=1
                    )
                else:
                    # Just use high data
                    price_data["52_week_position_pct"] = (
                        row["current_price"] / row["52_week_high"] * 100
                    ).clip(0, 100)
                
                # Identify near-high positions
                price_data["near_52w_high"] = price_data.apply(
                    lambda row: self.calc.is_near_52_week_high(
                        row["current_price"], row["52_week_high"], 15.0
                    ), axis=1
                )
                
                sell_summary = sell_summary.join(price_data, how="inner")
                
                # Filter for positions being sold near 52-week highs
                high_sells = sell_summary[
                    (sell_summary["near_52w_high"] == True) |
                    (sell_summary["52_week_position_pct"] > 75)  # Top 25% of range
                ].copy()
                
                if not high_sells.empty:
                    # Calculate profit-taking score
                    high_sells["profit_taking_score"] = (
                        high_sells["sell_count"] * 2 +  # More sells = more conviction
                        high_sells["52_week_position_pct"] / 10 +  # Higher position = better timing
                        high_sells["value"] / 1000000  # Position size factor
                    )
                    
                    # Categorize selling type  
                    high_sells["selling_type"] = "Profit Taking"
                    high_sells.loc[
                        high_sells["52_week_position_pct"] > 90, "selling_type"
                    ] = "Peak Selling"
                    high_sells.loc[
                        high_sells["sell_count"] >= 5, "selling_type"
                    ] = "Heavy Distribution"
                    
                    # Sort by profit-taking score
                    high_sells = high_sells.sort_values("profit_taking_score", ascending=False)
                    
                    return self.format_output(high_sells.reset_index()).head(40)
        
        # Fallback: Show all recent selling activity
        sell_summary["sell_signal"] = "Recent Selling"
        sell_summary = sell_summary.sort_values("sell_count", ascending=False)
        
        return self.format_output(sell_summary.reset_index()).head(30)
    
    def analyze_most_sold_stocks(self) -> pd.DataFrame:
        """
        Find stocks with the most selling activity across all periods.
        
        Returns:
            DataFrame with heavily sold stocks  
        """
        if (self.data.history_df is None or 
            self.data.history_df.empty or
            "action_type" not in self.data.history_df.columns):
            return pd.DataFrame()
        
        # Get all sell activities
        all_sells = self.data.history_df[
            self.data.history_df["action_type"].isin(["Sell", "Reduce"])
        ].copy()
        
        if all_sells.empty:
            return pd.DataFrame()
        
        # Group by ticker for selling analysis
        sell_analysis = all_sells.groupby("ticker").agg({
            "manager_id": ["count", self.get_manager_summary],
            "action_type": lambda x: dict(x.value_counts()),
            "period": lambda x: ", ".join(sorted(x.unique(), reverse=True)),
            "shares": "sum" if "shares" in all_sells.columns else "count",
        })
        
        sell_analysis.columns = ["total_sells", "selling_managers", "action_breakdown", "periods", "shares_sold"]
        
        # Get current holders (if any remain)
        if self.data.holdings_df is not None and not self.data.holdings_df.empty:
            current_holders = self.data.holdings_df.groupby("ticker").agg({
                "manager_id": "count",
                "value": "sum",
                "stock": "first" if "stock" in self.data.holdings_df.columns else "count",
            })
            current_holders.columns = ["remaining_holders", "remaining_value", "company_name"]
            
            sell_analysis = sell_analysis.join(current_holders, how="left")
            sell_analysis["remaining_holders"] = sell_analysis["remaining_holders"].fillna(0)
            sell_analysis["remaining_value"] = sell_analysis["remaining_value"].fillna(0)
        else:
            sell_analysis["remaining_holders"] = 0
            sell_analysis["remaining_value"] = 0
        
        # Calculate exit metrics
        sell_analysis["exit_rate_pct"] = (
            sell_analysis["total_sells"] / 
            (sell_analysis["total_sells"] + sell_analysis["remaining_holders"]) * 100
        ).fillna(100)
        
        # Format action breakdown for display
        sell_analysis["sell_pattern"] = sell_analysis["action_breakdown"].apply(
            lambda x: ", ".join([f"{action}: {count}" for action, count in x.items()])
        )
        
        # Determine exit status
        sell_analysis["exit_status"] = "Partial Exit"
        sell_analysis.loc[sell_analysis["remaining_holders"] == 0, "exit_status"] = "Complete Exit"
        sell_analysis.loc[sell_analysis["exit_rate_pct"] < 50, "exit_status"] = "Light Selling"
        sell_analysis.loc[sell_analysis["exit_rate_pct"] > 80, "exit_status"] = "Heavy Exit"
        
        # Filter for meaningful selling activity (at least 2 sell actions)
        meaningful_sells = sell_analysis[sell_analysis["total_sells"] >= 2].copy()
        
        # Sort by total sells and exit rate
        meaningful_sells = meaningful_sells.sort_values(
            ["total_sells", "exit_rate_pct"], ascending=[False, False]
        )
        
        return self.format_output(meaningful_sells.reset_index()).head(50)
    
    def analyze_concentration_changes(self) -> pd.DataFrame:
        """
        Analyze recent changes in portfolio concentration.
        
        Returns:
            DataFrame with concentration change analysis
        """
        if (self.data.holdings_df is None or 
            self.data.history_df is None or
            self.data.holdings_df.empty or
            self.data.history_df.empty or
            "portfolio_percent" not in self.data.holdings_df.columns):
            return pd.DataFrame()
        
        # Get current high-concentration positions (>3%)
        high_concentration = self.data.holdings_df[
            self.data.holdings_df["portfolio_percent"] > 3.0
        ].copy()
        
        if high_concentration.empty:
            return pd.DataFrame()
        
        # Group by ticker and manager for concentration analysis
        concentration_summary = high_concentration.groupby(["ticker", "manager_id"]).agg({
            "portfolio_percent": "first",
            "value": "first", 
            "stock": "first" if "stock" in high_concentration.columns else "count",
            "manager_name": "first" if "manager_name" in high_concentration.columns else "count",
        }).reset_index()
        
        # Add recent activity context
        if "action_type" in self.data.history_df.columns:
            recent_activity = self.data.history_df[
                self.data.history_df["period"].isin(self.get_recent_quarters(3))
            ]
            
            if not recent_activity.empty:
                # Get recent actions for these positions
                activity_by_position = recent_activity.groupby(["ticker", "manager_id"]).agg({
                    "action_type": lambda x: list(x),
                    "action": lambda x: "; ".join(x.astype(str)),
                    "period": lambda x: ", ".join(x.unique()),
                }).reset_index()
                
                activity_by_position.columns = [
                    "ticker", "manager_id", "recent_actions", "action_details", "periods"
                ]
                
                # Merge with concentration data
                concentration_summary = concentration_summary.merge(
                    activity_by_position, on=["ticker", "manager_id"], how="left"
                )
                
                # Determine concentration change type
                concentration_summary["change_type"] = "Maintained"
                concentration_summary["recent_actions"] = concentration_summary["recent_actions"].fillna("").astype(str)
                
                concentration_summary.loc[
                    concentration_summary["recent_actions"].str.contains("Add", na=False), 
                    "change_type"
                ] = "Increased"
                concentration_summary.loc[
                    concentration_summary["recent_actions"].str.contains("Buy", na=False),
                    "change_type" 
                ] = "New High Conviction"
                concentration_summary.loc[
                    concentration_summary["recent_actions"].str.contains("Reduce", na=False),
                    "change_type"
                ] = "Reduced"
                concentration_summary.loc[
                    concentration_summary["recent_actions"].str.contains("Sell", na=False),
                    "change_type"
                ] = "Partial Exit"
        
        # Calculate concentration score (position size * recency of activity)
        concentration_summary["concentration_score"] = concentration_summary["portfolio_percent"]
        
        if "change_type" in concentration_summary.columns:
            # Bonus for recent increases
            concentration_summary.loc[
                concentration_summary["change_type"] == "Increased", "concentration_score"
            ] *= 1.5
            concentration_summary.loc[
                concentration_summary["change_type"] == "New High Conviction", "concentration_score" 
            ] *= 2.0
        
        # Sort by concentration score
        concentration_summary = concentration_summary.sort_values("concentration_score", ascending=False)
        
        return self.format_output(concentration_summary).head(100)