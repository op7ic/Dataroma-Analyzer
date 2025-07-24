"""
Enhanced hidden gems detection analyzer.

Uses sophisticated multi-factor scoring to identify under-followed quality stocks
with potential for significant returns based on smart money activity.
"""

import logging
import pandas as pd
from typing import Dict, List, Optional, Any, Union

from .base_analyzer import BaseAnalyzer, MultiAnalyzer
from ..utils.calculations import TextAnalysisUtils
from ..data.data_loader import DataLoader


class GemsAnalyzer(MultiAnalyzer):
    """Advanced hidden gems detection with multi-factor scoring."""
    
    def __init__(self, data_loader: DataLoader) -> None:
        """Initialize with data loader."""
        super().__init__(data_loader)
    
    def _fix_grouped_columns(self, df, expected_columns):
        """Helper method to handle multi-level column names from groupby operations."""
        if len(df.columns) == len(expected_columns):
            df.columns = expected_columns
            return df
        
        # Don't try to rename if lengths don't match
        print(f"Column length mismatch: expected {len(expected_columns)}, got {len(df.columns)}")
        print(f"Expected: {expected_columns}")
        print(f"Actual: {list(df.columns)}")
        return df
    
    def analyze_all(self) -> Dict[str, pd.DataFrame]:
        """Run all hidden gems analyses."""
        results = {}
        
        # Core gems analyses  
        results["hidden_gems"] = self.analyze_hidden_gems()
        results["deep_value_plays"] = self.analyze_deep_value_plays()
        results["contrarian_opportunities"] = self.analyze_contrarian_opportunities()
        results["under_radar_picks"] = self.analyze_under_radar_picks()
        
        # Log summaries
        for name, df in results.items():
            self.log_analysis_summary(df, name)
        
        return self.format_all_outputs(results)
    
    def analyze_hidden_gems(self) -> pd.DataFrame:
        """
        Find under-followed quality stocks with sophisticated multi-factor scoring.
        
        Enhanced algorithm uses 5 factors:
        1. Exclusivity (low manager count but high conviction)
        2. Conviction (high portfolio percentages)  
        3. Recent activity (buying momentum)
        4. Price momentum (technical factors)
        5. Manager quality (track record weighting)
        
        Returns:
            DataFrame with hidden gems ranked by sophisticated score
        """
        if self.data.holdings_df is None or self.data.holdings_df.empty:
            return pd.DataFrame()
            
        if not self.validate_required_columns(
            self.data.holdings_df, ["ticker", "manager_id", "value"]
        ):
            return pd.DataFrame()
        
        holdings = self.filter_active_holdings(self.data.holdings_df)
        if holdings.empty:
            return pd.DataFrame()
        
        # Group by ticker for analysis
        agg_dict = {
            "manager_id": ["count", list],
            "portfolio_percent": ["mean", "max"] if "portfolio_percent" in holdings.columns else "mean",
            "value": "sum",
            "shares": "sum" if "shares" in holdings.columns else "count",
        }
        
        # Add date aggregation if available
        if "portfolio_date" in holdings.columns:
            agg_dict["portfolio_date"] = "max"
        elif "reporting_date" in holdings.columns:
            agg_dict["reporting_date"] = "max"
            
        holdings_by_ticker = holdings.groupby("ticker").agg(agg_dict)
        
        # Reset index first
        holdings_by_ticker = holdings_by_ticker.reset_index()
        
        # Handle multi-level column names carefully
        new_columns = ['ticker']  # First column after reset_index
        for col in holdings_by_ticker.columns[1:]:  # Skip ticker column
            if isinstance(col, tuple):
                # Multi-level column
                if col[1] == 'count':
                    new_columns.append('manager_count')
                elif col[1] == 'list' or 'lambda' in str(col[1]):
                    new_columns.append('manager_ids')
                elif col[1] == 'mean' and 'portfolio' in col[0]:
                    new_columns.append('avg_portfolio_pct')
                elif col[1] == 'max' and 'portfolio' in col[0]:
                    new_columns.append('max_portfolio_pct')
                elif col[1] == 'sum' and 'value' in col[0]:
                    new_columns.append('total_value')
                elif col[1] == 'sum' and 'shares' in col[0]:
                    new_columns.append('total_shares')
                elif col[1] == 'count' and 'shares' in col[0]:
                    new_columns.append('total_shares')
                elif col[1] == 'max' and ('date' in col[0]):
                    new_columns.append('latest_date')
                else:
                    new_columns.append('_'.join(map(str, col)))
            else:
                # Single level column
                new_columns.append(str(col))
        
        holdings_by_ticker.columns = new_columns
            
        # Convert manager IDs to names
        if "manager_ids" in holdings_by_ticker.columns:
            holdings_by_ticker["managers"] = holdings_by_ticker["manager_ids"].apply(
                lambda ids: ", ".join([self.data.manager_names.get(id, id) for id in ids])
            )
            holdings_by_ticker = holdings_by_ticker.drop(columns=["manager_ids"])
            
        # Ensure max_portfolio_pct exists
        if "max_portfolio_pct" not in holdings_by_ticker.columns:
            holdings_by_ticker["max_portfolio_pct"] = holdings_by_ticker.get("avg_portfolio_pct", 0)
        
        # Filter for potential gems: 1-4 managers with meaningful positions
        if "manager_count" not in holdings_by_ticker.columns:
            return pd.DataFrame()
            
        # Ensure manager_count is numeric
        holdings_by_ticker["manager_count"] = pd.to_numeric(holdings_by_ticker["manager_count"], errors='coerce')
        
        hidden_gems = holdings_by_ticker[
            (holdings_by_ticker["manager_count"] <= 4) &
            (holdings_by_ticker["max_portfolio_pct"] > 2.0)  # At least 2% position
        ].copy()
        
        if hidden_gems.empty:
            return pd.DataFrame()
        
        # Add company names
        if "stock" in holdings.columns:
            company_names = holdings.groupby("ticker")["stock"].first()
            hidden_gems = hidden_gems.join(company_names.to_frame("company_name"), on="ticker")
        
        # Calculate recent activity score
        hidden_gems["recent_activity_score"] = 0.0
        if (self.data.history_df is not None and 
            not self.data.history_df.empty and
            "action_type" in self.data.history_df.columns):
            
            # Get recent buying activity (last 2 quarters)
            recent_buys = self.data.history_df[
                (self.data.history_df["action_type"].isin(["Buy", "Add"])) &
                (self.data.history_df["period"].isin(self.get_recent_quarters(3)))
            ]
            
            if not recent_buys.empty:
                buy_scores = recent_buys.groupby("ticker").size() / 10.0  # Normalize
                buy_scores = buy_scores.clip(0, 1.0)  # Cap at 1.0
                
                for ticker in hidden_gems.index:
                    if ticker in buy_scores.index:
                        hidden_gems.loc[ticker, "recent_activity_score"] = buy_scores[ticker]
        
        # Calculate price momentum score using Dataroma price data
        hidden_gems["price_momentum_score"] = 0.5  # Default neutral
        if "current_price" in holdings.columns and "reported_price" in holdings.columns:
            price_data = holdings.groupby("ticker").agg({
                "current_price": "first",
                "reported_price": "first"
            })
            
            # Add 52-week data if available
            if "52_week_low" in holdings.columns and "52_week_high" in holdings.columns:
                week_52_data = holdings.groupby("ticker").agg({
                    "52_week_low": "first", 
                    "52_week_high": "first"
                })
                price_data = price_data.join(week_52_data)
                
                # Calculate momentum based on 52-week position
                for ticker in hidden_gems.index:
                    if ticker in price_data.index:
                        row = price_data.loc[ticker]
                        if not pd.isna(row.get("52_week_low")) and not pd.isna(row.get("52_week_high")):
                            week_52_pos = self.calc.calculate_52_week_position(
                                row["current_price"], row["52_week_low"], row["52_week_high"]
                            )
                            # Lower position = better value opportunity (higher score)
                            momentum_score = max(0.1, (100 - week_52_pos) / 100)
                            hidden_gems.loc[ticker, "price_momentum_score"] = momentum_score
        
        # Calculate manager quality scores
        hidden_gems["manager_quality_score"] = 1.0
        manager_lists = hidden_gems["managers"].str.split(", ")
        
        for idx, managers in manager_lists.items():
            if managers and isinstance(managers, list):
                # Get average quality score for all managers of this stock
                quality_scores = []
                for manager_display in managers:
                    # Find original manager ID from display name
                    manager_id = None
                    for mid, display in self.data.manager_names.items():
                        if display == manager_display:
                            manager_id = mid
                            break
                    
                    if manager_id:
                        quality = self.scoring.calculate_manager_quality_score(manager_id)
                        quality_scores.append(quality)
                    else:
                        quality_scores.append(1.0)  # Default
                
                if quality_scores:
                    avg_quality = sum(quality_scores) / len(quality_scores)
                    hidden_gems.loc[idx, "manager_quality_score"] = avg_quality
        
        # Calculate sophisticated hidden gem score
        hidden_gems["hidden_gem_score"] = hidden_gems.apply(
            lambda row: self.scoring.calculate_hidden_gem_score(
                manager_count=row["manager_count"],
                max_portfolio_pct=row["max_portfolio_pct"],
                avg_portfolio_pct=row["avg_portfolio_pct"],
                recent_activity_score=row["recent_activity_score"],
                price_momentum_score=row["price_momentum_score"],
                manager_quality_score=row["manager_quality_score"]
            ), axis=1
        )
        
        # Categorize gems by type
        hidden_gems["gem_type"] = "Under-Radar"
        hidden_gems.loc[
            (hidden_gems["manager_count"] == 1) & (hidden_gems["max_portfolio_pct"] > 5),
            "gem_type"
        ] = "Conviction Play"
        hidden_gems.loc[
            (hidden_gems["price_momentum_score"] > 0.7),
            "gem_type"  
        ] = "Value Opportunity"
        hidden_gems.loc[
            (hidden_gems["recent_activity_score"] > 0.5) & (hidden_gems["manager_count"] <= 2),
            "gem_type"
        ] = "Emerging Pick"
        
        
        # CRITICAL: Only show stocks that had activity in recent quarters
        if (self.data.history_df is not None and 
            not self.data.history_df.empty):
            
            recent_tickers = set(
                self.data.history_df[
                    self.data.history_df["period"].isin(self.get_recent_quarters(3))
                ]["ticker"].unique()
            )
            
            # Filter hidden_gems to only include recently active stocks
            hidden_gems = hidden_gems[hidden_gems.index.isin(recent_tickers)]
        
        
        # CRITICAL: Only show stocks that had activity in recent quarters
        if (self.data.history_df is not None and 
            not self.data.history_df.empty):
            
            recent_tickers = set(
                self.data.history_df[
                    self.data.history_df["period"].isin(self.get_recent_quarters(3))
                ]["ticker"].unique()
            )
            
            # Filter hidden_gems to only include recently active stocks
            hidden_gems = hidden_gems[hidden_gems.index.isin(recent_tickers)]
        
        
        # CRITICAL: Only show stocks that had activity in recent quarters
        if (self.data.history_df is not None and 
            not self.data.history_df.empty):
            
            recent_tickers = set(
                self.data.history_df[
                    self.data.history_df["period"].isin(self.get_recent_quarters(3))
                ]["ticker"].unique()
            )
            
            # Filter hidden_gems to only include recently active stocks
            hidden_gems = hidden_gems[hidden_gems.index.isin(recent_tickers)]
        
        # Add first discovery information if available
        if (self.data.history_df is not None and 
            not self.data.history_df.empty and
            "action_type" in self.data.history_df.columns):
            
            # Only show discoveries from recent quarters (last 3 quarters)
            recent_buys = self.data.history_df[
                (self.data.history_df["action_type"] == "Buy") & 
                (self.data.history_df["period"].isin(self.get_recent_quarters(3)))
            ]
            
            # Only include stocks that had Buy action in recent quarters
            first_buys = (
                recent_buys
                .groupby("ticker")["period"]
                .first()
                .rename("first_discovered")
            )
            hidden_gems = hidden_gems.join(first_buys, how="left")
        
        # Sort by hidden gem score
        hidden_gems = hidden_gems.sort_values(by="hidden_gem_score", ascending=False)
        
        return self.format_output(hidden_gems.reset_index()).head(50)
    
    def analyze_deep_value_plays(self) -> pd.DataFrame:
        """
        Find value plays using price analysis from Dataroma data.
        
        Uses reported_price vs current_price and 52-week ranges to identify
        stocks being bought at attractive valuations.
        
        Returns:
            DataFrame with deep value opportunities  
        """
        if not self.validate_required_columns(
            self.data.holdings_df, ["ticker", "current_price", "reported_price"]
        ):
            return pd.DataFrame()
        
        holdings = self.filter_active_holdings(self.data.holdings_df)
        if holdings.empty:
            return pd.DataFrame()
        
        # Calculate price metrics
        price_analysis = holdings.groupby("ticker").agg({
            "manager_id": ["count", self.get_manager_summary],
            "current_price": "first",
            "reported_price": "mean",  # Average reported price across managers
            "value": "sum",
            "portfolio_percent": ["mean", "max"] if "portfolio_percent" in holdings.columns else "mean",
        })
        
        # Reset index to make ticker a regular column
        price_analysis = price_analysis.reset_index()
        
        # Flatten columns with proper handling
        if "portfolio_percent" in holdings.columns:
            expected_cols = [
                "ticker", "manager_count", "managers", "current_price", 
                "avg_reported_price", "total_value", "avg_portfolio_pct", "max_portfolio_pct"
            ]
        else:
            expected_cols = [
                "ticker", "manager_count", "managers", "current_price", 
                "avg_reported_price", "total_value", "avg_portfolio_pct"
            ]
        
        price_analysis = self._fix_grouped_columns(price_analysis, expected_cols)
        
        # Calculate discount/premium to reported prices
        price_analysis["price_change_pct"] = (
            (price_analysis["current_price"] - price_analysis["avg_reported_price"]) /
            price_analysis["avg_reported_price"] * 100
        )
        
        # Add 52-week analysis if available
        if "52_week_low" in holdings.columns and "52_week_high" in holdings.columns:
            week_52_data = holdings.groupby("ticker").agg({
                "52_week_low": "first",
                "52_week_high": "first"
            })
            price_analysis = price_analysis.join(week_52_data)
            
            # Calculate position in 52-week range
            price_analysis["52_week_position_pct"] = price_analysis.apply(
                lambda row: self.calc.calculate_52_week_position(
                    row["current_price"], row["52_week_low"], row["52_week_high"]
                ), axis=1
            )
            
            # Identify stocks near 52-week lows (potential value)
            price_analysis["near_52w_low"] = price_analysis.apply(
                lambda row: self.calc.is_near_52_week_low(
                    row["current_price"], row["52_week_low"], 15.0  # Within 15% of low
                ), axis=1
            )
        else:
            price_analysis["52_week_position_pct"] = 50.0  # Default middle
            price_analysis["near_52w_low"] = False
        
        # Filter for value opportunities
        # Criteria: Stocks held by smart money that are either:
        # 1. Trading below reported prices, OR
        # 2. Near 52-week lows with conviction positions  
        value_plays = price_analysis[
            (
                (price_analysis["price_change_pct"] < -2.0) |  # Down 2%+ from reported
                (price_analysis["near_52w_low"] == True) |     # Near 52w low
                (price_analysis["52_week_position_pct"] < 30)  # In bottom 30% of range
            ) &
            (price_analysis["avg_portfolio_pct"] > 1.5)  # Meaningful position
        ].copy()
        
        if value_plays.empty:
            return pd.DataFrame()
        
        # Calculate value score
        value_plays["value_score"] = 0
        
        # Factor 1: Price discount (higher discount = higher score)
        value_plays["value_score"] += (
            value_plays["price_change_pct"].clip(-50, 5).apply(lambda x: max(0, -x)) / 10
        )
        
        # Factor 2: 52-week position (lower position = higher score)
        value_plays["value_score"] += (
            (100 - value_plays["52_week_position_pct"]) / 20
        )
        
        # Factor 3: Manager conviction (higher allocation = higher score)
        value_plays["value_score"] += (
            value_plays["avg_portfolio_pct"] / 5
        )
        
        # Factor 4: Manager count (more managers = more conviction)
        value_plays["value_score"] += (
            value_plays["manager_count"] / 10
        )
        
        # Add company names
        if "stock" in holdings.columns:
            company_names = holdings.groupby("ticker")["stock"].first()
            value_plays = value_plays.join(company_names.rename("company_name"))
        
        # Categorize value type
        value_plays["value_type"] = "Value Play"
        value_plays.loc[
            value_plays["near_52w_low"] == True, "value_type"
        ] = "52-Week Low Value"
        value_plays.loc[
            value_plays["price_change_pct"] < -10, "value_type"  
        ] = "Deep Discount"
        
        # Sort by value score
        value_plays = value_plays.sort_values(by="value_score", ascending=False)
        
        return self.format_output(value_plays.reset_index()).head(30)
    
    def analyze_contrarian_opportunities(self) -> pd.DataFrame:
        """
        Find stocks with mixed buy/sell signals indicating contrarian opportunities.
        
        Returns:
            DataFrame with contrarian plays based on opposing manager actions
        """
        if (self.data.history_df is None or 
            self.data.history_df.empty or
            "action_type" not in self.data.history_df.columns):
            logging.warning("No activity data available for contrarian analysis")
            return pd.DataFrame()
        
        # Get recent activities (last 2 quarters)
        recent_quarters = self.get_recent_quarters(2)
        recent_activities = self.data.history_df[
            self.data.history_df["period"].isin(recent_quarters)
        ].copy()
        
        if recent_activities.empty:
            return pd.DataFrame()
        
        # Count buy vs sell actions by ticker
        activity_summary = recent_activities.groupby("ticker").agg({
            "action_type": lambda x: dict(x.value_counts()),
            "manager_id": lambda x: len(x.unique()),
            "period": lambda x: ", ".join(x.unique()),
        })
        
        activity_summary.columns = ["action_breakdown", "active_managers", "periods"]
        
        # Extract buy and sell counts
        activity_summary["buy_count"] = activity_summary["action_breakdown"].apply(
            lambda x: x.get("Buy", 0) + x.get("Add", 0)
        )
        activity_summary["sell_count"] = activity_summary["action_breakdown"].apply(
            lambda x: x.get("Sell", 0) + x.get("Reduce", 0)
        )
        
        # Filter for contrarian signals: both buying AND selling activity
        contrarian_stocks = activity_summary[
            (activity_summary["buy_count"] >= 1) &
            (activity_summary["sell_count"] >= 1) &
            (activity_summary["active_managers"] >= 3)  # Multiple managers involved
        ].copy()
        
        if contrarian_stocks.empty:
            return pd.DataFrame()
        
        # Get current holdings data for context
        if self.data.holdings_df is not None and not self.data.holdings_df.empty:
            current_holdings = self.data.holdings_df.groupby("ticker").agg({
                "manager_id": "count",
                "value": "sum",
                "portfolio_percent": "mean" if "portfolio_percent" in self.data.holdings_df.columns else "count"
            })
            current_holdings.columns = ["current_holders", "total_value", "avg_portfolio_pct"]
            
            contrarian_stocks = contrarian_stocks.join(current_holdings, how="left")
            contrarian_stocks.fillna(0, inplace=True)
        
        # Calculate contrarian score
        contrarian_stocks["contrarian_score"] = (
            (contrarian_stocks["buy_count"] + contrarian_stocks["sell_count"]) *
            contrarian_stocks["active_managers"] / 10
        )
        
        # Determine contrarian signal direction
        contrarian_stocks["contrarian_signal"] = contrarian_stocks.apply(
            lambda row: "Net Buying" if row["buy_count"] > row["sell_count"] 
            else "Net Selling" if row["sell_count"] > row["buy_count"]
            else "Mixed Signal", axis=1
        )
        
        # Add company names  
        if (self.data.holdings_df is not None and
            "stock" in self.data.holdings_df.columns):
            company_names = self.data.holdings_df.groupby("ticker")["stock"].first()
            contrarian_stocks = contrarian_stocks.join(company_names.rename("company_name"))
        
        # Add manager details
        buying_managers = recent_activities[
            recent_activities["action_type"].isin(["Buy", "Add"])
        ].groupby("ticker")["manager_id"].apply(
            lambda x: ", ".join(x.unique())
        ).rename("buying_managers")
        
        selling_managers = recent_activities[
            recent_activities["action_type"].isin(["Sell", "Reduce"])  
        ].groupby("ticker")["manager_id"].apply(
            lambda x: ", ".join(x.unique())
        ).rename("selling_managers")
        
        contrarian_stocks = contrarian_stocks.join([buying_managers, selling_managers])
        
        # Sort by contrarian score
        contrarian_stocks = contrarian_stocks.sort_values(by="contrarian_score", ascending=False)
        
        return self.format_output(contrarian_stocks.reset_index()).head(30)
    
    def analyze_under_radar_picks(self) -> pd.DataFrame:
        """
        Find quality stocks held by only 1-2 top-tier managers.
        
        These represent potential early-stage picks before broader institutional discovery.
        
        Returns:
            DataFrame with under-radar opportunities from premium managers
        """
        if self.data.holdings_df is None or self.data.holdings_df.empty:
            return pd.DataFrame()
            
        if not self.validate_required_columns(
            self.data.holdings_df, ["ticker", "manager_id", "value"]
        ):
            return pd.DataFrame()
        
        holdings = self.filter_active_holdings(self.data.holdings_df)
        if holdings.empty:
            return pd.DataFrame()
        
        # Group by ticker, filter for 1-2 managers only
        under_radar = holdings.groupby("ticker").agg({
            "manager_id": ["count", self.get_manager_summary],
            "portfolio_percent": ["mean", "max"] if "portfolio_percent" in holdings.columns else "mean",
            "value": "sum",
            "shares": "sum" if "shares" in holdings.columns else "count",
        })
        
        # Flatten columns
        if "portfolio_percent" in holdings.columns:
            under_radar.columns = [
                "manager_count", "managers", "avg_portfolio_pct", 
                "max_portfolio_pct", "total_value", "total_shares"
            ]
        else:
            under_radar.columns = [
                "manager_count", "managers", "avg_portfolio_pct",
                "total_value", "total_shares"
            ]
            under_radar["max_portfolio_pct"] = under_radar["avg_portfolio_pct"]
        
        # Filter for 1-2 managers with meaningful positions
        under_radar = under_radar[
            (under_radar["manager_count"] <= 2) &
            (under_radar["max_portfolio_pct"] > 3.0)  # At least 3% position
        ].copy()
        
        if under_radar.empty:
            return pd.DataFrame()
        
        # Calculate manager quality for each position
        under_radar["manager_quality"] = 1.0
        manager_lists = under_radar["managers"].str.split(", ")
        
        for idx, managers in manager_lists.items():
            if managers and isinstance(managers, list):
                quality_scores = []
                for manager_display in managers:
                    # Find manager ID from display name
                    manager_id = None
                    for mid, display in self.data.manager_names.items():
                        if display == manager_display:
                            manager_id = mid
                            break
                    
                    if manager_id:
                        quality = self.scoring.calculate_manager_quality_score(manager_id)
                        quality_scores.append(quality)
                
                if quality_scores:
                    # For under-radar picks, use the MAXIMUM quality score
                    # (even one premium manager makes it interesting)
                    max_quality = max(quality_scores)
                    under_radar.loc[idx, "manager_quality"] = max_quality
        
        # Filter for premium manager involvement (quality > 1.2)
        premium_picks = under_radar[under_radar["manager_quality"] > 1.2].copy()
        
        if premium_picks.empty:
            # If no premium picks, return top quality scores anyway
            premium_picks = under_radar.nlargest(20, "manager_quality").copy()
        
        # Calculate under-radar score
        premium_picks["under_radar_score"] = (
            premium_picks["max_portfolio_pct"] *
            premium_picks["manager_quality"] *
            (3 - premium_picks["manager_count"])  # Fewer managers = higher score
        )
        
        # Add recent activity context
        if (self.data.history_df is not None and 
            not self.data.history_df.empty and
            "action_type" in self.data.history_df.columns):
            
            # Find when position was first established
            first_buys = (
                self.data.history_df[self.data.history_df["action_type"] == "Buy"]
                .groupby("ticker")["period"]
                .first()
                .rename("first_established")
            )
            premium_picks = premium_picks.join(first_buys, how="left")
            
            # Check for recent additions
            recent_adds = (
                self.data.history_df[
                    (self.data.history_df["action_type"] == "Add") &
                    (self.data.history_df["period"].isin(self.get_recent_quarters(3)))
                ]
                .groupby("ticker")["manager_id"]
                .count()
                .rename("recent_additions")
            )
            premium_picks = premium_picks.join(recent_adds, how="left")
            premium_picks["recent_additions"] = premium_picks["recent_additions"].fillna(0)
        
        # Add company names
        if "stock" in holdings.columns:
            company_names = holdings.groupby("ticker")["stock"].first()
            premium_picks = premium_picks.join(company_names.rename("company_name"))
        
        # Categorize pick type
        premium_picks["pick_type"] = "Under Radar"
        premium_picks.loc[
            premium_picks["manager_count"] == 1, "pick_type"
        ] = "Exclusive Pick"
        premium_picks.loc[
            premium_picks["recent_additions"] > 0, "pick_type"
        ] = "Growing Interest"
        premium_picks.loc[
            premium_picks["manager_quality"] >= 1.8, "pick_type"
        ] = "Premium Pick"
        
        # Sort by under-radar score
        premium_picks = premium_picks.sort_values(by="under_radar_score", ascending=False)
        
        return self.format_output(premium_picks.reset_index()).head(40)