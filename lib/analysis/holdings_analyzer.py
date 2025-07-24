"""
Core holdings analysis module.

Analyzes current holdings data to identify top positions, multi-manager favorites,
and comprehensive stock overviews.
"""

import logging
import pandas as pd
from typing import Dict, List, Optional, Any, Union, Callable

from .base_analyzer import BaseAnalyzer, MultiAnalyzer
from ..data.data_loader import DataLoader


class HoldingsAnalyzer(MultiAnalyzer):
    """Analyzes current holdings and multi-manager positions."""
    
    def __init__(self, data_loader: DataLoader) -> None:
        """Initialize with data loader."""
        super().__init__(data_loader)
    
    def analyze_all(self) -> Dict[str, pd.DataFrame]:
        """Run all holdings analyses."""
        results = {}
        
        # Core holdings analyses
        results["top_holdings"] = self.analyze_top_holdings()
        results["multi_manager_favorites"] = self.analyze_multi_manager_favorites()
        results["interesting_stocks_overview"] = self.analyze_interesting_stocks_overview()
        results["high_conviction_stocks"] = self.analyze_high_conviction_stocks()
        results["manager_performance"] = self.analyze_manager_performance()
        results["highest_portfolio_concentration"] = self.analyze_highest_concentration()
        
        # Log summaries
        for name, df in results.items():
            self.log_analysis_summary(df, name)
        
        return self.format_all_outputs(results)
    
    def analyze_top_holdings(self) -> pd.DataFrame:
        """
        Analyze top holdings across all managers.
        
        Returns:
            DataFrame with top holdings analysis sorted by manager count and total value
        """
        if not self.validate_required_columns(
            self.data.holdings_df, ["ticker", "manager_id", "shares", "value"]
        ):
            return pd.DataFrame()
        
        holdings = self.filter_active_holdings(self.data.holdings_df)
        if holdings.empty:
            return pd.DataFrame()
        
        # Group by ticker for aggregation
        agg_dict = {
            "manager_id": ["count", list],
            "shares": "sum",
            "value": "sum",
        }
        
        # Add portfolio_percent aggregation if available
        if "portfolio_percent" in holdings.columns:
            agg_dict["portfolio_percent"] = ["mean", "max", "std"]
        
        grouped = holdings.groupby("ticker").agg(agg_dict)
        
        # Flatten column names
        if "portfolio_percent" in holdings.columns:
            grouped.columns = [
                "manager_count", "manager_ids", "total_shares", "total_value",
                "avg_portfolio_pct", "max_portfolio_pct", "portfolio_pct_std"
            ]
        else:
            grouped.columns = [
                "manager_count", "manager_ids", "total_shares", "total_value"
            ]
        
        # Convert manager IDs to manager names
        grouped["managers"] = grouped["manager_ids"].apply(
            lambda ids: ", ".join([self.data.manager_names.get(id, id) for id in ids])
        )
        grouped = grouped.drop(columns=["manager_ids"])
        
        # Add stock information if available
        if "stock" in holdings.columns:
            # Get company names
            company_names = holdings.groupby("ticker")["stock"].first()
            grouped = grouped.join(company_names.to_frame("company_name"))
        
        # Add current price information
        if "current_price" in holdings.columns:
            current_prices = holdings.groupby("ticker")["current_price"].first()
            grouped = grouped.join(current_prices)
        
        # Sort by manager count (most held) and total value
        grouped = grouped.sort_values(
            by=["manager_count", "total_value"], 
            ascending=[False, False]
        )
        
        return self.format_output(grouped.reset_index()).head(50)
    
    def analyze_multi_manager_favorites(self) -> pd.DataFrame:
        """
        Find stocks held by multiple managers (5+ managers = consensus picks).
        
        Returns:
            DataFrame with consensus stock picks
        """
        if not self.validate_required_columns(
            self.data.holdings_df, ["ticker", "manager_id", "value"]
        ):
            return pd.DataFrame()
        
        holdings = self.filter_active_holdings(self.data.holdings_df)
        if holdings.empty:
            return pd.DataFrame()
        
        # Group by ticker with flexible date columns
        agg_dict = {
            "manager_id": ["count", list],
            "shares": "sum" if "shares" in holdings.columns else "count",
            "value": "sum",
            "portfolio_percent": ["mean", "max"] if "portfolio_percent" in holdings.columns else "mean",
        }
        
        # Add date aggregation based on available columns
        if "reporting_date" in holdings.columns:
            agg_dict["reporting_date"] = ["min", "max"]
        elif "portfolio_date" in holdings.columns:
            agg_dict["portfolio_date"] = ["min", "max"]
        
        grouped = holdings.groupby("ticker").agg(agg_dict)
        
        # Flatten columns dynamically based on actual columns
        col_names = ["manager_count", "manager_ids", "total_shares", "total_value"]
        
        if "portfolio_percent" in holdings.columns:
            col_names.extend(["avg_portfolio_pct", "max_portfolio_pct"])
        else:
            col_names.append("avg_portfolio_pct")
            
        # Add date columns if present
        if "reporting_date" in agg_dict:
            col_names.extend(["earliest_date", "latest_date"])
        elif "portfolio_date" in agg_dict:
            col_names.extend(["earliest_date", "latest_date"])
        
        # Only set column names if we have the right number
        if len(col_names) == len(grouped.columns):
            grouped.columns = col_names
        
        # Convert manager IDs to names
        if "manager_ids" in grouped.columns:
            grouped["managers"] = grouped["manager_ids"].apply(
                lambda ids: ", ".join([self.data.manager_names.get(id, id) for id in ids])
            )
            grouped = grouped.drop(columns=["manager_ids"])
        
        # Filter for 5+ managers (consensus picks)
        consensus_picks = grouped[grouped["manager_count"] >= 5].copy()
        
        if consensus_picks.empty:
            return pd.DataFrame()
        
        # Calculate consensus score
        consensus_picks["consensus_score"] = (
            consensus_picks["manager_count"] * consensus_picks["avg_portfolio_pct"]
        )
        
        # Add company names if available
        if "stock" in holdings.columns:
            company_names = holdings.groupby("ticker")["stock"].first()
            consensus_picks = consensus_picks.join(company_names.rename("company_name"))
        
        # Add recent activity if available
        if (self.data.history_df is not None and 
            not self.data.history_df.empty and
            "action_type" in self.data.history_df.columns):
            
            # Get recent quarters for filtering
            recent_quarters = self.get_recent_quarters(3)
            
            recent_buys = (
                self.data.history_df[
                    (self.data.history_df["action_type"].isin(["Buy", "Add"]))
                    & (self.data.history_df["period"].isin(recent_quarters))
                ]
                .groupby("ticker")["manager_id"]
                .nunique()
                .rename("recent_buyers")
            )
            
            consensus_picks = consensus_picks.join(recent_buys, how="left")
            consensus_picks["recent_buyers"] = consensus_picks["recent_buyers"].fillna(0)
        
        # Sort by consensus score
        consensus_picks = consensus_picks.sort_values(by="consensus_score", ascending=False)
        
        return self.format_output(consensus_picks.reset_index()).head(50)
    
    def analyze_interesting_stocks_overview(self) -> pd.DataFrame:
        """
        Create comprehensive overview of most interesting stocks with appeal scoring.
        
        Returns:
            DataFrame with comprehensive stock analysis and appeal scores
        """
        if not self.validate_required_columns(
            self.data.holdings_df, ["ticker", "manager_id", "value"]
        ):
            return pd.DataFrame()
        
        holdings = self.filter_active_holdings(self.data.holdings_df)
        if holdings.empty:
            return pd.DataFrame()
        
        # Start with basic aggregation
        agg_dict = {
            "manager_id": ["count", list],
            "shares": "sum" if "shares" in holdings.columns else "count",
            "value": "sum",
            "portfolio_percent": ["mean", "max"] if "portfolio_percent" in holdings.columns else "mean",
        }
        
        # Add date aggregation based on available columns
        if "reporting_date" in holdings.columns:
            agg_dict["reporting_date"] = "max"
        elif "portfolio_date" in holdings.columns:
            agg_dict["portfolio_date"] = "max"
        
        overview = holdings.groupby("ticker").agg(agg_dict)
        
        # Flatten columns dynamically
        col_names = ["manager_count", "manager_ids", "total_shares", "total_value"]
        
        if "portfolio_percent" in holdings.columns:
            col_names.extend(["avg_portfolio_pct", "max_portfolio_pct"])
        else:
            col_names.append("avg_portfolio_pct")
            
        # Add date column if present
        if "reporting_date" in agg_dict or "portfolio_date" in agg_dict:
            col_names.append("latest_date")
        
        # Only set column names if we have the right number
        if len(col_names) == len(overview.columns):
            overview.columns = col_names
        
        # Convert manager IDs to names
        if "manager_ids" in overview.columns:
            overview["managers"] = overview["manager_ids"].apply(
                lambda ids: ", ".join([self.data.manager_names.get(id, id) for id in ids])
            )
            overview = overview.drop(columns=["manager_ids"])
        
        # Add company names
        if "stock" in holdings.columns:
            company_names = holdings.groupby("ticker")["stock"].first()
            overview = overview.join(company_names.rename("company_name"))
        
        # Add recent activity if available
        recent_buy_count = 0
        if (self.data.history_df is not None and 
            not self.data.history_df.empty and
            "action_type" in self.data.history_df.columns):
            
            recent_activity = (
                self.data.history_df[
                    self.data.history_df["action_type"].isin(["Buy", "Add"])
                ]
                .groupby("ticker")
                .agg({
                    "period": ["count", "max"], 
                    "manager_id": "nunique"
                })
            )
            
            recent_activity.columns = [
                "buy_count", "last_buy_period", "active_managers"
            ]
            
            overview = overview.merge(
                recent_activity, left_index=True, right_index=True, how="left"
            )
            overview["buy_count"] = overview["buy_count"].fillna(0)
            recent_buy_count = overview["buy_count"]
        
        # Calculate appeal score using our sophisticated algorithm
        overview["appeal_score"] = overview.apply(
            lambda row: self.scoring.calculate_appeal_score(
                manager_count=row["manager_count"],
                avg_portfolio_pct=row["avg_portfolio_pct"],
                recent_buy_count=row.get("buy_count", 0),
                value_factor=0,  # No PE ratio available from Dataroma
                max_score=10.0
            ), axis=1
        )
        
        # Add investment timing assessment
        overview["investment_timing"] = "Consider"
        if "last_buy_period" in overview.columns:
            # Recent activity indicates good timing
            # Use the most recent quarter for "Good" timing signal
            recent_quarters = self.get_recent_quarters(1)
            if recent_quarters:
                overview.loc[
                    overview["last_buy_period"].str.contains(recent_quarters[0], na=False), 
                    "investment_timing"
                ] = "Good"
            overview.loc[
                overview.get("buy_count", 0) > 5, 
                "investment_timing"
            ] = "Strong"
        
        # Add price information if available
        if "current_price" in holdings.columns:
            current_prices = holdings.groupby("ticker")["current_price"].first()
            overview = overview.join(current_prices)
            
        if "reported_price" in holdings.columns:
            reported_prices = holdings.groupby("ticker")["reported_price"].first()
            overview = overview.join(reported_prices)
        
        # Sort by appeal score
        overview = overview.sort_values(by="appeal_score", ascending=False)
        
        return self.format_output(overview.reset_index()).head(100)
    
    def analyze_high_conviction_stocks(self) -> pd.DataFrame:
        """
        Analyze stocks where managers have high conviction (>5% of portfolio).
        
        Returns:
            DataFrame with high conviction positions
        """
        if not self.validate_required_columns(
            self.data.holdings_df, ["ticker", "manager_id", "portfolio_percent"]
        ):
            return pd.DataFrame()
        
        holdings = self.filter_active_holdings(self.data.holdings_df)
        if holdings.empty:
            return pd.DataFrame()
        
        # Filter for positions > 5% of portfolio
        high_conviction = holdings[holdings["portfolio_percent"] > 5.0].copy()
        
        if high_conviction.empty:
            return pd.DataFrame()
        
        # Aggregate by ticker
        agg_dict = {
            "manager_id": ["count", self.get_manager_summary],
            "portfolio_percent": ["mean", "max"],
            "value": "sum"
        }
        
        if "stock" in high_conviction.columns:
            agg_dict["stock"] = "first"
            
        result = high_conviction.groupby("ticker").agg(agg_dict)
        
        # Flatten columns
        col_names = ["manager_count", "managers", "avg_portfolio_pct", 
                     "max_portfolio_pct", "total_value"]
        
        if "stock" in high_conviction.columns:
            col_names.append("company_name")
            
        result.columns = col_names
        
        # Add current price separately if available
        if "current_price" in high_conviction.columns:
            result["current_price"] = high_conviction.groupby("ticker")["current_price"].first()
        
        # Add conviction score
        result["conviction_score"] = (
            result["avg_portfolio_pct"] * 0.5 + 
            result["max_portfolio_pct"] * 0.3 + 
            result["manager_count"] * 0.2
        )
        
        # Sort by conviction score
        result = result.sort_values(by="conviction_score", ascending=False)
        
        return self.format_output(result.reset_index())
    
    def analyze_manager_performance(self) -> pd.DataFrame:
        """
        Analyze manager portfolio metrics and performance indicators.
        
        Returns:
            DataFrame with manager performance metrics
        """
        if not self.validate_required_columns(
            self.data.holdings_df, ["manager_id", "value"]
        ):
            return pd.DataFrame()
        
        holdings = self.filter_active_holdings(self.data.holdings_df)
        if holdings.empty:
            return pd.DataFrame()
        
        # Get manager names
        manager_names = self.data.manager_names
        
        # Aggregate by manager
        manager_stats = holdings.groupby("manager_id").agg({
            "ticker": "count",
            "value": ["sum", "mean", "std"],
            "portfolio_percent": "mean" if "portfolio_percent" in holdings.columns else "count"
        })
        
        # Flatten columns
        if "portfolio_percent" in holdings.columns:
            manager_stats.columns = ["position_count", "total_value", "avg_position_value", 
                                   "position_value_std", "avg_portfolio_pct"]
        else:
            manager_stats.columns = ["position_count", "total_value", "avg_position_value", 
                                   "position_value_std"]
        
        # Add manager names
        manager_stats["manager_name"] = manager_stats.index.map(manager_names)
        
        # Calculate concentration metrics
        manager_stats["concentration_ratio"] = (
            manager_stats["position_value_std"] / manager_stats["avg_position_value"]
        ).fillna(0)
        
        # Find top positions for each manager
        top_positions_cols = ["manager_id", "ticker", "value"]
        if "portfolio_percent" in holdings.columns:
            top_positions_cols.append("portfolio_percent")
            
        top_positions = holdings.loc[
            holdings.groupby("manager_id")["value"].idxmax()
        ][top_positions_cols]
        
        if "portfolio_percent" in holdings.columns:
            top_positions.columns = ["manager_id", "top_holding", "top_holding_value", "top_holding_pct"]
        else:
            top_positions.columns = ["manager_id", "top_holding", "top_holding_value"]
        manager_stats = manager_stats.merge(
            top_positions.set_index("manager_id"), 
            left_index=True, 
            right_index=True,
            how="left"
        )
        
        # Calculate diversification score
        manager_stats["diversification_score"] = (
            manager_stats["position_count"] * 0.4 +
            (1 / (1 + manager_stats["concentration_ratio"])) * 30 +
            (50 - manager_stats.get("top_holding_pct", 20)) * 0.3
        ).clip(0, 100)
        
        # Calculate approximate returns using historical data
        if self.data.history_df is not None and not self.data.history_df.empty:
            # Dynamically determine current year from latest period in data
            import re
            all_periods = self.data.history_df['period'].unique()
            current_year = max([int(re.search(r'(\d{4})', period).group(1)) 
                               for period in all_periods 
                               if re.search(r'(\d{4})', period)])
            
            # Get first year portfolio values for each manager
            first_year_values = {}
            for manager_id in manager_stats.index:
                manager_history = self.data.history_df[
                    self.data.history_df['manager_id'] == manager_id
                ].sort_values(by='period')
                
                if not manager_history.empty:
                    # Get the first year this manager appeared
                    first_period = manager_history['period'].iloc[0]
                    # Extract year from period (e.g., "Q1 2010" -> 2010)
                    year_match = re.search(r'(\d{4})', first_period)
                    if year_match:
                        first_year = int(year_match.group(1))
                        years_active = current_year - first_year
                        
                        # Estimate initial portfolio value (current value / compound growth)
                        # Assuming average market return of 10% per year
                        estimated_initial_value = manager_stats.loc[manager_id, "total_value"] / (1.1 ** years_active)
                        total_return_pct = ((manager_stats.loc[manager_id, "total_value"] - estimated_initial_value) 
                                          / estimated_initial_value * 100) if estimated_initial_value > 0 else 0
                        annualized_return = (total_return_pct / years_active) if years_active > 0 else 0
                        
                        first_year_values[manager_id] = {
                            'first_year': first_year,
                            'years_active': years_active,
                            'total_return_pct': round(total_return_pct, 2),
                            'annualized_return_pct': round(annualized_return, 2)
                        }
            
            # Add return columns
            for manager_id, return_data in first_year_values.items():
                if manager_id in manager_stats.index:
                    manager_stats.loc[manager_id, 'first_year'] = return_data['first_year']
                    manager_stats.loc[manager_id, 'years_active'] = return_data['years_active']
                    manager_stats.loc[manager_id, 'total_return_pct'] = return_data['total_return_pct']
                    manager_stats.loc[manager_id, 'annualized_return_pct'] = return_data['annualized_return_pct']
        
        # Sort by total value
        manager_stats = manager_stats.sort_values(by="total_value", ascending=False)
        
        return self.format_output(manager_stats.reset_index())
    
    def analyze_highest_concentration(self) -> pd.DataFrame:
        """
        Analyze positions with highest portfolio concentration.
        
        Returns:
            DataFrame with highest concentration positions
        """
        if not self.validate_required_columns(
            self.data.holdings_df, ["ticker", "manager_id", "portfolio_percent", "value"]
        ):
            return pd.DataFrame()
        
        holdings = self.filter_active_holdings(self.data.holdings_df)
        if holdings.empty:
            return pd.DataFrame()
        
        # Get manager names
        manager_names = self.data.manager_names
        
        # Sort by portfolio percentage
        highest_concentration = holdings.nlargest(100, "portfolio_percent").copy()
        
        # Add manager names
        highest_concentration["manager_name"] = highest_concentration["manager_id"].map(manager_names)
        
        # Select relevant columns
        result_columns = ["ticker", "manager_name", "portfolio_percent", "value"]
        if "stock" in highest_concentration.columns:
            result_columns.insert(1, "stock")
        if "current_price" in highest_concentration.columns:
            result_columns.append("current_price")
        
        result = highest_concentration[result_columns].copy()
        result.columns = [col if col != "stock" else "company_name" for col in result.columns]
        
        # Add risk assessment
        result["risk_level"] = pd.cut(
            result["portfolio_percent"],
            bins=[0, 10, 20, 30, 100],
            labels=["Moderate", "High", "Very High", "Extreme"]
        )
        
        return self.format_output(result)


class TopHoldingsAnalyzer(BaseAnalyzer):
    """Focused analyzer for just top holdings."""
    
    def __init__(self, data_loader: DataLoader) -> None:
        """Initialize with data loader."""
        super().__init__(data_loader)
    
    def analyze(self) -> pd.DataFrame:
        """Analyze top holdings across all managers."""
        holdings_analyzer = HoldingsAnalyzer(self.data)
        return holdings_analyzer.analyze_top_holdings()