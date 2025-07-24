"""
Centralized data loading for analysis modules.
"""

import json
import logging
import os
import pandas as pd
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# Manager names mapping from original analyze_holdings.py
MANAGER_NAMES = {
    "akre": "Chuck Akre - Akre Capital Management",
    "am": "Mason Hawkins - Longleaf Partners",
    "aq": "Guy Spier - Aquamarine Capital",
    "berkshire": "Warren Buffett - Berkshire Hathaway",
    "bh": "Warren Buffett - Berkshire Hathaway",
    "br": "Thomas Russo - Gardner Russo & Gardner",
    "cb": "Christopher Browne - Tweedy Browne",
    "cm": "Charlie Munger - Daily Journal",
    "diamond": "Glenn Greenberg - Brave Warrior Advisors",
    "einhorn": "David Einhorn - Greenlight Capital",
    "fpacx": "Steven Romick - FPA Crescent Fund",
    "gg": "Joel Greenblatt - Gotham Asset Management",
    "icahn": "Carl Icahn - Icahn Enterprises",
    "ka": "Kristian Siem - Siem Capital",
    "loeb": "Daniel Loeb - Third Point",
    "lt": "Leon Cooperman - Omega Advisors",
    "mohnish": "Mohnish Pabrai - Pabrai Investment Funds",
    "munger": "Charlie Munger - Daily Journal",
    "pershing": "Bill Ackman - Pershing Square",
    "soros": "George Soros - Soros Fund Management",
    "value": "Li Lu - Himalaya Capital",
    "yacktman": "Don Yacktman - Yacktman Asset Management",
}


class DataLoader:
    """Centralized data loader for all analysis modules."""
    
    def __init__(self, cache_dir: str = "cache"):
        """Initialize data loader with cache directory."""
        self.cache_dir = Path(cache_dir)
        self.manager_names = MANAGER_NAMES.copy()
        
        # Core data containers
        self.holdings_df: Optional[pd.DataFrame] = None
        self.history_df: Optional[pd.DataFrame] = None
        self.managers_df: Optional[pd.DataFrame] = None
        self.activities_df: Optional[pd.DataFrame] = None
        
        # Data status
        self.data_loaded = False
        self.data_timestamp: Optional[str] = None
    
    def load_all_data(self) -> bool:
        """Load all available data from cache directory."""
        try:
            self._load_holdings()
            self._load_activities()  
            self._load_managers()
            self._add_manager_names()
            
            self.data_loaded = True
            logging.info("All data loaded successfully")
            return True
            
        except Exception as e:
            logging.error(f"Failed to load data: {e}")
            return False
    
    def _load_holdings(self) -> None:
        """Load holdings data from JSON."""
        holdings_path = self.cache_dir / "json" / "holdings.json"
        
        if not holdings_path.exists():
            raise FileNotFoundError(f"Holdings file not found: {holdings_path}")
        
        with open(holdings_path, "r") as f:
            holdings_data = json.load(f)
        
        # Handle both formats: array of objects or dict of arrays
        if isinstance(holdings_data, list):
            # Direct array format
            holdings_list = holdings_data
        elif isinstance(holdings_data, dict):
            # Dict with manager_id keys
            holdings_list = []
            for manager_id, stocks in holdings_data.items():
                for stock_data in stocks:
                    stock_data["manager_id"] = manager_id
                    holdings_list.append(stock_data)
        else:
            raise ValueError(f"Unexpected holdings data format: {type(holdings_data)}")
        
        self.holdings_df = pd.DataFrame(holdings_list)
        
        # Rename columns to match our expected format
        column_mapping = {
            "symbol": "ticker",
            "company_name": "stock", 
            "week_52_low": "52_week_low",
            "week_52_high": "52_week_high",
            "reporting_quarter": "quarter",  # Map new temporal fields
        }
        
        # Remove 'percentage' column if 'portfolio_percent' already exists
        if 'portfolio_percent' in self.holdings_df.columns and 'percentage' in self.holdings_df.columns:
            self.holdings_df = self.holdings_df.drop(columns=['percentage'])
        
        for old_name, new_name in column_mapping.items():
            if old_name in self.holdings_df.columns:
                self.holdings_df = self.holdings_df.rename(columns={old_name: new_name})
        
        logging.info(f"Loaded {len(self.holdings_df)} holdings")
    
    def _load_activities(self) -> None:
        """Load activity history from JSON."""
        # Try both activities.json and history.json
        activities_path = self.cache_dir / "json" / "activities.json"
        if not activities_path.exists():
            activities_path = self.cache_dir / "json" / "history.json"
        
        if not activities_path.exists():
            logging.warning(f"No activities file found")
            return
        
        with open(activities_path, "r") as f:
            activities_data = json.load(f)
        
        # Handle both formats: array of objects or dict of arrays
        if isinstance(activities_data, list):
            # Direct array format
            activities_list = activities_data
        elif isinstance(activities_data, dict):
            # Dict with nested structure
            activities_list = []
            for manager_id, periods in activities_data.items():
                for period, stocks in periods.items():
                    for stock_data in stocks:
                        stock_data["manager_id"] = manager_id
                        stock_data["period"] = period
                        activities_list.append(stock_data)
        else:
            raise ValueError(f"Unexpected activities data format: {type(activities_data)}")
        
        self.activities_df = pd.DataFrame(activities_list)
        
        # Rename columns to match our expected format
        column_mapping = {
            "symbol": "ticker",
            "date": "period",
            "company_name": "stock",  # Map company names from activities
        }
        
        for old_name, new_name in column_mapping.items():
            if old_name in self.activities_df.columns:
                self.activities_df = self.activities_df.rename(columns={old_name: new_name})
        
        # Create history_df as alias for backward compatibility
        self.history_df = self.activities_df.copy()
        
        # Add action_type extraction
        if "action" in self.history_df.columns:
            self.history_df["action_type"] = self.history_df["action"].apply(
                self._extract_action_type
            )
        
        logging.info(f"Loaded {len(self.activities_df)} activities")
    
    def _load_managers(self) -> None:
        """Load managers data from JSON."""
        managers_path = self.cache_dir / "json" / "managers.json"
        
        if not managers_path.exists():
            logging.warning(f"Managers file not found: {managers_path}")
            return
        
        with open(managers_path, "r") as f:
            managers_data = json.load(f)
        
        # Handle both formats: array of objects or dict of objects
        if isinstance(managers_data, list):
            # Direct array format - managers already have 'id' field
            managers_list = managers_data
        elif isinstance(managers_data, dict):
            # Dict with manager_id keys
            managers_list = []
            for manager_id, data in managers_data.items():
                data["id"] = manager_id
                managers_list.append(data)
        else:
            raise ValueError(f"Unexpected managers data format: {type(managers_data)}")
        
        self.managers_df = pd.DataFrame(managers_list)
        logging.info(f"Loaded {len(self.managers_df)} managers")
    
    def _add_manager_names(self) -> None:
        """Add human-readable manager names to dataframes."""
        # Update manager mapping with any additional managers from JSON
        if self.managers_df is not None:
            for _, row in self.managers_df.iterrows():
                manager_id = row.get("id", row.get("manager_id", ""))
                name = row.get("name", "")
                firm = row.get("firm", "")
                
                # Clean manager names by removing "Updated" dates
                if name:
                    # Remove "Updated DD Mon YYYY" pattern from name
                    name = re.sub(r'\s+Updated\s+\d{1,2}\s+\w+\s+\d{4}$', '', name)
                    
                if firm:
                    # Remove "Updated DD Mon YYYY" pattern from firm  
                    firm = re.sub(r'\s+Updated\s+\d{1,2}\s+\w+\s+\d{4}$', '', firm)
                
                # Use the best available name
                if name and name != manager_id and manager_id not in self.manager_names:
                    self.manager_names[manager_id] = name
                elif firm and firm != manager_id and manager_id not in self.manager_names:
                    self.manager_names[manager_id] = firm
        
        # Add manager names to holdings
        if self.holdings_df is not None and not self.holdings_df.empty:
            manager_col = "manager_id" if "manager_id" in self.holdings_df.columns else "manager"
            self.holdings_df["manager_name"] = self.holdings_df[manager_col].map(
                lambda x: self.manager_names.get(x, x)
            )
        
        # Add manager names to activities/history
        if self.history_df is not None and not self.history_df.empty:
            manager_col = "manager_id" if "manager_id" in self.history_df.columns else "manager"
            self.history_df["manager_name"] = self.history_df[manager_col].map(
                lambda x: self.manager_names.get(x, x)
            )
    
    def _extract_action_type(self, action: str) -> str:
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
    
    def get_manager_list(self, manager_ids: pd.Series) -> str:
        """Get formatted list of manager names from IDs."""
        unique_managers = list(manager_ids.unique())
        manager_names = [self.manager_names.get(m, m) for m in unique_managers]
        return ", ".join(manager_names[:10])  # Limit display to 10
    
    def get_activity_summary(self, activities: pd.Series) -> str:
        """Get summary of recent activities."""
        clean_activities = []
        for activity in activities.dropna().unique():
            activity_str = str(activity).strip()
            if activity_str and activity_str != "nan":
                clean_activities.append(activity_str)
        return "; ".join(clean_activities[:5])
    
    def get_data_summary(self) -> Dict[str, any]:
        """Get summary of loaded data."""
        if not self.data_loaded:
            return {"status": "No data loaded"}
        
        summary = {
            "status": "Data loaded successfully",
            "holdings_count": len(self.holdings_df) if self.holdings_df is not None else 0,
            "activities_count": len(self.activities_df) if self.activities_df is not None else 0,
            "managers_count": len(self.managers_df) if self.managers_df is not None else 0,
            "unique_tickers": len(self.holdings_df["ticker"].unique()) if self.holdings_df is not None else 0,
            "data_timestamp": self.data_timestamp,
        }
        
        return summary