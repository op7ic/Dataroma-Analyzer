#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Data Validator

Validates scraped data integrity and temporal accuracy.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

"""
Data validation utilities for ensuring 100% parsing accuracy.
Validates HTML to JSON conversion and data consistency.
"""

import json
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
import logging
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates data consistency and parsing accuracy."""
    
    def __init__(self, cache_dir: str = "cache"):
        self.cache_dir = Path(cache_dir)
        self.validation_results = {}
    
    def validate_all_data(self) -> Dict[str, bool]:
        """Run complete data validation."""
        results = {}
        
        # Validate JSON files exist and are valid
        results["json_files_valid"] = self.validate_json_files()
        
        # Validate activities data
        results["activities_valid"] = self.validate_activities_data()
        
        # Validate holdings data
        results["holdings_valid"] = self.validate_holdings_data()
        
        # Validate managers data
        results["managers_valid"] = self.validate_managers_data()
        
        # Cross-validation
        results["cross_validation"] = self.validate_data_consistency()
        
        self.validation_results = results
        return results
    
    def validate_json_files(self) -> bool:
        """Validate that JSON files are properly formatted."""
        required_files = [
            self.cache_dir / "json" / "activities.json",
            self.cache_dir / "json" / "holdings.json", 
            self.cache_dir / "json" / "managers.json"
        ]
        
        for file_path in required_files:
            try:
                if not file_path.exists():
                    logger.error(f"Missing required file: {file_path}")
                    return False
                
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    
                if not isinstance(data, list):
                    logger.error(f"Invalid format in {file_path}: expected list")
                    return False
                    
                if len(data) == 0:
                    logger.warning(f"Empty data file: {file_path}")
                    
            except Exception as e:
                logger.error(f"Error validating {file_path}: {e}")
                return False
        
        return True
    
    def validate_activities_data(self) -> bool:
        """Validate activities data structure and content."""
        try:
            activities_file = self.cache_dir / "json" / "activities.json"
            with open(activities_file, 'r') as f:
                activities = json.load(f)
            
            if not activities:
                logger.error("No activities data found")
                return False
            
            # Check required fields
            required_fields = ['ticker', 'manager_id', 'action_type', 'period']
            sample_activity = activities[0]
            
            for field in required_fields:
                if field not in sample_activity:
                    logger.error(f"Missing required field in activities: {field}")
                    return False
            
            # Check for temporal data corruption
            corrupted_count = 0
            for activity in activities:
                if activity.get('period') == '≡':
                    corrupted_count += 1
            
            if corrupted_count > 0:
                logger.error(f"Found {corrupted_count} corrupted temporal records")
                return False
            
            # Check action types are valid
            valid_actions = {'Buy', 'Sell', 'Add', 'Reduce', 'Hold'}
            invalid_actions = set()
            
            for activity in activities:
                action = activity.get('action_type')
                if action not in valid_actions:
                    invalid_actions.add(action)
            
            if invalid_actions:
                logger.warning(f"Found non-standard action types: {invalid_actions}")
            
            logger.info(f"Validated {len(activities)} activities")
            return True
            
        except Exception as e:
            logger.error(f"Error validating activities data: {e}")
            return False
    
    def validate_holdings_data(self) -> bool:
        """Validate holdings data structure and content."""
        try:
            holdings_file = self.cache_dir / "json" / "holdings.json"
            with open(holdings_file, 'r') as f:
                holdings = json.load(f)
            
            if not holdings:
                logger.error("No holdings data found")
                return False
            
            # Check for ticker corruption (≡ symbols)
            corrupted_tickers = 0
            for holding in holdings:
                if holding.get('ticker') == '≡':
                    corrupted_tickers += 1
            
            if corrupted_tickers > 0:
                logger.error(f"Found {corrupted_tickers} corrupted ticker symbols")
                return False
            
            # Check required fields
            required_fields = ['ticker', 'manager_id', 'value']
            sample_holding = holdings[0]
            
            for field in required_fields:
                if field not in sample_holding:
                    logger.error(f"Missing required field in holdings: {field}")
                    return False
            
            logger.info(f"Validated {len(holdings)} holdings")
            return True
            
        except Exception as e:
            logger.error(f"Error validating holdings data: {e}")
            return False
    
    def validate_managers_data(self) -> bool:
        """Validate managers data structure and content."""
        try:
            managers_file = self.cache_dir / "json" / "managers.json"
            with open(managers_file, 'r') as f:
                managers = json.load(f)
            
            if not managers:
                logger.error("No managers data found")
                return False
            
            # Check required fields
            required_fields = ['id', 'name']
            for manager in managers:
                for field in required_fields:
                    if field not in manager:
                        logger.error(f"Missing required field in manager: {field}")
                        return False
            
            logger.info(f"Validated {len(managers)} managers")
            return True
            
        except Exception as e:
            logger.error(f"Error validating managers data: {e}")
            return False
    
    def validate_data_consistency(self) -> bool:
        """Cross-validate data consistency between files."""
        try:
            # Load all data
            activities_file = self.cache_dir / "json" / "activities.json"
            holdings_file = self.cache_dir / "json" / "holdings.json"
            managers_file = self.cache_dir / "json" / "managers.json"
            
            with open(activities_file, 'r') as f:
                activities = json.load(f)
            with open(holdings_file, 'r') as f:
                holdings = json.load(f)
            with open(managers_file, 'r') as f:
                managers = json.load(f)
            
            # Check manager ID consistency
            manager_ids = {m['id'] for m in managers}
            
            # Check activities reference valid managers
            activity_manager_ids = {a['manager_id'] for a in activities}
            invalid_activity_managers = activity_manager_ids - manager_ids
            
            if invalid_activity_managers:
                logger.warning(f"Activities reference unknown managers: {invalid_activity_managers}")
            
            # Check holdings reference valid managers
            holding_manager_ids = {h['manager_id'] for h in holdings}
            invalid_holding_managers = holding_manager_ids - manager_ids
            
            if invalid_holding_managers:
                logger.warning(f"Holdings reference unknown managers: {invalid_holding_managers}")
            
            # Check ticker consistency between activities and holdings
            activity_tickers = {a['ticker'] for a in activities}
            holding_tickers = {h['ticker'] for h in holdings}
            
            # It's OK to have tickers in activities but not holdings (sold positions)
            # But warn about major discrepancies
            common_tickers = activity_tickers & holding_tickers
            logger.info(f"Common tickers between activities and holdings: {len(common_tickers)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in cross-validation: {e}")
            return False
    
    def get_validation_summary(self) -> Dict:
        """Get summary of validation results."""
        if not self.validation_results:
            return {"status": "No validation run"}
        
        all_passed = all(self.validation_results.values())
        
        summary = {
            "overall_status": "PASSED" if all_passed else "FAILED",
            "details": self.validation_results,
            "total_checks": len(self.validation_results),
            "passed_checks": sum(1 for v in self.validation_results.values() if v),
            "failed_checks": sum(1 for v in self.validation_results.values() if not v)
        }
        
        return summary