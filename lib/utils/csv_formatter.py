#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - CSV Formatter

Formats analysis results into clean CSV files with proper headers.
CSV formatting utilities for clean, consistent output with manager name mapping
and column standardization.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

import csv
import json
import re
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class CSVFormatter:
    """Formats CSV files with clean manager names and consistent columns."""
    
    def __init__(self, cache_dir: str = "cache"):
        self.cache_dir = Path(cache_dir)
        self.manager_mapping = self._load_manager_mapping()
    
    def _load_manager_mapping(self) -> Dict[str, str]:
        """Load manager ID to name mapping."""
        mapping = {}
        
        try:
            with open(self.cache_dir / "json" / "managers.json", 'r') as f:
                managers = json.load(f)
            
            for mgr in managers:
                mgr_id = mgr.get('id', mgr.get('manager_id', ''))
                name = mgr.get('name', '')
                if mgr_id and name:
                    # Clean the name - remove any "Updated" timestamps
                    clean_name = re.sub(r'\s+Updated\s+\d+\s+\w+\s+\d+', '', name).strip()
                    mapping[mgr_id] = clean_name
        except Exception as e:
            logger.warning(f"Could not load manager mapping: {e}")
        
        return mapping
    
    def clean_manager_name(self, name: str) -> str:
        """Remove 'Updated' timestamps from manager names."""
        if pd.isna(name) or not name:
            return ""
        # Remove "Updated DD Mon YYYY" pattern
        cleaned = re.sub(r'\s+Updated\s+\d+\s+\w+\s+\d+', '', str(name)).strip()
        # Also remove double spaces
        cleaned = re.sub(r'\s+', ' ', cleaned)
        return cleaned
    
    def format_manager_list(self, managers_str: str) -> str:
        """Format manager list with newlines for readability."""
        if pd.isna(managers_str) or not managers_str:
            return ""
        
        # Split by comma and clean each
        manager_entries = [m.strip() for m in str(managers_str).split(',')]
        clean_managers = []
        
        for entry in manager_entries:
            # First try to map if it's an ID
            mapped_name = self.manager_mapping.get(entry, entry)
            # Then clean any timestamps
            clean_name = self.clean_manager_name(mapped_name)
            if clean_name and clean_name not in clean_managers:  # Avoid duplicates
                clean_managers.append(clean_name)
        
        # Join with newlines for multi-manager cells
        return "\n".join(clean_managers) if len(clean_managers) > 1 else (clean_managers[0] if clean_managers else "")
    
    def standardize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names and order."""
        if df.empty:
            return df
        
        # Standard column mappings
        column_mappings = {
            'manager_id': 'manager',
            'manager_name': 'manager', 
            'stock': 'ticker',
            'symbol': 'ticker',
            'name': 'company_name',
            'company': 'company_name',
            'price': 'current_price',
            'latest_price': 'current_price',
            'activity_date': 'action_date',
            'date': 'action_date',
            'latest_date': 'action_date',
            'activity': 'action',
            'activity_type': 'action',
            'transaction': 'action'
        }
        
        # Rename columns to standard names
        df = df.rename(columns=column_mappings)
        
        # Standard column order (put most important first)
        standard_order = [
            'ticker', 'company_name', 'action', 'action_date', 'current_price', 
            'manager', 'managers', 'total_value', 'portfolio_pct', 'avg_portfolio_pct'
        ]
        
        # Get columns in preferred order
        ordered_cols = [col for col in standard_order if col in df.columns]
        other_cols = [col for col in df.columns if col not in ordered_cols]
        
        return df[ordered_cols + other_cols]
    
    def format_csv_file(self, filepath: Path) -> bool:
        """Format a single CSV file."""
        try:
            df = pd.read_csv(filepath)
            
            # Clean manager columns
            manager_cols = ['manager', 'managers', 'buying_managers', 'selling_managers', 
                          'top_managers', 'active_managers']
            
            for col in manager_cols:
                if col in df.columns:
                    if col == 'manager':
                        # Single manager - just clean the name
                        df[col] = df[col].apply(self.clean_manager_name)
                    else:
                        # Multiple managers - format with newlines
                        df[col] = df[col].apply(self.format_manager_list)
            
            # Standardize column order
            df = self.standardize_columns(df)
            
            # Save formatted CSV
            df.to_csv(filepath, index=False)
            logger.info(f"Formatted CSV: {filepath.name}")
            return True
            
        except Exception as e:
            logger.error(f"Error formatting {filepath}: {e}")
            return False
    
    def format_all_csvs(self, directory: Path) -> int:
        """Format all CSV files in a directory."""
        csv_files = list(directory.glob("**/*.csv"))
        formatted_count = 0
        
        logger.info(f"Formatting {len(csv_files)} CSV files in {directory}")
        
        for csv_file in csv_files:
            if self.format_csv_file(csv_file):
                formatted_count += 1
        
        logger.info(f"Successfully formatted {formatted_count}/{len(csv_files)} CSV files")
        return formatted_count