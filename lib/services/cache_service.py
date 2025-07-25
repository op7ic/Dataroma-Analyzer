#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Cache Service

Manages data caching with expiration and serialization.
Cache service for storing scraped data with JSON serialization.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

from ..models import Manager, Holding, Activity, StockData


class CacheService:
    """Service for caching scraped data."""

    def __init__(self, cache_dir: str = "cache"):
        """Initialize cache service.

        Args:
            cache_dir: Directory for cache files
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)

        # Create json subdirectory
        self.json_dir = self.cache_dir / "json"
        self.json_dir.mkdir(exist_ok=True)

        # Define cache file paths in json directory
        self.managers_file = self.json_dir / "managers.json"
        self.holdings_file = self.json_dir / "holdings.json"
        self.activities_file = self.json_dir / "history.json"  # Keep legacy name
        self.stocks_file = self.json_dir / "stocks.json"
        self.metadata_file = self.json_dir / "metadata.json"

        # Also create subdirectories for organized JSON data
        self.holdings_by_manager_dir = self.json_dir / "holdings_by_manager"
        self.holdings_by_manager_dir.mkdir(exist_ok=True)

        self.history_by_manager_dir = self.json_dir / "history_by_manager"
        self.history_by_manager_dir.mkdir(exist_ok=True)

    def save_managers(self, managers: List[Manager]) -> None:
        """Save managers to cache.

        Args:
            managers: List of Manager objects
        """
        data = [m.to_dict() for m in managers]
        self._save_json(self.managers_file, data)
        logging.info("Saved %d managers to cache", len(managers))

    def load_managers(self) -> List[Manager]:
        """Load managers from cache.

        Returns:
            List of Manager objects
        """
        data = self._load_json(self.managers_file)
        if data:
            return [Manager.from_dict(d) for d in data]
        return []

    def save_holdings(self, holdings: List[Holding]) -> None:
        """Save holdings to cache.

        Args:
            holdings: List of Holding objects
        """
        data = [h.to_dict() for h in holdings]
        self._save_json(self.holdings_file, data)

        # Also save by manager
        holdings_by_manager = {}
        for holding in holdings:
            manager_id = holding.manager_id
            if manager_id not in holdings_by_manager:
                holdings_by_manager[manager_id] = []
            holdings_by_manager[manager_id].append(holding.to_dict())

        # Save individual manager holdings files
        for manager_id, manager_holdings in holdings_by_manager.items():
            manager_file = self.holdings_by_manager_dir / f"{manager_id}.json"
            self._save_json(
                manager_file,
                {
                    "manager_id": manager_id,
                    "holdings": manager_holdings,
                    "timestamp": datetime.now().isoformat(),
                    "scraped_date": datetime.now().strftime("%Y-%m-%d"),
                },
            )

        logging.info("Saved %d holdings to cache", len(holdings))

    def load_holdings(self) -> List[Holding]:
        """Load holdings from cache.

        Returns:
            List of Holding objects
        """
        data = self._load_json(self.holdings_file)
        if data:
            return [Holding.from_dict(d) for d in data]
        return []

    def save_activities(self, activities: List[Activity]) -> None:
        """Save activities to cache.

        Args:
            activities: List of Activity objects
        """
        data = [a.to_dict() for a in activities]
        self._save_json(self.activities_file, data)

        # Also save by manager
        activities_by_manager = {}
        for activity in activities:
            manager_id = activity.manager_id
            if manager_id not in activities_by_manager:
                activities_by_manager[manager_id] = []
            activities_by_manager[manager_id].append(activity.to_dict())

        # Save individual manager activity files
        for manager_id, manager_activities in activities_by_manager.items():
            manager_file = self.history_by_manager_dir / f"{manager_id}.json"
            self._save_json(
                manager_file,
                {
                    "manager_id": manager_id,
                    "activities": manager_activities,
                    "timestamp": datetime.now().isoformat(),
                    "scraped_date": datetime.now().strftime("%Y-%m-%d"),
                },
            )

        logging.info("Saved %d activities to cache", len(activities))

    def load_activities(self) -> List[Activity]:
        """Load activities from cache.

        Returns:
            List of Activity objects
        """
        data = self._load_json(self.activities_file)
        if data:
            return [Activity.from_dict(d) for d in data]
        return []

    def save_stock_data(self, stocks: Dict[str, StockData]) -> None:
        """Save stock data to cache.

        Args:
            stocks: Dictionary mapping symbols to StockData objects
        """
        data = {symbol: stock.to_dict() for symbol, stock in stocks.items()}
        self._save_json(self.stocks_file, data)
        logging.info("Saved data for %d stocks to cache", len(stocks))

    def load_stock_data(self) -> Dict[str, StockData]:
        """Load stock data from cache.

        Returns:
            Dictionary mapping symbols to StockData objects
        """
        data = self._load_json(self.stocks_file)
        if data:
            return {
                symbol: StockData.from_dict(stock_data)
                for symbol, stock_data in data.items()
            }
        return {}

    def save_metadata(self, metadata: Dict[str, Any]) -> None:
        """Save metadata about the scraping session.

        Args:
            metadata: Metadata dictionary
        """
        metadata["last_updated"] = datetime.now().isoformat()
        self._save_json(self.metadata_file, metadata)

        # Also save overview file
        overview = {
            "timestamp": datetime.now().isoformat(),
            "num_managers": metadata.get("num_managers", 0),
            "num_holdings": metadata.get("num_holdings", 0),
            "num_activities": metadata.get("num_activities", 0),
            "unique_stocks": metadata.get("unique_stocks", 0),
            "progress": metadata.get("progress", {}),
        }
        self._save_json(self.json_dir / "overview.json", overview)

        # Save last update file
        self._save_json(
            self.json_dir / "last_update.json",
            {"timestamp": datetime.now().isoformat()},
        )

    def load_metadata(self) -> Dict[str, Any]:
        """Load metadata from cache.

        Returns:
            Metadata dictionary
        """
        data = self._load_json(self.metadata_file)
        return data if data else {}

    def is_cache_valid(self, max_age_hours: int = 24) -> bool:
        """Check if cache is still valid.

        Args:
            max_age_hours: Maximum age in hours

        Returns:
            True if cache is valid
        """
        metadata = self.load_metadata()
        if not metadata or "last_updated" not in metadata:
            return False

        try:
            last_updated = datetime.fromisoformat(metadata["last_updated"])
            age_hours = (datetime.now() - last_updated).total_seconds() / 3600
            return age_hours < max_age_hours
        except Exception:
            return False

    def clear_cache(self) -> None:
        """Clear all cache files."""
        for file_path in [
            self.managers_file,
            self.holdings_file,
            self.activities_file,
            self.stocks_file,
            self.metadata_file,
        ]:
            if file_path.exists():
                file_path.unlink()
                logging.info("Deleted cache file: %s", file_path)

    def _save_json(self, file_path: Path, data: Any) -> None:
        """Save data as JSON.

        Args:
            file_path: Path to save to
            data: Data to save
        """
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logging.error("Failed to save %s: %s", file_path, str(e))
            raise

    def _load_json(self, file_path: Path) -> Optional[Any]:
        """Load data from JSON file.

        Args:
            file_path: Path to load from

        Returns:
            Loaded data or None
        """
        if not file_path.exists():
            return None

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.error("Failed to load %s: %s", file_path, str(e))
            return None
