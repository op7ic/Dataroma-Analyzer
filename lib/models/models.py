#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Data Models

Core data structures for managers, holdings, activities, and analysis results.
Defines all data models used throughout the Dataroma scraper and analyzer.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict, Any


@dataclass
class Manager:
    """Investment manager data model."""

    id: str
    name: str
    firm: str = ""
    portfolio_value: float = 0.0
    num_holdings: int = 0
    url: str = ""
    last_updated: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "name": self.name,
            "firm": self.firm,
            "portfolio_value": self.portfolio_value,
            "num_holdings": self.num_holdings,
            "url": self.url,
            "last_updated": self.last_updated.isoformat()
            if self.last_updated
            else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Manager":
        """Create from dictionary."""
        if data.get("last_updated"):
            data["last_updated"] = datetime.fromisoformat(data["last_updated"])
        return cls(**data)


@dataclass
class Holding:
    """Stock holding data model."""

    symbol: str
    company_name: str
    manager_id: str
    shares: int
    value: float
    percentage: float
    portfolio_percent: float = 0.0

    # Price information
    reported_price: float = 0.0  # Purchase/reported price
    current_price: float = 0.0  # Current market price
    price_change_percent: float = 0.0  # % change from reported to current

    # Additional data
    recent_activity: str = ""
    week_52_low: float = 0.0
    week_52_high: float = 0.0

    # Temporal data fields
    reporting_date: str = ""  # When this holding data was reported
    reporting_quarter: str = ""  # Quarter of the report (e.g., "Q1 2025")
    
    # Enriched data fields
    market_cap: float = 0.0
    pe_ratio: float = 0.0
    dividend_yield: float = 0.0
    price_to_book: float = 0.0
    sector: str = ""
    industry: str = ""
    data_quality: str = "Low"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "symbol": self.symbol,
            "company_name": self.company_name,
            "manager_id": self.manager_id,
            "shares": self.shares,
            "value": self.value,
            "percentage": self.percentage,
            "portfolio_percent": self.portfolio_percent,
            "reported_price": self.reported_price,
            "current_price": self.current_price,
            "price_change_percent": self.price_change_percent,
            "recent_activity": self.recent_activity,
            "week_52_low": self.week_52_low,
            "week_52_high": self.week_52_high,
            "reporting_date": self.reporting_date,
            "reporting_quarter": self.reporting_quarter,
            "market_cap": self.market_cap,
            "pe_ratio": self.pe_ratio,
            "dividend_yield": self.dividend_yield,
            "price_to_book": self.price_to_book,
            "sector": self.sector,
            "industry": self.industry,
            "data_quality": self.data_quality,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Holding":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class Activity:
    """Trading activity data model."""

    symbol: str
    manager_id: str
    action: str
    date: str
    percentage_change: float = 0.0
    action_type: str = ""
    shares: int = 0
    portfolio_percentage: float = 0.0
    company_name: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "symbol": self.symbol,
            "manager_id": self.manager_id,
            "action": self.action,
            "date": self.date,
            "percentage_change": self.percentage_change,
            "action_type": self.action_type,
            "shares": self.shares,
            "portfolio_percentage": self.portfolio_percentage,
            "company_name": self.company_name,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Activity":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class StockData:
    """Stock market data model."""

    symbol: str
    market_cap: float = 0.0
    pe_ratio: float = 0.0
    dividend_yield: float = 0.0
    price_to_book: float = 0.0
    sector: str = ""
    industry: str = ""
    current_price: float = 0.0
    fifty_two_week_high: float = 0.0
    fifty_two_week_low: float = 0.0
    avg_volume: int = 0
    beta: float = 0.0
    last_updated: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "symbol": self.symbol,
            "market_cap": self.market_cap,
            "pe_ratio": self.pe_ratio,
            "dividend_yield": self.dividend_yield,
            "price_to_book": self.price_to_book,
            "sector": self.sector,
            "industry": self.industry,
            "current_price": self.current_price,
            "fifty_two_week_high": self.fifty_two_week_high,
            "fifty_two_week_low": self.fifty_two_week_low,
            "avg_volume": self.avg_volume,
            "beta": self.beta,
            "last_updated": self.last_updated.isoformat()
            if self.last_updated
            else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "StockData":
        """Create from dictionary."""
        if data.get("last_updated"):
            data["last_updated"] = datetime.fromisoformat(data["last_updated"])
        return cls(**data)


@dataclass
class ScraperProgress:
    """Progress tracking for scraping operations."""

    managers_processed: int = 0
    holdings_found: int = 0
    activities_found: int = 0
    stocks_processed: int = 0
    stocks_enriched: int = 0
    errors_encountered: int = 0
    start_time: Optional[datetime] = field(default_factory=datetime.now)

    def get_duration(self) -> float:
        """Get duration in seconds."""
        if self.start_time:
            return (datetime.now() - self.start_time).total_seconds()
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "managers_processed": self.managers_processed,
            "holdings_found": self.holdings_found,
            "activities_found": self.activities_found,
            "stocks_processed": self.stocks_processed,
            "stocks_enriched": self.stocks_enriched,
            "errors_encountered": self.errors_encountered,
            "duration_seconds": self.get_duration(),
        }
