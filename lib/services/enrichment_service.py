#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Enrichment Service

Enriches scraped data with additional financial information.
Enhanced stock enrichment service with persistent caching and IP limit learning.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple

from ..models import StockData, Holding
from ..clients.yahoo_finance import YahooFinanceClient


class EnrichmentService:
    """Enhanced stock enrichment with persistent caching across sessions."""

    def __init__(self, cache_dir: str = "cache", rate_limit: float = 0.5):
        """Initialize enrichment service.

        Args:
            cache_dir: Directory for cache files
            rate_limit: Seconds between Yahoo Finance requests
        """
        self.cache_dir = Path(cache_dir)
        self.yahoo_client = YahooFinanceClient(
            rate_limit=rate_limit, cache_dir=cache_dir
        )

        # Persistent cache files
        self.stock_cache_file = self.cache_dir / "json" / "stocks.json"
        self.enrichment_status_file = self.cache_dir / "json" / "enrichment_status.json"

        # Load existing data
        self.persistent_stock_cache = self._load_persistent_stock_cache()
        self.enrichment_status = self._load_enrichment_status()

        logging.info(
            "EnrichmentService initialized with %d cached stocks",
            len(self.persistent_stock_cache),
        )

    def _load_persistent_stock_cache(self) -> Dict[str, StockData]:
        """Load previously enriched stock data from disk."""
        if self.stock_cache_file.exists():
            try:
                with open(self.stock_cache_file, "r") as f:
                    data = json.load(f)
                    return {
                        symbol: StockData.from_dict(stock_data)
                        for symbol, stock_data in data.items()
                    }
            except Exception as e:
                logging.error("Error loading stock cache: %s", str(e))
        return {}

    def _save_persistent_stock_cache(self) -> None:
        """Save enriched stock data to disk for future sessions."""
        try:
            data = {
                symbol: stock.to_dict()
                for symbol, stock in self.persistent_stock_cache.items()
            }
            self.stock_cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.stock_cache_file, "w") as f:
                json.dump(data, f, indent=2)
            logging.info("Saved %d stocks to persistent cache", len(data))
        except Exception as e:
            logging.error("Error saving stock cache: %s", str(e))

    def _load_enrichment_status(self) -> Dict:
        """Load enrichment tracking status."""
        if self.enrichment_status_file.exists():
            try:
                with open(self.enrichment_status_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logging.error("Error loading enrichment status: %s", str(e))

        return {
            "enriched_symbols": [],
            "failed_symbols": [],
            "ip_sessions": [],
            "last_updated": None,
        }

    def _save_enrichment_status(self) -> None:
        """Save enrichment tracking status."""
        try:
            self.enrichment_status["last_updated"] = datetime.now().isoformat()
            self.enrichment_status_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.enrichment_status_file, "w") as f:
                json.dump(self.enrichment_status, f, indent=2)
        except Exception as e:
            logging.error("Error saving enrichment status: %s", str(e))

    def _get_unenriched_symbols(self, all_symbols: List[str]) -> List[str]:
        """Get symbols that haven't been successfully enriched yet.

        Args:
            all_symbols: List of all symbols to check

        Returns:
            List of symbols needing enrichment
        """
        enriched_set = set(self.enrichment_status.get("enriched_symbols", []))
        unenriched = [s for s in all_symbols if s not in enriched_set]

        # Also check if we have actual data for claimed enriched symbols
        verified_unenriched = []
        for symbol in unenriched:
            if symbol not in self.persistent_stock_cache:
                verified_unenriched.append(symbol)

        return verified_unenriched

    def _estimate_ip_limit(self) -> int:
        """Estimate IP limit based on previous sessions.

        Returns:
            Estimated safe IP limit
        """
        base_limit = 150  # Conservative default

        # Analyze previous sessions
        ip_sessions = self.enrichment_status.get("ip_sessions", [])
        if ip_sessions:
            # Find successful sessions (didn't hit limit)
            successful_counts = [
                session.get("requests_made", 0)
                for session in ip_sessions
                if not session.get("limit_hit", False)
                and session.get("requests_made", 0) > 0
            ]

            if successful_counts:
                # Use the highest successful count + margin
                max_successful = max(successful_counts)
                if max_successful >= base_limit:
                    logging.info(
                        "Previous session completed %d requests successfully",
                        max_successful,
                    )
                    # Try 20% more than highest successful
                    base_limit = int(max_successful * 1.2)

        # Cap at reasonable limit
        return min(base_limit, 500)

    def enrich_holdings(self, holdings: List[Holding]) -> Tuple[List[Holding], int]:
        """Enrich holdings with stock data, using persistent cache.

        Args:
            holdings: List of holdings to enrich

        Returns:
            Tuple of (enriched holdings, number of new stocks enriched)
        """
        # Extract unique symbols
        all_symbols = list(set(h.symbol for h in holdings))

        # First, update holdings from persistent cache
        cache_hits = 0
        for holding in holdings:
            if holding.symbol in self.persistent_stock_cache:
                stock_data = self.persistent_stock_cache[holding.symbol]
                self._update_holding_from_stock(holding, stock_data)
                cache_hits += 1

        logging.info(
            "Updated %d/%d holdings from persistent cache", cache_hits, len(holdings)
        )

        # Get symbols needing enrichment
        unenriched = self._get_unenriched_symbols(all_symbols)

        if not unenriched:
            logging.info("All symbols already enriched from previous sessions!")
            return holdings, 0

        logging.info(
            "Need to enrich %d new symbols (already have %d)",
            len(unenriched),
            len(all_symbols) - len(unenriched),
        )

        # Estimate safe IP limit
        ip_limit = self._estimate_ip_limit()
        logging.info("Using IP limit estimate: %d requests", ip_limit)

        # Start session tracking
        session_info = {
            "start_time": datetime.now().isoformat(),
            "ip_limit": ip_limit,
            "requests_made": 0,
            "symbols_enriched": 0,
            "symbols_failed": 0,
            "limit_hit": False,
        }

        # Enrich new symbols
        new_enriched = 0

        # Process in reasonable batches
        batch_size = min(50, ip_limit // 3)  # Don't process too many at once

        for i in range(0, len(unenriched), batch_size):
            # Check if we should stop
            if self.yahoo_client.request_count >= ip_limit - 5:  # Leave margin
                logging.warning(
                    "Approaching IP limit (%d/%d), stopping",
                    self.yahoo_client.request_count,
                    ip_limit,
                )
                session_info["limit_hit"] = True
                break

            batch = unenriched[i : i + batch_size]

            try:
                # Get stock data for batch
                batch_results = self.yahoo_client.get_bulk_quotes(batch)

                # Update persistent cache and holdings
                for symbol, stock_data in batch_results.items():
                    self.persistent_stock_cache[symbol] = stock_data
                    self.enrichment_status["enriched_symbols"].append(symbol)
                    new_enriched += 1

                    # Update any holdings with this symbol
                    for holding in holdings:
                        if holding.symbol == symbol:
                            self._update_holding_from_stock(holding, stock_data)

                # Track failed symbols
                failed_in_batch = set(batch) - set(batch_results.keys())
                for symbol in failed_in_batch:
                    if symbol not in self.enrichment_status.get("failed_symbols", []):
                        self.enrichment_status["failed_symbols"].append(symbol)
                    session_info["symbols_failed"] += 1

            except Exception as e:
                logging.error("Error enriching batch: %s", str(e))
                session_info["symbols_failed"] += len(batch)

        # Finalize session info
        session_info["end_time"] = datetime.now().isoformat()
        session_info["requests_made"] = self.yahoo_client.request_count
        session_info["symbols_enriched"] = new_enriched

        # Add to session history
        self.enrichment_status["ip_sessions"].append(session_info)

        # Save everything
        self._save_persistent_stock_cache()
        self._save_enrichment_status()

        # Log summary
        total_enriched = len(self.enrichment_status.get("enriched_symbols", []))
        logging.info(
            "Enrichment session complete: %d new stocks enriched, %d total in cache",
            new_enriched,
            total_enriched,
        )

        if session_info["limit_hit"]:
            remaining = len(unenriched) - new_enriched - session_info["symbols_failed"]
            logging.info(
                "IP limit reached. %d symbols remaining for next session", remaining
            )

        return holdings, new_enriched

    def _update_holding_from_stock(
        self, holding: Holding, stock_data: StockData
    ) -> None:
        """Update holding with stock data.

        Args:
            holding: Holding to update
            stock_data: Stock data to apply
        """
        # Market data
        holding.market_cap = stock_data.market_cap
        holding.pe_ratio = stock_data.pe_ratio
        holding.dividend_yield = stock_data.dividend_yield
        holding.price_to_book = stock_data.price_to_book
        holding.sector = stock_data.sector
        holding.industry = stock_data.industry

        # Price data
        if stock_data.current_price > 0:
            holding.current_price = stock_data.current_price

            # Calculate price change
            if holding.reported_price > 0:
                price_change = (
                    (holding.current_price - holding.reported_price)
                    / holding.reported_price
                ) * 100
                holding.price_change_percent = round(price_change, 2)

        # 52-week range
        holding.week_52_high = stock_data.fifty_two_week_high
        holding.week_52_low = stock_data.fifty_two_week_low

        # Data quality
        if stock_data.market_cap > 0 and stock_data.pe_ratio > 0:
            holding.data_quality = "High"
        elif stock_data.market_cap > 0:
            holding.data_quality = "Medium"
        else:
            holding.data_quality = "Low"

    def get_enrichment_summary(self) -> Dict:
        """Get summary of enrichment progress across all sessions.

        Returns:
            Summary dictionary with statistics
        """
        total_enriched = len(self.enrichment_status.get("enriched_symbols", []))
        total_failed = len(self.enrichment_status.get("failed_symbols", []))
        ip_sessions = self.enrichment_status.get("ip_sessions", [])

        summary = {
            "total_enriched": total_enriched,
            "total_failed": total_failed,
            "total_sessions": len(ip_sessions),
            "last_updated": self.enrichment_status.get("last_updated"),
            "stocks_in_cache": len(self.persistent_stock_cache),
            "ip_limit_estimates": [],
            "total_requests_made": sum(s.get("requests_made", 0) for s in ip_sessions),
            "average_enrichment_rate": 0.0,
        }

        # Calculate average enrichment rate
        if ip_sessions:
            total_enriched_in_sessions = sum(
                s.get("symbols_enriched", 0) for s in ip_sessions
            )
            total_requests = sum(s.get("requests_made", 0) for s in ip_sessions)
            if total_requests > 0:
                summary["average_enrichment_rate"] = round(
                    (total_enriched_in_sessions / total_requests) * 100, 1
                )

        # Analyze IP sessions for limit estimates
        for session in ip_sessions[-10:]:  # Last 10 sessions
            if session.get("requests_made", 0) > 0:
                summary["ip_limit_estimates"].append(
                    {
                        "date": session.get("start_time", "").split("T")[0],
                        "time": session.get("start_time", "")
                        .split("T")[1]
                        .split(".")[0],
                        "requests": session["requests_made"],
                        "enriched": session.get("symbols_enriched", 0),
                        "limit_hit": session.get("limit_hit", False),
                    }
                )

        return summary

    def close(self) -> None:
        """Clean up resources."""
        self.yahoo_client.close()
