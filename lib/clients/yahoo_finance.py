#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Yahoo Finance Client

Fetches real-time stock prices and financial data from Yahoo Finance.
API client for stock data enrichment with IP limit awareness.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

import logging
import re
import time
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

import requests

from .http_client import HTTPClient
from ..models import StockData


class YahooFinanceClient:
    """Client for Yahoo Finance API with IP limit awareness."""

    def __init__(self, rate_limit: float = 0.5, cache_dir: str = "cache"):
        """Initialize Yahoo Finance client.

        Args:
            rate_limit: Seconds between API requests
            cache_dir: Directory for cache files
        """
        self.client = HTTPClient(rate_limit=rate_limit, timeout=30)
        self.base_url = "https://query1.finance.yahoo.com"

        # Request tracking for IP limit
        self.request_count = 0
        self.max_requests_per_ip = 150  # Conservative limit
        self.session_start_time = time.time()

        # Crumb authentication - stored only in memory for the session
        self.crumb = None

        # Stock data cache to reduce repeated requests
        self.stock_cache = {}
        self.cache_duration = timedelta(hours=24)

        logging.info(
            "YahooFinanceClient initialized (IP limit: %d requests)",
            self.max_requests_per_ip,
        )

    def _check_ip_limit(self) -> bool:
        """Check if we're approaching IP request limit.

        Returns:
            True if safe to continue, False if limit reached
        """
        if self.request_count >= self.max_requests_per_ip:
            elapsed = time.time() - self.session_start_time
            logging.warning(
                "Yahoo Finance IP limit reached: %d requests in %.1f seconds",
                self.request_count,
                elapsed,
            )
            return False

        if self.request_count > 0 and self.request_count % 50 == 0:
            logging.info(
                "Yahoo Finance requests: %d/%d (%.1f%% of IP limit)",
                self.request_count,
                self.max_requests_per_ip,
                (self.request_count / self.max_requests_per_ip) * 100,
            )

        return True

    def _increment_request_count(self) -> None:
        """Increment request counter."""
        self.request_count += 1

    def _get_crumb(self) -> str:
        """Get or refresh Yahoo Finance crumb.

        Returns:
            Crumb string (empty if failed)
        """
        # Return existing crumb if we have one for this session
        if self.crumb:
            return self.crumb

        # Get fresh crumb - need to use the same session
        try:
            # First, visit Yahoo Finance to establish session and get cookies
            logging.debug("Establishing Yahoo Finance session...")
            response = self.client.session.get(
                "https://finance.yahoo.com/", timeout=30, allow_redirects=True
            )
            response.raise_for_status()
            self._increment_request_count()

            # Now get the crumb using the same session with cookies
            logging.debug("Fetching crumb...")
            crumb_response = self.client.session.get(
                "https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=30
            )
            crumb_response.raise_for_status()
            self._increment_request_count()

            if crumb_response.text:
                self.crumb = crumb_response.text.strip()
                logging.info("Got fresh Yahoo Finance crumb: %s...", self.crumb[:10])
            else:
                logging.warning("Empty crumb response")
                self.crumb = ""

        except requests.exceptions.RequestException as e:
            logging.warning("Could not get crumb: %s", str(e))
            self.crumb = ""
        except Exception as e:
            logging.error("Unexpected error getting crumb: %s", str(e))
            self.crumb = ""

        return self.crumb

    def get_stock_data(self, symbol: str) -> Optional[StockData]:
        """Get stock data for a symbol.

        Args:
            symbol: Stock ticker symbol

        Returns:
            StockData object or None if failed
        """
        if not self._validate_symbol(symbol):
            logging.error("Invalid symbol format: %s", symbol)
            return None

        # Check cache first
        if symbol in self.stock_cache:
            cached_data, cache_time = self.stock_cache[symbol]
            if datetime.now() - cache_time < self.cache_duration:
                logging.debug("Using cached data for %s", symbol)
                return cached_data

        # Check IP limit
        if not self._check_ip_limit():
            logging.error("IP limit reached, cannot fetch %s", symbol)
            return None

        # Get crumb for authenticated requests
        crumb = self._get_crumb()

        # Try v10 quoteSummary endpoint (more reliable)
        url = f"{self.base_url}/v10/finance/quoteSummary/{symbol}"
        params = {
            "modules": "defaultKeyStatistics,financialData,price,summaryDetail",
            "formatted": "true",
            "crumb": crumb,
        }

        # Try with current crumb
        self._increment_request_count()
        response = self.client.session.get(url, params=params, timeout=30)

        # If we get 401, refresh crumb and retry once
        if response.status_code == 401:
            logging.info("Crumb expired, refreshing...")
            self.crumb = None  # Force refresh

            crumb = self._get_crumb()
            params["crumb"] = crumb

            self._increment_request_count()
            response = self.client.session.get(url, params=params, timeout=30)

        try:
            response.raise_for_status()
            response_data = response.json()

            if response_data and "quoteSummary" in response_data:
                result = response_data["quoteSummary"].get("result", [])
                if result:
                    stock_data = self._parse_quote_summary(symbol, result[0])
                    if stock_data:
                        # Cache the result
                        self.stock_cache[symbol] = (stock_data, datetime.now())
                        return stock_data
        except Exception as e:
            logging.debug("QuoteSummary failed for %s: %s", symbol, str(e))

        # Fallback to v7 quote endpoint
        return self._get_quote_data(symbol)

    def get_bulk_quotes(self, symbols: List[str]) -> Dict[str, StockData]:
        """Get stock data for multiple symbols.

        Args:
            symbols: List of stock ticker symbols

        Returns:
            Dictionary mapping symbols to StockData objects
        """
        result = {}

        # Filter out cached symbols
        uncached_symbols = []
        for symbol in symbols:
            if not self._validate_symbol(symbol):
                continue

            # Check cache
            if symbol in self.stock_cache:
                cached_data, cache_time = self.stock_cache[symbol]
                if datetime.now() - cache_time < self.cache_duration:
                    result[symbol] = cached_data
                    continue

            uncached_symbols.append(symbol)

        if not uncached_symbols:
            logging.info("All %d symbols served from cache", len(result))
            return result

        # Check how many requests we can still make
        remaining_requests = self.max_requests_per_ip - self.request_count
        if remaining_requests <= 0:
            logging.warning(
                "IP limit reached. Returning %d cached results only", len(result)
            )
            return result

        # Limit symbols to process based on remaining requests
        symbols_to_process = uncached_symbols[:remaining_requests]
        if len(symbols_to_process) < len(uncached_symbols):
            logging.warning(
                "IP limit allows only %d more requests. Processing %d/%d symbols",
                remaining_requests,
                len(symbols_to_process),
                len(uncached_symbols),
            )

        # Get crumb
        crumb = self._get_crumb()

        # Yahoo allows up to 10 symbols per request
        batch_size = 10

        for i in range(0, len(symbols_to_process), batch_size):
            if not self._check_ip_limit():
                logging.warning("IP limit reached during bulk fetch")
                break

            batch = symbols_to_process[i : i + batch_size]
            symbols_str = ",".join(batch)

            url = f"{self.base_url}/v7/finance/quote"
            params = {
                "symbols": symbols_str,
                "fields": "symbol,marketCap,trailingPE,dividendYield,priceToBook,sector,industry,regularMarketPrice,fiftyTwoWeekHigh,fiftyTwoWeekLow,averageDailyVolume10Day,beta",
                "crumb": crumb,
            }

            self._increment_request_count()
            response_data = self.client.get_json(url, params=params)

            if response_data and "quoteResponse" in response_data:
                quotes = response_data["quoteResponse"].get("result", [])

                for quote in quotes:
                    stock_data = self._parse_quote_response(quote)
                    if stock_data:
                        result[stock_data.symbol] = stock_data
                        # Cache the result
                        self.stock_cache[stock_data.symbol] = (
                            stock_data,
                            datetime.now(),
                        )

        logging.info(
            "Retrieved data for %d/%d symbols (%d from cache, %d from API)",
            len(result),
            len(symbols),
            len(result) - len(symbols_to_process),
            len(symbols_to_process),
        )
        return result

    def _validate_symbol(self, symbol: str) -> bool:
        """Validate stock symbol format.

        Args:
            symbol: Stock ticker symbol

        Returns:
            True if valid
        """
        if not symbol or not isinstance(symbol, str):
            return False

        # Allow alphanumeric plus dots and hyphens (for BRK.A, etc.)
        return bool(re.match(r"^[A-Za-z0-9\.\-]+$", symbol))

    def _get_quote_data(self, symbol: str) -> Optional[StockData]:
        """Get stock data from quote endpoint.

        Args:
            symbol: Stock ticker symbol

        Returns:
            StockData object or None
        """
        url = f"{self.base_url}/v7/finance/quote"
        params = {
            "symbols": symbol,
            "fields": "symbol,marketCap,trailingPE,dividendYield,priceToBook,sector,industry,regularMarketPrice,fiftyTwoWeekHigh,fiftyTwoWeekLow,averageDailyVolume10Day,beta",
            "crumb": self._get_crumb(),
        }

        self._increment_request_count()
        response_data = self.client.get_json(url, params=params)

        if response_data and "quoteResponse" in response_data:
            quotes = response_data["quoteResponse"].get("result", [])
            if quotes:
                return self._parse_quote_response(quotes[0])

        return None

    def _parse_quote_summary(
        self, symbol: str, data: Dict[str, Any]
    ) -> Optional[StockData]:
        """Parse quoteSummary API response.

        Args:
            symbol: Stock symbol
            data: API response data containing modules

        Returns:
            StockData object or None
        """
        try:
            # Extract data from different modules
            price_data = data.get("price", {})
            summary = data.get("summaryDetail", {})
            default_stats = data.get("defaultKeyStatistics", {})
            financial_data = data.get("financialData", {})

            # Get current price (try multiple fields)
            current_price = (
                price_data.get("regularMarketPrice", {}).get("raw", 0.0)
                or summary.get("regularMarketPrice", {}).get("raw", 0.0)
                or financial_data.get("currentPrice", {}).get("raw", 0.0)
            )

            # Get market cap
            market_cap = price_data.get("marketCap", {}).get("raw", 0.0) or summary.get(
                "marketCap", {}
            ).get("raw", 0.0)

            # Get 52-week range
            fifty_two_week_high = summary.get("fiftyTwoWeekHigh", {}).get("raw", 0.0)
            fifty_two_week_low = summary.get("fiftyTwoWeekLow", {}).get("raw", 0.0)

            # Get other metrics
            pe_ratio = default_stats.get("trailingPE", {}).get(
                "raw", 0.0
            ) or summary.get("trailingPE", {}).get("raw", 0.0)

            dividend_yield = summary.get("dividendYield", {}).get("raw", 0.0)
            price_to_book = default_stats.get("priceToBook", {}).get("raw", 0.0)

            # Get volume
            avg_volume = summary.get("averageDailyVolume10Day", {}).get("raw", 0)

            # Get beta
            beta = default_stats.get("beta", {}).get("raw", 0.0)

            # Get sector/industry from price data
            sector = price_data.get("sector", "")
            industry = price_data.get("industry", "")

            return StockData(
                symbol=symbol,
                current_price=float(current_price),
                market_cap=float(market_cap),
                pe_ratio=float(pe_ratio),
                dividend_yield=float(dividend_yield),
                price_to_book=float(price_to_book),
                sector=sector,
                industry=industry,
                fifty_two_week_high=float(fifty_two_week_high),
                fifty_two_week_low=float(fifty_two_week_low),
                avg_volume=int(avg_volume) if avg_volume else 0,
                beta=float(beta),
                last_updated=datetime.now(),
            )

        except Exception as e:
            logging.error(
                "Error parsing quoteSummary response for %s: %s", symbol, str(e)
            )
            return None

    def _parse_chart_response(
        self, symbol: str, data: Dict[str, Any]
    ) -> Optional[StockData]:
        """Parse chart API response.

        Args:
            symbol: Stock symbol
            data: API response data

        Returns:
            StockData object or None
        """
        try:
            chart = data.get("chart", {})
            if not chart or chart.get("error"):
                return None

            result = chart.get("result", [])
            if not result:
                return None

            chart_data = result[0]
            meta = chart_data.get("meta", {})

            return StockData(
                symbol=symbol,
                current_price=meta.get("regularMarketPrice", 0.0),
                market_cap=meta.get("marketCap", 0.0),
                fifty_two_week_high=meta.get("fiftyTwoWeekHigh", 0.0),
                fifty_two_week_low=meta.get("fiftyTwoWeekLow", 0.0),
                last_updated=datetime.now(),
            )

        except Exception as e:
            logging.error("Error parsing chart response for %s: %s", symbol, str(e))
            return None

    def _parse_quote_response(self, quote: Dict[str, Any]) -> Optional[StockData]:
        """Parse quote API response.

        Args:
            quote: Quote data from API

        Returns:
            StockData object or None
        """
        try:
            symbol = quote.get("symbol", "")
            if not symbol:
                return None

            return StockData(
                symbol=symbol,
                market_cap=quote.get("marketCap", 0.0),
                pe_ratio=quote.get("trailingPE", 0.0),
                dividend_yield=quote.get("dividendYield", 0.0),
                price_to_book=quote.get("priceToBook", 0.0),
                sector=quote.get("sector", ""),
                industry=quote.get("industry", ""),
                current_price=quote.get("regularMarketPrice", 0.0),
                fifty_two_week_high=quote.get("fiftyTwoWeekHigh", 0.0),
                fifty_two_week_low=quote.get("fiftyTwoWeekLow", 0.0),
                avg_volume=int(quote.get("averageDailyVolume10Day", 0)),
                beta=quote.get("beta", 0.0),
                last_updated=datetime.now(),
            )

        except Exception as e:
            logging.error("Error parsing quote response: %s", str(e))
            return None

    def close(self) -> None:
        """Close the client."""
        self.client.close()

    def __enter__(self):
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        self.close()
