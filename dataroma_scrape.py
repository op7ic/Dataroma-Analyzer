#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Main Scraper

Orchestrates the scraping of investment manager data from Dataroma.com,
including manager profiles, portfolio holdings, and trading activities.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

import logging
from typing import List, Dict, Any
from bs4 import BeautifulSoup, Tag

from lib.models import Manager, Holding, Activity, ScraperProgress
from lib.clients import CachedHTTPClient
from lib.utils import DataromaParser
from lib.services import CacheService


class DataromaScraper:
    """Main scraper orchestrator following CLAUDE.md principles."""

    def __init__(
        self,
        cache_dir: str = "cache",
        rate_limit: float = 1.0,
        use_cache: bool = True,
    ):
        """Initialize scraper components.

        Args:
            cache_dir: Directory for cache files
            rate_limit: Seconds between Dataroma requests
            use_cache: Whether to use cached HTML
        """
        self.base_url = "https://www.dataroma.com/m/"
        self.use_cache = use_cache

        # Initialize components
        self.http_client = CachedHTTPClient(
            cache_dir=f"{cache_dir}/html", rate_limit=rate_limit
        )
        self.parser = DataromaParser()
        self.cache_service = CacheService(cache_dir)

        # Progress tracking
        self.progress = ScraperProgress()

        logging.info("DataromaScraper initialized (no external enrichment)")

    def scrape_all(self, force_refresh: bool = False) -> Dict[str, Any]:
        """Scrape all data from Dataroma.

        Args:
            force_refresh: Force refresh even if cache is valid

        Returns:
            Dictionary with all scraped data
        """
        # Check cache validity
        if not force_refresh and self.cache_service.is_cache_valid():
            logging.info("Using cached data (still valid)")
            return self._load_all_from_cache()

        logging.info("Starting full scrape")

        # Get managers
        managers = self._scrape_managers()
        if not managers:
            logging.error("No managers found")
            return self._empty_result()

        logging.info("Found %d managers to process", len(managers))

        # Get holdings and activities for each manager
        all_holdings = []
        all_activities = []

        for i, manager in enumerate(managers):
            # Get holdings
            holdings = self._scrape_manager_holdings(manager)
            all_holdings.extend(holdings)

            # Get activities
            activities = self._scrape_manager_activities(manager)
            all_activities.extend(activities)

            self.progress.managers_processed += 1

            # Log progress
            if self.progress.managers_processed % 5 == 0:
                logging.info(
                    "Progress: %d/%d managers, %d holdings, %d activities",
                    self.progress.managers_processed,
                    len(managers),
                    len(all_holdings),
                    len(all_activities),
                )

            # Save intermediate results every 10 managers
            if self.progress.managers_processed % 10 == 0:
                self._save_all_to_cache(managers[: i + 1], all_holdings, all_activities)
                logging.info("Saved intermediate results")

        # Skip Yahoo Finance enrichment - rely only on Dataroma data
        logging.info("Skipping external enrichment - using only Dataroma data")
        
        # Save to cache
        self._save_all_to_cache(managers, all_holdings, all_activities)

        # Log final stats
        logging.info(
            "Scraping complete: %d managers, %d holdings, %d activities in %.1f seconds",
            len(managers),
            len(all_holdings),
            len(all_activities),
            self.progress.get_duration(),
        )

        return {
            "managers": managers,
            "holdings": all_holdings,
            "activities": all_activities,
            "progress": self.progress.to_dict(),
        }

    def _scrape_managers(self) -> List[Manager]:
        """Scrape list of managers.

        Returns:
            List of Manager objects
        """
        url = f"{self.base_url}home.php"
        cache_key = "general/managers_page.html"
        html = self.http_client.get(url, use_cache=self.use_cache, cache_key=cache_key)

        if not html:
            logging.error("Failed to fetch managers page")
            return []

        managers = self.parser.parse_managers_list(html)
        logging.info("Found %d managers", len(managers))

        return managers

    def _scrape_manager_holdings(self, manager: Manager) -> List[Holding]:
        """Scrape holdings for a manager.

        Args:
            manager: Manager object

        Returns:
            List of Holding objects
        """
        url = f"{self.base_url}holdings.php?m={manager.id}"
        cache_key = f"managers/{manager.id}/holdings.html"
        html = self.http_client.get(url, use_cache=self.use_cache, cache_key=cache_key)

        if not html:
            logging.warning("Failed to fetch holdings for %s", manager.name)
            self.progress.errors_encountered += 1
            return []

        holdings = self.parser.parse_holdings_with_dates(html, manager.id)
        self.progress.holdings_found += len(holdings)

        return holdings

    def _scrape_manager_activities(
        self, manager: Manager, max_pages: int = 20
    ) -> List[Activity]:
        """Scrape activities for a manager with pagination support.

        Args:
            manager: Manager object
            max_pages: Maximum pages to fetch

        Returns:
            List of Activity objects
        """
        all_activities = []

        # Fetch first page
        url = f"{self.base_url}m_activity.php?m={manager.id}&typ=a"
        cache_key = f"managers/{manager.id}/activity_page1.html"
        html = self.http_client.get(url, use_cache=self.use_cache, cache_key=cache_key)

        if not html:
            logging.warning("Failed to fetch activities for %s", manager.name)
            self.progress.errors_encountered += 1
            return []

        # Parse first page
        activities = self.parser.parse_activities(html, manager.id)
        all_activities.extend(activities)

        # Check for pagination
        soup = BeautifulSoup(html, "html.parser")
        pages_div = soup.find("div", id="pages")
        total_pages = 1

        if pages_div and isinstance(pages_div, Tag):
            # Look for page links
            page_links = pages_div.find_all("a", href=True)
            for link in page_links:
                if isinstance(link, Tag):
                    import re

                    href = str(link.get("href", ""))
                    match = re.search(r"L=(\d+)", href)
                    if match:
                        total_pages = max(total_pages, int(match.group(1)))

        # Limit pages to fetch
        pages_to_fetch = min(total_pages, max_pages)

        # Fetch additional pages if needed
        for page_num in range(2, pages_to_fetch + 1):
            page_url = (
                f"{self.base_url}m_activity.php?m={manager.id}&typ=a&L={page_num}&o=a"
            )
            cache_key = f"managers/{manager.id}/activity_page{page_num}.html"
            html = self.http_client.get(
                page_url, use_cache=self.use_cache, cache_key=cache_key
            )

            if html:
                activities = self.parser.parse_activities(html, manager.id)
                all_activities.extend(activities)

        self.progress.activities_found += len(all_activities)

        return all_activities


    def _save_all_to_cache(
        self,
        managers: List[Manager],
        holdings: List[Holding],
        activities: List[Activity],
    ) -> None:
        """Save all data to cache.

        Args:
            managers: List of managers
            holdings: List of holdings
            activities: List of activities
        """
        self.cache_service.save_managers(managers)
        self.cache_service.save_holdings(holdings)
        self.cache_service.save_activities(activities)

        # Calculate unique stocks
        unique_stocks = len(set(h.symbol for h in holdings))

        # Save metadata
        metadata = {
            "num_managers": len(managers),
            "num_holdings": len(holdings),
            "num_activities": len(activities),
            "unique_stocks": unique_stocks,
            "progress": self.progress.to_dict(),
        }
        self.cache_service.save_metadata(metadata)

    def _load_all_from_cache(self) -> Dict[str, Any]:
        """Load all data from cache.

        Returns:
            Dictionary with cached data
        """
        return {
            "managers": self.cache_service.load_managers(),
            "holdings": self.cache_service.load_holdings(),
            "activities": self.cache_service.load_activities(),
            "progress": self.cache_service.load_metadata().get("progress", {}),
        }

    def _empty_result(self) -> Dict[str, Any]:
        """Return empty result structure.

        Returns:
            Empty result dictionary
        """
        return {
            "managers": [],
            "holdings": [],
            "activities": [],
            "progress": self.progress.to_dict(),
        }

    def close(self) -> None:
        """Clean up resources."""
        self.http_client.close()

    def __enter__(self):
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        self.close()


def main() -> None:
    """Main entry point."""
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Check if --skip-enrichment flag is passed
    skip_enrichment = "--skip-enrichment" in sys.argv

    # Use context manager for proper cleanup
    with DataromaScraper() as scraper:
        if skip_enrichment:
            # Just scrape without enrichment
            managers = scraper._scrape_managers()
            all_holdings = []
            all_activities = []

            for i, manager in enumerate(managers):
                holdings = scraper._scrape_manager_holdings(manager)
                all_holdings.extend(holdings)

                activities = scraper._scrape_manager_activities(manager)
                all_activities.extend(activities)

                scraper.progress.managers_processed += 1

                if scraper.progress.managers_processed % 5 == 0:
                    logging.info(
                        "Progress: %d/%d managers, %d holdings, %d activities",
                        scraper.progress.managers_processed,
                        len(managers),
                        len(all_holdings),
                        len(all_activities),
                    )

                # Save intermediate results
                if scraper.progress.managers_processed % 10 == 0:
                    scraper._save_all_to_cache(
                        managers[: i + 1], all_holdings, all_activities
                    )

            # Save final results
            scraper._save_all_to_cache(managers, all_holdings, all_activities)

            print("\nScraping Results (without enrichment):")
            print(f"- Managers: {len(managers)}")
            print(f"- Holdings: {len(all_holdings)}")
            print(f"- Activities: {len(all_activities)}")
        else:
            result = scraper.scrape_all()

            print("\nScraping Results:")
            print(f"- Managers: {len(result['managers'])}")
            print(f"- Holdings: {len(result['holdings'])}")
            print(f"- Activities: {len(result['activities'])}")
            print(f"- Duration: {result['progress'].get('duration_seconds', 0):.1f}s")


if __name__ == "__main__":
    main()
