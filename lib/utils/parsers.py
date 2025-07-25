#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dataroma Investment Analyzer - Parsers

HTML parsing utilities for extracting data from Dataroma pages.
Parser for Dataroma HTML pages with robust error handling.

MIT License
Copyright (c) 2020-present Jerzy 'Yuri' Kramarz
See LICENSE file for full license text.

Author: Jerzy 'Yuri' Kramarz
Source: https://github.com/op7ic/Dataroma-Analyzer
"""

import logging
import re
from typing import List, cast, Optional
from bs4 import BeautifulSoup, Tag

from ..models import Manager, Holding, Activity


class DataromaParser:
    """Parser for Dataroma HTML pages."""

    def __init__(self):
        """Initialize parser."""
        self.base_url = "https://www.dataroma.com/m/"

    def parse_managers_list(self, html: str) -> List[Manager]:
        """Parse managers from the main page.

        Args:
            html: HTML content of managers page

        Returns:
            List of Manager objects
        """
        managers = []
        soup = BeautifulSoup(html, "html.parser")
        seen_ids = set()

        # Find all manager links - they follow the pattern /m/holdings.php?m=
        for link in soup.find_all("a", href=True):
            if isinstance(link, Tag):
                href = str(link.get("href", ""))
                if "/m/holdings.php?m=" in href:
                    manager_id = href.split("m=")[-1]
                    manager_text = link.text.strip()

                    # Skip if already seen
                    if manager_id in seen_ids:
                        continue

                    seen_ids.add(manager_id)
                    
                    # Extract manager info
                    parts = manager_text.split(" - ", 1)
                    if len(parts) == 2:
                        manager_name = parts[0].strip()
                        company_name = parts[1].strip()
                    else:
                        # Handle cases where name format is different
                        manager_name = manager_text
                        company_name = ""

                    managers.append(
                        Manager(
                            id=manager_id,
                            name=manager_name,
                            firm=company_name,
                            portfolio_value=0,  # Not available on list page
                        )
                    )

        logging.info(f"Parsed {len(managers)} managers from page")
        return managers

    def parse_holdings(self, html: str, manager_id: str) -> List[Holding]:
        """Parse holdings from a manager's holdings page.

        Args:
            html: HTML content of holdings page
            manager_id: Manager ID for the holdings

        Returns:
            List of Holding objects
        """
        holdings = []
        soup = BeautifulSoup(html, "html.parser")

        # Try to find the main data table
        # Dataroma uses a table with id="grid"
        table = soup.find("table", id="grid")

        if not table:
            # Fallback to finding any table with relevant headers
            tables = soup.find_all("table")
            for t in tables:
                if isinstance(t, Tag):
                    headers = t.find_all("th") or t.find_all("td")
                    header_text = " ".join([h.text.lower() for h in headers[:10] if isinstance(h, Tag)])
                    if any(
                        word in header_text
                        for word in ["stock", "shares", "value", "portfolio"]
                    ):
                        table = t
                        break

        if table and isinstance(table, Tag):
            # Find tbody or use table directly
            tbody = table.find("tbody")
            if tbody and isinstance(tbody, Tag):
                rows = tbody.find_all("tr")
            else:
                all_rows = table.find_all("tr")
                rows = all_rows[1:] if all_rows else []  # Skip header row

            for row in rows:
                if not isinstance(row, Tag):
                    continue
                cells = row.find_all("td")

                if len(cells) >= 12:  # Need all columns including 52-week data
                    try:
                        # Column 0: History link (skip)
                        # Column 1: Stock (symbol + company name)
                        stock_cell = cells[1]
                        if isinstance(stock_cell, Tag):
                            stock_link = stock_cell.find("a")

                            if not stock_link or not isinstance(stock_link, Tag):
                                continue

                            # Extract symbol from link text (before the span)
                            symbol_text = stock_link.text.strip()
                            symbol = symbol_text.split()[0] if symbol_text else ""
                            
                            # Extract company name from span within the same cell
                            company_span = stock_link.find("span") if isinstance(stock_link, Tag) else None
                            if company_span and isinstance(company_span, Tag):
                                company = company_span.text.strip().lstrip("- ").strip()
                            else:
                                # Fallback: extract from full text after symbol
                                full_text = stock_link.text.strip()
                                if " - " in full_text:
                                    company = full_text.split(" - ", 1)[1].strip()
                                else:
                                    company = symbol  # Use symbol as fallback

                            # Column 2: % of portfolio
                            portfolio_pct = self._parse_percentage(cells[2].text)

                            # Column 3: Recent activity (text)
                            recent_activity = cells[3].text.strip() if hasattr(cells[3], 'text') else ""

                            # Column 4: Shares
                            shares = self._parse_number(cells[4].text)

                            # Column 5: Reported price
                            reported_price = self._parse_currency(cells[5].text)

                            # Column 6: Value
                            value = self._parse_number(cells[6].text.replace("$", "").replace(",", ""))

                            # Column 8: Current price (skip column 7 which is gap)
                            current_price = self._parse_currency(cells[8].text) if len(cells) > 8 else 0.0

                            # Column 10: 52-week low
                            week_52_low = self._parse_currency(cells[10].text)

                            # Column 11: 52-week high  
                            week_52_high = self._parse_currency(cells[11].text)

                            holding = Holding(
                                symbol=symbol,
                                company_name=company,
                                manager_id=manager_id,
                                shares=shares,
                                value=value,
                                percentage=portfolio_pct,
                                reported_price=reported_price,
                                current_price=current_price,
                                week_52_low=week_52_low,
                                week_52_high=week_52_high,
                                recent_activity=recent_activity,
                            )
                            
                            holdings.append(holding)

                    except (IndexError, AttributeError, ValueError) as e:
                        logging.warning(
                            f"Failed to parse holding row for {manager_id}: {e}"
                        )
                        continue

        logging.info(f"Parsed {len(holdings)} holdings for manager {manager_id}")
        return holdings

    def parse_activities(self, html: str, manager_id: str) -> List[Activity]:
        """Parse activities with FIXED temporal data extraction.
        
        Args:
            html: HTML content of activity page
            manager_id: Manager ID for the activities
            
        Returns:
            List of Activity objects with proper quarters and timestamps
        """
        activities = []
        soup = BeautifulSoup(html, "html.parser")
        
        # Find the activity table
        table = soup.find("table", id="grid")
        if not table or not isinstance(table, Tag):
            logging.warning(f"No activity table found for manager {manager_id}")
            return activities
        
        tbody = table.find("tbody")
        if not tbody or not isinstance(tbody, Tag):
            logging.warning(f"No tbody found in activity table for manager {manager_id}")
            return activities
        
        current_quarter = ""
        current_year = ""
        
        # Process all elements in tbody
        elements = list(tbody.children)
        i = 0
        
        while i < len(elements):
            element = elements[i]
            
            # Skip text nodes and non-Tag elements
            if not isinstance(element, Tag):
                i += 1
                continue
                
            # Check for quarter header row
            if element.name == "tr" and ("q_chg" in element.get("class", [])):
                # Extract quarter information
                quarter_text = element.text.strip()
                quarter_match = re.search(r"(Q[1-4])\s+(\d{4})", quarter_text)
                if quarter_match:
                    current_quarter = quarter_match.group(1)
                    current_year = quarter_match.group(2)
                    logging.debug(f"Found quarter: {current_quarter} {current_year}")
                i += 1
                continue
            
            # Look for activity data - need to handle malformed HTML
            if element.name == "td":
                # Try to collect 5 consecutive td elements for one activity row
                activity_cells = []
                j = i
                
                # Collect up to 5 td elements
                while j < len(elements) and len(activity_cells) < 5:
                    elem = elements[j]
                    if isinstance(elem, Tag) and elem.name == "td":
                        activity_cells.append(elem)
                    j += 1
                
                if len(activity_cells) >= 5:
                    try:
                        # Parse the activity data from collected cells
                        activity = self._parse_activity_cells(
                            activity_cells, manager_id, current_quarter, current_year
                        )
                        if activity:
                            activities.append(activity)
                    except Exception as e:
                        logging.warning(f"Failed to parse activity cells: {e}")
                
                # Move to next potential activity (skip processed cells)
                i = j
            else:
                i += 1
        
        logging.info(f"Parsed {len(activities)} activities for manager {manager_id}")
        return activities
    
    def _parse_activity_cells(
        self, 
        cells: List[Tag], 
        manager_id: str, 
        quarter: str, 
        year: str
    ) -> Optional[Activity]:
        """Parse activity from 5 table cells.
        
        Expected cell structure:
        0: History link (≡ symbol)
        1: Stock symbol and company name
        2: Action (Buy, Sell, Add X%, Reduce X%)
        3: Share change (number)
        4: Portfolio percentage (number)
        """
        if len(cells) < 5:
            return None
        
        try:
            # Cell 1: Stock symbol and company name
            stock_cell = cells[1]
            symbol = ""
            company_name = ""
            
            if isinstance(stock_cell, Tag):
                stock_link = stock_cell.find("a")
                if stock_link and isinstance(stock_link, Tag):
                    # Extract symbol from href or text
                    href = stock_link.get("href", "")
                    if "sym=" in href:
                        symbol = href.split("sym=")[-1]
                    else:
                        symbol = stock_link.text.strip()
                    
                    # Extract company name from span
                    span = stock_link.find("span")
                    if span and isinstance(span, Tag):
                        company_name = span.text.strip().lstrip("- ").strip()
                
                if not symbol:  # Fallback
                    symbol = stock_cell.text.strip()
            
            # Cell 2: Action
            action_cell = cells[2]
            action = action_cell.text.strip() if isinstance(action_cell, Tag) else ""
            
            # Cell 3: Share change
            shares_cell = cells[3]
            shares = self._parse_number(shares_cell.text) if isinstance(shares_cell, Tag) else 0
            
            # Cell 4: Portfolio percentage
            pct_cell = cells[4]
            portfolio_pct = self._parse_percentage(pct_cell.text) if isinstance(pct_cell, Tag) else 0.0
            
            # Build proper date string
            period = f"{quarter} {year}" if quarter and year else "Unknown"
            
            # Determine action type
            action_type = self._extract_action_type(action)
            
            return Activity(
                date=period,
                action=action,
                symbol=symbol,
                shares=shares,
                portfolio_percentage=portfolio_pct,
                manager_id=manager_id,
                action_type=action_type,
                company_name=company_name,
            )
            
        except Exception as e:
            logging.error(f"Error parsing activity cells: {e}")
            return None
    
    def _extract_action_type(self, action: str) -> str:
        """Extract standardized action type from action text."""
        if not action:
            return ""
            
        action_lower = action.lower()
        
        if "buy" in action_lower:
            return "Buy"
        elif "add" in action_lower:
            return "Add"  
        elif "reduce" in action_lower:
            return "Reduce"
        elif "sell" in action_lower or "sold" in action_lower:
            return "Sell"
        else:
            return "Hold"

    def parse_holdings_with_dates(self, html: str, manager_id: str) -> List[Holding]:
        """Parse holdings with proper reporting dates.
        
        Args:
            html: HTML content of holdings page
            manager_id: Manager ID for the holdings
            
        Returns:
            List of Holding objects with reporting dates
        """
        holdings = []
        soup = BeautifulSoup(html, "html.parser")
        
        # Extract reporting date from page
        reporting_date = self._extract_reporting_date(soup)
        current_quarter = self._extract_current_quarter()
        
        # Find the main holdings table
        table = soup.find("table", id="grid")
        if not table or not isinstance(table, Tag):
            return holdings
        
        tbody = table.find("tbody") or table
        if not isinstance(tbody, Tag):
            return holdings
        
        rows = tbody.find_all("tr")
        
        for row in rows:
            if not isinstance(row, Tag):
                continue
                
            cells = row.find_all("td")
            if len(cells) < 8:  # Need minimum columns
                continue
                
            try:
                holding = self._parse_holding_row(cells, manager_id, reporting_date, current_quarter)
                if holding:
                    holdings.append(holding)
            except Exception as e:
                logging.warning(f"Failed to parse holding row: {e}")
                continue
        
        logging.info(f"Parsed {len(holdings)} holdings for manager {manager_id}")
        return holdings
    
    def _extract_reporting_date(self, soup) -> str:
        """Extract reporting date from page.""" 
        # Look for date information in various places
        date_patterns = [
            r'Updated (\w+ \d+, \d{4})',
            r'As of (\w+ \d+, \d{4})', 
            r'(\w+ \d+, \d{4})',
        ]
        
        page_text = soup.text
        for pattern in date_patterns:
            match = re.search(pattern, page_text)
            if match:
                return match.group(1)
        
        return ""
    
    def _extract_current_quarter(self) -> str:
        """Extract current quarter based on date."""
        from datetime import datetime
        
        now = datetime.now()
        quarter = (now.month - 1) // 3 + 1
        return f"Q{quarter} {now.year}"
    
    def _parse_holding_row(self, cells: List[Tag], manager_id: str, reporting_date: str, quarter: str) -> Optional[Holding]:
        """Parse individual holding row with proper date information."""
        # Need at least 9 cells if history cell is present, 8 otherwise
        if len(cells) < 8:
            return None
            
        try:
            # Check if first cell is history link
            has_history_cell = cells[0].get("class") == ["hist"]
            
            # Stock info is in cell 1 if history cell exists, cell 0 otherwise
            stock_cell_idx = 1 if has_history_cell else 0
            stock_cell = cells[stock_cell_idx]
            symbol, company_name = self._extract_stock_info(stock_cell)
            
            if not symbol:
                return None
            
            # Parse other fields
            # When history cell exists: portfolio% is at 2, activity at 3, shares at 4, etc.
            # When no history cell: portfolio% is at 1, activity at 2, shares at 3, etc.
            base_idx = 1 if has_history_cell else 0
            
            portfolio_pct = self._parse_percentage(cells[base_idx + 1].text)
            recent_activity = cells[base_idx + 2].text.strip()
            shares = self._parse_number(cells[base_idx + 3].text)
            reported_price = self._parse_currency(cells[base_idx + 4].text)
            value = self._parse_number(cells[base_idx + 5].text.replace("$", "").replace(",", ""))
            # Skip gap cell at base_idx + 6
            current_price = self._parse_currency(cells[base_idx + 7].text) if len(cells) > base_idx + 7 else 0.0
            
            # Extract 52-week data if available
            week_52_low = 0.0
            week_52_high = 0.0
            # Price change % is at base_idx + 8, 52w low at base_idx + 9, 52w high at base_idx + 10
            if len(cells) > base_idx + 10:
                week_52_low = self._parse_currency(cells[base_idx + 9].text)  
                week_52_high = self._parse_currency(cells[base_idx + 10].text)
            
            return Holding(
                symbol=symbol,
                company_name=company_name,
                manager_id=manager_id,
                shares=shares,
                value=value,
                percentage=portfolio_pct,
                portfolio_percent=portfolio_pct,
                reported_price=reported_price,
                current_price=current_price, 
                recent_activity=recent_activity,
                week_52_low=week_52_low,
                week_52_high=week_52_high,
                reporting_date=reporting_date,
                reporting_quarter=quarter,
            )
            
        except Exception as e:
            logging.error(f"Error parsing holding row: {e}")
            return None
    
    def _extract_stock_info(self, cell: Tag) -> tuple[str, str]:
        """Extract symbol and company name from stock cell."""
        symbol = ""
        company_name = ""
        
        # Look for link with stock symbol
        stock_link = cell.find("a")
        if stock_link and isinstance(stock_link, Tag):
            # Try to get symbol from href
            href = stock_link.get("href", "")
            if "sym=" in href:
                symbol = href.split("sym=")[-1]
            elif "stock.php" in href:
                # Extract from URL path
                parts = href.split("/")[-1].split("?")[0]
                symbol = parts.replace("stock.php", "").strip()
            
            # Get company name from span or title
            span = stock_link.find("span")
            if span and isinstance(span, Tag):
                company_name = span.text.strip().lstrip("- ").strip()
            
            # Fallback: use link text as symbol
            if not symbol:
                symbol = stock_link.text.strip()
        else:
            # No link, use cell text
            text = cell.text.strip()
            if " - " in text:
                parts = text.split(" - ", 1)
                symbol = parts[0].strip()
                company_name = parts[1].strip()
            else:
                symbol = text
        
        return symbol, company_name

    def _parse_number(self, text: str) -> int:
        """Parse number from text, handling commas."""
        if not text:
            return 0
            
        # Remove commas and whitespace
        clean_text = re.sub(r'[,\s]', '', str(text))
        
        # Extract number
        match = re.search(r'\d+', clean_text)
        if match:
            return int(match.group())
        
        return 0

    def _parse_percentage(self, text: str) -> float:
        """Parse percentage from text."""
        if not text:
            return 0.0
            
        # Remove % symbol and whitespace
        clean_text = str(text).replace('%', '').strip()
        
        try:
            return float(clean_text)
        except ValueError:
            return 0.0

    def _parse_currency(self, text: str) -> float:
        """Parse currency from text."""
        if not text:
            return 0.0
            
        # Remove $ symbol, commas, and whitespace
        clean_text = re.sub(r'[\$,\s]', '', str(text))
        
        try:
            return float(clean_text)
        except ValueError:
            return 0.0