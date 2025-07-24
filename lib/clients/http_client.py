"""Simplified HTTP client for web scraping."""

import logging
import time
from typing import Optional, Dict, Any
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class RateLimiter:
    """Simple rate limiter for HTTP requests."""

    def __init__(self, delay: float = 1.0):
        """Initialize rate limiter.

        Args:
            delay: Seconds to wait between requests
        """
        self.delay = delay
        self.last_request_time = 0.0

    def wait_if_needed(self) -> None:
        """Wait if necessary to respect rate limit."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request_time = time.time()


class HTTPClient:
    """HTTP client with rate limiting and retry logic."""

    def __init__(
        self,
        rate_limit: float = 1.0,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        """Initialize HTTP client.

        Args:
            rate_limit: Seconds between requests
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            backoff_factor: Backoff multiplier between retries
        """
        self.rate_limiter = RateLimiter(rate_limit)
        self.timeout = timeout

        # Create session with retry strategy
        self.session = requests.Session()

        # Configure retries
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set Chrome browser headers
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
                "Connection": "keep-alive",
            }
        )

        logging.info("HTTPClient initialized with rate limit: %.2fs", rate_limit)

    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """Make GET request with rate limiting.

        Args:
            url: URL to fetch
            params: Query parameters
            headers: Additional headers

        Returns:
            Response text or None if failed
        """
        self.rate_limiter.wait_if_needed()

        try:
            response = self.session.get(
                url, params=params, headers=headers, timeout=self.timeout
            )
            response.raise_for_status()

            logging.debug("Successfully fetched: %s", url)
            return response.text

        except requests.exceptions.Timeout:
            logging.error("Timeout fetching %s", url)
            return None

        except requests.exceptions.ConnectionError:
            logging.error("Connection error fetching %s", url)
            return None

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else "Unknown"
            logging.error("HTTP error %s fetching %s", status_code, url)
            return None

        except Exception as e:
            logging.error("Unexpected error fetching %s: %s", url, str(e))
            return None

    def post(
        self,
        url: str,
        data: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """Make POST request with rate limiting.

        Args:
            url: URL to post to
            data: Form data
            json_data: JSON data
            headers: Additional headers

        Returns:
            Response text or None if failed
        """
        self.rate_limiter.wait_if_needed()

        try:
            response = self.session.post(
                url, data=data, json=json_data, headers=headers, timeout=self.timeout
            )
            response.raise_for_status()

            logging.debug("Successfully posted to: %s", url)
            return response.text

        except requests.RequestException as e:
            logging.error("Error posting to %s: %s", url, str(e))
            return None

    def get_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Make GET request and parse JSON response.

        Args:
            url: URL to fetch
            params: Query parameters
            headers: Additional headers

        Returns:
            Parsed JSON or None if failed
        """
        self.rate_limiter.wait_if_needed()

        try:
            response = self.session.get(
                url, params=params, headers=headers, timeout=self.timeout
            )
            response.raise_for_status()

            return response.json()

        except requests.RequestException as e:
            logging.error("Error fetching JSON from %s: %s", url, str(e))
            return None

        except ValueError as e:
            logging.error("Invalid JSON from %s: %s", url, str(e))
            return None

    def close(self) -> None:
        """Close the session."""
        self.session.close()

    def __enter__(self):
        """Context manager support."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        self.close()


class CachedHTTPClient(HTTPClient):
    """HTTP client with caching support."""

    def __init__(self, cache_dir: str = "cache/html", cache_ttl: int = 86400, **kwargs):
        """Initialize cached HTTP client.

        Args:
            cache_dir: Directory for cache files
            cache_ttl: Cache time-to-live in seconds
            **kwargs: Arguments for HTTPClient
        """
        super().__init__(**kwargs)
        self.cache_dir = Path(cache_dir)
        self.cache_ttl = cache_ttl

        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _get_cache_path(self, url: str, cache_key: Optional[str] = None) -> Path:
        """Get cache file path for URL.

        Args:
            url: URL being cached
            cache_key: Optional specific cache key/path

        Returns:
            Path object for cache file
        """
        if cache_key:
            # Use provided cache key (supports subdirectories)
            cache_path = self.cache_dir / cache_key
            # Ensure parent directory exists
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            return cache_path
        else:
            # Create safe filename from URL
            safe_name = url.replace("/", "_").replace(":", "").replace("?", "_")
            return self.cache_dir / f"{safe_name}.html"

    def _is_cache_valid(self, cache_path: Path) -> bool:
        """Check if cache file is still valid."""
        if not cache_path.exists():
            return False

        age = time.time() - cache_path.stat().st_mtime
        return age < self.cache_ttl

    def get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        use_cache: bool = True,
        cache_key: Optional[str] = None,
    ) -> Optional[str]:
        """Get with caching support.

        Args:
            url: URL to fetch
            params: Query parameters
            headers: Additional headers
            use_cache: Whether to use cache
            cache_key: Optional specific cache key/path

        Returns:
            Response text or None if failed
        """
        if use_cache:
            cache_path = self._get_cache_path(url, cache_key)

            # Check cache
            if self._is_cache_valid(cache_path):
                logging.debug("Using cached response for: %s", url)
                return cache_path.read_text(encoding="utf-8")

        # Fetch fresh
        response = super().get(url, params, headers)

        # Cache if successful
        if response and use_cache:
            cache_path = self._get_cache_path(url, cache_key)
            cache_path.write_text(response, encoding="utf-8")
            logging.debug("Cached response for: %s", url)

        return response
