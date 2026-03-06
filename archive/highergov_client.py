import logging
import time

import requests
from django.conf import settings
from django.core.cache import cache

from catalog.constants import HIGHERGOV_BASE_URL, HIGHERGOV_RATE_LIMIT
from catalog.exceptions import (
    HigherGovAPIError,
    MonthlyQuotaExceeded,
    RateLimitExceeded,
)

logger = logging.getLogger(__name__)

# Redis keys for rate limiting
RATE_LIMIT_KEY = "highergov:rate_limit"
MONTHLY_COUNTER_KEY = "highergov:monthly_records:{year}_{month}"


class HigherGovClient:
    """
    HTTP client for HigherGov API with rate limiting, retry, and pagination.

    HigherGov uses `api_key` as a query param (not header auth).
    Rate limits: 10 req/sec, 10K records/month.
    """

    MAX_RETRIES = 3
    BACKOFF_FACTOR = 2  # seconds

    def __init__(self):
        self.api_key = settings.HIGHERGOV_API_KEY
        if not self.api_key:
            raise HigherGovAPIError("HIGHERGOV_API_KEY not configured in settings")
        self.base_url = HIGHERGOV_BASE_URL
        self.session = requests.Session()
        self._api_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
        return False

    @property
    def api_calls_made(self):
        return self._api_calls

    # -------------------------------------------------------------------------
    # Rate limiting
    # -------------------------------------------------------------------------

    def _check_rate_limit(self):
        """Check and enforce per-second rate limit using Redis."""
        try:
            current = cache.get(RATE_LIMIT_KEY, 0)
            if current >= HIGHERGOV_RATE_LIMIT:
                logger.debug("Rate limit reached, sleeping 1 second")
                time.sleep(1)
                cache.set(RATE_LIMIT_KEY, 1, timeout=1)
            else:
                cache.set(RATE_LIMIT_KEY, current + 1, timeout=1)
        except Exception:
            # If Redis is unavailable, use a simple sleep throttle
            time.sleep(0.15)

    def _track_monthly_records(self, count):
        """Track monthly record consumption."""
        from datetime import datetime

        now = datetime.now()
        key = MONTHLY_COUNTER_KEY.format(year=now.year, month=now.month)
        try:
            current = cache.get(key, 0)
            new_total = current + count
            # Set with 32-day timeout to auto-expire
            cache.set(key, new_total, timeout=32 * 24 * 3600)
            if new_total > 9000:
                logger.warning(
                    "Monthly API record usage: %d/10000 (%.0f%%)",
                    new_total,
                    new_total / 100,
                )
        except Exception:
            pass

    def get_monthly_usage(self):
        """Return current monthly record count."""
        from datetime import datetime

        now = datetime.now()
        key = MONTHLY_COUNTER_KEY.format(year=now.year, month=now.month)
        try:
            return cache.get(key, 0)
        except Exception:
            return -1

    # -------------------------------------------------------------------------
    # Core HTTP
    # -------------------------------------------------------------------------

    def _request(self, endpoint, params=None):
        """Make a single API request with rate limiting and retry."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        params = params or {}
        params["api_key"] = self.api_key

        last_error = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            self._check_rate_limit()
            try:
                response = self.session.get(url, params=params, timeout=30)
                self._api_calls += 1

                if response.status_code == 429:
                    wait = self.BACKOFF_FACTOR ** attempt
                    logger.warning(
                        "Rate limited (429) on %s, retrying in %ds (attempt %d/%d)",
                        endpoint, wait, attempt, self.MAX_RETRIES,
                    )
                    time.sleep(wait)
                    last_error = RateLimitExceeded(
                        "Rate limited", status_code=429
                    )
                    continue

                if response.status_code >= 500:
                    wait = self.BACKOFF_FACTOR ** attempt
                    logger.warning(
                        "Server error %d on %s, retrying in %ds (attempt %d/%d)",
                        response.status_code, endpoint, wait, attempt, self.MAX_RETRIES,
                    )
                    time.sleep(wait)
                    last_error = HigherGovAPIError(
                        f"Server error {response.status_code}",
                        status_code=response.status_code,
                    )
                    continue

                if response.status_code != 200:
                    raise HigherGovAPIError(
                        f"API error {response.status_code}: {response.text[:500]}",
                        status_code=response.status_code,
                        response_body=response.text[:2000],
                    )

                data = response.json()
                # Track record consumption
                results = data.get("results", [])
                if results:
                    self._track_monthly_records(len(results))
                return data

            except requests.exceptions.Timeout:
                wait = self.BACKOFF_FACTOR ** attempt
                logger.warning(
                    "Timeout on %s, retrying in %ds (attempt %d/%d)",
                    endpoint, wait, attempt, self.MAX_RETRIES,
                )
                time.sleep(wait)
                last_error = HigherGovAPIError(f"Timeout on {endpoint}")
                continue
            except requests.exceptions.ConnectionError as e:
                wait = self.BACKOFF_FACTOR ** attempt
                logger.warning(
                    "Connection error on %s, retrying in %ds (attempt %d/%d)",
                    endpoint, wait, attempt, self.MAX_RETRIES,
                )
                time.sleep(wait)
                last_error = HigherGovAPIError(f"Connection error: {e}")
                continue

        raise last_error or HigherGovAPIError(f"Failed after {self.MAX_RETRIES} retries")

    def _paginate(self, endpoint, params=None, max_pages=100):
        """Auto-paginate through all results."""
        params = params or {}
        all_results = []
        page = 1

        while page <= max_pages:
            params["page_number"] = page
            data = self._request(endpoint, params)

            results = data.get("results", [])
            if not results:
                break

            all_results.extend(results)
            logger.debug(
                "Page %d: got %d results (total: %d)",
                page, len(results), len(all_results),
            )

            # Check for next page in both top-level and nested links
            links = data.get("links") or {}
            if not data.get("next") and not links.get("next"):
                break
            page += 1

        return all_results

    # -------------------------------------------------------------------------
    # API Methods
    # -------------------------------------------------------------------------

    def get_opportunities(self, source_type="dibbs", posted_since=None, **extra_params):
        """Fetch opportunities by source type (dibbs, sam, or sled)."""
        params = {"source_type": source_type}
        if posted_since:
            params["posted_date"] = posted_since
        params.update(extra_params)
        return self._paginate("opportunity/", params)

    def get_dibbs_opportunities(self, posted_since=None, **extra_params):
        """Fetch DIBBS opportunities. Backward-compat wrapper."""
        return self.get_opportunities(
            source_type="dibbs", posted_since=posted_since, **extra_params
        )

    def get_nsn(self, nsn):
        """Fetch NSN catalog data (suppliers, awards, pricing)."""
        params = {"nsn": nsn}
        data = self._request("nsn/", params)
        results = data.get("results", [])
        return results[0] if results else None

    def get_awardee(self, cage_code):
        """Fetch awardee/company info by CAGE code."""
        params = {"cage_code": cage_code}
        data = self._request("awardee/", params)
        results = data.get("results", [])
        return results[0] if results else None

    def get_contract(self, contract_number):
        """Fetch contract details."""
        params = {"contract_number": contract_number}
        data = self._request("contract/", params)
        results = data.get("results", [])
        return results[0] if results else None
