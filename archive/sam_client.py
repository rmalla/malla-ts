"""
SAM.gov API client for CAGE resolution and opportunity imports.

Free APIs — register for an API key at https://api.sam.gov/

Endpoints:
    Entity API:        https://open.gsa.gov/api/entity-api/
    Opportunities API: https://open.gsa.gov/api/get-opportunities-public-api/
"""

import logging
import time

import requests
from django.conf import settings

logger = logging.getLogger(__name__)

SAM_ENTITY_API_URL = "https://api.sam.gov/entity-information/v2/entities"
SAM_OPPORTUNITIES_API_URL = "https://api.sam.gov/opportunities/v2/search"


class SAMGovClient:
    """Query SAM.gov Entity API for CAGE code lookups."""

    MAX_RETRIES = 2
    BACKOFF_FACTOR = 2

    def __init__(self):
        self.api_key = getattr(settings, "SAM_GOV_API_KEY", "")
        if not self.api_key:
            raise ValueError(
                "SAM_GOV_API_KEY not configured. "
                "Register for a free key at https://api.sam.gov/"
            )
        self.session = requests.Session()
        self._api_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.session.close()
        return False

    @property
    def api_calls_made(self):
        return self._api_calls

    def lookup_cage(self, cage_code):
        """
        Look up a CAGE code via SAM.gov Entity API.

        Returns a dict with normalised keys:
            company_name, city, state, country, uei
        or None if not found.
        """
        params = {
            "api_key": self.api_key,
            "cageCode": cage_code,
        }

        last_error = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = self.session.get(
                    SAM_ENTITY_API_URL, params=params, timeout=30
                )
                self._api_calls += 1

                if resp.status_code == 429:
                    wait = self.BACKOFF_FACTOR ** attempt
                    logger.warning("SAM.gov rate limited, retrying in %ds", wait)
                    time.sleep(wait)
                    continue

                if resp.status_code >= 500:
                    wait = self.BACKOFF_FACTOR ** attempt
                    logger.warning("SAM.gov server error %d, retrying in %ds", resp.status_code, wait)
                    time.sleep(wait)
                    last_error = Exception(f"SAM.gov error {resp.status_code}")
                    continue

                if resp.status_code != 200:
                    logger.warning("SAM.gov API error %d: %s", resp.status_code, resp.text[:300])
                    return None

                data = resp.json()
                return self._extract_entity(data)

            except requests.exceptions.RequestException as e:
                wait = self.BACKOFF_FACTOR ** attempt
                logger.warning("SAM.gov request error: %s, retrying in %ds", e, wait)
                time.sleep(wait)
                last_error = e

        if last_error:
            logger.error("SAM.gov lookup failed after retries: %s", last_error)
        return None

    def lookup_cages_batch(self, cage_codes):
        """
        Batch-resolve up to 100 CAGE codes in a single API call.

        Returns dict: {cage_code: {company_name, city, ...}} for found entities.
        Missing CAGE codes are omitted from the result.
        """
        if not cage_codes:
            return {}

        results = {}
        # SAM.gov supports comma-separated cageCode, max ~100 per request
        for i in range(0, len(cage_codes), 100):
            batch = cage_codes[i:i + 100]
            cage_param = ",".join(batch)
            params = {
                "api_key": self.api_key,
                "cageCode": cage_param,
            }

            last_error = None
            for attempt in range(1, self.MAX_RETRIES + 1):
                try:
                    resp = self.session.get(
                        SAM_ENTITY_API_URL, params=params, timeout=30
                    )
                    self._api_calls += 1

                    if resp.status_code == 429:
                        wait = self.BACKOFF_FACTOR ** attempt
                        logger.warning("SAM.gov rate limited, retrying in %ds", wait)
                        time.sleep(wait)
                        continue

                    if resp.status_code >= 500:
                        wait = self.BACKOFF_FACTOR ** attempt
                        logger.warning("SAM.gov server error %d, retrying", resp.status_code)
                        time.sleep(wait)
                        last_error = Exception(f"SAM.gov error {resp.status_code}")
                        continue

                    if resp.status_code != 200:
                        logger.warning("SAM.gov batch error %d: %s", resp.status_code, resp.text[:300])
                        break

                    data = resp.json()
                    entities = data.get("entityData") or data.get("results") or []
                    for entity in entities:
                        info = self._extract_single_entity(entity)
                        if info and info.get("cage_code"):
                            results[info["cage_code"]] = info
                    break

                except requests.exceptions.RequestException as e:
                    wait = self.BACKOFF_FACTOR ** attempt
                    logger.warning("SAM.gov batch request error: %s", e)
                    time.sleep(wait)
                    last_error = e

            if last_error:
                logger.error("SAM.gov batch lookup failed: %s", last_error)

        return results

    def get_opportunities(self, posted_from, posted_to, ptype=None):
        """
        Fetch opportunities from SAM.gov Opportunities API (free).

        Args:
            posted_from: Start date string (MM/dd/yyyy)
            posted_to: End date string (MM/dd/yyyy)
            ptype: Procurement type filter (e.g. 'p' presol, 'o' solicitation, 'k' combined)

        Returns list of opportunity dicts.
        """
        all_results = []
        offset = 0
        limit = 1000  # max per SAM.gov docs

        while True:
            params = {
                "api_key": self.api_key,
                "postedFrom": posted_from,
                "postedTo": posted_to,
                "limit": limit,
                "offset": offset,
            }
            if ptype:
                params["ptype"] = ptype

            last_error = None
            for attempt in range(1, self.MAX_RETRIES + 1):
                try:
                    resp = self.session.get(
                        SAM_OPPORTUNITIES_API_URL, params=params, timeout=60
                    )
                    self._api_calls += 1

                    if resp.status_code == 429:
                        wait = self.BACKOFF_FACTOR ** attempt
                        logger.warning("SAM.gov opportunities rate limited, retrying in %ds", wait)
                        time.sleep(wait)
                        continue

                    if resp.status_code >= 500:
                        wait = self.BACKOFF_FACTOR ** attempt
                        logger.warning("SAM.gov opportunities error %d", resp.status_code)
                        time.sleep(wait)
                        last_error = Exception(f"SAM.gov error {resp.status_code}")
                        continue

                    if resp.status_code != 200:
                        logger.warning("SAM.gov opportunities error %d: %s", resp.status_code, resp.text[:300])
                        return all_results

                    data = resp.json()
                    opportunities = data.get("opportunitiesData") or []
                    if not opportunities:
                        return all_results

                    all_results.extend(opportunities)
                    logger.debug(
                        "SAM.gov opportunities page: got %d (total: %d)",
                        len(opportunities), len(all_results),
                    )

                    total = data.get("totalRecords", 0)
                    if len(all_results) >= total or len(opportunities) < limit:
                        return all_results

                    offset += limit
                    break  # next page

                except requests.exceptions.RequestException as e:
                    wait = self.BACKOFF_FACTOR ** attempt
                    logger.warning("SAM.gov opportunities request error: %s", e)
                    time.sleep(wait)
                    last_error = e

            if last_error:
                logger.error("SAM.gov opportunities failed: %s", last_error)
                return all_results

        return all_results

    @staticmethod
    def _extract_single_entity(entity):
        """Extract company info from a single SAM.gov entity record, including CAGE code."""
        reg = entity.get("entityRegistration", {})
        core = entity.get("coreData", {})
        entity_info = core.get("entityInformation", {})
        phys_addr = core.get("physicalAddress", {}) or core.get("mailingAddress", {})

        company_name = (
            reg.get("legalBusinessName")
            or reg.get("dbaName")
            or ""
        )

        website = entity_info.get("entityURL") or ""
        if website and not website.startswith(("http://", "https://")):
            website = f"https://{website}"

        return {
            "cage_code": reg.get("cageCode", ""),
            "company_name": company_name,
            "website": website,
            "address": phys_addr.get("addressLine1", ""),
            "city": phys_addr.get("city", ""),
            "state": phys_addr.get("stateOrProvinceCode", ""),
            "zip_code": phys_addr.get("zipCode", ""),
            "country": phys_addr.get("countryCode", ""),
            "uei": reg.get("ueiSAM", ""),
        }

    @staticmethod
    def _extract_entity(data):
        """Pull company info from SAM.gov response structure."""
        results = data.get("entityData") or []
        if not results:
            results = data.get("results") or []
        if not results:
            return None

        entity = results[0]

        reg = entity.get("entityRegistration", {})
        core = entity.get("coreData", {})
        entity_info = core.get("entityInformation", {})
        phys_addr = core.get("physicalAddress", {}) or core.get("mailingAddress", {})

        company_name = (
            reg.get("legalBusinessName")
            or reg.get("dbaName")
            or ""
        )

        website = entity_info.get("entityURL") or ""
        if website and not website.startswith(("http://", "https://")):
            website = f"https://{website}"

        return {
            "company_name": company_name,
            "website": website,
            "address": phys_addr.get("addressLine1", ""),
            "city": phys_addr.get("city", ""),
            "state": phys_addr.get("stateOrProvinceCode", ""),
            "zip_code": phys_addr.get("zipCode", ""),
            "country": phys_addr.get("countryCode", ""),
            "uei": reg.get("ueiSAM", ""),
        }

    @staticmethod
    def is_configured():
        """Check if a SAM.gov API key is available."""
        return bool(getattr(settings, "SAM_GOV_API_KEY", ""))
