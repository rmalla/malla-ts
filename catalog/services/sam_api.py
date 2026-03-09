"""
SAM.gov Entity API client — fetches manufacturer website URLs by CAGE code.
"""
import logging
import re

import requests
from django.conf import settings

logger = logging.getLogger(__name__)

SAM_API_URL = "https://api.sam.gov/entity-information/v3/entities"
DEFAULT_TIMEOUT = 30


def fetch_website_by_cage(cage_code: str) -> str | None:
    """
    Look up a manufacturer's website URL via the SAM.gov Entity API.

    Returns the entityURL string (without scheme) or None if not found.
    """
    api_key = getattr(settings, "SAM_GOV_API_KEY", "")
    if not api_key:
        logger.warning("SAM_GOV_API_KEY not configured")
        return None

    try:
        resp = requests.get(
            SAM_API_URL,
            params={
                "cageCode": cage_code,
                "api_key": api_key,
                "samRegistered": "Yes",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        entities = data.get("entityData", [])
        if not entities:
            logger.info(f"No SAM.gov entity found for CAGE {cage_code}")
            return None

        entity_url = (
            entities[0]
            .get("coreData", {})
            .get("entityInformation", {})
            .get("entityURL", "")
        )
        if entity_url:
            entity_url = entity_url.strip()
            # Validate: must look like a URL, not a company name
            # Reject if it contains spaces or lacks a dot
            if " " in entity_url or "." not in entity_url:
                logger.info(
                    f"SAM.gov returned invalid URL for CAGE {cage_code}: "
                    f"{entity_url!r}"
                )
                return None
            # Reject if it looks like a company suffix, not a domain
            if re.search(
                r"\b(LLC|INC|CORP|LTD|CO)\b", entity_url, re.IGNORECASE
            ):
                logger.info(
                    f"SAM.gov returned company name as URL for CAGE "
                    f"{cage_code}: {entity_url!r}"
                )
                return None
            # Normalize: ensure it has a scheme
            if not entity_url.startswith(("http://", "https://")):
                entity_url = f"https://{entity_url}"
            return entity_url

        logger.info(f"SAM.gov entity for CAGE {cage_code} has no website URL")
        return None

    except requests.exceptions.RequestException as e:
        logger.warning(f"SAM.gov API error for CAGE {cage_code}: {e}")
        return None


def fetch_naics_by_cage(cage_code: str) -> list[dict] | None:
    """
    Look up a manufacturer's NAICS codes via the SAM.gov Entity API.

    Returns a list of dicts like:
        [{"code": "334511", "description": "...", "primary": True}, ...]
    or None if the lookup fails.
    """
    api_key = getattr(settings, "SAM_GOV_API_KEY", "")
    if not api_key:
        logger.warning("SAM_GOV_API_KEY not configured")
        return None

    try:
        resp = requests.get(
            SAM_API_URL,
            params={
                "cageCode": cage_code,
                "api_key": api_key,
                "samRegistered": "Yes",
            },
            timeout=DEFAULT_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        entities = data.get("entityData", [])
        if not entities:
            logger.info(f"No SAM.gov entity found for CAGE {cage_code}")
            return None

        naics_list = (
            entities[0]
            .get("assertions", {})
            .get("goodsAndServices", {})
            .get("naicsList", [])
        )

        results = []
        for entry in naics_list:
            code = entry.get("naicsCode", "")
            if code:
                results.append({
                    "code": str(code),
                    "description": entry.get("naicsDescription", ""),
                    "primary": entry.get("primaryNaics", False),
                })

        return results if results else None

    except requests.exceptions.RequestException as e:
        logger.warning(f"SAM.gov API error for CAGE {cage_code}: {e}")
        return None
