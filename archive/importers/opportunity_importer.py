import logging
from datetime import date

from catalog.constants import JobType, LogLevel, SourceType
from catalog.models import Opportunity, CatalogItem
from .base import BaseImporter

logger = logging.getLogger(__name__)

# Map source_type → JobType
SOURCE_JOB_TYPE = {
    SourceType.DIBBS: JobType.DIBBS_SYNC,
    SourceType.SAM: JobType.SAM_SYNC,
    SourceType.SLED: JobType.SLED_SYNC,
}

# DIBBS-specific status values that map to generic statuses
DIBBS_STATUS_MAP = {
    "active": "active",
    "closed": "closed",
    "awarded": "awarded",
    "cancelled": "cancelled",
}


class OpportunityImporter(BaseImporter):
    """
    Generic opportunity importer parameterized by source_type.

    For SAM opportunities: uses SAM.gov API directly (free) if sam_client provided.
    For DIBBS/SLED: uses HigherGov API (costs quota).
    """

    def __init__(self, client, source_type=SourceType.DIBBS, stdout=None, sam_client=None):
        super().__init__(client, stdout=stdout)
        self.source_type = source_type
        self.job_type = SOURCE_JOB_TYPE[source_type]
        self.sam_client = sam_client

    def run(self, since=None, **kwargs):
        since_str = since or date.today().strftime("%Y-%m-%d")
        label = self.source_type.upper()

        # Use SAM.gov API (free) for SAM opportunities
        if self.source_type == SourceType.SAM and self.sam_client:
            results = self._fetch_from_sam_gov(since_str)
        else:
            self.log(f"Fetching {label} opportunities posted since {since_str}")
            results = self.client.get_opportunities(
                source_type=self.source_type, posted_since=since_str
            )

        self.job.records_fetched = len(results)
        self.job.save(update_fields=["records_fetched"])
        self.log(f"Fetched {len(results)} {label} opportunities from API")

        for item in results:
            try:
                self._process_opportunity(item)
            except Exception as e:
                self.job.increment("records_errored")
                self.log(
                    f"Error processing opportunity {item.get('opp_key', '?')}: {e}",
                    level=LogLevel.ERROR,
                    context={"opp_key": item.get("opp_key"), "error": str(e)},
                )

    def _process_opportunity(self, data):
        """Parse and upsert a single opportunity."""
        opp_key = data.get("opp_key", "")
        if not opp_key:
            self.log("Skipping opportunity with no opp_key", level=LogLevel.WARNING)
            return

        # Parse nested fields — actual API field names
        agency_dict = data.get("agency") or {}
        psc_dict = data.get("psc_code") or {}
        contact_dict = data.get("primary_contact_email") or {}

        # Agency
        if isinstance(agency_dict, dict):
            agency = agency_dict.get("agency_name", "")
            agency_code = str(agency_dict.get("agency_key", ""))
        else:
            agency = str(agency_dict)
            agency_code = ""

        # PSC code
        if isinstance(psc_dict, dict):
            psc_code = psc_dict.get("psc_code", "")
            psc_description = psc_dict.get("description", "")
        else:
            psc_code = str(psc_dict)
            psc_description = ""

        # Contact
        if isinstance(contact_dict, dict):
            contact_name = contact_dict.get("contact_name", "") or ""
            contact_email = contact_dict.get("contact_email", "") or ""
            contact_phone = contact_dict.get("contact_phone", "") or ""
        else:
            contact_name = str(contact_dict)
            contact_email = ""
            contact_phone = ""

        # NSN — comes as a list from the API
        raw_nsn = data.get("nsn", "") or ""
        if isinstance(raw_nsn, list):
            nsn = raw_nsn[0] if raw_nsn else ""
        else:
            nsn = str(raw_nsn)

        # Normalize status
        status = self._normalize_status(data)

        # Check pipeline filters — skip excluded opportunities before DB write
        if self.filter_service:
            result = self.filter_service.check_opportunity(
                psc_code=psc_code,
                nsn=nsn,
                agency_code=agency_code,
                nomenclature=data.get("title", ""),
            )
            if result:
                self.log(
                    f"Skipping opportunity {opp_key}: {result.reason}",
                    context={"opp_key": opp_key, "filter": str(result.rule)},
                )
                return

        # Link to CatalogItem if it exists
        catalog_item = None
        if nsn:
            catalog_item = CatalogItem.objects.filter(nsn=nsn).first()

        defaults = {
            "source_type": self.source_type,
            "source_id": (data.get("source_id", "") or "")[:100],
            "title": (data.get("title") or "")[:500],
            "description": data.get("description_text", "") or "",
            "nsn": nsn or "",
            "catalog_item": catalog_item,
            "status": (status or "")[:30],
            "estimated_value": self._parse_decimal(data.get("val_est_high")),
            "quantity": self._parse_int(data.get("quantity")),
            "unit_of_issue": (data.get("unit_of_issue", "") or "")[:20],
            "posted_date": self._parse_date(data.get("posted_date")),
            "response_date": self._parse_date(data.get("due_date")),
            "agency": (agency or "")[:255],
            "agency_code": (agency_code or "")[:50],
            "psc_code": (psc_code or "")[:10],
            "psc_description": (psc_description or "")[:255],
            "contact_name": (contact_name or "")[:255],
            "contact_email": (contact_email or "")[:254] if "@" in (contact_email or "") else "",
            "contact_phone": (contact_phone or "")[:50],
            "url": (data.get("source_path") or data.get("path") or "")[:500],
            "raw_api_response": data,
            # SAM/SLED fields — API returns dicts or strings depending on source
            "naics_code": self._extract_str(data.get("naics_code"), "naics_code")[:10],
            "set_aside": self._extract_str(data.get("set_aside"), "set_aside")[:100],
            "opp_type": self._extract_str(data.get("opp_type"), "description")[:100],
            "agency_type": self._extract_str(data.get("agency_type"), "agency_type")[:50],
        }

        # Add DIBBS-specific fields
        defaults.update(self._extract_source_fields(data))

        _, created = Opportunity.objects.update_or_create(
            opp_key=opp_key, defaults=defaults
        )

        if created:
            self.job.increment("records_created")
        else:
            self.job.increment("records_updated")

    def _extract_source_fields(self, data):
        """Extract source-specific fields. DIBBS has extra fields; SAM/SLED don't."""
        if self.source_type == SourceType.DIBBS:
            return {
                "dibbs_status": (data.get("dibbs_status") or "")[:30],
                "dibbs_quantity": self._parse_int(data.get("dibbs_quantity")),
                "dibbs_days_to_deliver": self._parse_int(
                    data.get("dibbs_days_to_deliver") or data.get("days_to_deliver")
                ),
            }
        return {}

    @staticmethod
    def _extract_str(value, key=None):
        """Extract a string from a value that may be a dict, string, or None."""
        if value is None:
            return ""
        if isinstance(value, dict):
            # Try the specific key first, then first non-null string value
            if key and key in value:
                return str(value[key] or "")
            for v in value.values():
                if v is not None:
                    return str(v)
            return ""
        return str(value)

    def _fetch_from_sam_gov(self, since_str):
        """Fetch opportunities from SAM.gov API (free) and map to HigherGov-compatible format.

        SAM.gov limits date range to 1 year, so we chunk requests if needed.
        """
        from datetime import datetime, timedelta

        start = datetime.strptime(since_str, "%Y-%m-%d").date()
        today = date.today()

        self.log(f"Fetching SAM opportunities from SAM.gov API (free) since {since_str}")

        # Chunk into 1-year segments
        raw_results = []
        chunk_start = start
        while chunk_start < today:
            chunk_end = min(chunk_start + timedelta(days=364), today)
            posted_from = chunk_start.strftime("%m/%d/%Y")
            posted_to = chunk_end.strftime("%m/%d/%Y")
            self.log(f"  SAM.gov chunk: {posted_from} → {posted_to}")
            chunk_results = self.sam_client.get_opportunities(posted_from, posted_to)
            raw_results.extend(chunk_results)
            chunk_start = chunk_end + timedelta(days=1)

        # Map SAM.gov response to HigherGov-compatible format
        mapped = []
        for item in raw_results:
            mapped.append({
                "opp_key": item.get("solicitationNumber") or item.get("noticeId", ""),
                "source_id": item.get("noticeId", ""),
                "title": item.get("title", ""),
                "description_text": item.get("description", ""),
                "nsn": "",  # SAM.gov opportunities don't include NSN
                "status": item.get("active", "active") if item.get("active") != "No" else "closed",
                "val_est_high": item.get("award", {}).get("amount") if isinstance(item.get("award"), dict) else None,
                "quantity": None,
                "unit_of_issue": "",
                "posted_date": item.get("postedDate", ""),
                "due_date": item.get("responseDeadLine", ""),
                "agency": {
                    "agency_name": item.get("fullParentPathName", "").split(".")[-1].strip() if item.get("fullParentPathName") else "",
                    "agency_key": item.get("fullParentPathCode", "").split(".")[-1].strip() if item.get("fullParentPathCode") else "",
                },
                "psc_code": {
                    "psc_code": item.get("classificationCode", ""),
                    "description": "",
                },
                "primary_contact_email": {
                    "contact_name": "",
                    "contact_email": item.get("pointOfContact", [{}])[0].get("email", "") if isinstance(item.get("pointOfContact"), list) and item.get("pointOfContact") else "",
                    "contact_phone": item.get("pointOfContact", [{}])[0].get("phone", "") if isinstance(item.get("pointOfContact"), list) and item.get("pointOfContact") else "",
                },
                "source_path": item.get("uiLink", ""),
                "naics_code": {"naics_code": item.get("naicsCode", "")},
                "set_aside": {"set_aside": item.get("typeOfSetAside", "")},
                "opp_type": {"description": item.get("type", "")},
                "agency_type": {"agency_type": "federal"},
            })
        return mapped

    def _normalize_status(self, data):
        """Map source-specific statuses to generic values."""
        if self.source_type == SourceType.DIBBS:
            raw = data.get("dibbs_status", "") or ""
            return DIBBS_STATUS_MAP.get(raw.lower(), raw)[:30]
        # SAM/SLED use status directly
        return (data.get("status") or "")[:30]
