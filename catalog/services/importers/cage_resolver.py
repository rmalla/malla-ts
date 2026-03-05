import logging

from catalog.constants import JobType, LogLevel
from catalog.models import Organization
from .base import BaseImporter

logger = logging.getLogger(__name__)


def _get_sam_client():
    from catalog.services.sam_client import SAMGovClient
    try:
        if SAMGovClient.is_configured():
            return SAMGovClient()
    except Exception:
        pass
    return None


def resolve_cage_inline(entity, highergov_client, sam_client=None):
    """Resolve a single Organization entity in-place. Returns source name or None."""
    if entity.company_name and entity.resolved_from_api:
        return None

    if sam_client is None:
        sam_client = _get_sam_client()

    # Source 1: SAM.gov (free)
    if sam_client:
        sam_data = sam_client.lookup_cage(entity.cage_code)
        if sam_data and sam_data.get("company_name"):
            _apply_fields(entity, {
                "company_name": sam_data["company_name"][:255],
                "website": sam_data.get("website") or "",
                "address": sam_data.get("address") or "",
                "city": (sam_data.get("city") or "")[:255],
                "state": (sam_data.get("state") or "")[:100],
                "zip_code": sam_data.get("zip_code") or "",
                "country": (sam_data.get("country") or "")[:100],
                "uei": (sam_data.get("uei") or "")[:12],
            })
            return "SAM.gov"

    # Source 2: HigherGov (costs quota)
    data = highergov_client.get_awardee(entity.cage_code)
    if data:
        company_name = (
            data.get("clean_name")
            or data.get("legal_business_name")
            or data.get("company_name")
            or ""
        )[:255]
        if company_name:
            _apply_fields(entity, {
                "company_name": company_name,
                "city": (data.get("physical_city") or data.get("city") or "")[:255],
                "state": (data.get("physical_state") or data.get("state") or "")[:100],
                "country": (data.get("physical_country") or data.get("country") or "")[:100],
                "uei": (data.get("uei") or "")[:12],
            })
            return "HigherGov"

    entity.resolved_from_api = True
    entity.save(update_fields=["resolved_from_api"])
    return None


def _apply_fields(entity, info):
    entity.company_name = info["company_name"]
    entity.website = info.get("website") or entity.website
    entity.address = info.get("address") or entity.address
    entity.city = info.get("city") or entity.city
    entity.state = info.get("state") or entity.state
    entity.zip_code = info.get("zip_code") or entity.zip_code
    entity.country = info.get("country") or entity.country
    entity.uei = info.get("uei") or entity.uei
    entity.resolved_from_api = True
    entity.save(update_fields=[
        "company_name", "slug", "website", "address",
        "city", "state", "zip_code", "country", "uei",
        "resolved_from_api",
    ])


class CAGEResolver(BaseImporter):
    """Resolve CAGE codes to company names."""

    job_type = JobType.CAGE_RESOLVE

    def run(self, batch_size=50, retry_failed=False, **kwargs):
        if retry_failed:
            unresolved = Organization.objects.filter(
                resolved_from_api=True, company_name=""
            )[:batch_size]
            self.log(f"Retrying {len(unresolved)} previously-failed CAGE codes")
        else:
            unresolved = Organization.objects.filter(
                resolved_from_api=False, company_name=""
            )[:batch_size]

        count = len(unresolved)
        self.log(f"Resolving {count} CAGE codes")
        self.job.records_fetched = count
        self.job.save(update_fields=["records_fetched"])

        sam_client = self._init_sam_client()

        for entity in unresolved:
            try:
                self._resolve_cage(entity, sam_client=sam_client)
            except Exception as e:
                self.job.increment("records_errored")
                self.log(
                    f"Error resolving CAGE {entity.cage_code}: {e}",
                    level=LogLevel.ERROR,
                    context={"cage_code": entity.cage_code, "error": str(e)},
                )

        if sam_client:
            sam_client.session.close()

    def _init_sam_client(self):
        try:
            from catalog.services.sam_client import SAMGovClient
            if SAMGovClient.is_configured():
                client = SAMGovClient()
                self.log("SAM.gov fallback enabled")
                return client
        except Exception as e:
            self.log(
                f"SAM.gov fallback unavailable: {e}",
                level=LogLevel.WARNING,
            )
        return None

    def _resolve_cage(self, entity, sam_client=None):
        resolved = resolve_cage_inline(
            entity, self.client, sam_client=sam_client
        )
        if resolved:
            self.job.increment("records_updated")
            self.log(
                f"Resolved CAGE {entity.cage_code} -> {entity.company_name} (via {resolved})",
                context={
                    "cage_code": entity.cage_code,
                    "company": entity.company_name,
                    "source": resolved,
                },
            )
        else:
            self.log(
                f"CAGE {entity.cage_code}: no company name found in any source",
                level=LogLevel.WARNING,
            )
