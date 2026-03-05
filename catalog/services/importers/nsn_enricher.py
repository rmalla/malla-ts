import logging

from home.models import FederalSupplyClass
from catalog.constants import JobType, LogLevel
from catalog.models import CatalogItem, Organization, AwardHistory
from catalog.models.catalog import SupplierLink, CatalogPricing
from .base import BaseImporter
from .cage_resolver import resolve_cage_inline

logger = logging.getLogger(__name__)


class NSNEnricher(BaseImporter):
    """Enrich NSN catalog entries from HigherGov /nsn/ endpoint."""

    job_type = JobType.NSN_ENRICH

    def run(self, nsns=None, batch_size=50, **kwargs):
        if nsns is None:
            nsns = self._get_nsns_to_enrich(batch_size)

        self.log(f"Enriching {len(nsns)} NSNs")
        self.job.records_fetched = len(nsns)
        self.job.save(update_fields=["records_fetched"])

        for nsn in nsns:
            try:
                self._enrich_nsn(nsn)
            except Exception as e:
                self.job.increment("records_errored")
                self.log(
                    f"Error enriching NSN {nsn}: {e}",
                    level=LogLevel.ERROR,
                    context={"nsn": nsn, "error": str(e)},
                )

    def _get_nsns_to_enrich(self, batch_size):
        from catalog.models import Opportunity

        existing = set(CatalogItem.objects.values_list("nsn", flat=True))
        opp_nsns = (
            Opportunity.objects.exclude(nsn="")
            .values_list("nsn", flat=True)
            .distinct()
        )
        new_nsns = [n for n in opp_nsns if n and n not in existing]

        if self.filter_service:
            excluded_fsc = self.filter_service.get_excluded_fsc_codes()
            excluded_nsns = self.filter_service.get_excluded_nsns()
            before = len(new_nsns)
            new_nsns = [
                n for n in new_nsns
                if n[:4] not in excluded_fsc
                and n.replace("-", "") not in excluded_nsns
            ]
            skipped = before - len(new_nsns)
            if skipped:
                self.log(f"Pre-filtered {skipped} NSN(s) by pipeline filter rules")

        self.log(f"Found {len(new_nsns)} un-enriched NSNs from opportunity data")
        return new_nsns[:batch_size]

    def _enrich_nsn(self, nsn):
        if self.filter_service:
            result = self.filter_service.check_nsn(nsn)
            if result:
                self.log(
                    f"Skipping NSN {nsn}: {result.reason}",
                    context={"nsn": nsn, "filter": str(result.rule)},
                )
                return

        data = self.client.get_nsn(nsn)
        if not data:
            self.log(f"No data returned for NSN {nsn}", level=LogLevel.WARNING)
            return

        unit_price = self._parse_decimal(data.get("unit_price"))
        if self.filter_service and unit_price:
            result = self.filter_service.check_unit_price(unit_price)
            if result:
                self.log(
                    f"Skipping NSN {nsn} (price ${unit_price}): {result.reason}",
                    context={"nsn": nsn, "unit_price": str(unit_price), "filter": str(result.rule)},
                )
                return

        fsc = None
        fsc_code = nsn[:4] if len(nsn) >= 4 else ""
        if fsc_code:
            fsc = FederalSupplyClass.objects.filter(code=fsc_code).first()

        catalog, created = CatalogItem.objects.update_or_create(
            nsn=nsn,
            defaults={
                "nomenclature": (data.get("nomenclature") or "")[:500],
                "part_numbers": data.get("part_numbers", "") or "",
                "fsc": fsc,
                "unit_of_issue": data.get("unit_of_issue", "") or "",
                "distributor_use": bool(data.get("distributor_use", False)),
                "raw_api_response": data,
            },
        )

        # Create/update pricing
        last_price = self._parse_decimal(data.get("last_price"))
        if unit_price or last_price:
            CatalogPricing.objects.update_or_create(
                catalog_item=catalog,
                defaults={
                    "unit_price": unit_price,
                    "last_price": last_price,
                    "unit_price_source": "highergov",
                },
            )

        if created:
            self.job.increment("records_created")
        else:
            self.job.increment("records_updated")

        self._batch_resolve_cages(data)

        suppliers = data.get("suppliers") or []
        supplier_count = self._process_suppliers(catalog, suppliers)

        awards = data.get("awards") or []
        award_count = self._process_awards(catalog, awards)

        opp_count = data.get("opp_count", 0) or 0

        catalog.supplier_count = supplier_count
        catalog.award_count = award_count
        catalog.opportunity_count = opp_count
        catalog.save(update_fields=["supplier_count", "award_count", "opportunity_count"])

        self.log(
            f"Enriched NSN {nsn}: {supplier_count} suppliers, {award_count} awards",
            context={"nsn": nsn, "suppliers": supplier_count, "awards": award_count},
        )

    @property
    def _sam_client(self):
        if not hasattr(self, "_sam_client_instance"):
            from catalog.services.sam_client import SAMGovClient
            try:
                self._sam_client_instance = SAMGovClient() if SAMGovClient.is_configured() else None
            except Exception:
                self._sam_client_instance = None
        return self._sam_client_instance

    def _batch_resolve_cages(self, data):
        if not hasattr(self, "_name_cache"):
            self._name_cache = {}

        cage_codes = set()
        for s in data.get("suppliers") or []:
            cage = s.get("cage_code", "") or ""
            if cage and cage not in self._name_cache:
                cage_codes.add(cage)
        for a in data.get("awards") or []:
            cage = a.get("cage") or a.get("cage_code", "") or ""
            if cage and cage not in self._name_cache:
                cage_codes.add(cage)

        if cage_codes:
            existing = set(
                Organization.objects.filter(cage_code__in=cage_codes)
                .values_list("cage_code", flat=True)
            )
            cage_codes -= existing

        if not cage_codes or not self._sam_client:
            return

        results = self._sam_client.lookup_cages_batch(list(cage_codes))
        for cage_code, info in results.items():
            name = (info.get("company_name") or "")[:255]
            self._name_cache[cage_code] = name

        for cage_code in cage_codes:
            if cage_code not in self._name_cache:
                self._name_cache[cage_code] = None

        if results:
            self.log(f"Batch-resolved {len(results)}/{len(cage_codes)} CAGE codes via SAM.gov (free)")

    def _resolve_name_for_cage(self, cage_code):
        if not hasattr(self, "_name_cache"):
            self._name_cache = {}

        cached = self._name_cache.get(cage_code)
        if cached is not None and cached != "":
            return cached
        if cage_code in self._name_cache and self._name_cache[cage_code] == "":
            return ""

        sam_tried = cage_code in self._name_cache and self._name_cache[cage_code] is None
        if not sam_tried and self._sam_client:
            sam_data = self._sam_client.lookup_cage(cage_code)
            if sam_data and sam_data.get("company_name"):
                name = sam_data["company_name"][:255]
                self._name_cache[cage_code] = name
                return name

        data = self.client.get_awardee(cage_code)
        name = ""
        if data:
            name = (
                data.get("clean_name")
                or data.get("legal_business_name")
                or data.get("company_name")
                or ""
            )[:255]
        self._name_cache[cage_code] = name
        return name

    def _get_or_create_organization(self, cage_code, is_manufacturer=False, is_awardee=False):
        if not hasattr(self, "_blocked_cages"):
            self._blocked_cages = set()
        if cage_code in self._blocked_cages:
            return None, True

        existing = Organization.objects.filter(cage_code=cage_code).first()
        if existing:
            # Check profile status
            try:
                if existing.profile.status == Organization.DISABLED:
                    return existing, True
            except Exception:
                pass
            if not existing.company_name:
                resolve_cage_inline(existing, self.client)
                if existing.company_name:
                    self.log(f"Resolved CAGE {cage_code} -> {existing.company_name}")
            if not existing.company_name:
                return existing, True
            if self.filter_service:
                result = self.filter_service.check_manufacturer_name(existing.company_name)
                if result:
                    from catalog.models import OrganizationProfile
                    OrganizationProfile.objects.update_or_create(
                        organization=existing,
                        defaults={"status": Organization.DISABLED},
                    )
                    self.log(
                        f"Disabled org {existing.company_name} (CAGE {cage_code}): {result.reason}",
                        context={"cage_code": cage_code, "filter": str(result.rule)},
                    )
                    return existing, True
            return existing, False

        company_name = self._resolve_name_for_cage(cage_code)

        if not company_name:
            self.log(f"Skipping CAGE {cage_code}: could not resolve company name")
            self._blocked_cages.add(cage_code)
            return None, True

        if self.filter_service:
            result = self.filter_service.check_manufacturer_name(company_name)
            if result:
                self.log(
                    f"Blocked org {company_name} (CAGE {cage_code}): {result.reason}",
                    context={"cage_code": cage_code, "filter": str(result.rule)},
                )
                self._blocked_cages.add(cage_code)
                return None, True

        self.log(f"Resolved CAGE {cage_code} -> {company_name}")

        org = Organization.objects.create(
            cage_code=cage_code,
            company_name=company_name,
            is_manufacturer=is_manufacturer,
            is_awardee=is_awardee,
            resolution_status="highergov",
            resolved_from_api=True,
        )
        return org, False

    def _process_suppliers(self, catalog, suppliers):
        count = 0
        for s in suppliers:
            cage_code = s.get("cage_code", "") or ""
            if not cage_code:
                continue

            org, skip = self._get_or_create_organization(cage_code, is_manufacturer=True)
            if skip or org is None:
                continue

            SupplierLink.objects.update_or_create(
                catalog_item=catalog,
                organization=org,
                defaults={
                    "part_number": s.get("part_number", "") or "",
                    "source": "highergov",
                },
            )
            count += 1
        return count

    def _process_awards(self, catalog, awards):
        count = 0
        for a in awards:
            cage_code = a.get("cage") or a.get("cage_code", "") or ""
            contract_number = a.get("contract") or a.get("contract_number", "") or ""
            if not cage_code or not contract_number:
                continue

            org, skip = self._get_or_create_organization(cage_code, is_awardee=True)
            if skip or org is None:
                continue

            AwardHistory.objects.update_or_create(
                catalog_item=catalog,
                awardee=org,
                contract_number=contract_number,
                defaults={
                    "quantity": self._parse_int(a.get("qty") or a.get("quantity")),
                    "unit_cost": self._parse_decimal(a.get("unit_cost")),
                    "award_date": self._parse_date(a.get("date") or a.get("award_date")),
                    "surplus": bool(a.get("surplus") or False),
                    "part_number": a.get("part_number", "") or "",
                },
            )
            count += 1
        return count
