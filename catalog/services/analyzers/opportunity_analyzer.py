import logging
from decimal import Decimal

from django.db.models import Avg, Max, Sum
from django.utils import timezone

from catalog.constants import (
    JobType,
    LogLevel,
    OpportunityRating,
)
from catalog.models import (
    ImportJob,
    ImportJobLog,
    CatalogItem,
    MarketOpportunity,
    Opportunity,
)

logger = logging.getLogger(__name__)


class OpportunityAnalyzer:
    """
    Compute market opportunities by comparing manufacturer CAGE codes
    (from supplier_links) vs awardee CAGE codes (from awards).

    When awardee NOT in supplier_links -> reseller win -> opportunity for Malla.
    """

    def __init__(self, stdout=None):
        self.stdout = stdout
        self.job = None

    def log(self, message, level=LogLevel.INFO, context=None):
        log_fn = {
            LogLevel.INFO: logger.info,
            LogLevel.WARNING: logger.warning,
            LogLevel.ERROR: logger.error,
        }.get(level, logger.info)
        log_fn(message)

        if self.job:
            ImportJobLog.objects.create(
                job=self.job,
                level=level,
                message=message,
                context=context or {},
            )

        if self.stdout:
            self.stdout.write(message)

    def analyze(self):
        self.job = ImportJob.objects.create(
            job_type=JobType.OPPORTUNITY_ANALYSIS
        )
        self.job.mark_running()

        try:
            catalogs = CatalogItem.objects.filter(
                supplier_count__gt=0, award_count__gt=0
            ).prefetch_related("supplier_links__organization", "awards__awardee")

            total = catalogs.count()
            self.log(f"Analyzing {total} NSN entries with suppliers and awards")
            self.job.records_fetched = total
            self.job.save(update_fields=["records_fetched"])

            for catalog in catalogs:
                try:
                    self._analyze_nsn(catalog)
                except Exception as e:
                    self.job.increment("records_errored")
                    self.log(
                        f"Error analyzing NSN {catalog.nsn}: {e}",
                        level=LogLevel.ERROR,
                        context={"nsn": catalog.nsn, "error": str(e)},
                    )

            self.job.mark_completed()
            self.log(
                f"Analysis complete: {self.job.records_created} created, "
                f"{self.job.records_updated} updated, "
                f"{self.job.records_errored} errored"
            )
        except Exception as e:
            self.job.mark_failed(str(e))
            self.log(f"Analysis failed: {e}", level=LogLevel.ERROR)
            raise

        return self.job

    def _analyze_nsn(self, catalog):
        manufacturer_cages = set(
            catalog.supplier_links.values_list("organization__cage_code", flat=True)
        )

        awardee_cages = set(
            catalog.awards.values_list("awardee__cage_code", flat=True)
        )

        reseller_cages = awardee_cages - manufacturer_cages
        has_reseller_wins = len(reseller_cages) > 0

        reseller_awards = catalog.awards.filter(
            awardee__cage_code__in=reseller_cages
        )
        total_reseller_awards = reseller_awards.count()

        stats = reseller_awards.aggregate(
            latest_date=Max("award_date"),
            avg_cost=Avg("unit_cost"),
            total_value=Sum("unit_cost"),
        )

        total_value = Decimal("0")
        for award in reseller_awards.filter(
            quantity__isnull=False, unit_cost__isnull=False
        ):
            total_value += Decimal(str(award.quantity)) * award.unit_cost

        has_active_opportunity = Opportunity.objects.filter(
            nsn=catalog.nsn, status__in=["active", ""]
        ).exists()

        rating = self._compute_rating(
            has_reseller_wins=has_reseller_wins,
            distributor_use=catalog.distributor_use,
            has_active_opportunity=has_active_opportunity,
            total_reseller_awards=total_reseller_awards,
            latest_date=stats["latest_date"],
        )

        notes = self._build_notes(
            catalog, manufacturer_cages, reseller_cages,
            total_reseller_awards, has_active_opportunity,
        )

        _, created = MarketOpportunity.objects.update_or_create(
            catalog_item=catalog,
            defaults={
                "rating": rating,
                "manufacturer_cage_codes": sorted(manufacturer_cages),
                "reseller_cage_codes": sorted(reseller_cages),
                "has_reseller_wins": has_reseller_wins,
                "distributor_use": catalog.distributor_use,
                "has_active_opportunity": has_active_opportunity,
                "total_reseller_awards": total_reseller_awards,
                "total_award_value": total_value if total_value else None,
                "latest_reseller_award_date": stats["latest_date"],
                "avg_reseller_unit_cost": stats["avg_cost"],
                "analysis_notes": notes,
            },
        )

        if created:
            self.job.increment("records_created")
        else:
            self.job.increment("records_updated")

    def _compute_rating(self, has_reseller_wins, distributor_use,
                        has_active_opportunity, total_reseller_awards, latest_date):
        score = 0
        if has_reseller_wins:
            score += 3
        if distributor_use:
            score += 2
        if has_active_opportunity:
            score += 2
        if total_reseller_awards >= 3:
            score += 2
        elif total_reseller_awards >= 1:
            score += 1

        if latest_date:
            days_ago = (timezone.now().date() - latest_date).days
            if days_ago < 365:
                score += 1

        if score >= 7:
            return OpportunityRating.HIGH
        elif score >= 4:
            return OpportunityRating.MEDIUM
        return OpportunityRating.LOW

    def _build_notes(self, catalog, manufacturer_cages, reseller_cages,
                     total_reseller_awards, has_active_opportunity):
        lines = []
        lines.append(f"NSN {catalog.nsn}: {catalog.nomenclature}")
        lines.append(f"Manufacturers: {len(manufacturer_cages)} ({', '.join(sorted(manufacturer_cages)[:5])})")

        if reseller_cages:
            lines.append(
                f"Reseller winners: {len(reseller_cages)} "
                f"({', '.join(sorted(reseller_cages)[:5])}) "
                f"with {total_reseller_awards} total awards"
            )
        else:
            lines.append("No reseller wins detected -- all awards went to OEMs.")

        if catalog.distributor_use:
            lines.append("Flagged for distributor use.")
        if has_active_opportunity:
            lines.append("Active solicitation(s) exist.")

        return "\n".join(lines)
