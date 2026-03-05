from django.db import models

from catalog.constants import OpportunityRating, SourceType


class Opportunity(models.Model):
    """Procurement opportunity from HigherGov (DIBBS, SAM, or SLED)."""

    source_type = models.CharField(
        max_length=20,
        choices=SourceType.choices,
        default=SourceType.DIBBS,
        db_index=True,
    )
    opp_key = models.CharField(max_length=100, unique=True, db_index=True)
    source_id = models.CharField(max_length=100, blank=True, db_index=True)
    title = models.CharField(max_length=500, blank=True)
    description = models.TextField(blank=True)
    nsn = models.CharField(max_length=16, blank=True, db_index=True)
    catalog_item = models.ForeignKey(
        "catalog.CatalogItem",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="opportunities",
    )
    status = models.CharField(max_length=30, blank=True, db_index=True)
    estimated_value = models.DecimalField(
        max_digits=14, decimal_places=2, null=True, blank=True
    )
    quantity = models.PositiveIntegerField(null=True, blank=True)
    unit_of_issue = models.CharField(max_length=20, blank=True)
    posted_date = models.DateField(null=True, blank=True, db_index=True)
    response_date = models.DateField(null=True, blank=True)
    # Agency info
    agency = models.CharField(max_length=255, blank=True)
    agency_code = models.CharField(max_length=50, blank=True)
    # PSC/FSC info
    psc_code = models.CharField(max_length=10, blank=True)
    psc_description = models.CharField(max_length=255, blank=True)
    # Contact info
    contact_name = models.CharField(max_length=255, blank=True)
    contact_email = models.EmailField(blank=True)
    contact_phone = models.CharField(max_length=50, blank=True)
    # Source URL
    url = models.URLField(max_length=500, blank=True)
    raw_api_response = models.JSONField(default=dict, blank=True)
    # SAM/SLED-specific fields
    naics_code = models.CharField(max_length=10, blank=True, db_index=True)
    set_aside = models.CharField(max_length=100, blank=True)
    opp_type = models.CharField(max_length=100, blank=True)
    agency_type = models.CharField(max_length=50, blank=True)
    # DIBBS-specific fields (blank for SAM/SLED)
    dibbs_status = models.CharField(max_length=30, blank=True)
    dibbs_quantity = models.PositiveIntegerField(null=True, blank=True)
    dibbs_days_to_deliver = models.PositiveIntegerField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Opportunity"
        verbose_name_plural = "Opportunities"
        ordering = ["-posted_date"]
        indexes = [
            models.Index(
                fields=["source_type", "posted_date"],
                name="idx_opp_source_posted",
            ),
            models.Index(
                fields=["source_type", "status"],
                name="idx_opp_source_status",
            ),
        ]

    def __str__(self):
        return f"[{self.get_source_type_display()}] {self.source_id or self.opp_key} — {self.title[:60]}"


# Backward-compatibility alias
DIBBSOpportunity = Opportunity


class MarketOpportunity(models.Model):
    """Computed intelligence: NSNs where manufacturer != awardee (reseller wins)."""

    catalog_item = models.OneToOneField(
        "catalog.CatalogItem",
        on_delete=models.CASCADE,
        related_name="market_opportunity",
    )
    rating = models.CharField(
        max_length=10, choices=OpportunityRating.choices, db_index=True
    )
    manufacturer_cage_codes = models.JSONField(
        default=list, help_text="List of manufacturer CAGE codes"
    )
    reseller_cage_codes = models.JSONField(
        default=list, help_text="List of reseller CAGE codes (awardees not in manufacturers)"
    )
    has_reseller_wins = models.BooleanField(default=False, db_index=True)
    distributor_use = models.BooleanField(default=False, db_index=True)
    has_active_opportunity = models.BooleanField(default=False, db_index=True)
    total_reseller_awards = models.PositiveIntegerField(default=0)
    total_award_value = models.DecimalField(
        max_digits=14, decimal_places=2, null=True, blank=True
    )
    latest_reseller_award_date = models.DateField(null=True, blank=True)
    avg_reseller_unit_cost = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True
    )
    analysis_notes = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Market Opportunity"
        verbose_name_plural = "Market Opportunities"
        ordering = ["-total_award_value"]

    def __str__(self):
        return f"{self.catalog_item.nsn} -- {self.get_rating_display()}"

    @property
    def opportunity_score(self):
        """Compute a 0-100 score based on multiple factors."""
        score = 0
        if self.has_reseller_wins:
            score += 30
        if self.distributor_use:
            score += 25
        if self.has_active_opportunity:
            score += 20
        # Award frequency bonus (up to 15 points)
        score += min(self.total_reseller_awards * 3, 15)
        # Recency bonus (up to 10 points)
        if self.latest_reseller_award_date:
            from django.utils import timezone
            import datetime

            days_ago = (timezone.now().date() - self.latest_reseller_award_date).days
            if days_ago < 90:
                score += 10
            elif days_ago < 180:
                score += 7
            elif days_ago < 365:
                score += 4
            elif days_ago < 730:
                score += 2
        return min(score, 100)
