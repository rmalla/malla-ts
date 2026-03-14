import re

from django.db import models
from django.db.models import Q
from wagtail.images.models import Image

from catalog.services.name_formatter import format_manufacturer_name


# ── Organization slug generation ────────────────────────────────────────────

_MAX_SLUG_LEN = 60

_NOISE_WORDS = frozenset({
    # Legal / corporate suffixes only — keep industry terms for SEO
    "inc", "llc", "corp", "corporation", "co", "ltd", "company",
    "incorporated", "limited", "lp", "plc", "gmbh", "sa", "ag",
    # Common filler
    "the", "of", "and", "for",
})


def slugify_manufacturer(name, cage_code):
    """Build an SEO-friendly URL slug from a manufacturer name."""
    company_name = name
    if not company_name or not company_name.strip():
        if cage_code:
            return f"mfr-{cage_code.lower()}"
        return ""

    name = company_name.strip().lower()
    name = name.replace("&", "-and-")
    tokens = re.split(r"[^a-z0-9]+", name)
    tokens = [t for t in tokens if t]

    significant = [t for t in tokens if t not in _NOISE_WORDS]

    if not significant:
        significant = [t for t in tokens if t]

    parts = []
    length = 0
    for word in significant:
        added_len = len(word) + (1 if parts else 0)
        if length + added_len > _MAX_SLUG_LEN and parts:
            break
        parts.append(word)
        length += added_len

    slug = "-".join(parts)
    if not slug and cage_code:
        return f"mfr-{cage_code.lower()}"
    return slug


# ── Models ──────────────────────────────────────────────────────────────────

class Manufacturer(models.Model):
    """Company/vendor identified by CAGE code, with optional marketing fields.

    Records with a cage_code represent government-registered entities.
    Records with cage_code=NULL are showcase-only brands (e.g. Caterpillar).
    """

    DISABLED = -1
    NEUTRAL = 0
    ENABLED = 1
    STATUS_CHOICES = [
        (DISABLED, "Disabled"),
        (NEUTRAL, "Neutral"),
        (ENABLED, "Enabled"),
    ]

    RESOLUTION_CHOICES = [
        ("unresolved", "Unresolved"),
        ("sam_gov", "SAM.gov"),
        ("highergov", "HigherGov"),
        ("cage_file", "CAGE File"),
        ("manual", "Manual"),
    ]

    # Identification
    cage_code = models.CharField(
        max_length=5, null=True, blank=True, unique=True,
    )
    company_name = models.CharField(max_length=255, blank=True)
    slug = models.SlugField(
        max_length=80, unique=True, blank=True, default="",
        help_text="URL-friendly alias, auto-generated from company name",
    )
    uei = models.CharField(max_length=12, blank=True, verbose_name="Unique Entity Identifier")
    website = models.URLField(max_length=500, blank=True)

    # Location
    address = models.CharField(max_length=500, blank=True)
    city = models.CharField(max_length=255, blank=True)
    state = models.CharField(max_length=100, blank=True)
    zip_code = models.CharField(max_length=20, blank=True)
    country = models.CharField(max_length=100, blank=True)

    # Role classification (-1 = No, 0 = Neutral/unverified, 1 = Yes)
    ROLE_NO = -1
    ROLE_NEUTRAL = 0
    ROLE_YES = 1
    ROLE_CHOICES = [
        (ROLE_NO, "No"),
        (ROLE_NEUTRAL, "Neutral"),
        (ROLE_YES, "Yes"),
    ]

    is_manufacturer = models.SmallIntegerField(
        choices=ROLE_CHOICES, default=ROLE_NEUTRAL,
        help_text="Verified manufacturer? -1=No, 0=Unverified, 1=Yes",
    )

    # Resolution tracking
    resolution_status = models.CharField(
        max_length=20, choices=RESOLUTION_CHOICES,
        default="unresolved",
    )
    resolution_source = models.CharField(max_length=30, blank=True)
    resolved_at = models.DateTimeField(null=True, blank=True)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "catalog_organization"
        verbose_name = "Manufacturer"
        verbose_name_plural = "Manufacturers"
        ordering = ["company_name"]
        constraints = [
            models.CheckConstraint(
                condition=Q(is_manufacturer__in=[-1, 0, 1]),
                name="catalog_organization_is_manufacturer_valid",
            ),
        ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._original_company_name = self.company_name
        self._original_status = getattr(self, 'status', None)

    def __str__(self):
        if self.company_name:
            if self.cage_code:
                return f"{self.cage_code} - {self.company_name}"
            return self.company_name
        return self.cage_code or "(unnamed)"

    @property
    def display_name(self):
        """Best available name for display."""
        try:
            profile = self.profile
            if profile.display_name:
                return profile.display_name
        except ManufacturerProfile.DoesNotExist:
            pass
        return self.company_name or self.cage_code or "(unnamed)"

    @property
    def status(self):
        """Status from profile, defaulting to NEUTRAL."""
        try:
            return self.profile.status
        except ManufacturerProfile.DoesNotExist:
            return self.NEUTRAL

    def save(self, *args, **kwargs):
        # Normalize empty cage_code to None
        if not self.cage_code:
            self.cage_code = None

        # Auto-generate slug only when missing (new records); never overwrite
        # existing slugs — they determine SEO URLs.
        if not self.slug:
            slug_name = format_manufacturer_name(self.company_name) or self.company_name
            base = slugify_manufacturer(slug_name, self.cage_code)
            if not base:
                super().save(*args, **kwargs)
                self.slug = f"mfr-{self.pk}"
                super().save(update_fields=["slug"])
                self._original_company_name = self.company_name
                return
            candidate = base
            qs = Manufacturer.objects.exclude(pk=self.pk)
            if qs.filter(slug=candidate).exists():
                # Append city for geographic disambiguation
                if self.city:
                    city_slug = re.sub(r'[^a-z0-9]+', '-', self.city.strip().lower()).strip('-')
                    if city_slug:
                        candidate = f"{base}-{city_slug}"
                # Fall back to cage code
                if qs.filter(slug=candidate).exists() and self.cage_code:
                    candidate = f"{base}-{self.cage_code.lower()}"
                # Last resort: pk
                if qs.filter(slug=candidate).exists():
                    if not self.pk:
                        super().save(*args, **kwargs)
                    candidate = f"{base}-{self.pk}"
            self.slug = candidate

        super().save(*args, **kwargs)
        self._original_company_name = self.company_name


# ── ManufacturerProfile ────────────────────────────────────────────────────

class ManufacturerProfile(models.Model):
    """Display/marketing data for a manufacturer."""

    organization = models.OneToOneField(
        Manufacturer, on_delete=models.CASCADE, related_name="profile"
    )
    display_name = models.CharField(max_length=255, blank=True)
    description = models.TextField(blank=True)
    logo = models.ForeignKey(
        Image, null=True, blank=True, on_delete=models.SET_NULL, related_name="+",
    )
    display_order = models.IntegerField(default=100)
    status = models.SmallIntegerField(
        choices=[
            (Manufacturer.DISABLED, "Disabled"),
            (Manufacturer.NEUTRAL, "Neutral"),
            (Manufacturer.ENABLED, "Enabled"),
        ],
        default=Manufacturer.NEUTRAL,
    )
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "catalog_organizationprofile"
        verbose_name = "Manufacturer Profile"
        verbose_name_plural = "Manufacturer Profiles"
        ordering = ["display_order", "organization__company_name"]
        constraints = [
            models.CheckConstraint(
                condition=Q(status__in=[-1, 0, 1]),
                name="catalog_organizationprofile_status_valid",
            ),
        ]

    def __str__(self):
        return f"Profile for {self.organization}"
