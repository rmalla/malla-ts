import re

from django.db import models
from django.contrib.postgres.indexes import GinIndex
from django.contrib.postgres.search import SearchVectorField


def slugify_part_number(part_number):
    """Create a clean URL slug from a part number."""
    if not part_number or not part_number.strip():
        return ''
    slug = part_number.strip().lower()
    slug = slug.replace('/', '-')
    slug = slug.replace('.', '-')
    slug = re.sub(r'[^a-z0-9-]', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    slug = slug.strip('-')
    return slug


class DataSource(models.TextChoices):
    PUBLOG = "publog", "PUB LOG"
    FLIS_HISTORY = "flis_history", "FLIS History"
    FLISV = "flisv", "FLISV"
    HIGHERGOV = "highergov", "HigherGov"
    SAM_GOV = "sam_gov", "SAM.gov"
    FOIA = "foia", "FOIA"
    MANUAL = "manual", "Manual"
    COMMERCIAL = "commercial", "Commercial"


# ── Product ────────────────────────────────────────────────────────────────

class ProductQuerySet(models.QuerySet):
    def published(self):
        """Products visible on the frontend.

        Visibility rules:
        - Product ENABLED (1)  → always shown (overrides manufacturer)
        - Product DISABLED (-1) → never shown (overrides manufacturer)
        - Product NEUTRAL (0)  → shown only if manufacturer is ENABLED
        """
        from catalog.models.entities import Manufacturer
        return self.exclude(is_active=-1).filter(
            models.Q(is_active=1)
            | models.Q(is_active=0, manufacturer__profile__status=Manufacturer.ENABLED)
        )


class Product(models.Model):
    """A product listing on the website. The primary public-facing entity.
    Identified by manufacturer + part_number.
    URL: /products/<manufacturer.slug>/<part_number_slug>/
    """

    # Identity
    manufacturer = models.ForeignKey(
        "catalog.Manufacturer", on_delete=models.CASCADE, related_name="products"
    )
    part_number = models.CharField(max_length=200, blank=True)
    part_number_slug = models.SlugField(
        max_length=220, blank=True, db_index=True,
        help_text="URL-friendly part number, auto-generated",
    )

    # Display
    name = models.CharField(
        max_length=500, blank=True,
        help_text="Title-cased nomenclature (auto-generated from import)",
    )
    display_name = models.CharField(
        max_length=500, blank=True,
        help_text="Natural-language product name (auto-generated, manually overridable)",
    )
    description = models.TextField(blank=True)

    # Denormalized from CatalogItem/CatalogPricing
    price = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True, db_index=True)
    nsn = models.CharField(max_length=300, blank=True, db_index=True)
    nomenclature = models.CharField(max_length=500, blank=True)
    fsc = models.ForeignKey(
        "home.FederalSupplyClass", on_delete=models.SET_NULL, null=True, blank=True,
    )
    unit_of_issue = models.CharField(max_length=20, blank=True)

    # Source tracking
    source = models.CharField(
        max_length=30, choices=DataSource.choices, default=DataSource.PUBLOG,
        help_text="Where this product record came from",
    )

    # Visibility (-1=Disabled, 0=Neutral/unreviewed, 1=Enabled)
    DISABLED = -1
    NEUTRAL = 0
    ENABLED = 1
    STATUS_CHOICES = [
        (DISABLED, "Disabled"),
        (NEUTRAL, "Neutral"),
        (ENABLED, "Enabled"),
    ]
    is_active = models.SmallIntegerField(
        choices=STATUS_CHOICES, default=NEUTRAL, db_index=True,
    )

    objects = ProductQuerySet.as_manager()

    # Search
    search_vector = SearchVectorField(null=True, blank=True)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Product"
        verbose_name_plural = "Products"
        unique_together = [("manufacturer", "part_number")]
        indexes = [
            models.Index(fields=["manufacturer", "part_number_slug"]),
            GinIndex(fields=["search_vector"]),
        ]

    def __str__(self):
        return self.get_display_name()

    def save(self, *args, **kwargs):
        self.part_number_slug = slugify_part_number(self.part_number)
        # Auto-populate display_name from nomenclature if not manually set
        if not self.display_name and self.nomenclature:
            from catalog.services.name_formatter import naturalize_nomenclature
            self.display_name = naturalize_nomenclature(self.nomenclature)
        super().save(*args, **kwargs)

    def get_display_name(self):
        """Fallback chain: display_name field → name → nomenclature → part_number."""
        return self.display_name or self.name or self.nomenclature or self.part_number or "(unnamed)"


# ── ProductSpecification ───────────────────────────────────────────────────

class ProductSpecification(models.Model):
    """Key-value specifications for any product, regardless of source."""

    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="specs")
    group = models.CharField(
        max_length=100, db_index=True,
        help_text="Category: Dimensions, Electrical, Material, etc.",
    )
    label = models.CharField(max_length=200)
    value = models.TextField()
    sort_order = models.PositiveIntegerField(default=0)

    class Meta:
        verbose_name = "Product Specification"
        verbose_name_plural = "Product Specifications"
        ordering = ["group", "sort_order", "label"]
        indexes = [models.Index(fields=["product", "group"])]

    def __str__(self):
        return f"{self.label}: {self.value}"
