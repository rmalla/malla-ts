import re

from django.db import models
from django.contrib.postgres.indexes import GinIndex
from django.contrib.postgres.search import SearchVectorField


def slugify_part_number(part_number):
    """Create a URL-friendly slug from a part number.

    Keeps the part number recognizable while making it URL-safe.
    E.g. 'M356380/SATWCS' -> 'm356380-satwcs'
         '15222001-001'   -> '15222001-001'
         '752-6405258 FIND NO.75' -> '752-6405258-find-no-75'
    """
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


# ── CatalogItem ────────────────────────────────────────────────────────────

class CatalogItem(models.Model):
    """Government NSN catalog reference. One per NSN.
    This is supply-chain / procurement data, not the public product listing."""

    nsn = models.CharField(max_length=300, unique=True, db_index=True)
    niin = models.CharField(max_length=9, db_index=True, blank=True)
    nomenclature = models.CharField(max_length=500, blank=True, db_index=True)
    part_numbers = models.TextField(blank=True, help_text="Comma-separated part numbers")
    fsc = models.ForeignKey(
        "home.FederalSupplyClass",
        on_delete=models.SET_NULL,
        related_name="catalog_items",
        null=True,
        blank=True,
    )
    unit_of_issue = models.CharField(max_length=20, blank=True)
    distributor_use = models.BooleanField(default=False, db_index=True)
    is_active = models.BooleanField(default=True, db_index=True)

    # Denormalized counts
    supplier_count = models.PositiveIntegerField(default=0)
    product_count = models.PositiveIntegerField(default=0)
    award_count = models.PositiveIntegerField(default=0)
    opportunity_count = models.PositiveIntegerField(default=0)

    search_vector = SearchVectorField(null=True, blank=True)
    raw_api_response = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Catalog Item"
        verbose_name_plural = "Catalog Items"
        ordering = ["nsn"]
        indexes = [
            GinIndex(fields=["search_vector"]),
        ]

    def __str__(self):
        if self.nomenclature:
            return f"{self.nsn} -- {self.nomenclature}"
        return self.nsn

    def save(self, *args, **kwargs):
        # Extract NIIN from NSN
        parts = self.nsn.split("-")
        if len(parts) == 4:
            self.niin = parts[1] + parts[2] + parts[3]
        super().save(*args, **kwargs)


# Backward compat alias
NSNCatalog = CatalogItem


# ── CatalogPricing ─────────────────────────────────────────────────────────

class CatalogPricing(models.Model):
    """Pricing data for a catalog item from various sources."""

    catalog_item = models.OneToOneField(
        CatalogItem, on_delete=models.CASCADE, related_name="pricing"
    )
    unit_price = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True, db_index=True
    )
    unit_price_source = models.CharField(max_length=30, blank=True)
    unit_price_date = models.DateField(null=True, blank=True)
    last_price = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True
    )
    publog_price = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True
    )
    flis_history_price = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True
    )
    foia_avg_price = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True
    )
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Catalog Pricing"
        verbose_name_plural = "Catalog Pricing"

    def __str__(self):
        return f"Pricing for {self.catalog_item.nsn}"


# ── CatalogSpecifications ─────────────────────────────────────────────────

class CatalogSpecifications(models.Model):
    """FLISV-decoded physical characteristics. Used to populate ProductSpecification."""

    catalog_item = models.OneToOneField(
        CatalogItem, on_delete=models.CASCADE, related_name="specifications"
    )

    # Tier 1 -- filterable fields
    material = models.CharField(max_length=500, blank=True, db_index=True)
    overall_length = models.CharField(max_length=200, blank=True)
    overall_width = models.CharField(max_length=200, blank=True)
    overall_height = models.CharField(max_length=200, blank=True)
    overall_diameter = models.CharField(max_length=200, blank=True)
    weight = models.CharField(max_length=200, blank=True)
    color = models.CharField(max_length=200, blank=True, db_index=True)
    end_item_identification = models.CharField(
        max_length=500, blank=True, db_index=True,
    )
    special_features = models.TextField(blank=True)

    # Full spec data
    specifications_json = models.JSONField(
        default=list, blank=True,
        help_text='Flat list of {label, value, group}',
    )
    characteristics_json = models.JSONField(
        default=dict, blank=True,
        help_text='Nested FLISV decode (legacy)',
    )

    source = models.CharField(max_length=30, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Catalog Specifications"
        verbose_name_plural = "Catalog Specifications"

    def __str__(self):
        return f"Specs for {self.catalog_item.nsn}"


# ── Product ────────────────────────────────────────────────────────────────

class Product(models.Model):
    """A product listing on the website. The primary public-facing entity.
    Identified by manufacturer + part_number.
    URL: /products/<manufacturer.slug>/<part_number_slug>/

    Can be linked to a CatalogItem (NSN) or standalone (commercial/manual).
    """

    # Identity
    manufacturer = models.ForeignKey(
        "catalog.Organization", on_delete=models.CASCADE, related_name="products"
    )
    part_number = models.CharField(max_length=200, blank=True)
    part_number_slug = models.SlugField(
        max_length=220, blank=True, db_index=True,
        help_text="URL-friendly part number, auto-generated",
    )

    # Display
    name = models.CharField(
        max_length=500, blank=True,
        help_text="Product name. Falls back to catalog_item.nomenclature if blank",
    )
    description = models.TextField(blank=True)

    # Government catalog link (optional)
    catalog_item = models.ForeignKey(
        CatalogItem, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="products",
    )

    # Source tracking
    source = models.CharField(
        max_length=30, choices=DataSource.choices, default=DataSource.PUBLOG,
        help_text="Where this product record came from",
    )

    # Visibility
    is_active = models.BooleanField(default=True, db_index=True)

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
        return self.display_name

    def save(self, *args, **kwargs):
        self.part_number_slug = slugify_part_number(self.part_number)
        super().save(*args, **kwargs)

    @property
    def display_name(self):
        if self.name:
            return self.name
        if self.catalog_item:
            return self.catalog_item.nomenclature or self.part_number or "(unnamed)"
        return self.part_number or "(unnamed)"

    @property
    def nsn(self):
        return self.catalog_item.nsn if self.catalog_item else None

    @property
    def unit_price(self):
        if self.catalog_item:
            try:
                return self.catalog_item.pricing.unit_price
            except CatalogPricing.DoesNotExist:
                pass
        return None


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


# ── SupplierLink ───────────────────────────────────────────────────────────

class SupplierLink(models.Model):
    """Supply chain data: who supplies what. Not the public product listing."""

    catalog_item = models.ForeignKey(
        CatalogItem, on_delete=models.CASCADE, related_name="supplier_links"
    )
    organization = models.ForeignKey(
        "catalog.Organization", on_delete=models.CASCADE,
        related_name="supplier_links",
    )
    part_number = models.CharField(max_length=100, blank=True)
    source = models.CharField(max_length=30, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Supplier Link"
        verbose_name_plural = "Supplier Links"
        unique_together = [("catalog_item", "organization")]

    def __str__(self):
        return f"{self.organization} -> {self.catalog_item.nsn}"


# Backward compat alias
Supplier = SupplierLink


# ── AwardHistory ───────────────────────────────────────────────────────────

class AwardHistory(models.Model):
    """Contract awards per NSN."""

    catalog_item = models.ForeignKey(
        CatalogItem, on_delete=models.CASCADE, related_name="awards"
    )
    awardee = models.ForeignKey(
        "catalog.Organization", on_delete=models.CASCADE,
        related_name="won_awards",
    )
    contract_number = models.CharField(max_length=100)
    quantity = models.PositiveIntegerField(null=True, blank=True)
    unit_cost = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True
    )
    award_date = models.DateField(null=True, blank=True)
    surplus = models.BooleanField(default=False)
    part_number = models.CharField(max_length=100, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Award History"
        verbose_name_plural = "Award Histories"
        unique_together = [("catalog_item", "awardee", "contract_number")]
        ordering = ["-award_date"]

    def __str__(self):
        return f"{self.contract_number} -> {self.awardee}"

    @property
    def total_value(self):
        if self.quantity and self.unit_cost:
            return self.quantity * self.unit_cost
        return None


# ── DataProvenance ─────────────────────────────────────────────────────────

class DataProvenance(models.Model):
    """Tracks where catalog data came from."""

    catalog_item = models.ForeignKey(
        CatalogItem, on_delete=models.CASCADE, related_name="provenance"
    )
    source = models.CharField(
        max_length=30, choices=DataSource.choices, db_index=True
    )
    field_group = models.CharField(max_length=30)
    raw_data = models.JSONField(default=dict, blank=True)
    import_job = models.ForeignKey(
        "catalog.ImportJob", on_delete=models.SET_NULL,
        null=True, blank=True,
    )
    imported_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Data Provenance"
        verbose_name_plural = "Data Provenance"
        unique_together = [("catalog_item", "source", "field_group")]

    def __str__(self):
        return f"{self.source} / {self.field_group} for {self.catalog_item.nsn}"
