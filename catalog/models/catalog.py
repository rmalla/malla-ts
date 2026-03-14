import logging
import re

from django.core.files.base import ContentFile
from django.db import models
from django.db.models import Q

logger = logging.getLogger(__name__)


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
        max_length=220, blank=True,
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

    # Pricing & NSN link
    price = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    nsn = models.ForeignKey(
        "catalog.NationalStockNumber", null=True, blank=True,
        on_delete=models.CASCADE, related_name="products",
    )

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

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Product"
        verbose_name_plural = "Products"
        indexes = [
            models.Index(fields=["manufacturer", "part_number_slug"]),
            models.Index(
                fields=["manufacturer"],
                name="catalog_prod_publishable",
                condition=Q(is_active__gte=0),
            ),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["manufacturer", "part_number"],
                name="catalog_product_mfr_partnum_uniq",
            ),
            models.CheckConstraint(
                condition=Q(is_active__in=[-1, 0, 1]),
                name="catalog_product_is_active_valid",
            ),
        ]

    def __str__(self):
        return self.get_display_name()

    def save(self, *args, **kwargs):
        self.part_number_slug = slugify_part_number(self.part_number)
        # Auto-populate display_name from NSN nomenclature if not manually set
        if not self.display_name and self.nsn_id and self.nsn and self.nsn.nomenclature:
            from catalog.services.name_formatter import naturalize_nomenclature
            self.display_name = naturalize_nomenclature(self.nsn.nomenclature)
        super().save(*args, **kwargs)

    def get_display_name(self):
        """Fallback chain: display_name → name → nsn.nomenclature → part_number."""
        return self.display_name or self.name or (self.nsn.nomenclature if self.nsn_id else "") or self.part_number or "(unnamed)"


# ── ProductSpecification ───────────────────────────────────────────────────

class ProductSpecification(models.Model):
    """Key-value specifications for any product, regardless of source."""

    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="specs")
    group = models.CharField(
        max_length=100,
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


# ── ProductImage ──────────────────────────────────────────────────────────

class ProductImage(models.Model):
    """Images attached to a product."""

    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name="images")
    image = models.ImageField(upload_to="product_images/")
    caption = models.CharField(max_length=255, blank=True)
    sort_order = models.PositiveIntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Product Image"
        verbose_name_plural = "Product Images"
        ordering = ["sort_order", "created_at"]

    def save(self, *args, **kwargs):
        if self.image and hasattr(self.image.file, 'read'):
            from catalog.services.image_processor import process_product_image

            raw = self.image.file.read()
            self.image.file.seek(0)

            try:
                webp_bytes, meta = process_product_image(raw)
                filename = f"product-{self.product_id}-{self.sort_order}.webp"
                self.image.save(filename, ContentFile(webp_bytes), save=False)
            except Exception:
                logger.exception("Failed to process product image, saving raw")

        super().save(*args, **kwargs)

    def __str__(self):
        return self.caption or f"Image {self.pk}"
