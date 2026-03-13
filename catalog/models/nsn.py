from django.db import models


class NationalStockNumber(models.Model):
    """A unique National Stock Number (NSN) identifying a supply item.
    Multiple products from different manufacturers may fulfill the same NSN.
    """

    DISABLED = -1
    NEUTRAL = 0
    ENABLED = 1
    STATUS_CHOICES = [
        (DISABLED, "Disabled"),
        (NEUTRAL, "Neutral"),
        (ENABLED, "Enabled"),
    ]

    nsn = models.CharField(max_length=16, unique=True, db_index=True)
    niin = models.CharField(max_length=9, db_index=True, blank=True)
    fsc = models.ForeignKey(
        "home.FederalSupplyClass",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="nsns",
    )
    nomenclature = models.CharField(max_length=500, blank=True)
    unit_of_issue = models.CharField(max_length=20, blank=True)
    is_active = models.SmallIntegerField(
        default=0, choices=STATUS_CHOICES, db_index=True,
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "catalog_nsn"
        verbose_name = "National Stock Number"
        verbose_name_plural = "National Stock Numbers"
        indexes = [
            models.Index(fields=["fsc"]),
            models.Index(fields=["niin"]),
        ]

    def __str__(self):
        if self.nomenclature:
            return f"{self.nsn} — {self.nomenclature}"
        return self.nsn
