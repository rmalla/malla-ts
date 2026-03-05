from django.db import models


class PurchaseTransaction(models.Model):
    """Government purchase transaction from DLA FOIA reports."""

    nsn = models.CharField(max_length=16, db_index=True, blank=True)
    catalog_item = models.ForeignKey(
        "catalog.CatalogItem",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="transactions",
    )
    item_name = models.CharField(max_length=500, blank=True)
    manufacturer_name = models.CharField(max_length=255, blank=True)
    manufacturer_part_number = models.CharField(max_length=200, blank=True)
    manufacturer = models.ForeignKey(
        "catalog.Organization",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="transactions",
    )
    supplier_name = models.CharField(max_length=255, blank=True)
    quantity = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True
    )
    unit_of_measure = models.CharField(max_length=20, blank=True)
    unit_price = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True
    )
    extended_price = models.DecimalField(
        max_digits=14, decimal_places=2, null=True, blank=True
    )
    transaction_date = models.DateField(null=True, blank=True, db_index=True)
    department = models.CharField(max_length=100, blank=True)
    agency = models.CharField(max_length=255, blank=True)
    source_of_supply = models.CharField(max_length=20, blank=True)
    part_number_raw = models.CharField(max_length=300, blank=True)
    source_file = models.CharField(max_length=255, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Purchase Transaction"
        verbose_name_plural = "Purchase Transactions"
        ordering = ["-transaction_date"]
        indexes = [
            models.Index(fields=["department", "agency"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["nsn", "transaction_date", "part_number_raw", "supplier_name", "unit_price"],
                name="unique_purchase_transaction",
            ),
        ]

    def __str__(self):
        return f"{self.nsn} — {self.item_name[:50]} ({self.transaction_date})"

    @property
    def total_value(self):
        if self.quantity and self.unit_price:
            return self.quantity * self.unit_price
        return self.extended_price
