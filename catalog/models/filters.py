import re
from fnmatch import fnmatch

from django.conf import settings
from django.core.exceptions import ValidationError
from django.db import models

from catalog.constants import FilterFieldType, FilterAction, PipelineStage


class PipelineFilter(models.Model):
    """Exclusion rule applied during import pipeline stages."""

    field_type = models.CharField(
        max_length=20,
        choices=FilterFieldType.choices,
        help_text="Which data field this filter matches against.",
    )
    field_value = models.CharField(
        max_length=100,
        help_text="Value to match. Nomenclature supports * wildcards. Manufacturer Name uses substring matching.",
    )
    action = models.CharField(
        max_length=20,
        choices=FilterAction.choices,
        default=FilterAction.EXCLUDE,
    )
    stage = models.CharField(
        max_length=20,
        choices=PipelineStage.choices,
        default=PipelineStage.ALL,
        help_text="Pipeline stage(s) where this filter applies.",
    )
    is_active = models.BooleanField(default=True, db_index=True)
    reason = models.CharField(
        max_length=255,
        help_text="Why this filter exists (shown in audit logs).",
    )
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="pipeline_filters",
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Pipeline Filter"
        verbose_name_plural = "Pipeline Filters"
        constraints = [
            models.UniqueConstraint(
                fields=["field_type", "field_value", "stage"],
                name="catalog_pipelinefilter_type_val_stage_uniq",
            ),
        ]
        indexes = [
            models.Index(
                fields=["is_active", "field_type"],
                name="idx_filter_active_type",
            ),
        ]
        ordering = ["field_type", "field_value"]

    def __str__(self):
        status = "active" if self.is_active else "inactive"
        return f"{self.get_field_type_display()} = {self.field_value} ({status})"

    def clean(self):
        self.field_value = self.field_value.strip()
        ft = self.field_type

        if ft == FilterFieldType.FSC_CODE:
            if not re.fullmatch(r"\d{4}", self.field_value):
                raise ValidationError(
                    {"field_value": "FSC code must be exactly 4 digits."}
                )
        elif ft == FilterFieldType.CAGE_CODE:
            if not re.fullmatch(r"[A-Za-z0-9]{5}", self.field_value):
                raise ValidationError(
                    {"field_value": "CAGE code must be exactly 5 alphanumeric characters."}
                )
            self.field_value = self.field_value.upper()
        elif ft == FilterFieldType.NSN:
            cleaned = self.field_value.replace("-", "")
            if not re.fullmatch(r"\d{13}", cleaned):
                raise ValidationError(
                    {"field_value": "NSN must be 13 digits (with or without dashes)."}
                )
        elif ft == FilterFieldType.PSC_CODE:
            if not self.field_value:
                raise ValidationError(
                    {"field_value": "PSC code cannot be empty."}
                )
        elif ft == FilterFieldType.MANUFACTURER_NAME:
            if len(self.field_value) < 2:
                raise ValidationError(
                    {"field_value": "Manufacturer name filter must be at least 2 characters."}
                )
        elif ft in (FilterFieldType.PRICE_MIN, FilterFieldType.PRICE_MAX):
            try:
                val = float(self.field_value)
                if val < 0:
                    raise ValueError
            except (ValueError, TypeError):
                raise ValidationError(
                    {"field_value": "Price filter must be a positive number."}
                )

    def matches(self, value):
        """Check if the given value matches this filter rule."""
        if not value:
            return False
        if self.field_type == FilterFieldType.PRICE_MIN:
            try:
                return float(value) < float(self.field_value)
            except (ValueError, TypeError):
                return False
        if self.field_type == FilterFieldType.PRICE_MAX:
            try:
                return float(value) > float(self.field_value)
            except (ValueError, TypeError):
                return False
        value = str(value).strip()
        if self.field_type == FilterFieldType.NOMENCLATURE:
            return fnmatch(value.upper(), self.field_value.upper())
        if self.field_type == FilterFieldType.NSN:
            return value.replace("-", "") == self.field_value.replace("-", "")
        if self.field_type == FilterFieldType.MANUFACTURER_NAME:
            return self.field_value.upper() in value.upper()
        return value.upper() == self.field_value.upper()
