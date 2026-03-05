import logging
from collections import defaultdict

from catalog.constants import FilterFieldType, PipelineStage

logger = logging.getLogger(__name__)


class FilterResult:
    """Result of a filter check — truthy when the record should be excluded."""

    __slots__ = ("is_filtered", "rule")

    def __init__(self, is_filtered=False, rule=None):
        self.is_filtered = is_filtered
        self.rule = rule

    def __bool__(self):
        return self.is_filtered

    @property
    def reason(self):
        if self.rule:
            return (
                f"Filtered by {self.rule.get_field_type_display()}"
                f"={self.rule.field_value}: {self.rule.reason}"
            )
        return ""


PASS = FilterResult()


class FilterService:
    """
    Loads active pipeline filters once and provides fast check methods.

    Instantiate at the start of an import run with the current pipeline stage.
    """

    def __init__(self, stage):
        self.stage = stage
        self._rules = defaultdict(list)  # field_type → [PipelineFilter, ...]
        self._match_counts = defaultdict(int)  # rule.pk → count
        self._load_rules()

    def _load_rules(self):
        from catalog.models import PipelineFilter

        qs = PipelineFilter.objects.filter(is_active=True).filter(
            stage__in=[PipelineStage.ALL, self.stage]
        )
        for rule in qs:
            self._rules[rule.field_type].append(rule)

    @property
    def rule_count(self):
        return sum(len(rules) for rules in self._rules.values())

    # ------------------------------------------------------------------
    # Check methods — return FilterResult (truthy = should skip)
    # ------------------------------------------------------------------

    def _check_field(self, field_type, value):
        """Check a single value against all rules of the given field_type."""
        if not value:
            return PASS
        for rule in self._rules.get(field_type, []):
            if rule.matches(value):
                self._match_counts[rule.pk] += 1
                return FilterResult(is_filtered=True, rule=rule)
        return PASS

    def check_opportunity(self, psc_code=None, nsn=None, agency_code=None,
                          nomenclature=None):
        """Check a DIBBS opportunity against all relevant filters."""
        # Derive FSC from first 4 chars of NSN
        fsc_code = nsn[:4] if nsn and len(nsn) >= 4 else None

        for field_type, value in [
            (FilterFieldType.FSC_CODE, fsc_code),
            (FilterFieldType.PSC_CODE, psc_code),
            (FilterFieldType.NSN, nsn),
            (FilterFieldType.AGENCY_CODE, agency_code),
            (FilterFieldType.NOMENCLATURE, nomenclature),
        ]:
            result = self._check_field(field_type, value)
            if result:
                return result
        return PASS

    def check_nsn(self, nsn, nomenclature=None):
        """Check an NSN against FSC, NSN, and nomenclature filters."""
        fsc_code = nsn[:4] if nsn and len(nsn) >= 4 else None

        for field_type, value in [
            (FilterFieldType.FSC_CODE, fsc_code),
            (FilterFieldType.NSN, nsn),
            (FilterFieldType.NOMENCLATURE, nomenclature),
        ]:
            result = self._check_field(field_type, value)
            if result:
                return result
        return PASS

    def check_cage(self, cage_code):
        """Check a CAGE code against cage_code filters."""
        return self._check_field(FilterFieldType.CAGE_CODE, cage_code)

    def check_manufacturer_name(self, company_name):
        """Check a manufacturer name against manufacturer name filters."""
        return self._check_field(FilterFieldType.MANUFACTURER_NAME, company_name)

    def check_unit_price(self, price):
        """Check a unit price against min/max price filters. Skips if price is None/0."""
        if not price:
            return PASS
        result = self._check_field(FilterFieldType.PRICE_MIN, price)
        if result:
            return result
        return self._check_field(FilterFieldType.PRICE_MAX, price)

    # ------------------------------------------------------------------
    # Bulk helpers — for queryset pre-filtering
    # ------------------------------------------------------------------

    def get_excluded_fsc_codes(self):
        """Return set of FSC codes that should be excluded."""
        return {
            rule.field_value
            for rule in self._rules.get(FilterFieldType.FSC_CODE, [])
        }

    def get_excluded_nsns(self):
        """Return set of raw NSN values that should be excluded."""
        return {
            rule.field_value.replace("-", "")
            for rule in self._rules.get(FilterFieldType.NSN, [])
        }

    # ------------------------------------------------------------------
    # Audit
    # ------------------------------------------------------------------

    def get_summary(self):
        """Return per-rule match counts for audit logging."""
        all_rules = [
            rule for rules in self._rules.values() for rule in rules
        ]
        return {
            str(rule): self._match_counts.get(rule.pk, 0)
            for rule in all_rules
        }
