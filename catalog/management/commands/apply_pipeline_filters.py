"""
Apply all active PipelineFilter rules retroactively to existing products.

Scans products against every active filter (FSC, nomenclature,
manufacturer name, CAGE code) and disables matches.  Designed to be run
after seeding new filters so the existing catalog matches the import-time
rules.

Usage:
    python manage.py apply_pipeline_filters                 # dry-run
    python manage.py apply_pipeline_filters --execute       # disable matches
    python manage.py apply_pipeline_filters --type fsc_code # only FSC filters
"""

import re
from fnmatch import fnmatch

from django.db import models
from django.core.management.base import BaseCommand

from catalog.constants import FilterFieldType, PipelineStage
from catalog.models import PipelineFilter, Product
from catalog.models.entities import Manufacturer


class Command(BaseCommand):
    help = "Apply active PipelineFilter rules to existing products, disabling matches."

    def add_arguments(self, parser):
        parser.add_argument(
            "--execute", action="store_true",
            help="Actually disable products (default is dry-run).",
        )
        parser.add_argument(
            "--type",
            choices=[ft.value for ft in FilterFieldType],
            help="Only apply filters of this type.",
        )

    def handle(self, *args, **options):
        execute = options["execute"]
        filter_type = options["type"]

        if not execute:
            self.stdout.write(self.style.WARNING(
                "Dry-run mode — use --execute to apply changes.\n"
            ))

        rules = PipelineFilter.objects.filter(is_active=True)
        if filter_type:
            rules = rules.filter(field_type=filter_type)

        if not rules.exists():
            self.stdout.write("No active filters found.")
            return

        self.stdout.write(f"Loaded {rules.count()} active filter(s)\n")

        total = 0

        # Group rules by type for efficient processing
        rules_by_type = {}
        for rule in rules:
            rules_by_type.setdefault(rule.field_type, []).append(rule)

        # ── FSC code filters → bulk query ──
        if FilterFieldType.FSC_CODE in rules_by_type:
            fsc_rules = rules_by_type[FilterFieldType.FSC_CODE]
            codes = [r.field_value for r in fsc_rules]
            qs = Product.objects.filter(
                is_active=Product.NEUTRAL,
                nsn__fsc__code__in=codes,
            )
            count = qs.count()
            total += count
            label = "Disabling" if execute else "[DRY RUN]"
            self.stdout.write(f"  FSC codes ({len(codes)} rules): {count:>7,} products  {label}")
            for r in fsc_rules:
                sub = Product.objects.filter(
                    is_active=Product.NEUTRAL,
                    nsn__fsc__code=r.field_value,
                ).count()
                if sub:
                    self.stdout.write(f"    FSC {r.field_value}: {sub:,}  — {r.reason}")
            if execute and count:
                qs.update(is_active=Product.DISABLED)

        # ── CAGE code filters → bulk query ──
        if FilterFieldType.CAGE_CODE in rules_by_type:
            cage_rules = rules_by_type[FilterFieldType.CAGE_CODE]
            codes = [r.field_value for r in cage_rules]
            qs = Product.objects.filter(
                is_active=Product.NEUTRAL,
                manufacturer__cage_code__in=codes,
            )
            count = qs.count()
            total += count
            label = "Disabling" if execute else "[DRY RUN]"
            self.stdout.write(f"  CAGE codes ({len(codes)} rules): {count:>7,} products  {label}")
            if execute and count:
                qs.update(is_active=Product.DISABLED)

        # ── Manufacturer name filters → word-boundary matching per rule ──
        if FilterFieldType.MANUFACTURER_NAME in rules_by_type:
            mfr_rules = rules_by_type[FilterFieldType.MANUFACTURER_NAME]
            q = models.Q()
            for r in mfr_rules:
                # PostgreSQL word boundaries: \m = start of word, \M = end of word
                pattern = r"\\m" + re.escape(r.field_value) + r"\\M"
                q |= models.Q(manufacturer__company_name__iregex=pattern)
            qs = Product.objects.filter(is_active=Product.NEUTRAL).filter(q)
            count = qs.count()
            total += count
            label = "Disabling" if execute else "[DRY RUN]"
            self.stdout.write(f"  Manufacturer names ({len(mfr_rules)} rules): {count:>7,} products  {label}")
            if execute and count:
                qs.update(is_active=Product.DISABLED)

        # ── Nomenclature filters → fnmatch per product (can't do glob in SQL) ──
        if FilterFieldType.NOMENCLATURE in rules_by_type:
            nom_rules = rules_by_type[FilterFieldType.NOMENCLATURE]
            # Products with an NSN that has a nomenclature
            qs = Product.objects.filter(
                is_active=Product.NEUTRAL,
                nsn__nomenclature__gt="",
            ).values_list("id", "nsn__nomenclature")

            matched_ids = []
            for pid, nom in qs.iterator():
                nom_upper = nom.upper()
                for r in nom_rules:
                    if fnmatch(nom_upper, r.field_value.upper()):
                        matched_ids.append(pid)
                        break

            count = len(matched_ids)
            total += count
            label = "Disabling" if execute else "[DRY RUN]"
            self.stdout.write(f"  Nomenclature ({len(nom_rules)} rules): {count:>7,} products  {label}")
            if execute and matched_ids:
                # Batch update
                batch_size = 5000
                for i in range(0, len(matched_ids), batch_size):
                    Product.objects.filter(
                        id__in=matched_ids[i:i + batch_size],
                    ).update(is_active=Product.DISABLED)

        # ── NSN exact-match filters ──
        if FilterFieldType.NSN in rules_by_type:
            nsn_rules = rules_by_type[FilterFieldType.NSN]
            nsn_values = [r.field_value.replace("-", "") for r in nsn_rules]
            qs = Product.objects.filter(
                is_active=Product.NEUTRAL,
                nsn__nsn__in=nsn_values,
            )
            count = qs.count()
            total += count
            label = "Disabling" if execute else "[DRY RUN]"
            self.stdout.write(f"  NSN exact ({len(nsn_rules)} rules): {count:>7,} products  {label}")
            if execute and count:
                qs.update(is_active=Product.DISABLED)

        verb = "Disabled" if execute else "Would disable"
        self.stdout.write(self.style.SUCCESS(
            f"\n{'=' * 50}\n{verb} {total:,} total products."
        ))
