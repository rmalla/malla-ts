from django.core.management.base import BaseCommand

from catalog.constants import FilterFieldType
from catalog.models import (
    CatalogItem,
    Manufacturer,
    PipelineFilter,
    Product,
    SupplierLink,
    AwardHistory,
)


class Command(BaseCommand):
    help = "Delete organizations matching active manufacturer name pipeline filters, cascading to products etc."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be deleted without making changes.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        rules = PipelineFilter.objects.filter(
            is_active=True,
            field_type=FilterFieldType.MANUFACTURER_NAME,
        )
        if not rules.exists():
            self.stdout.write("No active manufacturer name filters found.")
            return

        self.stdout.write(f"Loaded {rules.count()} active manufacturer name filter(s).")

        # Find matching organizations
        matched_pks = []
        for mfr in Manufacturer.objects.iterator():
            for rule in rules:
                if rule.matches(mfr.company_name):
                    matched_pks.append(mfr.pk)
                    self.stdout.write(self.style.WARNING(
                        f"  MATCH: {mfr.cage_code or '--'} | {mfr.company_name} "
                        f"(rule: '{rule.field_value}')"
                    ))
                    break

        if not matched_pks:
            self.stdout.write(self.style.SUCCESS("No matching organizations found."))
            return

        # Collect affected CatalogItem PKs before deletion (for count refresh)
        affected_catalog_pks = set(
            SupplierLink.objects.filter(organization_id__in=matched_pks)
                .values_list("catalog_item_id", flat=True)
        ) | set(
            Product.objects.filter(manufacturer_id__in=matched_pks)
                .values_list("catalog_item_id", flat=True)
        ) | set(
            AwardHistory.objects.filter(awardee_id__in=matched_pks)
                .values_list("catalog_item_id", flat=True)
        )
        affected_catalog_pks.discard(None)

        if dry_run:
            self.stdout.write(self.style.WARNING(
                f"\n[DRY RUN] Would delete {len(matched_pks)} organization(s) "
                f"and refresh counts on {len(affected_catalog_pks)} catalog item(s)."
            ))
            return

        deleted_count, deleted_detail = Manufacturer.objects.filter(
            pk__in=matched_pks
        ).delete()
        self.stdout.write(self.style.SUCCESS(f"\nDeleted {deleted_count} object(s):"))
        for model_label, count in deleted_detail.items():
            if count:
                self.stdout.write(f"  {model_label}: {count}")

        # Refresh denormalized counts on affected CatalogItems
        if affected_catalog_pks:
            for item in CatalogItem.objects.filter(pk__in=affected_catalog_pks):
                item.supplier_count = item.supplier_links.count()
                item.product_count = item.products.count()
                item.award_count = item.awards.count()
                item.save(update_fields=["supplier_count", "product_count", "award_count"])
            self.stdout.write(
                f"Refreshed counts on {len(affected_catalog_pks)} catalog item(s)."
            )

        self.stdout.write(self.style.SUCCESS("Done."))
