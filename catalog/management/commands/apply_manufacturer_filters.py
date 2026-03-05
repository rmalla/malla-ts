from django.core.management.base import BaseCommand

from catalog.constants import FilterFieldType, PipelineStage
from catalog.models import PipelineFilter, Manufacturer


class Command(BaseCommand):
    help = "Retroactively disable manufacturers whose names match active manufacturer name filters."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be disabled without making changes.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        rules = PipelineFilter.objects.filter(
            is_active=True,
            field_type=FilterFieldType.MANUFACTURER_NAME,
            stage__in=[PipelineStage.ALL, PipelineStage.NSN_ENRICH],
        )
        if not rules.exists():
            self.stdout.write("No active manufacturer name filters found.")
            return

        self.stdout.write(f"Loaded {rules.count()} manufacturer name filter(s)")

        manufacturers = Manufacturer.objects.exclude(
            status=Manufacturer.DISABLED,
        ).exclude(company_name="")

        disabled_count = 0
        for mfr in manufacturers.iterator():
            for rule in rules:
                if rule.matches(mfr.company_name):
                    if dry_run:
                        self.stdout.write(
                            f"  [DRY RUN] Would disable: {mfr.company_name} "
                            f"(CAGE {mfr.cage_code}) — matched '{rule.field_value}'"
                        )
                    else:
                        mfr.status = Manufacturer.DISABLED
                        mfr.save(update_fields=["status"])
                        self.stdout.write(
                            self.style.WARNING(
                                f"  Disabled: {mfr.company_name} "
                                f"(CAGE {mfr.cage_code}) — matched '{rule.field_value}'"
                            )
                        )
                    disabled_count += 1
                    break  # one match is enough

        label = "Would disable" if dry_run else "Disabled"
        self.stdout.write(
            self.style.SUCCESS(f"\nDone — {label} {disabled_count} manufacturer(s).")
        )
