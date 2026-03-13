from django.core.management.base import BaseCommand

from catalog.models.nsn import NationalStockNumber
from home.models import FederalSupplyClass


class Command(BaseCommand):
    help = "Create missing FederalSupplyClass records from NationalStockNumber FSC prefixes"

    def handle(self, *args, **options):
        # Get all distinct 4-digit FSC prefixes from NSN records
        prefixes = set()
        for nsn_val in (
            NationalStockNumber.objects.exclude(nsn="")
            .values_list("nsn", flat=True)
            .iterator(chunk_size=10000)
        ):
            prefix = nsn_val.replace("-", "")[:4]
            if len(prefix) == 4 and prefix.isdigit():
                prefixes.add(prefix)

        self.stdout.write(f"Found {len(prefixes)} distinct FSC prefixes in NSN records")

        # Find which ones are missing
        existing = set(
            FederalSupplyClass.objects.filter(code__in=prefixes)
            .values_list("code", flat=True)
        )
        missing = prefixes - existing

        if not missing:
            self.stdout.write(self.style.SUCCESS("All FSC codes already exist. Nothing to do."))
            return

        # Bulk create missing FSC records
        to_create = [
            FederalSupplyClass(
                code=code,
                name="",
                group=code[:2],
                group_name="",
            )
            for code in sorted(missing)
        ]
        FederalSupplyClass.objects.bulk_create(to_create, batch_size=500)

        self.stdout.write(self.style.SUCCESS(
            f"Created {len(to_create)} new FSC codes. "
            f"Total: {FederalSupplyClass.objects.count()}"
        ))
