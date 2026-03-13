from django.core.management.base import BaseCommand
from django.db import connection
from django.db.models import Min

from catalog.models import Product
from catalog.models.nsn import NationalStockNumber
from home.models import FederalSupplyClass


class Command(BaseCommand):
    help = "Create NationalStockNumber records from Product data and link via FK"

    def add_arguments(self, parser):
        parser.add_argument(
            "--skip-link",
            action="store_true",
            help="Skip the Product.nsn_obj FK update step",
        )

    def handle(self, *args, **options):
        self._create_nsn_records()
        if not options["skip_link"]:
            self._link_products()
        self._print_summary()

    def _create_nsn_records(self):
        # Build FSC lookup: code → id
        fsc_map = dict(
            FederalSupplyClass.objects.values_list("code", "id")
        )

        # Aggregate distinct NSNs with their metadata
        qs = (
            Product.objects.exclude(nsn="")
            .values("nsn")
            .annotate(
                nom=Min("nomenclature"),
                uoi=Min("unit_of_issue"),
            )
            .iterator(chunk_size=10000)
        )

        batch = []
        created = 0
        existing_nsns = set(
            NationalStockNumber.objects.values_list("nsn", flat=True).iterator(chunk_size=50000)
        )

        for row in qs:
            nsn_val = row["nsn"]
            if nsn_val in existing_nsns:
                continue

            digits = nsn_val.replace("-", "")
            niin = digits[-9:] if len(digits) >= 9 else ""
            fsc_code = digits[:4] if len(digits) >= 4 else ""

            batch.append(NationalStockNumber(
                nsn=nsn_val,
                niin=niin,
                fsc_id=fsc_map.get(fsc_code),
                nomenclature=row["nom"] or "",
                unit_of_issue=row["uoi"] or "",
            ))

            if len(batch) >= 5000:
                NationalStockNumber.objects.bulk_create(batch, batch_size=5000)
                created += len(batch)
                self.stdout.write(f"  Created {created} NSN records...")
                batch = []

        if batch:
            NationalStockNumber.objects.bulk_create(batch, batch_size=5000)
            created += len(batch)

        self.stdout.write(self.style.SUCCESS(f"Created {created} NSN records"))

    def _link_products(self):
        self.stdout.write("Linking products to NSN records...")
        with connection.cursor() as cursor:
            cursor.execute("""
                UPDATE catalog_product
                SET nsn_obj_id = nsn_rec.id
                FROM catalog_nsn nsn_rec
                WHERE catalog_product.nsn = nsn_rec.nsn
                  AND catalog_product.nsn != ''
                  AND catalog_product.nsn_obj_id IS NULL
            """)
            updated = cursor.rowcount
        self.stdout.write(self.style.SUCCESS(f"Linked {updated} products to NSN records"))

    def _print_summary(self):
        total_nsns = NationalStockNumber.objects.count()
        total_products_with_nsn = Product.objects.exclude(nsn="").count()
        linked = Product.objects.exclude(nsn="").filter(nsn_obj__isnull=False).count()
        orphans = total_products_with_nsn - linked

        self.stdout.write(f"\nSummary:")
        self.stdout.write(f"  NSN records:        {total_nsns:,}")
        self.stdout.write(f"  Products with NSN:  {total_products_with_nsn:,}")
        self.stdout.write(f"  Linked:             {linked:,}")
        self.stdout.write(f"  Orphaned:           {orphans:,}")
