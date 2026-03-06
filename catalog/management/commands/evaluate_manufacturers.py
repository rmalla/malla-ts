"""
Evaluate and classify manufacturers using FLIS REFERENCE data.

Scans the full REFERENCE file (~16M rows) to find every CAGE code that
appears in at least one RNCC=3 row. RNCC=3 means "part number assigned
by the actual manufacturer" — definitive proof the entity manufactures.

Classification:
  - CAGE has RNCC=3 in REFERENCE → YES (1)  — confirmed manufacturer
  - CAGE in REFERENCE but no RNCC=3         → NEUTRAL (0) — insufficient evidence
  - CAGE not in REFERENCE at all            → NEUTRAL (0) — no data

Only updates manufacturers currently set to NEUTRAL (0). Manually set
values (-1 or 1) are preserved — human overrides are never touched.

Also enriches manufacturer records with CAGE file data (company_name,
city, state, country) where missing.

Usage:
    python manage.py evaluate_manufacturers --dry-run   # preview
    python manage.py evaluate_manufacturers             # apply
"""

import csv
import io
import time
import zipfile

from django.core.management.base import BaseCommand

from catalog.constants import DLA_DATA_DIR
from catalog.models import Manufacturer

REFERENCE_ZIP = DLA_DATA_DIR / "REFERENCE.zip"
CAGE_ZIP = DLA_DATA_DIR / "CAGE.zip"


class Command(BaseCommand):
    help = "Evaluate is_manufacturer flag using RNCC=3 from FLIS REFERENCE data"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run", action="store_true",
            help="Preview classification without making changes",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        if not REFERENCE_ZIP.exists():
            self.stderr.write(self.style.ERROR(
                f"REFERENCE.zip not found at {REFERENCE_ZIP}"
            ))
            return

        # Step 1: Scan REFERENCE for all RNCC=3 cage codes
        rncc3_cages = self._scan_rncc3_cages()

        # Step 2: Classify neutral manufacturers
        self._classify(rncc3_cages, dry_run)

        # Step 3: Enrich missing data from CAGE file
        if CAGE_ZIP.exists():
            self._enrich_from_cage(dry_run)

    def _scan_rncc3_cages(self):
        """Scan the full REFERENCE file for all CAGE codes with RNCC=3."""
        self.stdout.write("Scanning REFERENCE for RNCC=3 cage codes...")
        t0 = time.time()
        cages = set()
        total = 0

        with zipfile.ZipFile(REFERENCE_ZIP, "r") as zf:
            with zf.open("V_FLIS_PART.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                header = next(reader)
                i_cage = header.index("CAGE_CODE")
                i_rncc = header.index("RNCC")

                for row in reader:
                    total += 1
                    if row[i_rncc].strip() == "3":
                        cage = row[i_cage].strip()
                        if cage:
                            cages.add(cage)

        elapsed = time.time() - t0
        self.stdout.write(self.style.SUCCESS(
            f"  Scanned {total:,} rows in {elapsed:.1f}s"
        ))
        self.stdout.write(self.style.SUCCESS(
            f"  Found {len(cages):,} unique CAGE codes with RNCC=3"
        ))
        return cages

    def _classify(self, rncc3_cages, dry_run):
        """Classify neutral manufacturers against the RNCC=3 cage set."""
        self.stdout.write("\nClassifying manufacturers...")

        # Only touch neutral manufacturers — respect manual overrides
        neutral = list(
            Manufacturer.objects.filter(is_manufacturer=Manufacturer.ROLE_NEUTRAL)
            .values_list("id", "cage_code")
        )

        already_set = Manufacturer.objects.exclude(
            is_manufacturer=Manufacturer.ROLE_NEUTRAL,
        ).count()

        set_yes_ids = []   # CAGE has RNCC=3 → confirmed manufacturer
        stay_neutral = []  # No RNCC=3 evidence → leave as neutral

        for mid, cage_code in neutral:
            if cage_code and cage_code in rncc3_cages:
                set_yes_ids.append(mid)
            else:
                stay_neutral.append((mid, cage_code))

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("=== Classification Results ==="))
        self.stdout.write(f"  Set to YES (RNCC=3 confirmed):    {len(set_yes_ids):,}")
        self.stdout.write(f"  Stay NEUTRAL (no RNCC=3 evidence): {len(stay_neutral):,}")
        self.stdout.write(f"  Already set (manual/previous):     {already_set:,}")

        if stay_neutral:
            sample = Manufacturer.objects.filter(
                pk__in=[mid for mid, _ in stay_neutral[:10]]
            ).only("cage_code", "company_name")
            self.stdout.write("")
            self.stdout.write("  Stay NEUTRAL (sample):")
            for mfr in sample:
                self.stdout.write(f"    {mfr.cage_code or '(none)'} | {mfr.company_name}")

        if dry_run:
            self.stdout.write(self.style.WARNING("\nDRY RUN — no changes made."))
            return

        if set_yes_ids:
            updated = Manufacturer.objects.filter(pk__in=set_yes_ids).update(
                is_manufacturer=Manufacturer.ROLE_YES,
            )
            self.stdout.write(self.style.SUCCESS(f"\n  Updated {updated:,} to YES"))

        self.stdout.write(self.style.SUCCESS("  Done."))

    def _enrich_from_cage(self, dry_run):
        """Fill in missing company_name, city, state, country from CAGE.zip."""
        self.stdout.write("\nEnriching from CAGE file...")
        t0 = time.time()

        missing_name = set(
            Manufacturer.objects.filter(company_name="", cage_code__isnull=False)
            .values_list("cage_code", flat=True)
        )
        if not missing_name:
            self.stdout.write("  All manufacturers have company names.")
            return

        self.stdout.write(f"  {len(missing_name):,} manufacturers missing company_name")

        cage_data = {}
        with zipfile.ZipFile(CAGE_ZIP, "r") as zf:
            with zf.open("P_CAGE.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                header = next(reader)
                i_cage = header.index("CAGE_CODE")
                i_company = header.index("COMPANY")
                i_city = header.index("CITY")
                i_state = header.index("STATE_PROVINCE")
                i_zip = header.index("ZIP_POSTAL_ZONE")
                i_country = header.index("COUNTRY")

                for row in reader:
                    cage = row[i_cage].strip()
                    if cage in missing_name:
                        cage_data[cage] = {
                            "company_name": row[i_company].strip(),
                            "city": row[i_city].strip(),
                            "state": row[i_state].strip(),
                            "zip_code": row[i_zip].strip(),
                            "country": row[i_country].strip(),
                        }

        enriched = 0
        if not dry_run and cage_data:
            for cage, data in cage_data.items():
                Manufacturer.objects.filter(
                    cage_code=cage, company_name="",
                ).update(**data)
                enriched += 1

        elapsed = time.time() - t0
        self.stdout.write(self.style.SUCCESS(
            f"  Enriched {enriched if not dry_run else len(cage_data):,} "
            f"manufacturers in {elapsed:.1f}s"
        ))
