"""
Audit manufacturers against the CAGE file and FLIS REFERENCE data.

Uses two signals:
1. CAGE TYPE field (primary) — classifies the entity itself
   A = US/Canada commercial   E = Foreign entity
   H = NATO (NCAGE)           M = US subsidiary of foreign parent
   F = Federal/state/local govt   G = Foreign government
   C = Contractor/service     I = Other

2. RNCC from REFERENCE (secondary) — confirms part number assignment
   RNCC=3 = manufacturer-assigned part number

Reports each manufacturer with products as:
- Confirmed: commercial entity (TYPE A/E/H/M) with RNCC=3 evidence
- Likely manufacturer: commercial entity without RNCC=3 (e.g. govt-assigned PNs)
- Government: TYPE F/G
- Suspect: non-commercial type or not in CAGE file

Usage:
    python manage.py audit_manufacturers              # summary
    python manage.py audit_manufacturers --detail     # list every category
    python manage.py audit_manufacturers --fix        # update is_manufacturer flags
"""

import csv
import io
import time
import zipfile

from django.core.management.base import BaseCommand
from django.db import connection

from catalog.constants import DLA_DATA_DIR
from catalog.models import Manufacturer

REFERENCE_ZIP = DLA_DATA_DIR / "REFERENCE.zip"
CAGE_ZIP = DLA_DATA_DIR / "CAGE.zip"

# CAGE TYPE codes that indicate a real commercial/manufacturing entity
COMMERCIAL_TYPES = {"A", "E", "H", "M"}
GOVERNMENT_TYPES = {"F", "G"}


class Command(BaseCommand):
    help = "Audit manufacturers against CAGE file TYPE and REFERENCE RNCC data"

    def add_arguments(self, parser):
        parser.add_argument(
            "--detail", action="store_true",
            help="List manufacturers in each category",
        )
        parser.add_argument(
            "--fix", action="store_true",
            help="Update is_manufacturer flags based on audit results",
        )

    def handle(self, *args, **options):
        detail = options["detail"]
        fix = options["fix"]

        if not CAGE_ZIP.exists():
            self.stderr.write(self.style.ERROR(f"CAGE.zip not found at {CAGE_ZIP}"))
            return

        # Step 1: Load CAGE TYPE data
        cage_info = self._load_cage_info()

        # Step 2: Load RNCC=3 cages from REFERENCE (secondary signal)
        rncc3_cages = set()
        if REFERENCE_ZIP.exists():
            rncc3_cages = self._load_rncc3_cages()
        else:
            self.stdout.write(self.style.WARNING(
                "REFERENCE.zip not found — skipping RNCC check"
            ))

        # Step 3: Audit
        self._audit(cage_info, rncc3_cages, detail, fix)

    def _load_cage_info(self):
        """Load CAGE_CODE -> {type, status} from CAGE.zip."""
        self.stdout.write("Loading CAGE file...")
        t0 = time.time()
        info = {}
        with zipfile.ZipFile(CAGE_ZIP, "r") as zf:
            with zf.open("V_CAGE_STATUS_AND_TYPE.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                header = next(reader)
                i_cage = header.index("CAGE_CODE")
                i_type = header.index("TYPE")
                i_status = header.index("STATUS")
                for row in reader:
                    cage = row[i_cage].strip()
                    if cage:
                        info[cage] = {
                            "type": row[i_type].strip(),
                            "status": row[i_status].strip(),
                        }
        elapsed = time.time() - t0
        self.stdout.write(self.style.SUCCESS(
            f"  Loaded {len(info):,} CAGE records in {elapsed:.1f}s"
        ))
        return info

    def _load_rncc3_cages(self):
        """Get set of cages with ANY RNCC=3 row in REFERENCE.zip."""
        self.stdout.write("Scanning REFERENCE for RNCC=3 cages...")
        t0 = time.time()
        cages = set()
        with zipfile.ZipFile(REFERENCE_ZIP, "r") as zf:
            with zf.open("V_FLIS_PART.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                header = next(reader)
                i_cage = header.index("CAGE_CODE")
                i_rncc = header.index("RNCC")
                for row in reader:
                    if row[i_rncc].strip() == "3":
                        cage = row[i_cage].strip()
                        if cage:
                            cages.add(cage)
        elapsed = time.time() - t0
        self.stdout.write(self.style.SUCCESS(
            f"  Found {len(cages):,} cages with RNCC=3 in {elapsed:.1f}s"
        ))
        return cages

    def _audit(self, cage_info, rncc3_cages, detail, fix):
        self.stdout.write("\nAuditing manufacturers...")

        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT m.id, m.cage_code, m.company_name,
                       m.is_manufacturer,
                       COUNT(p.id) FILTER (WHERE p.is_active = true) AS active_products,
                       COUNT(p.id) FILTER (WHERE p.is_active = false) AS inactive_products
                FROM catalog_organization m
                LEFT JOIN catalog_product p ON p.manufacturer_id = m.id
                GROUP BY m.id, m.cage_code, m.company_name, m.is_manufacturer
                HAVING COUNT(p.id) > 0
                ORDER BY COUNT(p.id) FILTER (WHERE p.is_active = true) DESC
            """)
            mfrs = cursor.fetchall()

        # Classify
        confirmed = []        # commercial type + RNCC=3
        likely_mfr = []       # commercial type, no RNCC=3
        government = []       # TYPE F/G
        suspect = []          # non-commercial type or unknown
        not_in_cage = []      # cage not in CAGE file
        no_cage = []          # no cage code at all

        for mid, cage, name, is_mfr, active, inactive in mfrs:
            row = (mid, cage, name, is_mfr, active, inactive)

            if not cage:
                no_cage.append(row)
                continue

            info = cage_info.get(cage)
            if not info:
                not_in_cage.append(row)
                continue

            cage_type = info["type"]

            if cage_type in COMMERCIAL_TYPES:
                if cage in rncc3_cages:
                    confirmed.append(row)
                else:
                    likely_mfr.append(row)
            elif cage_type in GOVERNMENT_TYPES:
                government.append(row)
            else:
                suspect.append(row)

        # Print summary
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("=== Audit Results ==="))
        self._print_category("Confirmed (commercial + RNCC=3)", confirmed)
        self._print_category("Likely manufacturer (commercial, no RNCC=3)", likely_mfr)
        self._print_category("Government entity (TYPE F/G)", government)
        self._print_category("Suspect (TYPE C/I/unknown)", suspect)
        self._print_category("Not in CAGE file", not_in_cage)
        self._print_category("No CAGE code", no_cage)

        # Flag mismatches
        ROLE_LABELS = {-1: "NO", 0: "NEUTRAL", 1: "YES"}
        wrong_flags = []
        for row in confirmed + likely_mfr:
            if row[3] != 1:  # should be YES
                wrong_flags.append(row)
        for row in government + suspect:
            if row[3] != -1:  # should be NO
                wrong_flags.append(row)

        if wrong_flags:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING(f"=== Flag Mismatches: {len(wrong_flags)} ==="))
            for mid, cage, name, is_mfr, active, inactive in wrong_flags:
                info = cage_info.get(cage, {})
                cage_type = info.get("type", "?")
                has_rncc3 = cage in rncc3_cages
                self.stdout.write(
                    f"  {cage} | TYPE={cage_type} | RNCC3={'Y' if has_rncc3 else 'N'} | "
                    f"is_mfr={ROLE_LABELS.get(is_mfr, '?')} | {name} | "
                    f"{active} active, {inactive} inactive"
                )

        if detail:
            for label, rows in [
                ("Confirmed (commercial + RNCC=3)", confirmed),
                ("Likely manufacturer (commercial, no RNCC=3)", likely_mfr),
                ("Government entity", government),
                ("Suspect", suspect),
                ("Not in CAGE file", not_in_cage),
                ("No CAGE code", no_cage),
            ]:
                if rows:
                    self.stdout.write("")
                    self.stdout.write(self.style.WARNING(f"=== {label} ==="))
                    for mid, cage, name, is_mfr, active, inactive in rows:
                        info = cage_info.get(cage or "", {})
                        cage_type = info.get("type", "?")
                        flag_str = ROLE_LABELS.get(is_mfr, "?")
                        self.stdout.write(
                            f"  {cage or '(none)':<5s} | TYPE={cage_type} | "
                            f"{name:<50s} | {active:>5,} active {inactive:>5,} inactive | "
                            f"is_mfr: {flag_str}"
                        )

        if fix:
            self.stdout.write("")
            # Set is_manufacturer=True for confirmed + likely manufacturers
            mfr_ids = [r[0] for r in confirmed + likely_mfr]
            non_mfr_ids = [r[0] for r in government + suspect]

            if mfr_ids:
                updated = Manufacturer.objects.filter(id__in=mfr_ids).exclude(
                    is_manufacturer=Manufacturer.ROLE_YES,
                ).update(is_manufacturer=Manufacturer.ROLE_YES)
                self.stdout.write(self.style.SUCCESS(
                    f"  Set is_manufacturer=YES on {updated} manufacturers"
                ))

            if non_mfr_ids:
                updated = Manufacturer.objects.filter(id__in=non_mfr_ids).exclude(
                    is_manufacturer=Manufacturer.ROLE_NO,
                ).update(is_manufacturer=Manufacturer.ROLE_NO)
                self.stdout.write(self.style.SUCCESS(
                    f"  Set is_manufacturer=NO on {updated} non-manufacturers"
                ))

            self.stdout.write(self.style.SUCCESS("  Flags updated."))
        else:
            self.stdout.write("")
            self.stdout.write("Run with --fix to update is_manufacturer flags.")

    def _print_category(self, label, rows):
        active = sum(r[4] for r in rows)
        inactive = sum(r[5] for r in rows)
        self.stdout.write(
            f"  {label:<45s} {len(rows):>6,} manufacturers | "
            f"{active:>8,} active, {inactive:>6,} inactive products"
        )
