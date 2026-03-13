"""
Reassign vendor/distributor products to real manufacturers using FLIS REFERENCE data.

The publog importer imported ALL REFERENCE rows without RNCC filtering. RNCC=3
means actual manufacturer; others (e.g. 5) are vendor/distributor entries. This
command uses a staging table + SQL joins to:

1. Load ~16M REFERENCE rows into a temp staging table
2. Identify products already correctly assigned (RNCC=3)
3. Reassign incorrect products to the real manufacturer via NIIN linkage
4. Disable orphan products whose NIIN has no RNCC=3 row

Usage:
    python manage.py disable_vendor_products --dry-run   # preview counts
    python manage.py disable_vendor_products              # apply changes
"""

import csv
import io
import time
import zipfile

from django.core.management.base import BaseCommand
from django.db import connection

from catalog.constants import DLA_DATA_DIR
from catalog.models import Manufacturer, Product
from catalog.models.entities import slugify_manufacturer
from catalog.services.name_formatter import format_manufacturer_name

REFERENCE_ZIP = DLA_DATA_DIR / "REFERENCE.zip"
CAGE_ZIP = DLA_DATA_DIR / "CAGE.zip"

STAGING_TABLE = "catalog_reference_staging"
BATCH_SIZE = 50_000


class Command(BaseCommand):
    help = "Reassign vendor products to real manufacturers via RNCC=3 REFERENCE data"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run", action="store_true",
            help="Preview counts without making changes",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        for path, label in [(REFERENCE_ZIP, "REFERENCE.zip"), (CAGE_ZIP, "CAGE.zip")]:
            if not path.exists():
                self.stderr.write(self.style.ERROR(f"{label} not found at {path}"))
                return

        try:
            self._create_staging_table()
            self._load_reference_data()
            correct, incorrect, reassigned, dupes, orphans = self._reassign_products(dry_run)

            self.stdout.write("")
            self.stdout.write(self.style.SUCCESS("=== Summary ==="))
            self.stdout.write(f"  Already correct:           {correct:,}")
            self.stdout.write(f"  Needed reassignment:       {incorrect:,}")
            self.stdout.write(f"    Reassigned to real mfr:  {reassigned:,}")
            self.stdout.write(f"    Skipped (already exists): {dupes:,}")
            self.stdout.write(f"    Orphan (deactivated):    {orphans:,}")

            if dry_run:
                self.stdout.write(self.style.WARNING("\nDRY RUN — no changes were made."))
        finally:
            self._drop_staging_table()

    # ── Step 1: Create staging table ──────────────────────────────────────

    def _create_staging_table(self):
        self.stdout.write("Step 1: Creating staging table...")
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
            cursor.execute(f"""
                CREATE TABLE {STAGING_TABLE} (
                    niin VARCHAR(9),
                    cage_code VARCHAR(5),
                    part_number VARCHAR(200),
                    rncc VARCHAR(2)
                )
            """)

    # ── Step 2: Bulk-load REFERENCE CSV ───────────────────────────────────

    def _load_reference_data(self):
        self.stdout.write("Step 2: Loading REFERENCE data into staging table...")
        t0 = time.time()
        total = 0

        with zipfile.ZipFile(REFERENCE_ZIP, "r") as zf:
            with zf.open("V_FLIS_PART.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                header = next(reader)
                i_niin = header.index("NIIN")
                i_cage = header.index("CAGE_CODE")
                i_pn = header.index("PART_NUMBER")
                i_rncc = header.index("RNCC")

                batch = []
                with connection.cursor() as cursor:
                    for row in reader:
                        niin = row[i_niin].strip()
                        cage = row[i_cage].strip()
                        pn = row[i_pn].strip()
                        rncc = row[i_rncc].strip()
                        if niin and cage and pn:
                            batch.append((niin, cage, pn, rncc))

                        if len(batch) >= BATCH_SIZE:
                            self._insert_batch(cursor, batch)
                            total += len(batch)
                            batch.clear()
                            if total % 1_000_000 == 0:
                                self.stdout.write(f"  ...{total:,} rows loaded")

                    if batch:
                        self._insert_batch(cursor, batch)
                        total += len(batch)

        elapsed = time.time() - t0
        self.stdout.write(self.style.SUCCESS(
            f"  Loaded {total:,} rows in {elapsed:.1f}s"
        ))

        # Create indexes after bulk load (faster than indexing during insert)
        self.stdout.write("  Creating indexes...")
        t0 = time.time()
        with connection.cursor() as cursor:
            cursor.execute(f"""
                CREATE INDEX idx_ref_staging_niin_rncc
                ON {STAGING_TABLE}(niin, rncc)
            """)
            cursor.execute(f"""
                CREATE INDEX idx_ref_staging_cage_pn_rncc
                ON {STAGING_TABLE}(cage_code, part_number, rncc)
            """)
        elapsed = time.time() - t0
        self.stdout.write(self.style.SUCCESS(f"  Indexes created in {elapsed:.1f}s"))

    def _insert_batch(self, cursor, batch):
        """Insert a batch using COPY for maximum speed."""
        buf = io.StringIO()
        for row in batch:
            # Escape tabs/newlines in part_number (other fields are short codes)
            niin, cage, pn, rncc = row
            pn = pn.replace("\t", " ").replace("\n", " ").replace("\r", " ").replace("\\", "\\\\")
            buf.write(f"{niin}\t{cage}\t{pn}\t{rncc}\n")
        buf.seek(0)
        cursor.copy_from(buf, STAGING_TABLE, columns=("niin", "cage_code", "part_number", "rncc"))

    # ── Steps 3-5: Reassign products ──────────────────────────────────────

    def _reassign_products(self, dry_run):
        self.stdout.write("Step 3: Identifying correct vs incorrect products...")
        t0 = time.time()

        with connection.cursor() as cursor:
            # Case A: exact (cage, pn) has RNCC=3
            cursor.execute(f"""
                SELECT COUNT(DISTINCT p.id)
                FROM catalog_product p
                JOIN catalog_organization m ON p.manufacturer_id = m.id
                JOIN {STAGING_TABLE} r
                    ON r.cage_code = m.cage_code
                    AND r.part_number = p.part_number
                    AND r.rncc = '3'
                WHERE p.source = 'publog'
            """)
            exact_correct = cursor.fetchone()[0]

            # Case C: exact (cage, pn) is NOT RNCC=3, but the cage IS a real
            # manufacturer for this NIIN (has a different RNCC=3 part number).
            # These are alternate part numbers — right manufacturer, leave alone.
            cursor.execute(f"""
                SELECT COUNT(DISTINCT p.id)
                FROM catalog_product p
                JOIN catalog_organization m ON p.manufacturer_id = m.id
                LEFT JOIN catalog_nsn nsn_rec ON nsn_rec.id = p.nsn_id
                LEFT JOIN {STAGING_TABLE} r_exact
                    ON r_exact.cage_code = m.cage_code
                    AND r_exact.part_number = p.part_number
                    AND r_exact.rncc = '3'
                JOIN {STAGING_TABLE} r_cage
                    ON r_cage.niin = nsn_rec.niin
                    AND r_cage.cage_code = m.cage_code
                    AND r_cage.rncc = '3'
                WHERE p.source = 'publog'
                    AND p.is_active = true
                    AND r_exact.niin IS NULL
            """)
            cage_correct = cursor.fetchone()[0]

            # Case B: wrong manufacturer — cage has NO RNCC=3 for this NIIN,
            # but another cage does. Find the real manufacturer via NIIN.
            cursor.execute(f"""
                SELECT
                    p.id AS product_id,
                    p.part_number AS old_pn,
                    nsn_rec.nsn,
                    m.cage_code AS old_cage,
                    r3.cage_code AS new_cage,
                    r3.part_number AS new_pn
                FROM catalog_product p
                JOIN catalog_organization m ON p.manufacturer_id = m.id
                LEFT JOIN catalog_nsn nsn_rec ON nsn_rec.id = p.nsn_id
                LEFT JOIN {STAGING_TABLE} r_exact
                    ON r_exact.cage_code = m.cage_code
                    AND r_exact.part_number = p.part_number
                    AND r_exact.rncc = '3'
                LEFT JOIN {STAGING_TABLE} r_cage
                    ON r_cage.niin = nsn_rec.niin
                    AND r_cage.cage_code = m.cage_code
                    AND r_cage.rncc = '3'
                JOIN {STAGING_TABLE} r3
                    ON r3.niin = nsn_rec.niin
                    AND r3.rncc = '3'
                WHERE p.source = 'publog'
                    AND p.is_active = true
                    AND r_exact.niin IS NULL
                    AND r_cage.niin IS NULL
            """)
            reassign_rows = cursor.fetchall()

            # Orphans: no RNCC=3 for this NIIN at all
            cursor.execute(f"""
                SELECT p.id
                FROM catalog_product p
                JOIN catalog_organization m ON p.manufacturer_id = m.id
                LEFT JOIN catalog_nsn nsn_rec ON nsn_rec.id = p.nsn_id
                LEFT JOIN {STAGING_TABLE} r_exact
                    ON r_exact.cage_code = m.cage_code
                    AND r_exact.part_number = p.part_number
                    AND r_exact.rncc = '3'
                LEFT JOIN {STAGING_TABLE} r_any
                    ON r_any.niin = nsn_rec.niin
                    AND r_any.rncc = '3'
                WHERE p.source = 'publog'
                    AND p.is_active = true
                    AND r_exact.niin IS NULL
                    AND r_any.niin IS NULL
            """)
            orphan_ids = [row[0] for row in cursor.fetchall()]

        correct = exact_correct + cage_correct
        elapsed = time.time() - t0
        self.stdout.write(self.style.SUCCESS(
            f"  Exact RNCC=3 match: {exact_correct:,}, "
            f"Same-cage alt part#: {cage_correct:,}, "
            f"To reassign: {len(reassign_rows):,}, "
            f"Orphans: {len(orphan_ids):,} ({elapsed:.1f}s)"
        ))

        if dry_run:
            # Print some examples
            if reassign_rows:
                self.stdout.write("\n  Sample reassignments:")
                for row in reassign_rows[:10]:
                    pid, old_pn, nsn, old_cage, new_cage, new_pn = row
                    self.stdout.write(
                        f"    Product {pid}: {old_cage}/{old_pn} -> {new_cage}/{new_pn} (NSN {nsn})"
                    )
            return correct, len(reassign_rows) + len(orphan_ids), 0, 0, len(orphan_ids)

        # Step 4: Execute reassignment
        self.stdout.write("Step 4: Reassigning products to real manufacturers...")
        t0 = time.time()

        # Load CAGE file for manufacturer names
        cage_names = self._load_cage_names()

        # Group by new_cage to batch manufacturer creation
        # For each product, pick the FIRST RNCC=3 match (deterministic via min cage)
        # Build a map: product_id -> (new_cage, new_pn)
        product_targets = {}
        for row in reassign_rows:
            pid, old_pn, nsn, old_cage, new_cage, new_pn = row
            # If multiple RNCC=3 rows for same NIIN, keep first encountered
            if pid not in product_targets:
                product_targets[pid] = (new_cage, new_pn)

        # Ensure all target manufacturers exist
        needed_cages = {cage for cage, pn in product_targets.values()}
        existing_cages = set(
            Manufacturer.objects.filter(cage_code__in=needed_cages)
            .values_list("cage_code", flat=True)
        )
        new_cages = needed_cages - existing_cages

        if new_cages:
            self.stdout.write(f"  Creating {len(new_cages):,} new manufacturer records...")
            for cage in sorted(new_cages):
                raw_name = cage_names.get(cage, "")
                mfr = Manufacturer(
                    cage_code=cage,
                    company_name=raw_name,
                )
                # Let save() handle slug generation + collision resolution
                mfr.save()

        # Build cage -> manufacturer id map
        cage_to_mfr_id = dict(
            Manufacturer.objects.filter(cage_code__in=needed_cages)
            .values_list("cage_code", "id")
        )

        # Check for duplicates: products that would collide on (manufacturer, part_number)
        # after reassignment
        existing_pairs = set()
        check_pairs = [(cage_to_mfr_id[cage], pn) for cage, pn in product_targets.values()
                       if cage in cage_to_mfr_id]
        if check_pairs:
            # Query existing products for these (manufacturer_id, part_number) combos
            # Do in batches to avoid huge IN clauses
            for i in range(0, len(check_pairs), 5000):
                batch = check_pairs[i:i+5000]
                with connection.cursor() as cursor:
                    # Build WHERE clause
                    conditions = " OR ".join(
                        f"(manufacturer_id = {mid} AND part_number = %s)"
                        for mid, pn in batch
                    )
                    params = [pn for mid, pn in batch]
                    cursor.execute(
                        f"SELECT manufacturer_id, part_number FROM catalog_product "
                        f"WHERE {conditions}", params
                    )
                    for row in cursor.fetchall():
                        existing_pairs.add((row[0], row[1]))

        # Execute updates
        reassigned = 0
        skipped = 0
        update_batch = []  # (product_id, new_manufacturer_id, new_part_number)

        for pid, (new_cage, new_pn) in product_targets.items():
            new_mfr_id = cage_to_mfr_id.get(new_cage)
            if not new_mfr_id:
                continue

            if (new_mfr_id, new_pn) in existing_pairs:
                # Target (manufacturer, part_number) already exists — leave as-is
                skipped += 1
            else:
                reassigned += 1
                update_batch.append((pid, new_mfr_id, new_pn))
                existing_pairs.add((new_mfr_id, new_pn))

        # Apply reassignments in SQL batches
        with connection.cursor() as cursor:
            for i in range(0, len(update_batch), 5000):
                batch = update_batch[i:i+5000]
                # Use a VALUES list + UPDATE FROM for efficiency
                values = ", ".join(
                    cursor.mogrify("(%s, %s, %s)", (pid, mid, pn)).decode()
                    for pid, mid, pn in batch
                )
                cursor.execute(f"""
                    UPDATE catalog_product AS p
                    SET manufacturer_id = v.new_mfr_id,
                        part_number = v.new_pn,
                        part_number_slug = ''
                    FROM (VALUES {values}) AS v(pid, new_mfr_id, new_pn)
                    WHERE p.id = v.pid
                """)
                if (i + 5000) % 50_000 == 0:
                    self.stdout.write(f"  ...{i + len(batch):,} reassigned")

            # Deactivate orphans
            if orphan_ids:
                for i in range(0, len(orphan_ids), 5000):
                    batch = orphan_ids[i:i+5000]
                    cursor.execute(
                        "UPDATE catalog_product SET is_active = false WHERE id = ANY(%s)",
                        [batch],
                    )

        # Fix part_number_slug for reassigned products
        if update_batch:
            self.stdout.write("  Regenerating part_number_slug for reassigned products...")
            pids = [pid for pid, _, _ in update_batch]
            for i in range(0, len(pids), 5000):
                batch = pids[i:i+5000]
                from catalog.models.catalog import slugify_part_number
                products = Product.objects.filter(id__in=batch).only("id", "part_number", "part_number_slug")
                to_update = []
                for p in products:
                    p.part_number_slug = slugify_part_number(p.part_number)
                    to_update.append(p)
                Product.objects.bulk_update(to_update, ["part_number_slug"], batch_size=5000)

        elapsed = time.time() - t0
        self.stdout.write(self.style.SUCCESS(
            f"  Reassigned: {reassigned:,}, Skipped (already exists): {skipped:,}, "
            f"Orphans deactivated: {len(orphan_ids):,} ({elapsed:.1f}s)"
        ))

        return correct, len(reassign_rows) + len(orphan_ids), reassigned, skipped, len(orphan_ids)

    # ── Step 7: Drop staging table ────────────────────────────────────────

    def _drop_staging_table(self):
        self.stdout.write("Step 7: Dropping staging table...")
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
        self.stdout.write(self.style.SUCCESS("  Done."))

    # ── Helpers ───────────────────────────────────────────────────────────

    def _load_cage_names(self):
        """Load CAGE_CODE -> COMPANY mapping from CAGE.zip."""
        self.stdout.write("  Loading CAGE names...")
        names = {}
        with zipfile.ZipFile(CAGE_ZIP, "r") as zf:
            with zf.open("P_CAGE.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                header = next(reader)
                i_cage = header.index("CAGE_CODE")
                i_company = header.index("COMPANY")
                for row in reader:
                    cage = row[i_cage].strip()
                    company = row[i_company].strip()
                    if cage and company:
                        names[cage] = company
        self.stdout.write(f"  Loaded {len(names):,} CAGE names")
        return names
