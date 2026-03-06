"""
Build a SQLite index from REFERENCE.zip and CAGE.zip for fast publog lookups.

One-time operation (~3 min). Re-run only when source ZIPs are updated.
The publog importer auto-detects this index and uses it for Phases 3+4.

Usage:
    python manage.py build_publog_index
    python manage.py build_publog_index --force   # rebuild from scratch
"""

import csv
import io
import sqlite3
import time
import zipfile

from django.core.management.base import BaseCommand

from catalog.constants import DLA_DATA_DIR

REFERENCE_ZIP = DLA_DATA_DIR / "REFERENCE.zip"
CAGE_ZIP = DLA_DATA_DIR / "CAGE.zip"
INDEX_DB = DLA_DATA_DIR / "publog_index.db"

BATCH_SIZE = 50000


class Command(BaseCommand):
    help = "Build SQLite index from REFERENCE.zip and CAGE.zip for fast publog imports"

    def add_arguments(self, parser):
        parser.add_argument(
            "--force", action="store_true",
            help="Delete existing index and rebuild from scratch",
        )

    def handle(self, *args, **options):
        force = options.get("force", False)

        if INDEX_DB.exists() and not force:
            self.stdout.write(self.style.WARNING(
                f"Index already exists at {INDEX_DB}\n"
                "Use --force to rebuild."
            ))
            return

        for zp in (REFERENCE_ZIP, CAGE_ZIP):
            if not zp.exists():
                self.stderr.write(self.style.ERROR(f"{zp.name} not found at {zp}"))
                return

        # Remove existing index
        if INDEX_DB.exists():
            INDEX_DB.unlink()

        t0 = time.time()

        conn = sqlite3.connect(str(INDEX_DB))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")

        try:
            self._build_reference(conn)
            self._build_cage(conn)
        except Exception:
            conn.close()
            if INDEX_DB.exists():
                INDEX_DB.unlink()
            raise

        conn.close()

        elapsed = time.time() - t0
        size_mb = INDEX_DB.stat().st_size / (1024 * 1024)
        self.stdout.write(self.style.SUCCESS(
            f"\nIndex built in {elapsed:.1f}s — {size_mb:.0f} MB at {INDEX_DB}"
        ))

    def _build_reference(self, conn):
        self.stdout.write("Phase 1: Extracting V_FLIS_PART.CSV → reference table...")
        t0 = time.time()

        conn.execute("DROP TABLE IF EXISTS reference")
        conn.execute(
            "CREATE TABLE reference (niin TEXT, cage_code TEXT, part_number TEXT)"
        )

        zf = zipfile.ZipFile(REFERENCE_ZIP, "r")
        row_count = 0
        try:
            with zf.open("V_FLIS_PART.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                header = next(reader)
                i_niin = header.index("NIIN")
                i_cage = header.index("CAGE_CODE")
                i_pn = header.index("PART_NUMBER")
                i_rncc = header.index("RNCC")

                batch = []
                for row in reader:
                    # Only index manufacturer references (RNCC=3),
                    # skip vendor/distributor entries (RNCC=5, etc.)
                    if row[i_rncc].strip() != "3":
                        continue

                    niin = row[i_niin].strip()
                    cage = row[i_cage].strip()
                    if not niin or not cage or len(cage) != 5:
                        continue

                    batch.append((niin.zfill(9), cage, row[i_pn].strip()))
                    row_count += 1

                    if len(batch) >= BATCH_SIZE:
                        conn.executemany(
                            "INSERT INTO reference VALUES (?, ?, ?)", batch
                        )
                        batch.clear()

                        if row_count % 2_000_000 == 0:
                            self.stdout.write(f"  {row_count:,} rows inserted...")

                if batch:
                    conn.executemany(
                        "INSERT INTO reference VALUES (?, ?, ?)", batch
                    )
        finally:
            zf.close()

        self.stdout.write(f"  Creating index on reference.niin...")
        conn.execute("CREATE INDEX idx_reference_niin ON reference(niin)")
        conn.commit()

        elapsed = time.time() - t0
        self.stdout.write(self.style.SUCCESS(
            f"  Reference: {row_count:,} rows in {elapsed:.1f}s"
        ))

    def _build_cage(self, conn):
        self.stdout.write("Phase 2: Extracting P_CAGE.CSV → cage table...")
        t0 = time.time()

        conn.execute("DROP TABLE IF EXISTS cage")
        conn.execute(
            "CREATE TABLE cage ("
            "cage_code TEXT PRIMARY KEY, company TEXT, "
            "city TEXT, state TEXT, zip TEXT, country TEXT)"
        )

        zf = zipfile.ZipFile(CAGE_ZIP, "r")
        row_count = 0
        try:
            with zf.open("P_CAGE.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                header = next(reader)
                i_cage = header.index("CAGE_CODE")
                i_company = header.index("COMPANY")
                i_city = header.index("CITY")
                i_state = header.index("STATE_PROVINCE")
                i_zip = header.index("ZIP_POSTAL_ZONE")
                i_country = header.index("COUNTRY")

                batch = []
                for row in reader:
                    cage = row[i_cage].strip()
                    if not cage:
                        continue

                    batch.append((
                        cage,
                        row[i_company].strip(),
                        row[i_city].strip(),
                        row[i_state].strip(),
                        row[i_zip].strip(),
                        row[i_country].strip(),
                    ))
                    row_count += 1

                    if len(batch) >= BATCH_SIZE:
                        conn.executemany(
                            "INSERT OR IGNORE INTO cage VALUES (?, ?, ?, ?, ?, ?)",
                            batch,
                        )
                        batch.clear()

                if batch:
                    conn.executemany(
                        "INSERT OR IGNORE INTO cage VALUES (?, ?, ?, ?, ?, ?)",
                        batch,
                    )
        finally:
            zf.close()

        conn.commit()

        elapsed = time.time() - t0
        self.stdout.write(self.style.SUCCESS(
            f"  CAGE: {row_count:,} rows in {elapsed:.1f}s"
        ))
