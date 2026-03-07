#!/usr/bin/env python3
"""
Bulk-load FLISV.CSV into catalog_flisv_characteristic via PostgreSQL COPY.

Usage:
    python scripts/load_flisv.py

Reads DB credentials from environment (DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
with the same defaults as Django settings.
"""

import csv
import io
import os
import sys
import tempfile
import time
import zipfile

import psycopg2

# ── Config ────────────────────────────────────────────────────────────────

FLISV_ZIP = "/var/www/html/malla-ts.com/imports/dla/FLISV.zip"
CSV_NAME = "FLISV.CSV"
TABLE = "catalog_flisv_characteristic"

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "mallats_db"),
    "user": os.getenv("DB_USER", "mallats_user"),
    "password": os.getenv("DB_PASSWORD", ""),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

LOG_EVERY = 5_000_000


def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def main():
    t_start = time.time()

    if not os.path.exists(FLISV_ZIP):
        log(f"ERROR: {FLISV_ZIP} not found")
        sys.exit(1)

    log(f"Connecting to PostgreSQL ({DB_CONFIG['dbname']}@{DB_CONFIG['host']})...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    # Step 1: Truncate
    log(f"Truncating {TABLE}...")
    cur.execute(f"TRUNCATE TABLE {TABLE}")

    # Step 2: Drop non-PK indexes for faster loading
    log("Dropping indexes...")
    cur.execute("""
        SELECT indexname FROM pg_indexes
        WHERE tablename = %s AND indexname != %s
    """, [TABLE, f"{TABLE}_pkey"])
    indexes = [row[0] for row in cur.fetchall()]
    for idx in indexes:
        log(f"  Dropping {idx}")
        cur.execute(f'DROP INDEX IF EXISTS "{idx}"')
    log(f"  Dropped {len(indexes)} indexes")

    # Step 3: Extract CSV → temp TSV (only columns we need)
    log(f"Extracting {CSV_NAME} from {FLISV_ZIP}...")
    t_extract = time.time()

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".tsv")
    row_count = 0
    skipped = 0

    try:
        with os.fdopen(tmp_fd, "w") as tmp, zipfile.ZipFile(FLISV_ZIP, "r") as zf:
            with zf.open(CSV_NAME) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                next(reader)  # skip header

                for row in reader:
                    niin = row[0].strip().zfill(9)
                    mrc = row[1].strip()
                    mode_code = row[2].strip()
                    coded_reply = row[6].strip()

                    if not mrc or not coded_reply:
                        skipped += 1
                        continue

                    # Escape tabs/newlines in coded_reply for TSV
                    coded_reply = coded_reply.replace("\\", "\\\\").replace("\t", " ").replace("\n", " ")

                    tmp.write(f"{niin}\t{mrc}\t{mode_code}\t{coded_reply}\n")
                    row_count += 1

                    if row_count % LOG_EVERY == 0:
                        elapsed = time.time() - t_extract
                        rate = row_count / elapsed
                        log(f"  Extracted {row_count:,} rows ({rate:,.0f} rows/s)")

        elapsed = time.time() - t_extract
        log(f"  Done: {row_count:,} rows extracted, {skipped:,} skipped in {elapsed:.1f}s")
        tmp_size_mb = os.path.getsize(tmp_path) / (1024 * 1024)
        log(f"  Temp file: {tmp_path} ({tmp_size_mb:.0f} MB)")

        # Step 4: COPY FROM temp file
        log("Loading via COPY FROM STDIN...")
        t_copy = time.time()

        with open(tmp_path, "r") as f:
            cur.copy_expert(
                f"COPY {TABLE} (niin, mrc, mode_code, coded_reply) FROM STDIN",
                f,
            )

        elapsed = time.time() - t_copy
        log(f"  COPY completed in {elapsed:.1f}s ({row_count / elapsed:,.0f} rows/s)")

    finally:
        # Clean up temp file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
            log(f"  Cleaned up {tmp_path}")

    # Step 5: Recreate index
    log("Creating index on niin...")
    t_idx = time.time()
    cur.execute(f"CREATE INDEX {TABLE}_niin_idx ON {TABLE} (niin)")
    elapsed = time.time() - t_idx
    log(f"  Index created in {elapsed:.1f}s")

    # Step 6: ANALYZE
    log("Running ANALYZE...")
    t_analyze = time.time()
    cur.execute(f"ANALYZE {TABLE}")
    elapsed = time.time() - t_analyze
    log(f"  ANALYZE in {elapsed:.1f}s")

    # Step 7: Verify
    cur.execute(f"SELECT count(*) FROM {TABLE}")
    final_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    total = time.time() - t_start
    log(f"DONE: {final_count:,} rows in {TABLE} — total time {total:.1f}s")


if __name__ == "__main__":
    main()
