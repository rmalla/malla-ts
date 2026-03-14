"""
Deduplicate products to keep 1 per NSN.

Two-step process:
  Step 1 — Merge duplicate manufacturer variants: same NSN + same part_number
           but different manufacturer IDs (name spelling variants). Keep the one
           whose manufacturer has the most products overall.

  Step 2 — Keep 1 product per NSN: for NSNs that still have multiple products,
           keep the "best" one (has price > prefer more products > lowest id).

Usage:
    python manage.py deduplicate_products                     # dry-run both
    python manage.py deduplicate_products --step 1            # preview merge only
    python manage.py deduplicate_products --execute           # run both steps
    python manage.py deduplicate_products --execute --step 1  # merge only
"""

import time

from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "Deduplicate products: merge manufacturer variants, then keep 1 per NSN"

    def add_arguments(self, parser):
        parser.add_argument(
            "--execute", action="store_true",
            help="Actually disable duplicates (default is dry-run)",
        )
        parser.add_argument(
            "--step", choices=["1", "2", "both"], default="both",
            help="Run just step 1 (merge), step 2 (keep-1), or both (default)",
        )

    def handle(self, *args, **options):
        execute = options["execute"]
        step = options["step"]
        mode = "EXECUTE" if execute else "DRY RUN"

        total_disabled = 0

        if step in ("1", "both"):
            count = self._step1_merge_manufacturer_variants(execute)
            total_disabled += count

        if step in ("2", "both"):
            count = self._step2_keep_one_per_nsn(execute)
            total_disabled += count

        self.stdout.write("")
        action = "Disabled" if execute else "Would disable"
        self.stdout.write(self.style.SUCCESS(
            f"Total: {action} {total_disabled:,} products  [{mode}]"
        ))

    def _step1_merge_manufacturer_variants(self, execute):
        """Disable duplicate products that share NSN + part_number but have
        different manufacturer IDs (spelling variants of the same company)."""
        self.stdout.write("")
        self.stdout.write(self.style.MIGRATE_HEADING(
            "Step 1 — Merge duplicate manufacturer variants"
        ))
        t0 = time.time()

        with connection.cursor() as cursor:
            # Find groups of (nsn_id, part_number) with multiple manufacturers.
            # For each group, the manufacturer with the most total products wins;
            # ties broken by lowest manufacturer id.
            # We disable every product in the group EXCEPT the winner.
            #
            # Subquery: for each product in a multi-mfr group, rank by
            # (mfr_product_count DESC, manufacturer_id ASC). Rank > 1 = disable.
            cursor.execute("""
                WITH mfr_counts AS (
                    SELECT manufacturer_id, COUNT(*) AS prod_count
                    FROM catalog_product
                    WHERE is_active = 0
                    GROUP BY manufacturer_id
                ),
                dupes AS (
                    SELECT p.id,
                           p.nsn_id,
                           p.part_number,
                           p.manufacturer_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY p.nsn_id, p.part_number
                               ORDER BY mc.prod_count DESC, p.manufacturer_id ASC
                           ) AS rn
                    FROM catalog_product p
                    JOIN mfr_counts mc ON mc.manufacturer_id = p.manufacturer_id
                    WHERE p.is_active = 0
                      AND p.nsn_id IS NOT NULL
                      AND p.part_number != ''
                ),
                groups_with_dupes AS (
                    SELECT nsn_id, part_number
                    FROM dupes
                    GROUP BY nsn_id, part_number
                    HAVING COUNT(DISTINCT manufacturer_id) > 1
                )
                SELECT d.id
                FROM dupes d
                JOIN groups_with_dupes g
                    ON g.nsn_id = d.nsn_id AND g.part_number = d.part_number
                WHERE d.rn > 1
            """)
            ids_to_disable = [row[0] for row in cursor.fetchall()]

            # Count affected NSNs for reporting
            cursor.execute("""
                SELECT COUNT(DISTINCT sub.nsn_id)
                FROM (
                    SELECT p.nsn_id
                    FROM catalog_product p
                    WHERE p.is_active = 0
                      AND p.nsn_id IS NOT NULL
                      AND p.part_number != ''
                    GROUP BY p.nsn_id, p.part_number
                    HAVING COUNT(DISTINCT p.manufacturer_id) > 1
                ) sub
            """)
            nsn_count = cursor.fetchone()[0]

        elapsed = time.time() - t0
        self.stdout.write(f"  NSNs with duplicate mfr name variants: {nsn_count:,}")
        self.stdout.write(f"  Products to disable: {len(ids_to_disable):,}")
        self.stdout.write(f"  (query took {elapsed:.1f}s)")

        if execute and ids_to_disable:
            t0 = time.time()
            self._disable_products(ids_to_disable)
            elapsed = time.time() - t0
            self.stdout.write(self.style.SUCCESS(
                f"  Disabled {len(ids_to_disable):,} products ({elapsed:.1f}s)"
            ))
        elif not execute:
            self.stdout.write(self.style.WARNING("  [DRY RUN]"))

        return len(ids_to_disable)

    def _step2_keep_one_per_nsn(self, execute):
        """For each NSN with multiple active products, keep the best one and
        disable the rest.

        Selection criteria (in order):
          1. Has price > 0
          2. Manufacturer with the most total products
          3. Lowest product id (earliest imported)
        """
        self.stdout.write("")
        self.stdout.write(self.style.MIGRATE_HEADING(
            "Step 2 — Keep 1 product per NSN"
        ))
        t0 = time.time()

        with connection.cursor() as cursor:
            cursor.execute("""
                WITH mfr_counts AS (
                    SELECT manufacturer_id, COUNT(*) AS prod_count
                    FROM catalog_product
                    WHERE is_active = 0
                    GROUP BY manufacturer_id
                ),
                ranked AS (
                    SELECT p.id,
                           p.nsn_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY p.nsn_id
                               ORDER BY
                                   (CASE WHEN p.price > 0 THEN 0 ELSE 1 END),
                                   mc.prod_count DESC,
                                   p.id ASC
                           ) AS rn
                    FROM catalog_product p
                    JOIN mfr_counts mc ON mc.manufacturer_id = p.manufacturer_id
                    WHERE p.is_active = 0
                      AND p.nsn_id IS NOT NULL
                ),
                multi_nsns AS (
                    SELECT nsn_id
                    FROM catalog_product
                    WHERE is_active = 0 AND nsn_id IS NOT NULL
                    GROUP BY nsn_id
                    HAVING COUNT(*) > 1
                )
                SELECT r.id
                FROM ranked r
                JOIN multi_nsns mn ON mn.nsn_id = r.nsn_id
                WHERE r.rn > 1
            """)
            ids_to_disable = [row[0] for row in cursor.fetchall()]

            cursor.execute("""
                SELECT COUNT(*)
                FROM (
                    SELECT nsn_id
                    FROM catalog_product
                    WHERE is_active = 0 AND nsn_id IS NOT NULL
                    GROUP BY nsn_id
                    HAVING COUNT(*) > 1
                ) sub
            """)
            nsn_count = cursor.fetchone()[0]

        elapsed = time.time() - t0
        self.stdout.write(f"  NSNs with multiple products remaining: {nsn_count:,}")
        self.stdout.write(f"  Products to disable: {len(ids_to_disable):,}")
        self.stdout.write(f"  (query took {elapsed:.1f}s)")

        if execute and ids_to_disable:
            t0 = time.time()
            self._disable_products(ids_to_disable)
            elapsed = time.time() - t0
            self.stdout.write(self.style.SUCCESS(
                f"  Disabled {len(ids_to_disable):,} products ({elapsed:.1f}s)"
            ))
        elif not execute:
            self.stdout.write(self.style.WARNING("  [DRY RUN]"))

        return len(ids_to_disable)

    def _disable_products(self, product_ids):
        """Set is_active = -1 (DISABLED) for the given product IDs."""
        batch_size = 5000
        with connection.cursor() as cursor:
            for i in range(0, len(product_ids), batch_size):
                batch = product_ids[i:i + batch_size]
                cursor.execute(
                    "UPDATE catalog_product SET is_active = -1 WHERE id = ANY(%s)",
                    [batch],
                )
