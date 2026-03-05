"""
PUB LOG Importer — reads DLA FLIS PUB LOG zip files to build a full
product catalog with prices, manufacturers, and part numbers.

Data files (in /imports/):
  MANAGEMENT.zip   → V_FLIS_MANAGEMENT.CSV  (12.7M rows) — prices
  CAGE.zip         → P_CAGE.CSV             (~800K rows)  — manufacturers
  IDENTIFICATION.zip → P_FLIS_NSN.CSV       (~6M rows)    — item names
  REFERENCE.zip    → V_FLIS_PART.CSV        (~20M rows)   — part numbers

Strategy: price-first filtering — scan MANAGEMENT for NIINs in the
$500–$45,000 range, then only process matching rows from other files.
Uses csv.reader (not DictReader) for ~2x throughput on large files.

Gentle mode: streams reference rows through a temp file instead of
holding them all in memory, and throttles DB writes with sleeps.
"""

import csv
import gc
import io
import logging
import sqlite3
import tempfile
import time
import zipfile
from decimal import Decimal, InvalidOperation

from django.db import connection, transaction

from catalog.constants import DLA_DATA_DIR, JobType, LogLevel
from catalog.models import CatalogItem, Organization
from catalog.models.catalog import SupplierLink, Product, CatalogPricing, slugify_part_number, DataSource
from catalog.models.entities import slugify_manufacturer, OrganizationProfile
from catalog.services.name_formatter import format_manufacturer_name, format_nomenclature
from .base import BaseImporter

logger = logging.getLogger(__name__)

MANAGEMENT_ZIP = DLA_DATA_DIR / "MANAGEMENT.zip"
CAGE_ZIP = DLA_DATA_DIR / "CAGE.zip"
IDENTIFICATION_ZIP = DLA_DATA_DIR / "IDENTIFICATION.zip"
REFERENCE_ZIP = DLA_DATA_DIR / "REFERENCE.zip"
PUBLOG_INDEX_DB = DLA_DATA_DIR / "publog_index.db"

BATCH_SIZE = 2000
SLEEP_BETWEEN_BATCHES = 0.1   # seconds — let DB and OS breathe
SLEEP_BETWEEN_PHASES = 2.0    # seconds — allow GC + cache flush

MIN_PRICE = Decimal("500")
MAX_PRICE = Decimal("45000")


class NullClient:
    api_calls_made = 0


def format_nsn(fsc, niin):
    """Format FSC + NIIN into standard NSN: XXXX-XX-XXX-XXXX."""
    fsc = str(fsc).strip().zfill(4)
    niin = str(niin).strip().zfill(9)
    return f"{fsc}-{niin[:2]}-{niin[2:5]}-{niin[5:]}"


def safe_decimal(val):
    if val is None or val == "":
        return None
    try:
        d = Decimal(str(val).strip())
        if d.is_nan() or d.is_infinite():
            return None
        return d
    except (ValueError, InvalidOperation):
        return None


def _col_index(header, name):
    """Get column index by name, raise if missing."""
    try:
        return header.index(name)
    except ValueError:
        raise ValueError(f"Column '{name}' not found in header: {header}")


def _phase_pause(log_fn, phase_name, skip_sleep=False):
    """GC + sleep between phases to release memory."""
    gc.collect()
    connection.close()
    if skip_sleep:
        log_fn(f"  [{phase_name}] Memory released")
    else:
        log_fn(f"  [{phase_name}] Memory released, pausing {SLEEP_BETWEEN_PHASES}s...")
        time.sleep(SLEEP_BETWEEN_PHASES)


class PUBLOGImporter(BaseImporter):
    """Import PUB LOG data from DLA FLIS Electronic Reading Room."""

    job_type = JobType.PUBLOG_IMPORT

    def __init__(self, stdout=None):
        super().__init__(client=NullClient(), stdout=stdout)
        self.last_mgmt_row = 0

    def run(self, limit=None, skip_suppliers=False, skip_rows=0, **kwargs):
        # Verify required zip files exist
        for zp in (MANAGEMENT_ZIP, IDENTIFICATION_ZIP):
            if not zp.exists():
                raise FileNotFoundError(f"{zp.name} not found at {zp}")

        # Phase 1: Scan prices → eligible NIIN set
        self.log("Phase 1: Scanning V_FLIS_MANAGEMENT.CSV for eligible prices...")
        price_map = self._scan_prices(limit=limit, skip_rows=skip_rows)
        self.log(f"Phase 1 complete: {len(price_map):,} eligible NIINs ($500–$45K)")

        if not price_map:
            self.log("No new eligible NIINs found — all caught up.")
            return

        price_map_size = len(price_map)
        eligible_niins = set(price_map.keys())
        small_batch = price_map_size <= 1000
        _phase_pause(self.log, "Phase 1→2", skip_sleep=small_batch)

        # Phase 2: Import identification → CatalogItem records
        self.log("Phase 2: Importing P_FLIS_NSN.CSV (catalog entries)...")
        nsn_created, nsn_errored = self._import_identification(eligible_niins, price_map)
        self.log(f"Phase 2 complete: {nsn_created:,} catalog entries created, {nsn_errored:,} errored")

        # Free price_map — no longer needed
        del price_map
        _phase_pause(self.log, "Phase 2→3", skip_sleep=small_batch)

        cage_created = 0
        sup_created = 0
        sup_errored = 0

        if not skip_suppliers:
            for zp in (REFERENCE_ZIP, CAGE_ZIP):
                if not zp.exists():
                    raise FileNotFoundError(f"{zp.name} not found at {zp}")

            # Phase 3: Scan references → stream to temp file + collect CAGE codes
            self.log("Phase 3: Scanning V_FLIS_PART.CSV (streaming to temp file)...")
            needed_cages, ref_file, ref_count = self._scan_references_to_file(eligible_niins)
            self.log(f"Phase 3 complete: {ref_count:,} supplier rows streamed, {len(needed_cages):,} unique CAGE codes")

            # Free eligible_niins before loading CAGE data
            del eligible_niins
            _phase_pause(self.log, "Phase 3→4", skip_sleep=small_batch)

            # Phase 4: Import only needed CAGE codes → Organization records
            self.log("Phase 4: Importing needed CAGE codes from P_CAGE.CSV...")
            cage_created = self._import_cage(needed_cages)
            self.log(f"Phase 4 complete: {cage_created:,} manufacturers created")

            del needed_cages
            _phase_pause(self.log, "Phase 4→5", skip_sleep=small_batch)

            # Phase 5: Create SupplierLink + Product records by streaming from temp file
            self.log("Phase 5: Creating supplier/product links (streaming from temp file)...")
            sup_created, sup_errored = self._create_suppliers_streaming(ref_file)
            self.log(f"Phase 5 complete: {sup_created:,} supplier links created, {sup_errored:,} errored")

            # Clean up temp file
            try:
                ref_file.close()
            except Exception:
                pass
        else:
            self.log("Skipping supplier/product linking (--skip-suppliers)")

        self.job.records_created = nsn_created + sup_created + cage_created
        self.job.records_errored = nsn_errored + sup_errored
        self.job.records_fetched = price_map_size
        self.job.save(update_fields=[
            "records_created", "records_errored", "records_fetched",
        ])

    # ── Phase 1: Scan prices ────────────────────────────────────────────

    def _scan_prices(self, limit=None, skip_rows=0):
        """Read V_FLIS_MANAGEMENT.CSV, return {niin: (price, unit_of_issue)} for eligible rows.

        Always incremental: skips NIINs already in the catalog.
        skip_rows: skip this many data rows before scanning (resume point).
        """
        price_map = {}
        row_count = 0

        # Always load existing NIINs for incremental behavior
        existing_niins = set()
        for nsn in CatalogItem.objects.values_list("nsn", flat=True).iterator(chunk_size=10000):
            parts = nsn.split("-")
            if len(parts) == 4:
                existing_niins.add(parts[1] + parts[2] + parts[3])
        self.log(f"  MANAGEMENT: {len(existing_niins):,} NIINs already in catalog, will skip")

        zf = zipfile.ZipFile(MANAGEMENT_ZIP, "r")
        try:
            with zf.open("V_FLIS_MANAGEMENT.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                header = next(reader)
                i_niin = _col_index(header, "NIIN")
                i_price = _col_index(header, "UNIT_PRICE")
                i_ui = _col_index(header, "UI")

                # Fast-skip rows already processed in previous rounds
                if skip_rows > 0:
                    self.log(f"  MANAGEMENT: skipping first {skip_rows:,} rows...")
                    for _ in range(skip_rows):
                        next(reader, None)
                        row_count += 1

                for row in reader:
                    row_count += 1

                    niin = row[i_niin].strip()
                    if not niin:
                        continue

                    price = safe_decimal(row[i_price])
                    if not price or price < MIN_PRICE or price > MAX_PRICE:
                        continue

                    ui = row[i_ui].strip()
                    niin_padded = niin.zfill(9)

                    # Always skip existing — incremental
                    if niin_padded in existing_niins:
                        continue

                    existing = price_map.get(niin_padded)
                    if not existing or price > existing[0]:
                        price_map[niin_padded] = (price, ui)

                    if limit and len(price_map) >= limit:
                        break

                    if row_count % 2_000_000 == 0:
                        self.log(
                            f"  MANAGEMENT: scanned {row_count:,} rows, "
                            f"{len(price_map):,} eligible NIINs so far"
                        )
        finally:
            zf.close()

        self.last_mgmt_row = row_count
        self.log(f"  MANAGEMENT: {row_count:,} rows scanned, {len(price_map):,} new NIINs found")
        return price_map

    # ── Phase 2: Import identification → CatalogItem ──────────────────────

    def _import_identification(self, eligible_niins, price_map):
        """Read P_FLIS_NSN.CSV, create CatalogItem for eligible NIINs."""
        created = 0
        errored = 0
        filtered = 0
        row_count = 0
        remaining = set(eligible_niins)

        from home.models import FederalSupplyClass

        existing_nsns = set(
            CatalogItem.objects.values_list("nsn", flat=True).iterator(chunk_size=10000)
        )
        fsc_map = {f.code: f for f in FederalSupplyClass.objects.all()}

        batch = []
        pricing_batch = []

        zf = zipfile.ZipFile(IDENTIFICATION_ZIP, "r")
        try:
            with zf.open("P_FLIS_NSN.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                header = next(reader)
                i_fsc = _col_index(header, "FSC")
                i_niin = _col_index(header, "NIIN")
                i_name = _col_index(header, "ITEM_NAME")

                for row in reader:
                    row_count += 1

                    try:
                        niin = row[i_niin].strip()
                        if not niin:
                            continue

                        niin_padded = niin.zfill(9)
                        if niin_padded not in remaining:
                            continue

                        remaining.discard(niin_padded)

                        fsc = row[i_fsc].strip()
                        item_name = row[i_name].strip()

                        if not fsc:
                            continue
                        if item_name == "NO ITEM NAME AVAILABLE":
                            item_name = ""

                        nsn = format_nsn(fsc, niin_padded)
                        if nsn in existing_nsns:
                            continue

                        # Pipeline filters: FSC, NSN, nomenclature, price
                        if self.filter_service:
                            result = self.filter_service.check_nsn(nsn, nomenclature=item_name)
                            if result:
                                filtered += 1
                                continue
                            price_data_check = price_map.get(niin_padded)
                            if price_data_check:
                                result = self.filter_service.check_unit_price(price_data_check[0])
                                if result:
                                    filtered += 1
                                    continue

                        price_data = price_map.get(niin_padded)
                        unit_price = price_data[0] if price_data else None
                        ui = price_data[1] if price_data else ""

                        batch.append(CatalogItem(
                            nsn=nsn,
                            nomenclature=item_name,
                            unit_of_issue=ui,
                            fsc=fsc_map.get(fsc),
                        ))
                        existing_nsns.add(nsn)

                        # Track pricing data for CatalogPricing creation after bulk_create
                        if unit_price:
                            pricing_batch.append((len(batch) - 1, unit_price))

                    except Exception:
                        errored += 1
                        continue

                    if len(batch) >= BATCH_SIZE:
                        c, e = self._flush_nsn_batch(batch, pricing_batch)
                        created += c
                        errored += e
                        batch.clear()
                        pricing_batch.clear()
                        time.sleep(SLEEP_BETWEEN_BATCHES)

                    if row_count % 1_000_000 == 0:
                        self.log(
                            f"  IDENTIFICATION: {row_count:,} rows, "
                            f"{created:,} created, {len(remaining):,} remaining"
                        )

                    if not remaining:
                        self.log(f"  IDENTIFICATION: all NIINs found at row {row_count:,}")
                        break
        finally:
            zf.close()

        if batch:
            c, e = self._flush_nsn_batch(batch, pricing_batch)
            created += c
            errored += e

        if filtered:
            self.log(f"  IDENTIFICATION: {filtered:,} filtered by pipeline rules")
        self.log(f"  IDENTIFICATION: {row_count:,} rows processed")
        return created, errored

    def _flush_nsn_batch(self, batch, pricing_batch=None):
        if not batch:
            return 0, 0
        try:
            with transaction.atomic():
                CatalogItem.objects.bulk_create(batch, ignore_conflicts=True)
                # bulk_create with ignore_conflicts doesn't set PKs on objects,
                # so query them back by NSN to create CatalogPricing records.
                if pricing_batch:
                    nsns = [batch[idx].nsn for idx, _ in pricing_batch]
                    nsn_to_pk = dict(
                        CatalogItem.objects.filter(nsn__in=nsns)
                        .values_list("nsn", "pk")
                    )
                    pricing_objects = []
                    for idx, unit_price in pricing_batch:
                        pk = nsn_to_pk.get(batch[idx].nsn)
                        if pk:
                            pricing_objects.append(CatalogPricing(
                                catalog_item_id=pk,
                                unit_price=unit_price,
                                publog_price=unit_price,
                                unit_price_source=DataSource.PUBLOG,
                            ))
                    if pricing_objects:
                        CatalogPricing.objects.bulk_create(pricing_objects, ignore_conflicts=True)
            return len(batch), 0
        except Exception as e:
            self.log(f"  CatalogItem batch error: {e}", level=LogLevel.WARNING)
            return 0, len(batch)

    # ── Phase 3: Scan references → stream to temp file ──────────────────

    def _scan_references_to_file(self, eligible_niins):
        """Get NIIN→CAGE mappings. Uses SQLite index if available, otherwise scans ZIP."""
        if PUBLOG_INDEX_DB.exists():
            return self._scan_references_sqlite(eligible_niins)
        return self._scan_references_zip(eligible_niins)

    def _scan_references_sqlite(self, eligible_niins):
        """Fast path: query SQLite index for matching NIINs."""
        needed_cages = set()
        ref_count = 0

        ref_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", prefix="publog_refs_",
            delete=False, newline="",
        )
        writer = csv.writer(ref_file)

        conn = sqlite3.connect(str(PUBLOG_INDEX_DB))
        try:
            niin_list = list(eligible_niins)
            # Query in chunks of 500 (SQLite variable limit)
            for i in range(0, len(niin_list), 500):
                chunk = niin_list[i:i + 500]
                placeholders = ",".join("?" * len(chunk))
                cursor = conn.execute(
                    f"SELECT niin, cage_code, part_number FROM reference WHERE niin IN ({placeholders})",
                    chunk,
                )
                for niin, cage_code, part_number in cursor:
                    needed_cages.add(cage_code)
                    writer.writerow([niin, cage_code, part_number])
                    ref_count += 1
        finally:
            conn.close()

        ref_file.flush()
        ref_file.close()

        self.log(f"  REFERENCE (SQLite): {ref_count:,} supplier rows found")
        return needed_cages, ref_file, ref_count

    def _scan_references_zip(self, eligible_niins):
        """Fallback: scan V_FLIS_PART.CSV from ZIP, stream matching rows to temp file."""
        needed_cages = set()
        ref_count = 0
        row_count = 0

        ref_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", prefix="publog_refs_",
            delete=False, newline="",
        )
        writer = csv.writer(ref_file)

        zf = zipfile.ZipFile(REFERENCE_ZIP, "r")
        try:
            with zf.open("V_FLIS_PART.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                header = next(reader)
                i_niin = _col_index(header, "NIIN")
                i_cage = _col_index(header, "CAGE_CODE")
                i_pn = _col_index(header, "PART_NUMBER")

                for row in reader:
                    row_count += 1

                    niin = row[i_niin].strip()
                    if not niin:
                        continue

                    niin_padded = niin.zfill(9)
                    if niin_padded not in eligible_niins:
                        continue

                    cage_code = row[i_cage].strip()
                    if not cage_code or len(cage_code) != 5:
                        continue

                    part_number = row[i_pn].strip()
                    needed_cages.add(cage_code)
                    writer.writerow([niin_padded, cage_code, part_number])
                    ref_count += 1

                    if row_count % 5_000_000 == 0:
                        self.log(
                            f"  REFERENCE scan: {row_count:,} rows, "
                            f"{ref_count:,} matches"
                        )
        finally:
            zf.close()

        ref_file.flush()
        ref_file.close()

        self.log(f"  REFERENCE scan: {row_count:,} rows scanned")
        return needed_cages, ref_file, ref_count

    # ── Phase 4: Import only needed CAGE → Organizations ─────────────────

    def _import_cage(self, needed_cages):
        """Create Organization records for needed CAGE codes. Uses SQLite index if available."""
        if PUBLOG_INDEX_DB.exists():
            return self._import_cage_sqlite(needed_cages)
        return self._import_cage_zip(needed_cages)

    def _import_cage_sqlite(self, needed_cages):
        """Fast path: query SQLite index for CAGE data."""
        created = 0
        filtered = 0

        existing_cages = set(
            Organization.objects.filter(cage_code__isnull=False)
            .values_list("cage_code", flat=True)
        )

        missing_cages = needed_cages - existing_cages
        if not missing_cages:
            self.log("  CAGE: all needed codes already exist")
            return 0

        self.log(f"  CAGE (SQLite): need {len(missing_cages):,} new codes")

        existing_slugs = set(
            Organization.objects.values_list("slug", flat=True)
        )

        batch = []
        conn = sqlite3.connect(str(PUBLOG_INDEX_DB))
        try:
            cage_list = list(missing_cages)
            for i in range(0, len(cage_list), 500):
                chunk = cage_list[i:i + 500]
                placeholders = ",".join("?" * len(chunk))
                cursor = conn.execute(
                    f"SELECT cage_code, company, city, state, zip, country "
                    f"FROM cage WHERE cage_code IN ({placeholders})",
                    chunk,
                )
                for cage_code, company_name, city, state, zip_code, country in cursor:
                    if self.filter_service:
                        result = self.filter_service.check_cage(cage_code)
                        if result:
                            filtered += 1
                            continue
                        if company_name:
                            result = self.filter_service.check_manufacturer_name(company_name)
                            if result:
                                filtered += 1
                                continue

                    base = slugify_manufacturer(company_name, cage_code)
                    slug = base
                    if slug in existing_slugs and cage_code:
                        slug = f"{base}-{cage_code.lower()}"
                    counter = 2
                    original_slug = slug
                    while slug in existing_slugs:
                        slug = f"{original_slug}-{counter}"
                        counter += 1
                    existing_slugs.add(slug)

                    batch.append(Organization(
                        cage_code=cage_code,
                        company_name=company_name,
                        city=city,
                        state=state,
                        zip_code=zip_code,
                        country=country,
                        is_manufacturer=True,
                        slug=slug,
                    ))

                    if len(batch) >= BATCH_SIZE:
                        created += self._flush_manufacturer_batch(batch)
                        batch.clear()
                        time.sleep(SLEEP_BETWEEN_BATCHES)
        finally:
            conn.close()

        if batch:
            created += self._flush_manufacturer_batch(batch)

        not_found = missing_cages - existing_cages - {o.cage_code for o in batch} if batch else missing_cages - existing_cages
        # Check how many we actually found
        found_count = created + filtered
        still_missing = len(missing_cages) - found_count
        if still_missing > 0:
            self.log(f"  CAGE (SQLite): {still_missing:,} codes not found in index")
        if filtered:
            self.log(f"  CAGE (SQLite): {filtered:,} manufacturers filtered by pipeline rules")

        return created

    def _import_cage_zip(self, needed_cages):
        """Fallback: scan P_CAGE.CSV from ZIP."""
        created = 0
        filtered = 0
        row_count = 0

        existing_cages = set(
            Organization.objects.filter(cage_code__isnull=False)
            .values_list("cage_code", flat=True)
        )

        missing_cages = needed_cages - existing_cages
        if not missing_cages:
            self.log("  CAGE: all needed codes already exist")
            return 0

        self.log(f"  CAGE: need {len(missing_cages):,} new codes")

        existing_slugs = set(
            Organization.objects.values_list("slug", flat=True)
        )

        batch = []

        zf = zipfile.ZipFile(CAGE_ZIP, "r")
        try:
            with zf.open("P_CAGE.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                header = next(reader)
                i_cage = _col_index(header, "CAGE_CODE")
                i_company = _col_index(header, "COMPANY")
                i_city = _col_index(header, "CITY")
                i_state = _col_index(header, "STATE_PROVINCE")
                i_zip = _col_index(header, "ZIP_POSTAL_ZONE")
                i_country = _col_index(header, "COUNTRY")

                for row in reader:
                    row_count += 1

                    cage_code = row[i_cage].strip()
                    if cage_code not in missing_cages:
                        continue

                    missing_cages.discard(cage_code)

                    company_name = row[i_company].strip()

                    if self.filter_service:
                        result = self.filter_service.check_cage(cage_code)
                        if result:
                            filtered += 1
                            continue
                        if company_name:
                            result = self.filter_service.check_manufacturer_name(company_name)
                            if result:
                                filtered += 1
                                continue

                    base = slugify_manufacturer(company_name, cage_code)
                    slug = base
                    if slug in existing_slugs and cage_code:
                        slug = f"{base}-{cage_code.lower()}"
                    counter = 2
                    original_slug = slug
                    while slug in existing_slugs:
                        slug = f"{original_slug}-{counter}"
                        counter += 1
                    existing_slugs.add(slug)

                    batch.append(Organization(
                        cage_code=cage_code,
                        company_name=company_name,
                        city=row[i_city].strip(),
                        state=row[i_state].strip(),
                        zip_code=row[i_zip].strip(),
                        country=row[i_country].strip(),
                        is_manufacturer=True,
                        slug=slug,
                    ))

                    if len(batch) >= BATCH_SIZE:
                        created += self._flush_manufacturer_batch(batch)
                        batch.clear()
                        time.sleep(SLEEP_BETWEEN_BATCHES)

                    if not missing_cages:
                        self.log(f"  CAGE: all codes found at row {row_count:,}")
                        break
        finally:
            zf.close()

        if batch:
            created += self._flush_manufacturer_batch(batch)

        if missing_cages:
            self.log(f"  CAGE: {len(missing_cages):,} codes not found in P_CAGE.CSV")
        if filtered:
            self.log(f"  CAGE: {filtered:,} manufacturers filtered by pipeline rules")

        self.log(f"  CAGE: {row_count:,} rows processed")
        return created

    def _flush_manufacturer_batch(self, batch):
        if not batch:
            return 0
        try:
            with transaction.atomic():
                Organization.objects.bulk_create(batch, ignore_conflicts=True)
                # Query back PKs + names for profile creation (bulk_create+ignore_conflicts doesn't set PKs)
                slugs = [o.slug for o in batch]
                orgs_needing_profile = list(
                    Organization.objects.filter(slug__in=slugs)
                    .exclude(profile__isnull=False)
                    .values_list("pk", "company_name")
                )
                if orgs_needing_profile:
                    OrganizationProfile.objects.bulk_create(
                        [OrganizationProfile(
                            organization_id=pk,
                            display_name=format_manufacturer_name(name),
                            status=1,
                        ) for pk, name in orgs_needing_profile],
                        ignore_conflicts=True,
                    )
            return len(batch)
        except Exception as e:
            self.log(f"  Organization batch error: {e}", level=LogLevel.WARNING)
            return 0

    # ── Phase 5: Create SupplierLink + Product records by streaming from temp file ─────

    def _create_suppliers_streaming(self, ref_file):
        """Create SupplierLink and Product records by streaming from temp file (low memory)."""
        created = 0
        errored = 0
        filtered = 0
        row_count = 0

        # Build lookup maps — these are needed but much smaller than ref_rows
        self.log("  Suppliers: building lookup maps...")
        niin_to_pk = {}
        for nsn, pk in CatalogItem.objects.values_list("nsn", "pk").iterator(chunk_size=10000):
            parts = nsn.split("-")
            if len(parts) == 4:
                niin_raw = parts[1] + parts[2] + parts[3]
                niin_to_pk[niin_raw] = pk

        cage_to_pk = dict(
            Organization.objects.filter(cage_code__isnull=False)
            .values_list("cage_code", "pk")
        )

        cage_to_name = dict(
            Organization.objects.filter(cage_code__isnull=False)
            .exclude(company_name="")
            .values_list("cage_code", "company_name")
        )

        # Build nomenclature lookup for Product.name formatting
        niin_to_nomenclature = {}
        for nsn, nom in CatalogItem.objects.exclude(nomenclature="").values_list("nsn", "nomenclature").iterator(chunk_size=10000):
            parts = nsn.split("-")
            if len(parts) == 4:
                niin_to_nomenclature[parts[1] + parts[2] + parts[3]] = nom

        # Load existing links in chunks to reduce peak memory
        self.log("  Suppliers: loading existing links...")
        existing_links = set()
        for catalog_id, org_id in SupplierLink.objects.values_list("catalog_item_id", "organization_id").iterator(chunk_size=10000):
            existing_links.add((catalog_id, org_id))

        existing_products = set()
        for mfr_id, pn in Product.objects.values_list("manufacturer_id", "part_number").iterator(chunk_size=10000):
            existing_products.add((mfr_id, pn))

        self.log(f"  Suppliers: {len(niin_to_pk):,} NIINs, {len(cage_to_pk):,} CAGEs, {len(existing_links):,} existing links, {len(existing_products):,} existing products")

        # Stream from temp file
        supplier_batch = []
        product_batch = []
        import os
        ref_path = ref_file.name
        with open(ref_path, "r", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                row_count += 1
                niin_padded, cage_code, part_number = row[0], row[1], row[2]

                nsn_pk = niin_to_pk.get(niin_padded)
                cage_pk = cage_to_pk.get(cage_code)
                if not nsn_pk or not cage_pk:
                    continue

                if (nsn_pk, cage_pk) in existing_links:
                    continue

                # Apply pipeline filters
                if self.filter_service:
                    result = self.filter_service.check_cage(cage_code)
                    if result:
                        filtered += 1
                        continue
                    company_name = cage_to_name.get(cage_code, "")
                    if company_name:
                        result = self.filter_service.check_manufacturer_name(company_name)
                        if result:
                            filtered += 1
                            continue

                supplier_batch.append(SupplierLink(
                    catalog_item_id=nsn_pk,
                    organization_id=cage_pk,
                    part_number=part_number,
                    source=DataSource.PUBLOG,
                ))
                existing_links.add((nsn_pk, cage_pk))

                # Only create Product if manufacturer+part_number is new
                product_key = (cage_pk, part_number)
                if product_key not in existing_products:
                    raw_nomenclature = niin_to_nomenclature.get(niin_padded, "")
                    product_name = format_nomenclature(raw_nomenclature) if raw_nomenclature else ""

                    product_batch.append(Product(
                        manufacturer_id=cage_pk,
                        catalog_item_id=nsn_pk,
                        part_number=part_number,
                        part_number_slug=slugify_part_number(part_number),
                        name=product_name,
                        source=DataSource.PUBLOG,
                    ))
                    existing_products.add(product_key)

                if len(supplier_batch) >= BATCH_SIZE:
                    c, e = self._flush_supplier_batch(supplier_batch, product_batch)
                    created += c
                    errored += e
                    supplier_batch.clear()
                    product_batch.clear()
                    time.sleep(SLEEP_BETWEEN_BATCHES)

                if row_count % 100_000 == 0:
                    self.log(
                        f"  Suppliers: {row_count:,} rows streamed, "
                        f"{created:,} created"
                    )

        if supplier_batch:
            c, e = self._flush_supplier_batch(supplier_batch, product_batch)
            created += c
            errored += e

        # Clean up temp file
        try:
            os.unlink(ref_path)
        except Exception:
            pass

        if filtered:
            self.log(f"  Suppliers: {filtered:,} filtered by pipeline rules")

        return created, errored

    def _flush_supplier_batch(self, supplier_batch, product_batch=None):
        if not supplier_batch:
            return 0, 0
        try:
            with transaction.atomic():
                SupplierLink.objects.bulk_create(supplier_batch, ignore_conflicts=True)
                if product_batch:
                    Product.objects.bulk_create(product_batch, ignore_conflicts=True)
            return len(supplier_batch), 0
        except Exception as e:
            self.log(f"  SupplierLink/Product batch error: {e}", level=LogLevel.WARNING)
            return 0, len(supplier_batch)
