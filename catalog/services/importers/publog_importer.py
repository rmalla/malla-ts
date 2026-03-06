"""
PUB LOG Importer — reads DLA FLIS PUB LOG zip files to create Products directly.

Data files (in /imports/):
  MANAGEMENT.zip   -> V_FLIS_MANAGEMENT.CSV  (12.7M rows) — prices
  CAGE.zip         -> P_CAGE.CSV             (~800K rows)  — manufacturers
  IDENTIFICATION.zip -> P_FLIS_NSN.CSV       (~6M rows)    — item names
  REFERENCE.zip    -> V_FLIS_PART.CSV        (~20M rows)   — part numbers

Strategy: price-first filtering — scan MANAGEMENT for NIINs in the
$500-$45,000 range, then match with IDENTIFICATION and REFERENCE to
build complete Product records directly.
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
from catalog.models import Manufacturer, Product
from catalog.models.catalog import DataSource, slugify_part_number
from catalog.models.entities import slugify_manufacturer, ManufacturerProfile
from catalog.services.name_formatter import format_manufacturer_name, format_nomenclature
from .base import BaseImporter

logger = logging.getLogger(__name__)

MANAGEMENT_ZIP = DLA_DATA_DIR / "MANAGEMENT.zip"
CAGE_ZIP = DLA_DATA_DIR / "CAGE.zip"
IDENTIFICATION_ZIP = DLA_DATA_DIR / "IDENTIFICATION.zip"
REFERENCE_ZIP = DLA_DATA_DIR / "REFERENCE.zip"
PUBLOG_INDEX_DB = DLA_DATA_DIR / "publog_index.db"

BATCH_SIZE = 2000
SLEEP_BETWEEN_BATCHES = 0.1
SLEEP_BETWEEN_PHASES = 2.0

MIN_PRICE = Decimal("500")
MAX_PRICE = Decimal("45000")


def format_nsn(fsc, niin):
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
    try:
        return header.index(name)
    except ValueError:
        raise ValueError(f"Column '{name}' not found in header: {header}")


def _phase_pause(log_fn, phase_name, skip_sleep=False):
    gc.collect()
    connection.close()
    if skip_sleep:
        log_fn(f"  [{phase_name}] Memory released")
    else:
        log_fn(f"  [{phase_name}] Memory released, pausing {SLEEP_BETWEEN_PHASES}s...")
        time.sleep(SLEEP_BETWEEN_PHASES)


class PUBLOGImporter(BaseImporter):
    """Import PUB LOG data directly into Product records."""

    job_type = JobType.PUBLOG_IMPORT

    def __init__(self, stdout=None):
        super().__init__(stdout=stdout)
        self.last_mgmt_row = 0

    def run(self, limit=None, skip_suppliers=False, skip_rows=0, **kwargs):
        for zp in (MANAGEMENT_ZIP, IDENTIFICATION_ZIP):
            if not zp.exists():
                raise FileNotFoundError(f"{zp.name} not found at {zp}")

        # Phase 1: Scan prices -> eligible NIIN set
        self.log("Phase 1: Scanning V_FLIS_MANAGEMENT.CSV for eligible prices...")
        price_map = self._scan_prices(limit=limit, skip_rows=skip_rows)
        self.log(f"Phase 1 complete: {len(price_map):,} eligible NIINs ($500-$45K)")

        if not price_map:
            self.log("No new eligible NIINs found — all caught up.")
            return

        price_map_size = len(price_map)
        eligible_niins = set(price_map.keys())
        small_batch = price_map_size <= 1000
        _phase_pause(self.log, "Phase 1->2", skip_sleep=small_batch)

        # Phase 2: Scan identification -> NSN + nomenclature for eligible NIINs
        self.log("Phase 2: Scanning P_FLIS_NSN.CSV (item names)...")
        niin_info = self._scan_identification(eligible_niins, price_map)
        self.log(f"Phase 2 complete: {len(niin_info):,} NIINs identified")

        del price_map
        _phase_pause(self.log, "Phase 2->3", skip_sleep=small_batch)

        cage_created = 0
        products_created = 0
        products_errored = 0

        if not skip_suppliers:
            for zp in (REFERENCE_ZIP, CAGE_ZIP):
                if not zp.exists():
                    raise FileNotFoundError(f"{zp.name} not found at {zp}")

            # Phase 3: Scan references -> stream to temp file + collect CAGE codes
            self.log("Phase 3: Scanning V_FLIS_PART.CSV (streaming to temp file)...")
            needed_cages, ref_file, ref_count = self._scan_references_to_file(eligible_niins)
            self.log(f"Phase 3 complete: {ref_count:,} supplier rows, {len(needed_cages):,} CAGE codes")

            del eligible_niins
            _phase_pause(self.log, "Phase 3->4", skip_sleep=small_batch)

            # Phase 4: Import needed CAGE codes -> Manufacturer records
            self.log("Phase 4: Importing needed CAGE codes from P_CAGE.CSV...")
            cage_created = self._import_cage(needed_cages)
            self.log(f"Phase 4 complete: {cage_created:,} manufacturers created")

            del needed_cages
            _phase_pause(self.log, "Phase 4->5", skip_sleep=small_batch)

            # Phase 5: Create Product records by streaming from temp file
            self.log("Phase 5: Creating products (streaming from temp file)...")
            products_created, products_errored = self._create_products_streaming(ref_file, niin_info)
            self.log(f"Phase 5 complete: {products_created:,} created, {products_errored:,} errored")

            try:
                ref_file.close()
            except Exception:
                pass
        else:
            self.log("Skipping supplier/product linking (--skip-suppliers)")

        self.job.records_created = products_created + cage_created
        self.job.records_errored = products_errored
        self.job.records_fetched = price_map_size
        self.job.save(update_fields=["records_created", "records_errored", "records_fetched"])

    # -- Phase 1: Scan prices --

    def _scan_prices(self, limit=None, skip_rows=0):
        """Read V_FLIS_MANAGEMENT.CSV, return {niin: (price, unit_of_issue)} for eligible rows."""
        price_map = {}
        row_count = 0

        # Load existing product NSNs for incremental behavior
        existing_niins = set()
        for nsn in Product.objects.values_list("nsn", flat=True).iterator(chunk_size=10000):
            if not nsn:
                continue
            parts = nsn.split("-")
            if len(parts) == 4:
                existing_niins.add(parts[1] + parts[2] + parts[3])
        self.log(f"  MANAGEMENT: {len(existing_niins):,} NIINs already have products, will skip")

        zf = zipfile.ZipFile(MANAGEMENT_ZIP, "r")
        try:
            with zf.open("V_FLIS_MANAGEMENT.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                header = next(reader)
                i_niin = _col_index(header, "NIIN")
                i_price = _col_index(header, "UNIT_PRICE")
                i_ui = _col_index(header, "UI")

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

                    if niin_padded in existing_niins:
                        continue

                    existing = price_map.get(niin_padded)
                    if not existing or price > existing[0]:
                        price_map[niin_padded] = (price, ui)

                    if limit and len(price_map) >= limit:
                        break

                    if row_count % 2_000_000 == 0:
                        self.log(f"  MANAGEMENT: {row_count:,} rows, {len(price_map):,} eligible")
        finally:
            zf.close()

        self.last_mgmt_row = row_count
        self.log(f"  MANAGEMENT: {row_count:,} rows scanned, {len(price_map):,} new NIINs")
        return price_map

    # -- Phase 2: Scan identification --

    def _scan_identification(self, eligible_niins, price_map):
        """Read P_FLIS_NSN.CSV, build niin -> {nsn, nomenclature, fsc_code, price, ui}."""
        niin_info = {}
        row_count = 0
        remaining = set(eligible_niins)

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

                    # Pipeline filters
                    if self.filter_service:
                        result = self.filter_service.check_nsn(nsn, nomenclature=item_name)
                        if result:
                            continue
                        price_data = price_map.get(niin_padded)
                        if price_data:
                            result = self.filter_service.check_unit_price(price_data[0])
                            if result:
                                continue

                    price_data = price_map.get(niin_padded)
                    niin_info[niin_padded] = {
                        "nsn": nsn,
                        "nomenclature": item_name,
                        "fsc_code": fsc,
                        "price": price_data[0] if price_data else None,
                        "unit_of_issue": price_data[1] if price_data else "",
                    }

                    if row_count % 1_000_000 == 0:
                        self.log(f"  IDENTIFICATION: {row_count:,} rows, {len(niin_info):,} matched")

                    if not remaining:
                        break
        finally:
            zf.close()

        self.log(f"  IDENTIFICATION: {row_count:,} rows processed")
        return niin_info

    # -- Phase 3: Scan references --

    def _scan_references_to_file(self, eligible_niins):
        if PUBLOG_INDEX_DB.exists():
            return self._scan_references_sqlite(eligible_niins)
        return self._scan_references_zip(eligible_niins)

    def _scan_references_sqlite(self, eligible_niins):
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
        return needed_cages, ref_file, ref_count

    def _scan_references_zip(self, eligible_niins):
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
                        self.log(f"  REFERENCE: {row_count:,} rows, {ref_count:,} matches")
        finally:
            zf.close()

        ref_file.flush()
        ref_file.close()
        return needed_cages, ref_file, ref_count

    # -- Phase 4: Import CAGE codes --

    def _import_cage(self, needed_cages):
        if PUBLOG_INDEX_DB.exists():
            return self._import_cage_sqlite(needed_cages)
        return self._import_cage_zip(needed_cages)

    def _import_cage_sqlite(self, needed_cages):
        created = 0
        existing_cages = set(
            Manufacturer.objects.filter(cage_code__isnull=False)
            .values_list("cage_code", flat=True)
        )
        missing_cages = needed_cages - existing_cages
        if not missing_cages:
            return 0

        existing_slugs = set(Manufacturer.objects.values_list("slug", flat=True))
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
                        if self.filter_service.check_cage(cage_code):
                            continue
                        if company_name and self.filter_service.check_manufacturer_name(company_name):
                            continue

                    display_name = format_manufacturer_name(company_name) if company_name else ""
                    base = slugify_manufacturer(display_name or company_name, cage_code)
                    slug = base
                    if slug in existing_slugs and cage_code:
                        slug = f"{base}-{cage_code.lower()}"
                    counter = 2
                    original_slug = slug
                    while slug in existing_slugs:
                        slug = f"{original_slug}-{counter}"
                        counter += 1
                    existing_slugs.add(slug)

                    batch.append(Manufacturer(
                        cage_code=cage_code, company_name=company_name,
                        city=city, state=state, zip_code=zip_code,
                        country=country, is_manufacturer=True, slug=slug,
                    ))

                    if len(batch) >= BATCH_SIZE:
                        created += self._flush_manufacturer_batch(batch)
                        batch.clear()
                        time.sleep(SLEEP_BETWEEN_BATCHES)
        finally:
            conn.close()

        if batch:
            created += self._flush_manufacturer_batch(batch)
        return created

    def _import_cage_zip(self, needed_cages):
        created = 0
        existing_cages = set(
            Manufacturer.objects.filter(cage_code__isnull=False)
            .values_list("cage_code", flat=True)
        )
        missing_cages = needed_cages - existing_cages
        if not missing_cages:
            return 0

        existing_slugs = set(Manufacturer.objects.values_list("slug", flat=True))
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
                    cage_code = row[i_cage].strip()
                    if cage_code not in missing_cages:
                        continue

                    missing_cages.discard(cage_code)
                    company_name = row[i_company].strip()

                    if self.filter_service:
                        if self.filter_service.check_cage(cage_code):
                            continue
                        if company_name and self.filter_service.check_manufacturer_name(company_name):
                            continue

                    display_name = format_manufacturer_name(company_name) if company_name else ""
                    base = slugify_manufacturer(display_name or company_name, cage_code)
                    slug = base
                    if slug in existing_slugs and cage_code:
                        slug = f"{base}-{cage_code.lower()}"
                    counter = 2
                    original_slug = slug
                    while slug in existing_slugs:
                        slug = f"{original_slug}-{counter}"
                        counter += 1
                    existing_slugs.add(slug)

                    batch.append(Manufacturer(
                        cage_code=cage_code, company_name=company_name,
                        city=row[i_city].strip(), state=row[i_state].strip(),
                        zip_code=row[i_zip].strip(), country=row[i_country].strip(),
                        is_manufacturer=True, slug=slug,
                    ))

                    if len(batch) >= BATCH_SIZE:
                        created += self._flush_manufacturer_batch(batch)
                        batch.clear()
                        time.sleep(SLEEP_BETWEEN_BATCHES)

                    if not missing_cages:
                        break
        finally:
            zf.close()

        if batch:
            created += self._flush_manufacturer_batch(batch)
        return created

    def _flush_manufacturer_batch(self, batch):
        if not batch:
            return 0
        try:
            with transaction.atomic():
                Manufacturer.objects.bulk_create(batch, ignore_conflicts=True)
                slugs = [o.slug for o in batch]
                orgs_needing_profile = list(
                    Manufacturer.objects.filter(slug__in=slugs)
                    .exclude(profile__isnull=False)
                    .values_list("pk", "company_name")
                )
                if orgs_needing_profile:
                    ManufacturerProfile.objects.bulk_create(
                        [ManufacturerProfile(
                            organization_id=pk,
                            display_name=format_manufacturer_name(name),
                            status=0,
                        ) for pk, name in orgs_needing_profile],
                        ignore_conflicts=True,
                    )
            return len(batch)
        except Exception as e:
            self.log(f"  Manufacturer batch error: {e}", level=LogLevel.WARNING)
            return 0

    # -- Phase 5: Create products --

    def _create_products_streaming(self, ref_file, niin_info):
        """Create Product records by streaming from temp file."""
        import os

        created = 0
        errored = 0
        row_count = 0

        cage_to_pk = dict(
            Manufacturer.objects.filter(cage_code__isnull=False)
            .values_list("cage_code", "pk")
        )

        from home.models import FederalSupplyClass
        fsc_map = {f.code: f.pk for f in FederalSupplyClass.objects.all()}

        existing_products = set(
            Product.objects.values_list("manufacturer_id", "part_number")
        )

        self.log(f"  Products: {len(cage_to_pk):,} CAGEs, {len(existing_products):,} existing products")

        batch = []
        ref_path = ref_file.name
        with open(ref_path, "r", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                row_count += 1
                niin_padded, cage_code, part_number = row[0], row[1], row[2]

                cage_pk = cage_to_pk.get(cage_code)
                if not cage_pk:
                    continue

                product_key = (cage_pk, part_number)
                if product_key in existing_products:
                    continue

                # Pipeline filters
                if self.filter_service:
                    if self.filter_service.check_cage(cage_code):
                        continue

                info = niin_info.get(niin_padded)
                if not info:
                    continue

                raw_nomenclature = info.get("nomenclature", "")
                product_name = format_nomenclature(raw_nomenclature) if raw_nomenclature else ""

                batch.append(Product(
                    manufacturer_id=cage_pk,
                    part_number=part_number,
                    part_number_slug=slugify_part_number(part_number),
                    name=product_name,
                    nsn=info["nsn"],
                    nomenclature=raw_nomenclature,
                    fsc_id=fsc_map.get(info["fsc_code"]),
                    price=info["price"],
                    unit_of_issue=info.get("unit_of_issue", ""),
                    source=DataSource.PUBLOG,
                ))
                existing_products.add(product_key)

                if len(batch) >= BATCH_SIZE:
                    c, e = self._flush_product_batch(batch)
                    created += c
                    errored += e
                    batch.clear()
                    time.sleep(SLEEP_BETWEEN_BATCHES)

                if row_count % 100_000 == 0:
                    self.log(f"  Products: {row_count:,} rows, {created:,} created")

        if batch:
            c, e = self._flush_product_batch(batch)
            created += c
            errored += e

        try:
            os.unlink(ref_path)
        except Exception:
            pass

        return created, errored

    def _flush_product_batch(self, batch):
        if not batch:
            return 0, 0
        try:
            with transaction.atomic():
                Product.objects.bulk_create(batch, ignore_conflicts=True)
            return len(batch), 0
        except Exception as e:
            self.log(f"  Product batch error: {e}", level=LogLevel.WARNING)
            return 0, len(batch)
