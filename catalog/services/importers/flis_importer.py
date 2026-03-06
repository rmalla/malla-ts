"""
FLIS History Importer — reads DLA HISTORY.zip to populate Products directly.

Data files (in imports/dla/):
  HISTORY.zip contains:
    1. P_HISTORY_PICK.CSV       (9.9M rows) — FSC, NIIN, item name, CAGE, part number
    2. V_MANAGEMENT_HISTORY.CSV (4.3M rows) — NIIN, unit price, unit of issue
    3. V_REFERENCE_NUMBER_HISTORY.CSV (2.9M rows) — NIIN, part number, CAGE code

Strategy: scan all 3 files to build a complete picture per NIIN, then create
Manufacturer + Product records directly (no intermediate CatalogItem).
"""

import csv
import io
import logging
import zipfile
from decimal import Decimal, InvalidOperation

from django.db import transaction

from catalog.constants import DLA_DATA_DIR, JobType, LogLevel
from catalog.models import Manufacturer, Product
from catalog.models.catalog import DataSource, slugify_part_number
from .base import BaseImporter

logger = logging.getLogger(__name__)

HISTORY_ZIP = DLA_DATA_DIR / "HISTORY.zip"
BATCH_SIZE = 5000


def _col(header, name):
    try:
        return header.index(name)
    except ValueError:
        raise ValueError(f"Column '{name}' not found in header: {header}")


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


class FLISHistoryImporter(BaseImporter):
    """Import FLIS history data from HISTORY.zip directly into Product."""

    job_type = JobType.FLIS_HISTORY_IMPORT

    def __init__(self, stdout=None):
        super().__init__(stdout=stdout)

    def run(self, skip_management=False, skip_references=False, limit=None, **kwargs):
        if not HISTORY_ZIP.exists():
            raise FileNotFoundError(f"HISTORY.zip not found at {HISTORY_ZIP}")

        if limit:
            self.log(f"Limit: {limit:,} products")

        zf = zipfile.ZipFile(HISTORY_ZIP, "r")

        try:
            # Phase 1: Scan P_HISTORY_PICK — collect NIIN→(nsn, nomenclature, fsc_code, [(cage, part_number)])
            self.log("Phase 1: P_HISTORY_PICK.CSV (NSN + nomenclature + CAGE/part refs)...")
            niin_data, cage_set = self._scan_pick(zf, limit=limit)
            self.log(f"Phase 1 complete: {len(niin_data):,} unique NIINs, {len(cage_set):,} unique CAGEs")

            # Phase 2: Scan V_MANAGEMENT_HISTORY — attach prices
            if not skip_management:
                self.log("Phase 2: V_MANAGEMENT_HISTORY.CSV (prices)...")
                price_count = self._scan_prices(zf, niin_data)
                self.log(f"Phase 2 complete: {price_count:,} NIINs with prices")
            else:
                self.log("Phase 2: Skipped (--skip-management)")

            # Phase 3: Scan V_REFERENCE_NUMBER_HISTORY — additional CAGE/part refs
            if not skip_references:
                self.log("Phase 3: V_REFERENCE_NUMBER_HISTORY.CSV (additional refs)...")
                ref_count, extra_cages = self._scan_references(zf, niin_data)
                cage_set.update(extra_cages)
                self.log(f"Phase 3 complete: {ref_count:,} additional refs")
            else:
                self.log("Phase 3: Skipped (--skip-references)")

        finally:
            zf.close()

        # Phase 4: Ensure manufacturers exist
        self.log("Phase 4: Creating missing manufacturers...")
        cage_created = self._ensure_manufacturers(cage_set)
        self.log(f"Phase 4 complete: {cage_created:,} manufacturers created")

        # Phase 5: Create products
        self.log("Phase 5: Creating products...")
        products_created, products_errored = self._create_products(niin_data)
        self.log(f"Phase 5 complete: {products_created:,} created, {products_errored:,} errored")

        self.job.records_created = products_created + cage_created
        self.job.records_errored = products_errored
        self.job.records_fetched = len(niin_data)
        self.job.save(update_fields=["records_created", "records_errored", "records_fetched"])

    def _scan_pick(self, zf, limit=None):
        """Scan P_HISTORY_PICK.CSV to collect NIIN data + CAGE/part refs."""
        niin_data = {}  # niin -> {nsn, nomenclature, fsc_code, refs: [(cage, part_number)]}
        cage_set = set()
        row_count = 0

        with zf.open("P_HISTORY_PICK.CSV") as f:
            reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
            header = next(reader)
            i_fsc = _col(header, "FSC")
            i_niin = _col(header, "NIIN")
            i_name = _col(header, "ITEM_NAME")
            i_cage = _col(header, "CAGE_CODE")
            i_pn = _col(header, "PART_NUMBER")

            for row in reader:
                row_count += 1
                try:
                    fsc = row[i_fsc].strip()
                    niin = row[i_niin].strip()
                    if not niin or not fsc:
                        continue

                    item_name = row[i_name].strip()
                    cage_code = row[i_cage].strip()
                    part_number = row[i_pn].strip()

                    if item_name == "NO ITEM NAME AVAILABLE":
                        item_name = ""

                    niin_padded = niin.zfill(9)
                    nsn = format_nsn(fsc, niin_padded)

                    if niin_padded not in niin_data:
                        niin_data[niin_padded] = {
                            "nsn": nsn,
                            "nomenclature": item_name,
                            "fsc_code": fsc,
                            "refs": [],
                            "price": None,
                            "unit_of_issue": "",
                        }
                    elif item_name and not niin_data[niin_padded]["nomenclature"]:
                        niin_data[niin_padded]["nomenclature"] = item_name

                    if cage_code and len(cage_code) == 5:
                        cage_set.add(cage_code)
                        niin_data[niin_padded]["refs"].append((cage_code, part_number))

                except Exception:
                    pass

                if row_count % 500_000 == 0:
                    self.log(f"  PICK: {row_count:,} rows, {len(niin_data):,} NIINs")

                if limit and len(niin_data) >= limit:
                    break

        self.log(f"  PICK: {row_count:,} rows processed")
        return niin_data, cage_set

    def _scan_prices(self, zf, niin_data):
        """Scan V_MANAGEMENT_HISTORY.CSV, attach best price to niin_data entries."""
        row_count = 0
        matched = 0

        with zf.open("V_MANAGEMENT_HISTORY.CSV") as f:
            reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
            header = next(reader)
            i_niin = _col(header, "NIIN")
            i_price = _col(header, "UNIT_PRICE")
            i_ui = _col(header, "UI")

            for row in reader:
                row_count += 1
                try:
                    niin = row[i_niin].strip()
                    price = safe_decimal(row[i_price])
                    ui = row[i_ui].strip()

                    if not niin or not price or price <= 0:
                        continue

                    niin_padded = niin.zfill(9)
                    entry = niin_data.get(niin_padded)
                    if not entry:
                        continue

                    if entry["price"] is None or price > entry["price"]:
                        entry["price"] = price
                        if ui:
                            entry["unit_of_issue"] = ui
                        matched += 1

                except Exception:
                    pass

                if row_count % 1_000_000 == 0:
                    self.log(f"  MGMT: {row_count:,} rows, {matched:,} matched")

        self.log(f"  MGMT: {row_count:,} rows scanned")
        return matched

    def _scan_references(self, zf, niin_data):
        """Scan V_REFERENCE_NUMBER_HISTORY.CSV for additional CAGE/part refs."""
        row_count = 0
        ref_count = 0
        extra_cages = set()

        with zf.open("V_REFERENCE_NUMBER_HISTORY.CSV") as f:
            reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
            header = next(reader)
            i_niin = _col(header, "NIIN")
            i_pn = _col(header, "PART_NUMBER")
            i_cage = _col(header, "CAGE_CODE")

            for row in reader:
                row_count += 1
                try:
                    niin = row[i_niin].strip()
                    cage_code = row[i_cage].strip()
                    if not niin or not cage_code or len(cage_code) != 5:
                        continue

                    niin_padded = niin.zfill(9)
                    entry = niin_data.get(niin_padded)
                    if not entry:
                        continue

                    part_number = row[i_pn].strip()
                    entry["refs"].append((cage_code, part_number))
                    extra_cages.add(cage_code)
                    ref_count += 1

                except Exception:
                    pass

                if row_count % 1_000_000 == 0:
                    self.log(f"  REF: {row_count:,} rows, {ref_count:,} refs")

        self.log(f"  REF: {row_count:,} rows scanned")
        return ref_count, extra_cages

    def _ensure_manufacturers(self, cage_set):
        """Create Manufacturer records for any new CAGE codes."""
        existing_cages = set(
            Manufacturer.objects.filter(cage_code__isnull=False)
            .values_list("cage_code", flat=True)
        )
        new_cages = cage_set - existing_cages
        if not new_cages:
            return 0

        batch = [
            Manufacturer(cage_code=code, company_name="", is_manufacturer=True)
            for code in new_cages
        ]
        created = 0
        for i in range(0, len(batch), BATCH_SIZE):
            chunk = batch[i:i + BATCH_SIZE]
            try:
                with transaction.atomic():
                    Manufacturer.objects.bulk_create(chunk, ignore_conflicts=True)
                created += len(chunk)
            except Exception as e:
                self.log(f"  CAGE batch error: {e}", level=LogLevel.WARNING)

        return created

    def _create_products(self, niin_data):
        """Create Product records from collected NIIN data."""
        from home.models import FederalSupplyClass

        # Build lookup maps
        cage_to_pk = dict(
            Manufacturer.objects.filter(cage_code__isnull=False)
            .values_list("cage_code", "pk")
        )
        fsc_map = {f.code: f.pk for f in FederalSupplyClass.objects.all()}

        # Load existing products to skip duplicates
        existing_products = set(
            Product.objects.values_list("manufacturer_id", "part_number")
        )

        created = 0
        errored = 0
        batch = []

        for niin, data in niin_data.items():
            # Deduplicate refs by (cage, part_number)
            seen_refs = set()
            for cage_code, part_number in data["refs"]:
                cage_pk = cage_to_pk.get(cage_code)
                if not cage_pk:
                    continue

                product_key = (cage_pk, part_number)
                if product_key in existing_products or product_key in seen_refs:
                    continue
                seen_refs.add(product_key)

                batch.append(Product(
                    manufacturer_id=cage_pk,
                    part_number=part_number,
                    part_number_slug=slugify_part_number(part_number),
                    nsn=data["nsn"],
                    nomenclature=data["nomenclature"],
                    fsc_id=fsc_map.get(data["fsc_code"]),
                    price=data["price"],
                    unit_of_issue=data["unit_of_issue"],
                    source=DataSource.FLIS_HISTORY,
                ))
                existing_products.add(product_key)

                if len(batch) >= BATCH_SIZE:
                    c, e = self._flush_product_batch(batch)
                    created += c
                    errored += e
                    batch.clear()

        if batch:
            c, e = self._flush_product_batch(batch)
            created += c
            errored += e

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
