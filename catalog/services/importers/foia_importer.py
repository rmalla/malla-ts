"""
FOIA Importer — reads DLA FOIA Excel reports and imports Products directly.

Creates:
  - Manufacturer entries (matched by name, no CAGE code)
  - Product entries (with price, NSN, nomenclature inline)

Handles two header formats:
  - Legacy format: Row 0 = "FOIA" banner, Row 1 = DAY/MONTH/YEAR/Date/DEPARTMENT/...
  - Jan2026 format: Row 0 = Source/Transaction Date/Department/Agency/...
  - November25 format: Row 0 = DAY/MONTH/YEAR/Date/DEPARTMENT/... (no FOIA banner)
"""

import re
import logging
from datetime import date
from decimal import Decimal, InvalidOperation

import openpyxl

from catalog.constants import DLA_DATA_DIR, JobType, LogLevel
from catalog.models import Manufacturer, Product
from catalog.models.catalog import DataSource, slugify_part_number
from .base import BaseImporter

logger = logging.getLogger(__name__)

MIN_PRODUCT_PRICE = Decimal("500")
MAX_PRODUCT_PRICE = Decimal("45000")


def normalize_nsn(raw):
    if raw is None:
        return ""
    digits = re.sub(r"\D", "", str(raw).strip())
    if len(digits) == 13:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:9]}-{digits[9:]}"
    return digits


def safe_str(val, max_len=None):
    if val is None:
        return ""
    s = str(val).strip()
    if max_len:
        s = s[:max_len]
    return s


def safe_decimal(val):
    if val is None:
        return None
    try:
        d = Decimal(str(val))
        if d.is_nan() or d.is_infinite():
            return None
        return d
    except (ValueError, InvalidOperation):
        return None


class FOIAImporter(BaseImporter):
    """Import products from DLA FOIA Excel reports."""

    job_type = JobType.FOIA_IMPORT

    def __init__(self, stdout=None):
        super().__init__(stdout=stdout)

    def run(self, file=None, limit=None, **kwargs):
        if file:
            files = [DLA_DATA_DIR / file]
            if not files[0].exists():
                raise FileNotFoundError(f"File not found: {files[0]}")
        else:
            files = sorted(DLA_DATA_DIR.glob("FOIA*.xlsx"))

        if not files:
            self.log("No FOIA Excel files found in imports directory", level=LogLevel.WARNING)
            return

        self.log(f"Found {len(files)} FOIA file(s) to import")
        if limit:
            self.log(f"Row limit: {limit}")

        # Pre-load caches
        mfr_name_cache = {}
        for pk, name in Manufacturer.objects.values_list("pk", "company_name"):
            if name:
                mfr_name_cache[name.strip().upper()] = pk

        existing_products = set(
            Product.objects.values_list("manufacturer_id", "part_number")
        )

        try:
            from home.models import FederalSupplyClass
            fsc_cache = {f.code: f.pk for f in FederalSupplyClass.objects.all()}
        except Exception:
            fsc_cache = {}

        self.log(
            f"Loaded {len(mfr_name_cache)} manufacturers, "
            f"{len(existing_products)} existing products"
        )

        total_products = 0
        total_mfrs = 0
        total_errored = 0
        rows_processed = 0

        for filepath in files:
            if limit and rows_processed >= limit:
                break

            filename = filepath.name
            self.log(f"Processing {filename}...")
            remaining = (limit - rows_processed) if limit else None
            try:
                stats = self._import_file(
                    filepath, mfr_name_cache, fsc_cache,
                    existing_products, row_limit=remaining,
                )
                total_products += stats["products_created"]
                total_mfrs += stats["mfrs_created"]
                total_errored += stats["errored"]
                rows_processed += stats["rows_processed"]
                self.log(
                    f"  {filename}: {stats['products_created']} products, "
                    f"{stats['mfrs_created']} manufacturers created, "
                    f"{stats['errored']} errored"
                )
            except Exception as e:
                self.log(f"  Error processing {filename}: {e}", level=LogLevel.ERROR)
                total_errored += 1

        self.job.records_created = total_products + total_mfrs
        self.job.records_errored = total_errored
        self.job.records_fetched = rows_processed
        self.job.save(update_fields=["records_created", "records_errored", "records_fetched"])

    def _import_file(self, filepath, mfr_name_cache, fsc_cache,
                     existing_products, row_limit=None):
        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        wb.close()

        if not rows:
            return {"products_created": 0, "mfrs_created": 0, "errored": 0, "rows_processed": 0}

        header_row, parser = self._detect_format(rows)
        if header_row is None:
            self.log(f"  Could not detect header format in {filepath.name}", level=LogLevel.WARNING)
            return {"products_created": 0, "mfrs_created": 0, "errored": 0, "rows_processed": 0}

        data_rows = rows[header_row + 1:]
        products_created = 0
        mfrs_created = 0
        errored = 0
        rows_processed = 0

        for row in data_rows:
            if row_limit and rows_processed >= row_limit:
                break

            try:
                record = parser(row)
                if not record:
                    continue

                rows_processed += 1

                nsn = normalize_nsn(record.get("nsn"))
                is_valid_nsn = bool(nsn) and len(re.sub(r"\D", "", nsn)) == 13

                unit_price = record.get("unit_price")
                price_in_range = (
                    unit_price is not None
                    and unit_price >= MIN_PRODUCT_PRICE
                    and unit_price <= MAX_PRODUCT_PRICE
                )

                if not price_in_range:
                    continue

                # Resolve manufacturer
                mfr_pk = None
                mfr_name = record.get("manufacturer_name", "")
                if not mfr_name:
                    mfr_name = record.get("supplier_name", "")
                if mfr_name:
                    mfr_key = mfr_name.strip().upper()
                    mfr_pk = mfr_name_cache.get(mfr_key)
                    if mfr_pk is None:
                        mfr_obj = Manufacturer(
                            cage_code=None,
                            company_name=mfr_name.strip(),
                            is_manufacturer=True,
                        )
                        mfr_obj.save()
                        mfr_name_cache[mfr_key] = mfr_obj.pk
                        mfr_pk = mfr_obj.pk
                        mfrs_created += 1

                if not mfr_pk:
                    continue

                # Create Product
                part_number = record.get("manufacturer_part_number", "")
                if not part_number:
                    part_number = record.get("part_number_raw", "")

                product_key = (mfr_pk, part_number)
                if product_key in existing_products:
                    continue

                item_name = record.get("item_name", "")
                fsc_pk = None
                if is_valid_nsn:
                    fsc_code = re.sub(r"\D", "", nsn)[:4]
                    fsc_pk = fsc_cache.get(fsc_code)

                Product(
                    manufacturer_id=mfr_pk,
                    part_number=part_number,
                    part_number_slug=slugify_part_number(part_number),
                    nsn=nsn if is_valid_nsn else "",
                    nomenclature=item_name,
                    fsc_id=fsc_pk,
                    price=unit_price,
                    source=DataSource.FOIA,
                ).save()
                existing_products.add(product_key)
                products_created += 1

            except Exception as e:
                errored += 1
                rows_processed += 1
                if errored <= 5:
                    self.log(f"  Row error: {e}", level=LogLevel.WARNING)

        return {
            "products_created": products_created,
            "mfrs_created": mfrs_created,
            "errored": errored,
            "rows_processed": rows_processed,
        }

    def _detect_format(self, rows):
        first = rows[0] if rows else ()

        if first and safe_str(first[0]).lower() == "source":
            return 0, self._parse_jan2026_row

        if first and safe_str(first[0]).upper() == "FOIA":
            if len(rows) > 1:
                second = rows[1]
                if safe_str(second[0]).upper() == "DAY":
                    return 1, self._parse_legacy_row
            return None, None

        if first and safe_str(first[0]).upper() == "DAY":
            return 0, self._parse_legacy_row

        return None, None

    def _parse_legacy_row(self, row):
        if not row or len(row) < 16:
            return None
        day = row[0]
        if not isinstance(day, (int, float)):
            return None
        return {
            "part_number_raw": safe_str(row[7], 300),
            "nsn": row[8],
            "item_name": safe_str(row[9], 500),
            "manufacturer_name": safe_str(row[10], 255),
            "manufacturer_part_number": safe_str(row[11], 200),
            "supplier_name": safe_str(row[12], 255),
            "unit_price": safe_decimal(row[15]),
        }

    def _parse_jan2026_row(self, row):
        if not row or len(row) < 14:
            return None
        source = safe_str(row[0])
        if not source or source.lower() == "source":
            return None
        return {
            "part_number_raw": safe_str(row[5], 300),
            "nsn": row[6],
            "item_name": safe_str(row[7], 500),
            "manufacturer_name": safe_str(row[8], 255),
            "manufacturer_part_number": safe_str(row[9], 200),
            "supplier_name": safe_str(row[10], 255),
            "unit_price": safe_decimal(row[13]),
        }
