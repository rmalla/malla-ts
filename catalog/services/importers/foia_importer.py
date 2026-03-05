"""
FOIA Transaction Importer — reads DLA FOIA Excel reports and imports
purchase transactions AND products into the database.

Creates:
  - CatalogItem entries (from NSN + item name)
  - CatalogPricing entries (unit price stored separately)
  - Organization entries (matched by name, no CAGE code)
  - SupplierLink + Product entries (visible in admin)
  - PurchaseTransaction entries (transaction history)

Handles two header formats:
  - Legacy format: Row 0 = "FOIA" banner, Row 1 = DAY/MONTH/YEAR/Date/DEPARTMENT/...
  - Jan2026 format: Row 0 = Source/Transaction Date/Department/Agency/...
  - November25 format: Row 0 = DAY/MONTH/YEAR/Date/DEPARTMENT/... (no FOIA banner)
"""

import os
import re
import logging
from datetime import date
from decimal import Decimal, InvalidOperation

import openpyxl

from catalog.constants import DLA_DATA_DIR, JobType, LogLevel
from catalog.models import ImportJob, ImportJobLog, CatalogItem, Organization
from catalog.models.catalog import SupplierLink, Product, CatalogPricing, DataSource, slugify_part_number
from catalog.models.transactions import PurchaseTransaction
from .base import BaseImporter

logger = logging.getLogger(__name__)

# Only create products in this price range
MIN_PRODUCT_PRICE = Decimal("500")
MAX_PRODUCT_PRICE = Decimal("45000")


def normalize_nsn(raw):
    """Strip non-digits and format as XXXX-XX-XXX-XXXX if 13 digits."""
    if raw is None:
        return ""
    digits = re.sub(r"\D", "", str(raw).strip())
    if len(digits) == 13:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:9]}-{digits[9:]}"
    return digits


def safe_str(val, max_len=None):
    """Convert value to stripped string, optionally truncated."""
    if val is None:
        return ""
    s = str(val).strip()
    if max_len:
        s = s[:max_len]
    return s


def safe_decimal(val):
    """Parse a value to Decimal, returning None on failure."""
    if val is None:
        return None
    try:
        d = Decimal(str(val))
        if d.is_nan() or d.is_infinite():
            return None
        return d
    except (ValueError, InvalidOperation):
        return None


class NullClient:
    """Dummy client for importers that don't use an API."""
    api_calls_made = 0


class FOIAImporter(BaseImporter):
    """Import government purchase transactions from DLA FOIA Excel reports."""

    job_type = JobType.FOIA_IMPORT

    def __init__(self, stdout=None):
        super().__init__(client=NullClient(), stdout=stdout)

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

        # Pre-load NSN catalog cache: nsn -> pk
        nsn_cache = dict(CatalogItem.objects.values_list("nsn", "pk"))

        # Pre-load manufacturer name cache: UPPER(company_name) -> pk
        mfr_name_cache = {}
        for pk, name in Organization.objects.values_list("pk", "company_name"):
            if name:
                mfr_name_cache[name.strip().upper()] = pk

        # Pre-load FSC cache: 4-digit code -> pk
        try:
            from home.models import FederalSupplyClass
            fsc_cache = dict(FederalSupplyClass.objects.values_list("code", "pk"))
        except Exception:
            fsc_cache = {}

        # Pre-load existing supplier pairs to avoid repeated queries
        supplier_cache = set(SupplierLink.objects.values_list("catalog_item_id", "organization_id"))

        self.log(
            f"Loaded {len(nsn_cache)} NSN catalog entries, "
            f"{len(mfr_name_cache)} manufacturers, "
            f"{len(supplier_cache)} suppliers for matching"
        )

        total_created = 0
        total_updated = 0
        total_errored = 0
        products_created = 0
        nsns_created = 0
        mfrs_created = 0
        rows_processed = 0

        for filepath in files:
            if limit and rows_processed >= limit:
                break

            filename = filepath.name
            self.log(f"Processing {filename}...")
            remaining = (limit - rows_processed) if limit else None
            try:
                created, updated, errored, stats = self._import_file(
                    filepath, nsn_cache, mfr_name_cache, fsc_cache,
                    supplier_cache, row_limit=remaining,
                )
                total_created += created
                total_updated += updated
                total_errored += errored
                nsns_created += stats["nsns_created"]
                mfrs_created += stats["mfrs_created"]
                products_created += stats["products_created"]
                rows_processed += stats["rows_processed"]
                self.log(
                    f"  {filename}: {created} txns created, {updated} updated, "
                    f"{errored} errored | {stats['nsns_created']} NSNs, "
                    f"{stats['mfrs_created']} manufacturers, "
                    f"{stats['products_created']} products created"
                )
            except Exception as e:
                self.log(f"  Error processing {filename}: {e}", level=LogLevel.ERROR)
                total_errored += 1

        self.log(
            f"Totals: {total_created} txns created, {total_updated} updated, "
            f"{total_errored} errored | {nsns_created} NSNs, {mfrs_created} manufacturers, "
            f"{products_created} products created"
        )

        self.job.records_created = total_created
        self.job.records_updated = total_updated
        self.job.records_errored = total_errored
        self.job.records_fetched = total_created + total_updated + total_errored
        self.job.save(update_fields=[
            "records_created", "records_updated", "records_errored", "records_fetched",
        ])

    def _import_file(self, filepath, nsn_cache, mfr_name_cache, fsc_cache,
                     supplier_cache, row_limit=None):
        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        ws = wb.active

        rows = list(ws.iter_rows(values_only=True))
        wb.close()

        if not rows:
            return 0, 0, 0, {"nsns_created": 0, "mfrs_created": 0, "products_created": 0, "rows_processed": 0}

        # Detect header format
        header_row, parser = self._detect_format(rows)
        if header_row is None:
            self.log(f"  Could not detect header format in {filepath.name}", level=LogLevel.WARNING)
            return 0, 0, 0, {"nsns_created": 0, "mfrs_created": 0, "products_created": 0, "rows_processed": 0}

        data_rows = rows[header_row + 1:]
        created = 0
        updated = 0
        errored = 0
        nsns_created = 0
        mfrs_created = 0
        products_created = 0
        rows_processed = 0

        for row in data_rows:
            if row_limit and rows_processed >= row_limit:
                break

            try:
                record = parser(row)
                if not record:
                    continue

                rows_processed += 1
                record["source_file"] = filepath.name

                # Normalize NSN
                nsn = normalize_nsn(record.get("nsn"))
                record["nsn"] = nsn
                is_valid_nsn = bool(nsn) and len(re.sub(r"\D", "", nsn)) == 13

                # --- Price filter for product creation ---
                unit_price = record.get("unit_price")
                price_in_range = (
                    unit_price is not None
                    and unit_price >= MIN_PRODUCT_PRICE
                    and unit_price <= MAX_PRODUCT_PRICE
                )

                # --- Step 1: get_or_create CatalogItem ---
                # Use real NSN if valid, otherwise fall back to part number
                # so products can be created even without an NSN.
                # Only create catalog entries for items in price range.
                nsn_catalog_pk = None
                catalog_key = None
                if is_valid_nsn:
                    catalog_key = nsn
                else:
                    # Use part_number_raw or manufacturer_part_number as catalog key
                    fallback = (
                        record.get("part_number_raw", "").strip()
                        or record.get("manufacturer_part_number", "").strip()
                    )
                    if fallback:
                        catalog_key = fallback

                if catalog_key and price_in_range:
                    nsn_catalog_pk = nsn_cache.get(catalog_key)
                    if nsn_catalog_pk is None:
                        fsc_pk = None
                        if is_valid_nsn:
                            fsc_code = re.sub(r"\D", "", nsn)[:4]
                            fsc_pk = fsc_cache.get(fsc_code)
                        item_name = record.get("item_name", "")
                        catalog_obj = CatalogItem.objects.create(
                            nsn=catalog_key,
                            nomenclature=item_name,
                            fsc_id=fsc_pk,
                        )
                        nsn_cache[catalog_key] = catalog_obj.pk
                        nsn_catalog_pk = catalog_obj.pk
                        nsns_created += 1
                        # Create CatalogPricing for the unit price
                        if unit_price is not None:
                            CatalogPricing.objects.create(
                                catalog_item_id=nsn_catalog_pk,
                                unit_price=unit_price,
                                source=DataSource.FOIA,
                            )
                elif catalog_key:
                    # Still look up existing catalog entry for transaction linking
                    nsn_catalog_pk = nsn_cache.get(catalog_key)

                record["catalog_item_id"] = nsn_catalog_pk

                # --- Step 2: get_or_create Organization ---
                # Use manufacturer_name; fall back to supplier_name
                mfr_pk = None
                mfr_name = record.get("manufacturer_name", "")
                if not mfr_name:
                    mfr_name = record.get("supplier_name", "")
                if mfr_name:
                    mfr_key = mfr_name.strip().upper()
                    mfr_pk = mfr_name_cache.get(mfr_key)
                    if mfr_pk is None:
                        mfr_obj = Organization(
                            cage_code=None,
                            company_name=mfr_name.strip(),
                            is_manufacturer=True,
                        )
                        mfr_obj.save()
                        mfr_name_cache[mfr_key] = mfr_obj.pk
                        mfr_pk = mfr_obj.pk
                        mfrs_created += 1

                record["manufacturer_id"] = mfr_pk

                # --- Step 3: get_or_create SupplierLink + Product ---
                # Only create products in the $500-$45,000 price range
                if nsn_catalog_pk and mfr_pk and price_in_range:
                    cache_key = (nsn_catalog_pk, mfr_pk)
                    if cache_key not in supplier_cache:
                        part_num = record.get("manufacturer_part_number", "")
                        SupplierLink(
                            catalog_item_id=nsn_catalog_pk,
                            organization_id=mfr_pk,
                            part_number=part_num,
                            source="foia",
                        ).save()
                        Product(
                            manufacturer_id=mfr_pk,
                            catalog_item_id=nsn_catalog_pk,
                            part_number=part_num,
                            source=DataSource.FOIA,
                        ).save()
                        supplier_cache.add(cache_key)
                        products_created += 1

                # --- Step 4: Create PurchaseTransaction ---
                dedup = {
                    "nsn": record["nsn"],
                    "transaction_date": record.get("transaction_date"),
                    "part_number_raw": record.get("part_number_raw", ""),
                    "supplier_name": record.get("supplier_name", ""),
                    "unit_price": record.get("unit_price"),
                }

                defaults = {
                    k: v for k, v in record.items()
                    if k not in dedup
                }

                _, was_created = PurchaseTransaction.objects.update_or_create(
                    defaults=defaults, **dedup
                )
                if was_created:
                    created += 1
                else:
                    updated += 1

            except Exception as e:
                errored += 1
                rows_processed += 1
                if errored <= 5:
                    self.log(f"  Row error: {e}", level=LogLevel.WARNING)

        stats = {
            "nsns_created": nsns_created,
            "mfrs_created": mfrs_created,
            "products_created": products_created,
            "rows_processed": rows_processed,
        }
        return created, updated, errored, stats

    def _detect_format(self, rows):
        """Detect which header format and return (header_row_index, parser_fn)."""
        first = rows[0] if rows else ()

        # Jan2026 format: first cell is "Source"
        if first and safe_str(first[0]).lower() == "source":
            return 0, self._parse_jan2026_row

        # Legacy format with FOIA banner row
        if first and safe_str(first[0]).upper() == "FOIA":
            if len(rows) > 1:
                second = rows[1]
                if safe_str(second[0]).upper() == "DAY":
                    return 1, self._parse_legacy_row
            return None, None

        # Legacy format without FOIA banner (e.g. November25)
        if first and safe_str(first[0]).upper() == "DAY":
            return 0, self._parse_legacy_row

        return None, None

    def _parse_legacy_row(self, row):
        """Parse a row from the legacy DAY/MONTH/YEAR format.

        Columns: DAY, MONTH, YEAR, Date, DEPARTMENT, AGENCY, SOS,
                 PARTNUMBER, NSN, NAME, MFNAME, MFPARTNUMBER,
                 SUPPLIER, QUANTITY, UNITOFMEASURE, PRICE, EXTNEDEDPRICE
        """
        if not row or len(row) < 16:
            return None

        day = row[0]
        month = row[1]
        year = row[2]

        # Skip non-data rows
        if not isinstance(day, (int, float)):
            return None

        try:
            txn_date = date(int(year), int(month), int(day))
        except (ValueError, TypeError):
            txn_date = None

        return {
            "transaction_date": txn_date,
            "department": safe_str(row[4], 100),
            "agency": safe_str(row[5], 255),
            "source_of_supply": safe_str(row[6], 20),
            "part_number_raw": safe_str(row[7], 300),
            "nsn": row[8],
            "item_name": safe_str(row[9], 500),
            "manufacturer_name": safe_str(row[10], 255),
            "manufacturer_part_number": safe_str(row[11], 200),
            "supplier_name": safe_str(row[12], 255),
            "quantity": safe_decimal(row[13]),
            "unit_of_measure": safe_str(row[14], 20),
            "unit_price": safe_decimal(row[15]),
            "extended_price": safe_decimal(row[16]) if len(row) > 16 else None,
        }

    def _parse_jan2026_row(self, row):
        """Parse a row from the Jan2026 format.

        Columns: Source, Transaction Date, Department, Agency, SOS,
                 Part Number, NSN, Name, Manufacturer Name,
                 Manufacturer Part Number, Supplier Name,
                 Quantity, Unit Of Measure, Price, Extended Price
        """
        if not row or len(row) < 14:
            return None

        source = safe_str(row[0])
        if not source or source.lower() == "source":
            return None

        # Parse date from "MM/DD/YYYY"
        txn_date = None
        date_str = safe_str(row[1])
        if date_str:
            try:
                parts = date_str.split("/")
                if len(parts) == 3:
                    txn_date = date(int(parts[2]), int(parts[0]), int(parts[1]))
            except (ValueError, IndexError):
                pass

        return {
            "transaction_date": txn_date,
            "department": safe_str(row[2], 100),
            "agency": safe_str(row[3], 255),
            "source_of_supply": safe_str(row[4], 20),
            "part_number_raw": safe_str(row[5], 300),
            "nsn": row[6],
            "item_name": safe_str(row[7], 500),
            "manufacturer_name": safe_str(row[8], 255),
            "manufacturer_part_number": safe_str(row[9], 200),
            "supplier_name": safe_str(row[10], 255),
            "quantity": safe_decimal(row[11]),
            "unit_of_measure": safe_str(row[12], 20),
            "unit_price": safe_decimal(row[13]),
            "extended_price": safe_decimal(row[14]) if len(row) > 14 else None,
        }
