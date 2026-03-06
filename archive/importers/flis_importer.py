"""
FLIS History Importer — reads DLA HISTORY.zip to populate/enrich the NSN catalog.

Data files (in imports/dla/):
  HISTORY.zip contains:
    1. P_HISTORY_PICK.CSV       (9.9M rows) — FSC, NIIN, item name, CAGE, part number
    2. V_MANAGEMENT_HISTORY.CSV (4.3M rows) — NIIN, unit price, unit of issue
    3. V_REFERENCE_NUMBER_HISTORY.CSV (2.9M rows) — NIIN, part number, CAGE code

Uses csv.reader with column indices (not DictReader) for ~2x throughput.
"""

import csv
import io
import logging
import zipfile
from decimal import Decimal, InvalidOperation

from django.db import transaction

from catalog.constants import DLA_DATA_DIR, JobType, LogLevel
from catalog.models import CatalogItem, Manufacturer
from catalog.models.catalog import SupplierLink, CatalogPricing, DataSource
from .base import BaseImporter

logger = logging.getLogger(__name__)

HISTORY_ZIP = DLA_DATA_DIR / "HISTORY.zip"
BATCH_SIZE = 5000


class NullClient:
    api_calls_made = 0


def _col(header, name):
    """Get column index by name."""
    try:
        return header.index(name)
    except ValueError:
        raise ValueError(f"Column '{name}' not found in header: {header}")


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


class FLISHistoryImporter(BaseImporter):
    """Import FLIS history data from HISTORY.zip into NSN catalog."""

    job_type = JobType.FLIS_HISTORY_IMPORT

    def __init__(self, stdout=None):
        super().__init__(client=NullClient(), stdout=stdout)

    def run(self, skip_management=False, skip_references=False, limit=None, **kwargs):
        if not HISTORY_ZIP.exists():
            raise FileNotFoundError(f"HISTORY.zip not found at {HISTORY_ZIP}")

        if limit:
            self.log(f"Limit: {limit:,} catalog items")

        zf = zipfile.ZipFile(HISTORY_ZIP, "r")

        try:
            self.log("Phase 1: P_HISTORY_PICK.CSV (catalog + organizations)...")
            pick_created, pick_updated, pick_errored = self._import_pick(zf, limit=limit)
            self.log(
                f"Phase 1 complete: {pick_created:,} created, "
                f"{pick_updated:,} updated, {pick_errored:,} errored"
            )

            if not skip_management:
                self.log("Phase 2: V_MANAGEMENT_HISTORY.CSV (prices)...")
                mgmt_updated, mgmt_errored = self._import_management(zf)
                self.log(f"Phase 2 complete: {mgmt_updated:,} prices updated, {mgmt_errored:,} errored")
            else:
                self.log("Phase 2: Skipped (--skip-management)")
                mgmt_updated = mgmt_errored = 0

            if not skip_references:
                self.log("Phase 3: V_REFERENCE_NUMBER_HISTORY.CSV (part numbers)...")
                ref_created, ref_errored = self._import_references(zf)
                self.log(f"Phase 3 complete: {ref_created:,} links created, {ref_errored:,} errored")
            else:
                self.log("Phase 3: Skipped (--skip-references)")
                ref_created = ref_errored = 0

        finally:
            zf.close()

        total_created = pick_created + ref_created
        total_updated = pick_updated + mgmt_updated
        total_errored = pick_errored + mgmt_errored + ref_errored

        self.job.records_created = total_created
        self.job.records_updated = total_updated
        self.job.records_errored = total_errored
        self.job.records_fetched = total_created + total_updated + total_errored
        self.job.save(update_fields=[
            "records_created", "records_updated", "records_errored", "records_fetched",
        ])

    # ── Phase 1: P_HISTORY_PICK ─────────────────────────────────────────

    def _import_pick(self, zf, limit=None):
        """Import P_HISTORY_PICK.CSV using csv.reader for speed."""
        created = 0
        updated = 0
        errored = 0
        row_count = 0

        nsn_batch = {}
        cage_batch = {}
        supplier_batch = []

        existing_nsns = set(CatalogItem.objects.values_list("nsn", flat=True))
        existing_cages = set(
            Manufacturer.objects.filter(cage_code__isnull=False)
            .values_list("cage_code", flat=True)
        )

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

                    nsn = format_nsn(fsc, niin)

                    if item_name == "NO ITEM NAME AVAILABLE":
                        item_name = ""

                    if nsn not in nsn_batch:
                        nsn_batch[nsn] = {"nomenclature": item_name, "fsc_code": fsc}
                    elif item_name and not nsn_batch[nsn]["nomenclature"]:
                        nsn_batch[nsn]["nomenclature"] = item_name

                    if cage_code and len(cage_code) == 5 and cage_code not in existing_cages:
                        cage_batch[cage_code] = True

                    if cage_code and len(cage_code) == 5:
                        supplier_batch.append((nsn, cage_code, part_number))

                except Exception:
                    errored += 1

                if row_count % BATCH_SIZE == 0:
                    c, u, e = self._flush_nsn_batch(nsn_batch, existing_nsns)
                    created += c
                    updated += u
                    errored += e
                    nsn_batch.clear()

                    self._flush_cage_batch(cage_batch, existing_cages)
                    cage_batch.clear()

                    self._flush_supplier_batch(supplier_batch)
                    supplier_batch.clear()

                    if row_count % 500_000 == 0:
                        self.log(f"  PICK: {row_count:,} rows ({created:,} created, {updated:,} updated)")

                    # Check limit on unique NSNs created
                    if limit and created >= limit:
                        self.log(f"  PICK: reached limit of {limit:,} at row {row_count:,}")
                        break

        # Final flush
        c, u, e = self._flush_nsn_batch(nsn_batch, existing_nsns)
        created += c
        updated += u
        errored += e
        self._flush_cage_batch(cage_batch, existing_cages)
        self._flush_supplier_batch(supplier_batch)

        self.log(f"  PICK: {row_count:,} rows processed, {created:,} created")
        return created, updated, errored

    def _flush_nsn_batch(self, nsn_batch, existing_nsns):
        if not nsn_batch:
            return 0, 0, 0

        from home.models import FederalSupplyClass

        fsc_codes = {v["fsc_code"] for v in nsn_batch.values() if v.get("fsc_code")}
        fsc_map = {}
        if fsc_codes:
            fsc_map = {f.code: f for f in FederalSupplyClass.objects.filter(code__in=fsc_codes)}

        to_create = []
        to_update = []

        for nsn, data in nsn_batch.items():
            fsc = fsc_map.get(data.get("fsc_code"))
            if nsn in existing_nsns:
                if data["nomenclature"]:
                    to_update.append(CatalogItem(nsn=nsn, nomenclature=data["nomenclature"], fsc=fsc))
            else:
                to_create.append(CatalogItem(nsn=nsn, nomenclature=data["nomenclature"], fsc=fsc))
                existing_nsns.add(nsn)

        created = updated = errored = 0

        if to_create:
            try:
                with transaction.atomic():
                    CatalogItem.objects.bulk_create(to_create, ignore_conflicts=True)
                created = len(to_create)
            except Exception as e:
                self.log(f"  Bulk create error: {e}", level=LogLevel.WARNING)
                errored = len(to_create)

        if to_update:
            nsns_to_update = [obj.nsn for obj in to_update]
            existing_objs = CatalogItem.objects.filter(nsn__in=nsns_to_update, nomenclature="")
            update_map = {obj.nsn: obj for obj in to_update}
            objs_to_save = []
            for existing in existing_objs:
                new_data = update_map.get(existing.nsn)
                if new_data and new_data.nomenclature:
                    existing.nomenclature = new_data.nomenclature
                    if new_data.fsc:
                        existing.fsc = new_data.fsc
                    objs_to_save.append(existing)

            if objs_to_save:
                try:
                    with transaction.atomic():
                        CatalogItem.objects.bulk_update(objs_to_save, ["nomenclature", "fsc"], batch_size=BATCH_SIZE)
                    updated = len(objs_to_save)
                except Exception as e:
                    self.log(f"  Bulk update error: {e}", level=LogLevel.WARNING)
                    errored += len(objs_to_save)

        return created, updated, errored

    def _flush_cage_batch(self, cage_batch, existing_cages):
        if not cage_batch:
            return
        new_cages = [
            Manufacturer(cage_code=code, company_name="", is_manufacturer=True)
            for code in cage_batch if code not in existing_cages
        ]
        if new_cages:
            try:
                with transaction.atomic():
                    Manufacturer.objects.bulk_create(new_cages, ignore_conflicts=True)
                existing_cages.update(cage_batch.keys())
            except Exception as e:
                self.log(f"  CAGE batch error: {e}", level=LogLevel.WARNING)

    def _flush_supplier_batch(self, supplier_batch):
        if not supplier_batch:
            return

        nsns = {item[0] for item in supplier_batch}
        cages = {item[1] for item in supplier_batch}

        nsn_pk_map = dict(CatalogItem.objects.filter(nsn__in=nsns).values_list("nsn", "pk"))
        cage_pk_map = dict(Manufacturer.objects.filter(cage_code__in=cages).values_list("cage_code", "pk"))

        existing_links = set(
            SupplierLink.objects.filter(
                catalog_item_id__in=nsn_pk_map.values(),
                organization_id__in=cage_pk_map.values(),
            ).values_list("catalog_item_id", "organization_id")
        )

        to_create = []
        for nsn, cage_code, part_number in supplier_batch:
            nsn_pk = nsn_pk_map.get(nsn)
            cage_pk = cage_pk_map.get(cage_code)
            if nsn_pk and cage_pk and (nsn_pk, cage_pk) not in existing_links:
                to_create.append(SupplierLink(
                    catalog_item_id=nsn_pk, organization_id=cage_pk,
                    part_number=part_number, source=DataSource.FLIS_HISTORY,
                ))
                existing_links.add((nsn_pk, cage_pk))

        if to_create:
            try:
                with transaction.atomic():
                    SupplierLink.objects.bulk_create(to_create, ignore_conflicts=True)
            except Exception as e:
                self.log(f"  Supplier batch error: {e}", level=LogLevel.WARNING)

    # ── Phase 2: V_MANAGEMENT_HISTORY ───────────────────────────────────

    def _import_management(self, zf):
        """Scan prices using csv.reader, then apply to catalog items."""
        updated = 0
        errored = 0
        row_count = 0

        niin_prices = {}

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
                    niin_suffix = f"{niin_padded[:2]}-{niin_padded[2:5]}-{niin_padded[5:]}"

                    existing = niin_prices.get(niin_suffix)
                    if not existing or price > existing[0]:
                        niin_prices[niin_suffix] = (price, ui)
                except Exception:
                    errored += 1

                if row_count % 1_000_000 == 0:
                    self.log(f"  MGMT: {row_count:,} rows, {len(niin_prices):,} NIINs with prices")

        self.log(f"  MGMT: {row_count:,} rows scanned, {len(niin_prices):,} NIINs with prices")

        existing_pricing_ids = set(CatalogPricing.objects.values_list("catalog_item_id", flat=True))

        pricing_batch = []
        ui_batch = []

        for item in CatalogItem.objects.exclude(pk__in=existing_pricing_ids).only("pk", "nsn", "unit_of_issue").iterator(chunk_size=BATCH_SIZE):
            parts = item.nsn.split("-")
            if len(parts) != 4:
                continue

            niin_suffix = f"{parts[1]}-{parts[2]}-{parts[3]}"
            price_data = niin_prices.get(niin_suffix)
            if not price_data:
                continue

            pricing_batch.append(CatalogPricing(
                catalog_item_id=item.pk, unit_price=price_data[0],
                flis_history_price=price_data[0], unit_price_source=DataSource.FLIS_HISTORY,
            ))
            if price_data[1] and not item.unit_of_issue:
                item.unit_of_issue = price_data[1]
                ui_batch.append(item)

            if len(pricing_batch) >= BATCH_SIZE:
                try:
                    with transaction.atomic():
                        CatalogPricing.objects.bulk_create(pricing_batch, ignore_conflicts=True)
                        if ui_batch:
                            CatalogItem.objects.bulk_update(ui_batch, ["unit_of_issue"], batch_size=BATCH_SIZE)
                    updated += len(pricing_batch)
                except Exception as e:
                    self.log(f"  Price batch error: {e}", level=LogLevel.WARNING)
                    errored += len(pricing_batch)
                pricing_batch.clear()
                ui_batch.clear()

        if pricing_batch:
            try:
                with transaction.atomic():
                    CatalogPricing.objects.bulk_create(pricing_batch, ignore_conflicts=True)
                    if ui_batch:
                        CatalogItem.objects.bulk_update(ui_batch, ["unit_of_issue"], batch_size=BATCH_SIZE)
                updated += len(pricing_batch)
            except Exception as e:
                self.log(f"  Price batch error: {e}", level=LogLevel.WARNING)
                errored += len(pricing_batch)

        return updated, errored

    # ── Phase 3: V_REFERENCE_NUMBER_HISTORY ─────────────────────────────

    def _import_references(self, zf):
        """Import additional part number → CAGE links using csv.reader."""
        created = 0
        errored = 0
        row_count = 0

        niin_to_pk = {}
        for nsn, pk in CatalogItem.objects.values_list("nsn", "pk"):
            parts = nsn.split("-")
            if len(parts) == 4:
                niin_to_pk[parts[1] + parts[2] + parts[3]] = pk

        cage_to_pk = dict(
            Manufacturer.objects.filter(cage_code__isnull=False).values_list("cage_code", "pk")
        )
        existing_links = set(
            SupplierLink.objects.values_list("catalog_item_id", "organization_id")
        )

        supplier_batch = []

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
                    nsn_pk = niin_to_pk.get(niin_padded)
                    if not nsn_pk:
                        continue

                    cage_pk = cage_to_pk.get(cage_code)
                    if not cage_pk:
                        try:
                            org, _ = Manufacturer.objects.get_or_create(
                                cage_code=cage_code,
                                defaults={"company_name": "", "is_manufacturer": True},
                            )
                            cage_pk = org.pk
                            cage_to_pk[cage_code] = cage_pk
                        except Exception:
                            continue

                    if (nsn_pk, cage_pk) not in existing_links:
                        supplier_batch.append(SupplierLink(
                            catalog_item_id=nsn_pk, organization_id=cage_pk,
                            part_number=row[i_pn].strip(), source=DataSource.FLIS_HISTORY,
                        ))
                        existing_links.add((nsn_pk, cage_pk))

                except Exception:
                    errored += 1

                if len(supplier_batch) >= BATCH_SIZE:
                    try:
                        with transaction.atomic():
                            SupplierLink.objects.bulk_create(supplier_batch, ignore_conflicts=True)
                        created += len(supplier_batch)
                    except Exception as e:
                        self.log(f"  Ref batch error: {e}", level=LogLevel.WARNING)
                        errored += len(supplier_batch)
                    supplier_batch.clear()

                if row_count % 1_000_000 == 0:
                    self.log(f"  REF: {row_count:,} rows, {created:,} links")

        if supplier_batch:
            try:
                with transaction.atomic():
                    SupplierLink.objects.bulk_create(supplier_batch, ignore_conflicts=True)
                created += len(supplier_batch)
            except Exception as e:
                self.log(f"  Ref batch error: {e}", level=LogLevel.WARNING)
                errored += len(supplier_batch)

        self.log(f"  REF: {row_count:,} rows, {created:,} links created")
        return created, errored
