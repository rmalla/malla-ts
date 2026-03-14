"""
FLISV Characteristics Importer — queries FLISVCharacteristic staging table
to enrich Product records with decoded physical characteristics
stored as ProductSpecification rows.

Data files (in /imports/):
  MRD_1.zip   -> MRD0107.CSV             — MRC code definitions
              -> MRD0300.CSV             — reply decode table

Prerequisite: run `sync_catalog load-flisv` to populate the staging table.
"""

import csv
import gc
import io
import logging
import re
import time
import zipfile
from collections import defaultdict

from django.db import transaction

from catalog.constants import DLA_DATA_DIR, JobType, LogLevel
from catalog.models import Product
from catalog.models.catalog import ProductSpecification
from .base import BaseImporter

logger = logging.getLogger(__name__)

MRD_ZIP = DLA_DATA_DIR / "MRD_1.zip"

# ── Categorization keywords ─────────────────────────────────────────────

DIMENSION_KEYWORDS = frozenset({
    "length", "width", "height", "depth", "diameter", "thickness",
    "radius", "circumference", "distance", "span", "bore", "stroke",
    "travel", "reach", "pitch", "thread", "overall", "size",
    "clearance", "gap", "spacing",
})
WEIGHT_KEYWORDS = frozenset({"weight", "mass"})
MATERIAL_KEYWORDS = frozenset({"material", "alloy", "composition", "metal"})
MATERIAL_EXCLUDE = frozenset({"hazardous", "classification", "code", "handling"})
COLOR_KEYWORDS = frozenset({"color", "hue", "tint"})
ELECTRICAL_KEYWORDS = frozenset({
    "voltage", "wattage", "amperage", "current", "resistance",
    "impedance", "capacitance", "inductance", "frequency", "power",
    "ohm", "volt", "watt", "amp", "hertz",
})
TEMPERATURE_KEYWORDS = frozenset({"temperature", "thermal", "heat"})
PRESSURE_KEYWORDS = frozenset({"pressure", "psi", "torque"})
CAPACITY_KEYWORDS = frozenset({"capacity", "volume", "flow", "rate", "speed", "rpm"})

GROUP_MAP = {
    "dimensions": "Dimensions",
    "weight": "Weight",
    "material": "Material",
    "color": "Color",
    "electrical": "Electrical",
    "temperature": "Temperature",
    "pressure": "Pressure",
    "performance": "Performance",
    "other": "General",
}


def _categorize(attr_name):
    lower = attr_name.lower()
    for kw in DIMENSION_KEYWORDS:
        if kw in lower:
            return "dimensions"
    for kw in WEIGHT_KEYWORDS:
        if kw in lower:
            return "weight"
    for kw in MATERIAL_KEYWORDS:
        if kw in lower:
            if any(ex in lower for ex in MATERIAL_EXCLUDE):
                return "other"
            return "material"
    for kw in COLOR_KEYWORDS:
        if kw in lower:
            return "color"
    for kw in ELECTRICAL_KEYWORDS:
        if kw in lower:
            return "electrical"
    for kw in TEMPERATURE_KEYWORDS:
        if kw in lower:
            return "temperature"
    for kw in PRESSURE_KEYWORDS:
        if kw in lower:
            return "pressure"
    for kw in CAPACITY_KEYWORDS:
        if kw in lower:
            return "performance"
    return "other"


def _label(name):
    return name.replace("_", " ").strip().title()


# ── Decode tables ────────────────────────────────────────────────────────

def _load_decode_tables(log_fn):
    mrc_defs = {}
    reply_decode = {}

    zf = zipfile.ZipFile(MRD_ZIP, "r")
    try:
        log_fn("  Loading MRC definitions (MRD0107.CSV)...")
        with zf.open("MRD0107.CSV") as f:
            reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
            next(reader)
            for row in reader:
                mrc = row[0].strip()
                if mrc in mrc_defs:
                    continue
                mrc_defs[mrc] = {
                    "name": row[7].strip(),
                    "tbl1": row[11].strip() if len(row) > 11 else "",
                    "len1": row[10].strip() if len(row) > 10 else "",
                    "tbl2": row[13].strip() if len(row) > 13 else "",
                    "len2": row[12].strip() if len(row) > 12 else "",
                }
        log_fn(f"  {len(mrc_defs):,} MRC definitions loaded")

        log_fn("  Loading reply decode table (MRD0300.CSV)...")
        with zf.open("MRD0300.CSV") as f:
            reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
            next(reader)
            for row in reader:
                reply_decode[(row[0].strip(), row[1].strip())] = row[3].strip()
        log_fn(f"  {len(reply_decode):,} reply entries loaded")
    finally:
        zf.close()

    return mrc_defs, reply_decode


def _decode_reply(mrc, mode, raw, mrc_defs, reply_decode):
    defn = mrc_defs.get(mrc)
    if not defn:
        return mrc, raw

    attr_name = defn["name"] or mrc

    if mode in ("E", "A"):
        return attr_name, raw

    if mode == "D":
        decoded = reply_decode.get((defn["tbl1"], raw))
        return attr_name, decoded or raw

    if mode == "J":
        try:
            cl1 = int(defn["len1"]) if defn["len1"] else 1
            cl2 = int(defn["len2"]) if defn["len2"] else 1
        except ValueError:
            cl1, cl2 = 1, 1

        prefix_len = cl1 + cl2
        if len(raw) <= prefix_len:
            return attr_name, raw

        unit_code = raw[:cl1]
        qual_code = raw[cl1:prefix_len]
        number = raw[prefix_len:]

        unit = reply_decode.get((defn["tbl1"], unit_code), "")
        qualifier = reply_decode.get((defn["tbl2"], qual_code), "")

        parts = [number]
        if unit and unit.upper() != "NOT APPLICABLE":
            parts.append(unit.lower())
        if qualifier and qualifier.upper() not in ("NOT APPLICABLE", "BLANK", "NOMINAL"):
            parts.append(f"({qualifier.lower()})")
        return attr_name, " ".join(parts)

    return attr_name, raw


def _build_specs(attrs):
    """Build a list of {group, label, value} from decoded attributes.

    Duplicate labels are consolidated into a single spec with values
    joined by "; " (e.g. multiple Material entries become one).
    """
    from collections import OrderedDict

    # Collect unique values per (group, label), preserving order
    label_data = OrderedDict()
    seen_values = {}

    for attr_name, value in attrs:
        if not attr_name or not value:
            continue
        if attr_name.upper() == "ITEM NAME":
            continue

        category = _categorize(attr_name)
        group = GROUP_MAP.get(category, category.title())
        label = _label(attr_name)

        key = (group, label)
        if key not in label_data:
            label_data[key] = []
            seen_values[key] = set()

        if value not in seen_values[key]:
            seen_values[key].add(value)
            if len(label_data[key]) < 2:
                label_data[key].append(value)

    specifications = []
    for (group, label), values in label_data.items():
        specifications.append({
            "group": group,
            "label": label,
            "value": "; ".join(values),
        })

    return specifications


# ── Importer class ───────────────────────────────────────────────────────

class FLISVImporter(BaseImporter):
    """Enrich Products with FLISV characteristics as ProductSpecification rows."""

    job_type = JobType.FLISV_ENRICH

    def __init__(self, stdout=None):
        super().__init__(stdout=stdout)

    def run(self, batch_size=10000, **kwargs):
        if not MRD_ZIP.exists():
            raise FileNotFoundError(f"MRD_1.zip not found at {MRD_ZIP}")

        # Check staging table is populated
        from django.db import connection
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM catalog_flisv_characteristic")
            staging_count = cursor.fetchone()[0]
        if staging_count == 0:
            raise RuntimeError(
                "catalog_flisv_characteristic staging table is empty. "
                "Run 'sync_catalog load-flisv' first."
            )
        self.log(f"Staging table has {staging_count:,} rows")

        # Phase 1: Load decode tables
        self.log("Phase 1: Loading decode tables...")
        mrc_defs, reply_decode = _load_decode_tables(self.log)
        gc.collect()

        # Phase 2: Build NIIN -> product PKs lookup, detect already-enriched
        self.log("Phase 2: Building NIIN -> Product lookup...")
        niin_to_product_pks = defaultdict(list)
        already_enriched_product_pks = set(
            ProductSpecification.objects.values_list("product_id", flat=True).distinct()
        )

        for nsn_val, pk in Product.objects.filter(nsn__isnull=False).values_list("nsn__nsn", "pk").iterator(chunk_size=10000):
            if not nsn_val:
                continue
            parts = nsn_val.split("-")
            if len(parts) == 4:
                niin = parts[1] + parts[2] + parts[3]
                if pk not in already_enriched_product_pks:
                    niin_to_product_pks[niin].append(pk)

        target_niins = set(niin_to_product_pks.keys())
        total_products = sum(len(pks) for pks in niin_to_product_pks.values())
        self.log(f"  {len(target_niins):,} NIINs to enrich ({total_products:,} products)")
        self.log(f"  {len(already_enriched_product_pks):,} products already have specs")

        del already_enriched_product_pks
        gc.collect()

        if not target_niins:
            self.log("All products already have specifications — nothing to do.")
            return

        # Phase 3: Query staging table instead of streaming 50M CSV rows
        self.log("Phase 3: Querying staging table for matching NIINs...")
        updated = 0
        errored = 0
        rows_matched = 0

        # Process in batches of NIINs
        niin_list = list(target_niins)
        niin_attrs = defaultdict(list)

        for i in range(0, len(niin_list), batch_size):
            batch_niins = niin_list[i:i + batch_size]

            from django.db import connection
            with connection.cursor() as cursor:
                placeholders = ",".join(["%s"] * len(batch_niins))
                cursor.execute(
                    f"SELECT niin, mrc, mode_code, coded_reply "
                    f"FROM catalog_flisv_characteristic WHERE niin IN ({placeholders})",
                    batch_niins,
                )
                chars = cursor.fetchall()

            for niin, mrc, mode, raw_reply in chars:
                rows_matched += 1
                attr_name, value = _decode_reply(
                    mrc, mode, raw_reply, mrc_defs, reply_decode
                )
                niin_attrs[niin].append((attr_name, value))

            u, e = self._flush(niin_attrs, niin_to_product_pks)
            updated += u
            errored += e
            niin_attrs.clear()

            if (i // batch_size + 1) % 10 == 0:
                self.log(
                    f"  Batch {i // batch_size + 1}: "
                    f"{rows_matched:,} rows matched, "
                    f"{updated:,} products enriched"
                )

        self.log(f"Complete: {rows_matched:,} rows matched, {updated:,} products enriched, {errored:,} errors")

        self.job.records_fetched = rows_matched
        self.job.records_updated = updated
        self.job.records_errored = errored
        self.job.save(update_fields=["records_fetched", "records_updated", "records_errored"])

    def _flush(self, niin_attrs, niin_to_product_pks):
        """Create ProductSpecification rows for all products matching these NIINs."""
        product_specs_to_create = []
        products_enriched = 0

        for niin, attrs in niin_attrs.items():
            product_pks = niin_to_product_pks.get(niin)
            if not product_pks:
                continue

            specifications = _build_specs(attrs)
            if not specifications:
                continue

            for product_pk in product_pks:
                for idx, spec in enumerate(specifications):
                    product_specs_to_create.append(ProductSpecification(
                        product_id=product_pk,
                        group=spec["group"],
                        label=spec["label"],
                        value=spec["value"],
                        sort_order=idx,
                    ))
                products_enriched += 1

        if not product_specs_to_create:
            return 0, 0

        try:
            with transaction.atomic():
                ProductSpecification.objects.bulk_create(
                    product_specs_to_create,
                    ignore_conflicts=True,
                    batch_size=5000,
                )
            return products_enriched, 0
        except Exception as e:
            self.log(f"  Batch error: {e}", level=LogLevel.WARNING)
            return 0, products_enriched
