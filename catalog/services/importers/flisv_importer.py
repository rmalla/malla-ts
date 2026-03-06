"""
FLISV Characteristics Importer — streams FLISV.CSV (50M rows, 2.2GB)
to enrich Product records with decoded physical characteristics
stored as ProductSpecification rows.

Data files (in /imports/):
  FLISV.zip   -> FLISV.CSV    (50M rows) — coded characteristics per NIIN
  MRD_1.zip   -> MRD0107.CSV             — MRC code definitions
              -> MRD0300.CSV             — reply decode table
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

FLISV_ZIP = DLA_DATA_DIR / "FLISV.zip"
MRD_ZIP = DLA_DATA_DIR / "MRD_1.zip"

SLEEP_BETWEEN_BATCHES = 0.05

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
    """Build a list of {group, label, value} from decoded attributes."""
    specifications = []
    seen = set()

    for attr_name, value in attrs:
        if not attr_name or not value:
            continue
        if attr_name.upper() == "ITEM NAME":
            continue

        category = _categorize(attr_name)
        group = GROUP_MAP.get(category, category.title())
        label = _label(attr_name)

        key = (label, value)
        if key in seen:
            continue
        seen.add(key)

        specifications.append({
            "group": group,
            "label": label,
            "value": value,
        })

    return specifications


# ── Importer class ───────────────────────────────────────────────────────

class FLISVImporter(BaseImporter):
    """Enrich Products with FLISV characteristics as ProductSpecification rows."""

    job_type = JobType.FLISV_ENRICH

    def __init__(self, stdout=None):
        super().__init__(stdout=stdout)

    def run(self, batch_size=10000, **kwargs):
        if not FLISV_ZIP.exists():
            raise FileNotFoundError(f"FLISV.zip not found at {FLISV_ZIP}")
        if not MRD_ZIP.exists():
            raise FileNotFoundError(f"MRD_1.zip not found at {MRD_ZIP}")

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

        for nsn, pk in Product.objects.values_list("nsn", "pk").iterator(chunk_size=10000):
            if not nsn:
                continue
            parts = nsn.split("-")
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

        # Phase 3: Stream FLISV.CSV
        self.log("Phase 3: Scanning FLISV.CSV (50M rows)...")
        updated = 0
        errored = 0
        row_count = 0

        niin_attrs = defaultdict(list)
        niins_in_batch = set()

        zf = zipfile.ZipFile(FLISV_ZIP, "r")
        try:
            with zf.open("FLISV.CSV") as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
                next(reader)

                for row in reader:
                    row_count += 1

                    niin = row[0].strip().zfill(9)
                    if niin not in target_niins:
                        continue

                    mrc = row[1].strip()
                    mode = row[2].strip()
                    raw_reply = row[6].strip()

                    if not mrc or not raw_reply:
                        continue

                    attr_name, value = _decode_reply(
                        mrc, mode, raw_reply, mrc_defs, reply_decode
                    )
                    niin_attrs[niin].append((attr_name, value))
                    niins_in_batch.add(niin)

                    if len(niins_in_batch) >= batch_size:
                        u, e = self._flush(niin_attrs, niin_to_product_pks)
                        updated += u
                        errored += e
                        niin_attrs.clear()
                        niins_in_batch.clear()
                        time.sleep(SLEEP_BETWEEN_BATCHES)

                    if row_count % 5_000_000 == 0:
                        self.log(
                            f"  FLISV: {row_count:,} rows, "
                            f"{len(niins_in_batch):,} in batch, "
                            f"{updated:,} updated so far"
                        )
        finally:
            zf.close()

        if niin_attrs:
            u, e = self._flush(niin_attrs, niin_to_product_pks)
            updated += u
            errored += e

        self.log(f"Complete: {row_count:,} rows scanned, {updated:,} products enriched, {errored:,} errors")

        self.job.records_fetched = row_count
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
