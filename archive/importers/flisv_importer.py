"""
FLISV Characteristics Importer — streams FLISV.CSV (50M rows, 2.2GB)
to enrich CatalogItem records with fully decoded physical characteristics
stored in CatalogSpecifications, and propagated to ProductSpecification.

Data files (in /imports/):
  FLISV.zip   → FLISV.CSV    (50M rows) — coded characteristics per NIIN
  MRD_1.zip   → MRD0107.CSV             — MRC code definitions
              → MRD0300.CSV             — reply decode table

Decode chain:
  1. MRC code  → MRD0107 → attribute name (e.g. ABHP → "OVERALL LENGTH")
  2. MODE_CODE determines reply format:
     - D: coded reply → look up (reply_table, code) in MRD0300
     - J: numeric with prefix → [unit_code][qualifier_code][number]
          unit from reply_tbl_1 (e.g. AA05: A=INCHES, L=MILLIMETERS)
          qualifier from reply_tbl_2 (e.g. AC20: A=NOMINAL, B=MINIMUM)
     - E: cleartext — reply is the value as-is
     - A: numeric count
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
from catalog.models import CatalogItem
from catalog.models.catalog import CatalogSpecifications, Product, ProductSpecification
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
# Exclude these from material categorization (they contain "material" but aren't materials)
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

# Friendly group names for specifications display
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

# Tier 1 extraction: (category, slug_key) → model field name
# These get extracted into dedicated DB columns for filtering/search
TIER1_MAP = {
    ("material", None): "material",
    ("dimensions", "overall_length"): "overall_length",
    ("dimensions", "overall_width"): "overall_width",
    ("dimensions", "overall_height"): "overall_height",
    ("dimensions", "overall_diameter"): "overall_diameter",
    ("weight", "weight"): "weight",
    ("weight", "vehicle_curb_weight"): "weight",
    ("weight", "weight_per_unit_measure"): "weight",
    ("color", None): "color",
    ("other", "end_item_identification"): "end_item_identification",
    ("other", "special_features"): "special_features",
}

# Max lengths for Tier 1 CharField fields
TIER1_MAX_LEN = {
    "material": 500,
    "end_item_identification": 500,
    "overall_length": 200,
    "overall_width": 200,
    "overall_height": 200,
    "overall_diameter": 200,
    "weight": 200,
    "color": 200,
}


def _categorize(attr_name):
    """Assign an attribute to a display category."""
    lower = attr_name.lower()
    for kw in DIMENSION_KEYWORDS:
        if kw in lower:
            return "dimensions"
    for kw in WEIGHT_KEYWORDS:
        if kw in lower:
            return "weight"
    for kw in MATERIAL_KEYWORDS:
        if kw in lower:
            # Exclude false positives like "HAZARDOUS MATERIAL CLASSIFICATION CODE"
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


def _slug(name):
    """'OVERALL LENGTH' → 'overall_length'."""
    return re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')


def _label(name):
    """'overall_length' or 'OVERALL LENGTH' → 'Overall Length'."""
    return name.replace("_", " ").strip().title()


def _flatten_value(val):
    """Convert value (string or list) to a display string."""
    if isinstance(val, list):
        return "; ".join(str(v) for v in val)
    return str(val) if val is not None else ""


# ── Decode tables ────────────────────────────────────────────────────────

def _load_decode_tables(log_fn):
    """Load MRC definitions and reply decode tables from MRD_1.zip."""
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
    """Decode a single FLISV reply → (attr_name, display_value)."""
    defn = mrc_defs.get(mrc)
    if not defn:
        return mrc, raw

    attr_name = defn["name"] or mrc

    if mode == "E":
        return attr_name, raw

    if mode == "A":
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


def _build_enrichment(attrs):
    """Organize [(attr_name, value), ...] into:
    - characteristics: nested JSON (legacy format, kept for reference)
    - tier1: dict of dedicated field values
    - specifications: flat list of {label, value, group} for display

    Returns (characteristics, tier1, specifications).
    """
    characteristics = {}
    tier1 = {}
    specifications = []

    for attr_name, value in attrs:
        if not attr_name or not value:
            continue
        if attr_name.upper() == "ITEM NAME":
            continue

        category = _categorize(attr_name)
        key = _slug(attr_name)
        group = GROUP_MAP.get(category, category.title())

        # ── Build specifications list (Tier 2) ──
        specifications.append({
            "label": _label(attr_name),
            "value": value,
            "group": group,
        })

        # ── Extract Tier 1 fields ──
        # Check direct value categories (material, color)
        if category in ("material", "color"):
            t1_key = (category, None)
        else:
            t1_key = (category, key)

        if t1_key in TIER1_MAP:
            field = TIER1_MAP[t1_key]
            if field == "special_features":
                # Accumulate special features (can have multiple)
                existing = tier1.get(field, "")
                if existing and value not in existing:
                    tier1[field] = f"{existing}; {value}"
                else:
                    tier1[field] = value
            elif category in ("material", "color"):
                # Accumulate material/color
                existing = tier1.get(field, "")
                if existing and value not in existing:
                    tier1[field] = f"{existing}; {value}"
                else:
                    tier1[field] = value
            elif field not in tier1:
                # First match wins for dimension/weight fields
                tier1[field] = value

        # ── Build characteristics JSON (legacy nested format) ──
        if category in ("material", "color"):
            existing = characteristics.get(category)
            if existing and value not in existing:
                characteristics[category] = f"{existing}; {value}"
            else:
                characteristics[category] = value
        else:
            if category not in characteristics:
                characteristics[category] = {}
            cat_dict = characteristics[category]
            existing = cat_dict.get(key)
            if existing is None:
                cat_dict[key] = value
            elif isinstance(existing, list):
                if value not in existing:
                    existing.append(value)
            elif existing != value:
                cat_dict[key] = [existing, value]

    # Clean empty categories
    characteristics = {k: v for k, v in characteristics.items() if v}

    # Truncate Tier 1 values to max lengths
    for field, max_len in TIER1_MAX_LEN.items():
        if field in tier1:
            tier1[field] = tier1[field][:max_len]

    # Deduplicate specs (same label+value can appear if MRC repeated)
    seen = set()
    unique_specs = []
    for spec in specifications:
        key = (spec["label"], spec["value"])
        if key not in seen:
            seen.add(key)
            unique_specs.append(spec)

    return characteristics, tier1, unique_specs


# ── Importer class ───────────────────────────────────────────────────────

class NullClient:
    api_calls_made = 0


class FLISVImporter(BaseImporter):
    """Enrich CatalogItem records with fully decoded FLISV characteristics."""

    job_type = JobType.FLISV_ENRICH

    def __init__(self, stdout=None):
        super().__init__(client=NullClient(), stdout=stdout)

    def run(self, batch_size=10000, **kwargs):
        if not FLISV_ZIP.exists():
            raise FileNotFoundError(f"FLISV.zip not found at {FLISV_ZIP}")
        if not MRD_ZIP.exists():
            raise FileNotFoundError(f"MRD_1.zip not found at {MRD_ZIP}")

        # Phase 1: Load decode tables
        self.log("Phase 1: Loading decode tables...")
        mrc_defs, reply_decode = _load_decode_tables(self.log)
        gc.collect()

        # Phase 2: Build NIIN lookup + detect already-enriched items
        self.log("Phase 2: Building NIIN → PK lookup...")
        niin_to_pk = {}
        done_pks = set(
            CatalogSpecifications.objects.values_list("catalog_item_id", flat=True)
        )
        already_done = set()

        for nsn, pk in CatalogItem.objects.values_list(
            "nsn", "pk"
        ).iterator(chunk_size=10000):
            parts = nsn.split("-")
            if len(parts) == 4:
                niin = parts[1] + parts[2] + parts[3]
                niin_to_pk[niin] = pk
                if pk in done_pks:
                    already_done.add(niin)

        del done_pks  # free memory
        target_niins = set(niin_to_pk.keys()) - already_done
        self.log(f"  {len(niin_to_pk):,} catalog NIINs, {len(already_done):,} already enriched")
        self.log(f"  {len(target_niins):,} NIINs to enrich")

        if not target_niins:
            self.log("All NIINs already have characteristics — nothing to do.")
            return

        gc.collect()

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
                        u, e = self._flush(niin_attrs, niin_to_pk)
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

        # Final flush
        if niin_attrs:
            u, e = self._flush(niin_attrs, niin_to_pk)
            updated += u
            errored += e

        self.log(f"Complete: {row_count:,} rows scanned, {updated:,} enriched, {errored:,} errors")

        self.job.records_fetched = row_count
        self.job.records_updated = updated
        self.job.records_errored = errored
        self.job.save(update_fields=["records_fetched", "records_updated", "records_errored"])

    def _flush(self, niin_attrs, niin_to_pk):
        """Build enrichment data and create CatalogSpecifications + ProductSpecification records."""
        pks_needed = [niin_to_pk[n] for n in niin_attrs if n in niin_to_pk]
        if not pks_needed:
            return 0, 0

        # Get CatalogItems for nomenclature backfill
        existing = {
            obj.pk: obj
            for obj in CatalogItem.objects.filter(pk__in=pks_needed).only(
                "pk", "nsn", "nomenclature"
            )
        }

        specs_to_create = []
        items_to_update = []

        for niin, attrs in niin_attrs.items():
            pk = niin_to_pk.get(niin)
            if not pk or pk not in existing:
                continue

            obj = existing[pk]
            characteristics, tier1, specifications = _build_enrichment(attrs)
            if not characteristics and not tier1:
                continue

            specs_to_create.append(CatalogSpecifications(
                catalog_item_id=pk,
                material=tier1.get("material", ""),
                overall_length=tier1.get("overall_length", ""),
                overall_width=tier1.get("overall_width", ""),
                overall_height=tier1.get("overall_height", ""),
                overall_diameter=tier1.get("overall_diameter", ""),
                weight=tier1.get("weight", ""),
                color=tier1.get("color", ""),
                end_item_identification=tier1.get("end_item_identification", ""),
                special_features=tier1.get("special_features", ""),
                specifications_json=specifications,
                characteristics_json=characteristics,
                source="flisv",
            ))

            # Backfill empty nomenclature from ITEM NAME MRC
            if not obj.nomenclature:
                for attr_name, value in attrs:
                    if attr_name.upper() == "ITEM NAME" and value:
                        obj.nomenclature = value[:500]
                        items_to_update.append(obj)
                        break

        if not specs_to_create:
            return 0, 0

        try:
            with transaction.atomic():
                CatalogSpecifications.objects.bulk_create(
                    specs_to_create, ignore_conflicts=True
                )
                if items_to_update:
                    CatalogItem.objects.bulk_update(
                        items_to_update, ["nomenclature"], batch_size=2000
                    )

            # Populate ProductSpecification for linked Products
            created_catalog_pks = [s.catalog_item_id for s in specs_to_create]
            self._create_product_specs(created_catalog_pks, specs_to_create)

            return len(specs_to_create), 0
        except Exception as e:
            self.log(f"  Batch error: {e}", level=LogLevel.WARNING)
            return 0, len(specs_to_create)

    def _create_product_specs(self, catalog_pks, catalog_specs):
        """Create ProductSpecification rows for Products linked to the enriched CatalogItems."""
        # Build lookup: catalog_item_id → specifications list
        specs_by_pk = {
            cs.catalog_item_id: cs.specifications_json
            for cs in catalog_specs
            if cs.specifications_json
        }
        if not specs_by_pk:
            return

        # Find Products linked to these catalog items that don't already have specs
        products = Product.objects.filter(
            catalog_item_id__in=catalog_pks,
            is_active=True,
        ).exclude(
            specs__isnull=False,
        ).values_list("pk", "catalog_item_id")

        product_specs_to_create = []
        for product_pk, catalog_item_id in products:
            spec_list = specs_by_pk.get(catalog_item_id, [])
            for idx, spec in enumerate(spec_list):
                product_specs_to_create.append(ProductSpecification(
                    product_id=product_pk,
                    group=spec.get("group", "General"),
                    label=spec.get("label", ""),
                    value=spec.get("value", ""),
                    sort_order=idx,
                ))

        if product_specs_to_create:
            try:
                ProductSpecification.objects.bulk_create(
                    product_specs_to_create,
                    ignore_conflicts=True,
                    batch_size=5000,
                )
            except Exception as e:
                self.log(
                    f"  ProductSpec batch error: {e}", level=LogLevel.WARNING
                )
