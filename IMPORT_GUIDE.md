# Product Import Pipeline — malla-ts.com

## Overview

The import pipeline reads DLA (Defense Logistics Agency) data files and creates Product, Manufacturer, and ProductSpecification records. All importers are incremental — safe to interrupt, resume, and re-run.

## Data Files

All source files live in `/var/www/html/malla-ts.com/imports/dla/`:

| File | Contents | Size |
|------|----------|------|
| `MANAGEMENT.zip` → `V_FLIS_MANAGEMENT.CSV` | Prices per NIIN (unit price, unit of issue) | ~12.7M rows |
| `IDENTIFICATION.zip` → `P_FLIS_NSN.CSV` | Item names per NIIN (FSC, nomenclature) | ~6M rows |
| `REFERENCE.zip` → `V_FLIS_PART.CSV` | Part numbers per NIIN (CAGE code, part number) | ~20M rows |
| `CAGE.zip` → `P_CAGE.CSV` | Manufacturer info per CAGE code (company, address) | ~800K rows |
| `HISTORY.zip` → 3 CSVs | Older FLIS history data (prices, refs, item names) | ~17M rows total |
| `FLISV.zip` → `FLISV.CSV` | Physical characteristics per NIIN (decoded via MRC codes) | ~50M rows |
| `MRD_1.zip` → `MRD0107.CSV`, `MRD0300.CSV` | MRC code definitions and decode tables (used by FLISV) | ~190K rows |
| `FOIA*.xlsx` | FOIA purchase reports (commercial purchases by agencies) | 13 files |

Optional index for faster REFERENCE/CAGE lookups:
| `publog_index.db` | SQLite index built from REFERENCE.zip + CAGE.zip | |

To build the index: `python manage.py build_publog_index`

## Database Schema

```
Manufacturer (catalog_organization)
  ├── cage_code (unique), company_name, slug, address fields
  ├── ManufacturerProfile (catalog_organizationprofile)
  │     └── display_name, description, logo, status (ENABLED/NEUTRAL/DISABLED)
  └── Product (catalog_product)
       ├── part_number, part_number_slug, name, description
       ├── price, nsn, nomenclature, fsc FK, unit_of_issue
       ├── source (publog / flis_history / foia / manual / etc.)
       └── ProductSpecification (catalog_productspecification)
            └── group, label, value, sort_order
```

## Import Commands

All commands use: `python manage.py sync_catalog <subcommand>`

### Step 1: PUBLOG (main catalog — creates ~95% of products)

```bash
python manage.py sync_catalog publog --batch-size 5000 --rounds 0 --cooldown 30
```

**What it does (5 phases per round):**
1. Scans MANAGEMENT.CSV for NIINs with prices in $500–$45,000 range
2. Scans IDENTIFICATION.CSV to get item names and FSC codes for those NIINs
3. Scans REFERENCE.CSV (or publog_index.db) to get CAGE code + part number per NIIN
4. Creates Manufacturer records for any new CAGE codes (from CAGE.CSV or index)
5. Creates Product records linking manufacturer → part number → NSN → price

**Options:**
- `--batch-size N` — NIINs per round (default: 5000)
- `--rounds N` — number of rounds, 0 = unlimited until done (default: 1)
- `--cooldown N` — seconds between rounds (default: 30)
- `--skip-rows N` — resume from a specific row in MANAGEMENT.CSV

**Incremental behavior:** Each round loads all existing product NSNs and skips them. The `--skip-rows` offset is tracked per round via `last_mgmt_row` so sequential rounds advance through the file.

**Expected volume:** ~12.7M MANAGEMENT rows, yields 300K–500K+ products depending on price eligibility and filters.

### Step 2: FLIS History (supplemental catalog data)

```bash
python manage.py sync_catalog flis-history
```

**What it does (5 phases):**
1. Scans P_HISTORY_PICK.CSV — collects NIIN → NSN, nomenclature, CAGE/part refs
2. Scans V_MANAGEMENT_HISTORY.CSV — attaches best price per NIIN
3. Scans V_REFERENCE_NUMBER_HISTORY.CSV — additional CAGE/part refs
4. Creates Manufacturer records for new CAGE codes
5. Creates Product records

**Options:**
- `--skip-management` — skip price import phase
- `--skip-references` — skip additional references phase
- `--limit N` — stop after N NIINs (for testing)

**Incremental behavior:** Checks existing (manufacturer_id, part_number) pairs and skips duplicates.

### Step 3: FOIA (commercial purchases)

```bash
python manage.py sync_catalog foia
```

**What it does:**
- Reads all `FOIA*.xlsx` files from the imports directory
- Creates Manufacturer records by company name (no CAGE code)
- Creates Product records with price, part number, and sometimes NSN
- Filters to $500–$45,000 price range

**Options:**
- `--file FILENAME` — import a specific file only
- `--limit N` — stop after N rows

**Note:** FOIA products often lack NSNs because they're commercial items (office supplies, equipment) not in the federal catalog. Manufacturers are matched by name, not CAGE code.

**Incremental behavior:** Checks existing (manufacturer_id, part_number) pairs and skips duplicates.

### Step 4: FLISV Characteristics (enrichment — run last)

```bash
python manage.py sync_catalog flis-chars
```

**What it does:**
1. Loads MRC decode tables from MRD_1.zip (27K definitions, 163K decode entries)
2. Builds NIIN → Product PK lookup for all products that don't have specs yet
3. Streams 50M-row FLISV.CSV, decodes characteristics per NIIN using MRC codes
4. Creates ProductSpecification rows (group, label, value) per product

**Options:**
- `--batch-size N` — NIINs per flush batch (default: 10000)

**Specification groups:** Dimensions, Weight, Material, Color, Electrical, Temperature, Pressure, Performance, General

**Incremental behavior:** Only enriches products that have zero ProductSpecification rows.

**Run this after all product-creating importers are done.** It takes ~2–3 minutes to scan the full 50M rows.

### Check Status

```bash
python manage.py sync_catalog status
```

Shows recent jobs and record counts (products, manufacturers, specifications, products with price, products with NSN).

## Pipeline Filters

91 active filters are defined in `PipelineFilter` (admin-editable). These exclude:
- Specific FSC codes (e.g. 6130)
- Manufacturer names matching keywords (e.g. "boeing", "lockheed", "raytheon", etc.)
- These are large defense contractors / OEMs whose parts are not suitable for resale

Filters are loaded automatically by each importer. Manage them at `/django-admin/catalog/pipelinefilter/`.

## Full Import Sequence (clean start)

```bash
cd /var/www/html/malla-ts.com && source venv/bin/activate

# 1. Main catalog (hours — can Ctrl+C and resume)
python manage.py sync_catalog publog --batch-size 5000 --rounds 0 --cooldown 30

# 2. Supplemental history data (~30 min)
python manage.py sync_catalog flis-history

# 3. FOIA commercial purchases (~5 min)
python manage.py sync_catalog foia

# 4. Physical characteristics/specs (~3 min, run last)
python manage.py sync_catalog flis-chars

# Check results
python manage.py sync_catalog status
```

## Re-importing From Scratch

If you need to wipe and reimport (e.g. new DLA data snapshot):

```sql
-- In psql or Django shell
TRUNCATE catalog_productspecification CASCADE;
TRUNCATE catalog_product CASCADE;
-- Optionally: TRUNCATE catalog_organization CASCADE; (wipes manufacturers too)
```

Then run the full sequence above.

## Troubleshooting

**Import seems stuck:** Check if the process is still running with `ps aux | grep sync_catalog`. The REFERENCE.CSV scan (Phase 3) and FLISV.CSV scan (Phase 4 of flis-chars) are the slowest — they scan 20M and 50M rows respectively.

**Memory usage:** Each round reports RSS peak. Typical usage is 200–400MB. The cooldown between rounds allows GC to reclaim memory.

**Slug errors in FOIA:** Occasionally two manufacturer names slugify identically (e.g. "ABC Inc" and "ABC Corp" → "abc"). These show as `duplicate key value violates unique constraint "catalog_organization_slug_key"` errors. They're harmless — the affected rows are skipped and logged.

**Resume after crash:** Just re-run the same command. All importers check existing data and skip what's already imported. For publog, you can also use `--skip-rows N` to jump ahead in the MANAGEMENT CSV if you know roughly where you left off (check the last job log for the row count).
