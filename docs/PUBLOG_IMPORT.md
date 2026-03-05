# PUB LOG Product Catalog Import

Import the full government product catalog from DLA FLIS PUB LOG data into
the procurement database (NSNCatalog, Manufacturer, Supplier/Product models).

## Data Source

Download the 4 PUB LOG zip files from the DLA FLIS Electronic Reading Room
and place them in `/var/www/html/malla-ts.com/imports/`:

| File | CSV Inside | Rows | Purpose |
|---|---|---|---|
| `MANAGEMENT.zip` | `V_FLIS_MANAGEMENT.CSV` | 12.7M | Unit prices, unit of issue |
| `IDENTIFICATION.zip` | `P_FLIS_NSN.CSV` | ~6M | FSC, NIIN, item name |
| `REFERENCE.zip` | `V_FLIS_PART.CSV` | ~20M | NIIN → CAGE code, part number |
| `CAGE.zip` | `P_CAGE.CSV` | ~800K | CAGE code → company name, location |

## How It Works

The importer runs 5 phases:

1. **Scan Prices** — Reads MANAGEMENT.zip, collects NIINs where $500 <= price <= $45,000 (~1.4M eligible)
2. **Create Catalog** — Reads IDENTIFICATION.zip, creates NSNCatalog entries for eligible NIINs with prices
3. **Scan References** — Reads REFERENCE.zip, collects supplier rows + needed CAGE codes for eligible NIINs
4. **Create Manufacturers** — Reads CAGE.zip, creates Manufacturer records only for needed CAGE codes
5. **Create Products** — Creates Supplier/Product links from collected reference data, applying pipeline filters

Each phase uses `bulk_create(ignore_conflicts=True)`, making the import **safe to re-run**.
Existing records are never overwritten or duplicated.

## Quick Start

```bash
# Activate virtualenv
source /var/www/html/malla-ts.com/venv/bin/activate

# Test with 50 products (fast — ~1 min)
python manage.py sync_procurement publog --limit 50

# Test catalog only, no supplier linking (instant — ~3 sec)
python manage.py sync_procurement publog --limit 100 --skip-suppliers
```

## Full Import Strategy

The full import processes ~50M CSV rows total. Estimated time: **5–10 minutes**.
The main bottleneck is Phase 3 (scanning 20M rows of V_FLIS_PART.CSV at ~300K rows/sec).

### Option A: Run All at Once (recommended)

Use `nohup` or `screen` so the import survives SSH disconnection:

```bash
source /var/www/html/malla-ts.com/venv/bin/activate

# Run in background with nohup
nohup python manage.py sync_procurement publog > /tmp/publog_import.log 2>&1 &

# Monitor progress
tail -f /tmp/publog_import.log
```

If it fails mid-run, just re-run the same command. The import is idempotent —
it skips all previously created records and picks up where it left off.

### Option B: Import in Batches (safer for low-memory servers)

Each `--limit N` run skips NIINs already in the catalog, so you can import
incrementally. Each batch takes ~1–2 minutes.

```bash
source /var/www/html/malla-ts.com/venv/bin/activate

# Import in chunks of 50,000
for i in $(seq 1 28); do
  echo "=== Batch $i/28 ==="
  python manage.py sync_procurement publog --limit 50000
done
```

Why this works:
- Phase 1 skips NIINs already in NSNCatalog
- Phases 2–5 use `ignore_conflicts=True`, so duplicates are harmless
- Each run creates the next 50K products that don't exist yet
- If a batch fails, just re-run — it resumes from where it stopped

### Option C: Catalog First, Then Products

Split into two passes if you want the catalog available quickly:

```bash
# Pass 1: Create all 1.4M catalog entries (fast — no REFERENCE scan)
python manage.py sync_procurement publog --skip-suppliers

# Pass 2: Add supplier/product links
python manage.py sync_procurement publog
```

## Pipeline Filters

The importer respects Pipeline Filter rules configured in Django admin:
**https://malla-ts.com/django-admin/procurement/pipelinefilter/**

Supported filter types for PUB LOG:
- **Manufacturer Name** — substring match (e.g., "boeing" excludes "THE BOEING COMPANY")
- **CAGE Code** — exact match (e.g., "81205")
- **FSC Code** — exact 4-digit match
- **Min/Max Unit Price** — price range enforcement

Filters are applied in Phase 5 when creating Supplier/Product records.
Excluded products are counted and logged in the job audit trail.

Set **Stage** to "All Stages" or "PUB LOG Import" for the filter to apply.

## Checking Results

```bash
source /var/www/html/malla-ts.com/venv/bin/activate

# Quick counts
python manage.py sync_procurement status

# Detailed check
python manage.py shell -c "
from procurement.models import NSNCatalog, Manufacturer
from procurement.models.catalog import Supplier
print(f'Catalog entries: {NSNCatalog.objects.count():,}')
print(f'Manufacturers:   {Manufacturer.objects.count():,}')
print(f'Products:        {Supplier.objects.count():,}')

# Sample product
p = Supplier.objects.select_related('nsn_catalog', 'cage').first()
if p:
    print(f'\nSample: {p.nsn_catalog.nomenclature}')
    print(f'  NSN:   {p.nsn_catalog.nsn}')
    print(f'  Price: \${p.nsn_catalog.unit_price}')
    print(f'  Mfr:   {p.cage.company_name} ({p.cage.cage_code})')
    print(f'  PN:    {p.part_number}')
"
```

## Troubleshooting

### Import is slow
- Phase 3 (REFERENCE scan) always takes ~60–70 seconds — this is unavoidable (20M rows)
- Use `--skip-suppliers` to skip Phases 3–5 for catalog-only imports
- Use `--limit N` for testing

### "0 catalog entries created"
- Those NIINs already exist in NSNCatalog (from FLIS History or a previous run)
- This is normal — the import skips existing records

### Memory issues on full import
- The full import holds ~1.4M NIIN entries + reference rows in memory (~2–3 GB)
- Use Option B (batch import with `--limit 50000`) to reduce memory usage

### Job failed mid-import
- Just re-run the exact same command — it's idempotent
- Check the job log: `python manage.py sync_procurement status`

## Expected Output (Full Import)

```
Phase 1 complete: ~1,400,000 eligible NIINs ($500–$45K)
Phase 2 complete: ~1,400,000 catalog entries created
Phase 3 complete: ~3,000,000 supplier rows, ~200,000 unique CAGE codes
Phase 4 complete: ~200,000 manufacturers created
Phase 5 complete: ~2,500,000 supplier links created, N filtered
```

## CLI Reference

```
python manage.py sync_procurement publog [OPTIONS]

Options:
  --limit N            Only import first N new eligible NIINs
  --skip-suppliers     Skip REFERENCE/CAGE phases (catalog entries only)
```
