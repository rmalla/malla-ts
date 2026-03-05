from pathlib import Path

from django.db import models


# ── Data directories ─────────────────────────────────────────────────────
# Organized by source so future sources (SAM, commercial) get their own folder.

IMPORTS_ROOT = Path("/var/www/html/malla-ts.com/imports")
DLA_DATA_DIR = IMPORTS_ROOT / "dla"


class SourceType(models.TextChoices):
    DIBBS = "dibbs", "DIBBS"
    SAM = "sam", "SAM.gov"
    SLED = "sled", "SLED"
    FOIA = "foia", "FOIA"


class JobType(models.TextChoices):
    DIBBS_SYNC = "dibbs_sync", "DIBBS Sync"
    SAM_SYNC = "sam_sync", "SAM Sync"
    SLED_SYNC = "sled_sync", "SLED Sync"
    NSN_ENRICH = "nsn_enrich", "NSN Enrichment"
    CAGE_RESOLVE = "cage_resolve", "CAGE Resolution"
    FULL_PIPELINE = "full_pipeline", "Full Pipeline"
    OPPORTUNITY_ANALYSIS = "opportunity_analysis", "Opportunity Analysis"
    FOIA_IMPORT = "foia_import", "FOIA Import"
    FLIS_HISTORY_IMPORT = "flis_history", "FLIS History Import"
    FLISV_ENRICH = "flisv_enrich", "FLISV Enrichment"
    FLIS_LINK = "flis_link", "FLIS Cross-Reference Link"
    PUBLOG_IMPORT = "publog_import", "PUB LOG Import"


class JobStatus(models.TextChoices):
    PENDING = "pending", "Pending"
    RUNNING = "running", "Running"
    COMPLETED = "completed", "Completed"
    FAILED = "failed", "Failed"


class LogLevel(models.TextChoices):
    INFO = "info", "Info"
    WARNING = "warning", "Warning"
    ERROR = "error", "Error"


class OpportunityRating(models.TextChoices):
    HIGH = "high", "High"
    MEDIUM = "medium", "Medium"
    LOW = "low", "Low"


class DIBBSStatus(models.TextChoices):
    ACTIVE = "active", "Active"
    CLOSED = "closed", "Closed"
    AWARDED = "awarded", "Awarded"
    CANCELLED = "cancelled", "Cancelled"


class FilterFieldType(models.TextChoices):
    FSC_CODE = "fsc_code", "FSC Code"
    PSC_CODE = "psc_code", "PSC Code"
    NSN = "nsn", "NSN"
    CAGE_CODE = "cage_code", "CAGE Code"
    AGENCY_CODE = "agency_code", "Agency Code"
    NOMENCLATURE = "nomenclature", "Nomenclature"
    MANUFACTURER_NAME = "mfr_name", "Manufacturer Name"
    PRICE_MIN = "price_min", "Min Unit Price"
    PRICE_MAX = "price_max", "Max Unit Price"


class FilterAction(models.TextChoices):
    EXCLUDE = "exclude", "Exclude"


class PipelineStage(models.TextChoices):
    ALL = "all", "All Stages"
    DIBBS_SYNC = "dibbs_sync", "DIBBS Sync"
    SAM_SYNC = "sam_sync", "SAM Sync"
    SLED_SYNC = "sled_sync", "SLED Sync"
    NSN_ENRICH = "nsn_enrich", "NSN Enrichment"
    CAGE_RESOLVE = "cage_resolve", "CAGE Resolution"
    PUBLOG_IMPORT = "publog_import", "PUB LOG Import"


# HigherGov API constants
HIGHERGOV_BASE_URL = "https://www.highergov.com/api-external"
HIGHERGOV_RATE_LIMIT = 10  # requests per second
HIGHERGOV_MONTHLY_LIMIT = 10_000  # records per month
