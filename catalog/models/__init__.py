from .entities import (
    Manufacturer, ManufacturerProfile,
    Organization, OrganizationProfile, CAGEEntity,  # backward compat aliases
)
from .filters import PipelineFilter
from .jobs import ImportJob, ImportJobLog
from .catalog import (
    Product, ProductSpecification,
    DataSource,
    slugify_part_number,
)

__all__ = [
    "Manufacturer",
    "ManufacturerProfile",
    "Product",
    "ProductSpecification",
    "DataSource",
    "PipelineFilter",
    "ImportJob",
    "ImportJobLog",
    "slugify_part_number",
    # Backward compat aliases
    "Organization",
    "OrganizationProfile",
    "CAGEEntity",
]
