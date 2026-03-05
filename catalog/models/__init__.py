from .entities import Organization, OrganizationProfile, DistributorStats, Manufacturer, CAGEEntity, Distributor
from .filters import PipelineFilter
from .jobs import ImportJob, ImportJobLog
from .catalog import (
    CatalogItem, CatalogPricing, CatalogSpecifications,
    Product, ProductSpecification, SupplierLink, AwardHistory,
    DataProvenance, DataSource,
    NSNCatalog, Supplier,  # backward compat aliases
)
from .opportunities import Opportunity, DIBBSOpportunity, MarketOpportunity
from .transactions import PurchaseTransaction

__all__ = [
    # New names
    "Organization",
    "OrganizationProfile",
    "DistributorStats",
    "CatalogItem",
    "CatalogPricing",
    "CatalogSpecifications",
    "Product",
    "ProductSpecification",
    "SupplierLink",
    "AwardHistory",
    "DataProvenance",
    "DataSource",
    "PipelineFilter",
    "ImportJob",
    "ImportJobLog",
    "Opportunity",
    "DIBBSOpportunity",
    "MarketOpportunity",
    "PurchaseTransaction",
    # Backward compat aliases
    "Manufacturer",
    "CAGEEntity",
    "Distributor",
    "NSNCatalog",
    "Supplier",
]
