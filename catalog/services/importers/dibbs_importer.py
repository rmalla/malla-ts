"""Backward-compatible thin wrapper around OpportunityImporter for DIBBS."""

from catalog.constants import SourceType
from .opportunity_importer import OpportunityImporter


class DIBBSImporter(OpportunityImporter):
    """Sync DIBBS opportunities from HigherGov. Thin wrapper for backward compat."""

    def __init__(self, client, stdout=None):
        super().__init__(client, source_type=SourceType.DIBBS, stdout=stdout)
