"""Shared parse helpers for importers."""

from decimal import Decimal, InvalidOperation


def parse_decimal(value):
    """Parse a value to Decimal, returning None on failure."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (ValueError, InvalidOperation):
        return None


def parse_int(value):
    """Parse a value to int, returning None on failure."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def parse_date(value):
    """Parse a date string in multiple formats, returning None on failure."""
    if not value:
        return None
    try:
        from datetime import datetime

        for fmt in (
            "%Y-%m-%d",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%m/%d/%Y",
        ):
            try:
                return datetime.strptime(str(value)[:19], fmt).date()
            except ValueError:
                continue
        return None
    except Exception:
        return None
