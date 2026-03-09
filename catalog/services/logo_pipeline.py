"""
Single-manufacturer logo extraction pipeline.

Reusable from management commands and admin views.
"""
import logging

from django.core.files.base import ContentFile

from catalog.services.logo_extractor import LogoExtractorService
from catalog.services.image_processor import process_logo
from catalog.services.sam_api import fetch_website_by_cage

logger = logging.getLogger(__name__)


def extract_logo_for_manufacturer(manufacturer, *, force=False, skip_sam=False):
    """
    Run the full logo pipeline for a single Manufacturer instance.

    Returns a dict:
        {"ok": True, "message": "...", "filename": "...", "strategy": "...", "confidence": 0.95}
        {"ok": False, "message": "reason it failed"}
    """
    from catalog.models.entities import ManufacturerProfile

    # Ensure profile exists
    profile, _ = ManufacturerProfile.objects.get_or_create(
        organization=manufacturer,
    )

    if profile.logo and not force:
        return {"ok": False, "message": "Logo already exists (use force to re-extract)"}

    # Step 1: ensure we have a website
    website = manufacturer.website
    if not website and not skip_sam and manufacturer.cage_code:
        website = fetch_website_by_cage(manufacturer.cage_code)
        if website:
            manufacturer.website = website
            manufacturer.save(update_fields=["website"])

    if not website:
        return {"ok": False, "message": "No website URL available"}

    # Step 2: extract logo candidates
    extractor = LogoExtractorService(min_confidence=0.5)
    try:
        candidates = extractor.extract(website)
    except Exception as e:
        logger.exception(f"Logo extraction error for {manufacturer.cage_code}")
        return {"ok": False, "message": f"Extraction error: {e}"}

    if not candidates:
        return {"ok": False, "message": f"No logo candidates found on {website}"}

    best = candidates[0]

    # Step 3: standardize
    try:
        webp_bytes, meta = process_logo(best.image_bytes)
    except Exception as e:
        logger.exception(f"Logo processing error for {manufacturer.cage_code}")
        return {"ok": False, "message": f"Processing error: {e}"}

    # Step 4: save as Wagtail Image
    try:
        from wagtail.images import get_image_model
        ImageModel = get_image_model()

        slug = manufacturer.slug or manufacturer.cage_code.lower()
        filename = f"logo-{slug}.webp"
        title = f"{manufacturer.display_name} Logo"

        wagtail_image = ImageModel(title=title)
        wagtail_image.file = ContentFile(webp_bytes, name=filename)
        wagtail_image.save()

        profile.logo = wagtail_image
        profile.save(update_fields=["logo"])

        return {
            "ok": True,
            "message": (
                f"Saved {filename} ({meta['file_size']:,} bytes, "
                f"{meta['width']}x{meta['height']}) — "
                f"{best.strategy} (confidence: {best.confidence:.2f})"
            ),
            "filename": filename,
            "strategy": best.strategy,
            "confidence": best.confidence,
        }
    except Exception as e:
        logger.exception(f"Failed to save logo for {manufacturer.cage_code}")
        return {"ok": False, "message": f"Save error: {e}"}
