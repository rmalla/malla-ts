"""
Enrich enabled manufacturers with logos.

Pipeline per manufacturer:
1. If no website → look it up via SAM.gov API (by CAGE code)
2. Extract logo candidates from the website
3. Standardize the best logo (400×400 WebP, metadata stripped)
4. Save as a Wagtail Image and link to ManufacturerProfile.logo

Usage:
    python manage.py enrich_manufacturer_logos                    # all enabled
    python manage.py enrich_manufacturer_logos --cage 3XWN1       # single
    python manage.py enrich_manufacturer_logos --dry-run           # preview
    python manage.py enrich_manufacturer_logos --force             # re-extract
    python manage.py enrich_manufacturer_logos --skip-sam          # no API calls
"""
import logging
import time

from django.core.files.base import ContentFile
from django.core.management.base import BaseCommand

from catalog.models import Manufacturer, ManufacturerProfile
from catalog.services.logo_extractor import LogoExtractorService
from catalog.services.image_processor import process_logo
from catalog.services.sam_api import fetch_website_by_cage

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Extract and standardize logos for enabled manufacturers"

    def add_arguments(self, parser):
        parser.add_argument("--cage", type=str, help="Process a single CAGE code")
        parser.add_argument("--limit", type=int, help="Max manufacturers to process")
        parser.add_argument("--delay", type=float, default=2.0, help="Seconds between requests")
        parser.add_argument("--min-confidence", type=float, default=0.5)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--force", action="store_true", help="Re-extract even if logo exists")
        parser.add_argument("--skip-sam", action="store_true", help="Skip SAM.gov website lookup")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        force = options["force"]
        delay = options["delay"]

        # Build queryset: enabled manufacturers
        qs = Manufacturer.objects.select_related("profile").filter(
            profile__status=Manufacturer.ENABLED,
        )

        if options["cage"]:
            qs = qs.filter(cage_code=options["cage"])

        if not force:
            qs = qs.filter(profile__logo__isnull=True)

        qs = qs.order_by("company_name")
        if options["limit"]:
            qs = qs[: options["limit"]]

        manufacturers = list(qs)
        total = len(manufacturers)

        if total == 0:
            self.stdout.write(self.style.WARNING("No eligible manufacturers found"))
            return

        self.stdout.write(f"Found {total} manufacturer(s) to process")
        if dry_run:
            self.stdout.write(self.style.WARNING("\n=== DRY RUN ===\n"))

        extractor = LogoExtractorService(min_confidence=options["min_confidence"])
        stats = {"extracted": 0, "sam_updated": 0, "no_website": 0, "no_logo": 0, "errors": 0}

        for i, mfr in enumerate(manufacturers, 1):
            label = f"[{i}/{total}] {mfr.display_name} ({mfr.cage_code})"
            self.stdout.write(f"\n{label}")

            # Step 1: ensure we have a website
            website = mfr.website
            if not website and not options["skip_sam"] and mfr.cage_code:
                self.stdout.write("  Looking up website via SAM.gov...")
                website = fetch_website_by_cage(mfr.cage_code)
                if website:
                    self.stdout.write(self.style.SUCCESS(f"  Found: {website}"))
                    if not dry_run:
                        mfr.website = website
                        mfr.save(update_fields=["website"])
                    stats["sam_updated"] += 1
                else:
                    self.stdout.write(self.style.WARNING("  No website found in SAM.gov"))

            if not website:
                stats["no_website"] += 1
                self.stdout.write(self.style.WARNING("  Skipped: no website URL"))
                continue

            # Step 2: extract logo from website
            self.stdout.write(f"  Extracting logo from {website}...")
            try:
                candidates = extractor.extract(website)
            except Exception as e:
                stats["errors"] += 1
                self.stdout.write(self.style.ERROR(f"  Error: {e}"))
                if i < total:
                    time.sleep(delay)
                continue

            if not candidates:
                stats["no_logo"] += 1
                self.stdout.write(self.style.WARNING("  No logo candidates found"))
                if i < total:
                    time.sleep(delay)
                continue

            best = candidates[0]
            self.stdout.write(
                f"  Best: {best.strategy} (confidence: {best.confidence:.2f}) "
                f"from {best.source_url}"
            )

            if dry_run:
                self.stdout.write(self.style.SUCCESS(
                    f"  Would save logo ({len(best.image_bytes):,} bytes)"
                ))
                if i < total:
                    time.sleep(delay)
                continue

            # Step 3: standardize the logo
            try:
                webp_bytes, meta = process_logo(best.image_bytes)
            except Exception as e:
                stats["errors"] += 1
                self.stdout.write(self.style.ERROR(f"  Processing error: {e}"))
                if i < total:
                    time.sleep(delay)
                continue

            # Step 4: save as Wagtail Image and link to profile
            try:
                from wagtail.images import get_image_model

                ImageModel = get_image_model()

                slug = mfr.slug or mfr.cage_code.lower()
                filename = f"logo-{slug}.webp"
                title = f"{mfr.display_name} Logo"

                wagtail_image = ImageModel(title=title)
                wagtail_image.file = ContentFile(webp_bytes, name=filename)
                wagtail_image.save()

                # Link to profile
                profile = mfr.profile
                profile.logo = wagtail_image
                profile.save(update_fields=["logo"])

                stats["extracted"] += 1
                self.stdout.write(self.style.SUCCESS(
                    f"  Saved: {filename} ({meta['file_size']:,} bytes, "
                    f"{meta['width']}x{meta['height']})"
                ))

            except Exception as e:
                stats["errors"] += 1
                self.stdout.write(self.style.ERROR(f"  Save error: {e}"))
                logger.exception(f"Failed to save logo for {mfr.cage_code}")

            if i < total:
                time.sleep(delay)

        # Summary
        self.stdout.write("\n=== Summary ===")
        self.stdout.write(f"  Total:         {total}")
        self.stdout.write(self.style.SUCCESS(f"  Logos saved:   {stats['extracted']}"))
        if stats["sam_updated"]:
            self.stdout.write(f"  SAM websites:  {stats['sam_updated']}")
        if stats["no_website"]:
            self.stdout.write(self.style.WARNING(f"  No website:    {stats['no_website']}"))
        if stats["no_logo"]:
            self.stdout.write(self.style.WARNING(f"  No logo found: {stats['no_logo']}"))
        if stats["errors"]:
            self.stdout.write(self.style.ERROR(f"  Errors:        {stats['errors']}"))
