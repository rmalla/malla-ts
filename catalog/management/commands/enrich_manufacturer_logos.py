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
import time

from django.core.management.base import BaseCommand

from catalog.models import Manufacturer
from catalog.services.logo_pipeline import extract_logo_for_manufacturer


class Command(BaseCommand):
    help = "Extract and standardize logos for enabled manufacturers"

    def add_arguments(self, parser):
        parser.add_argument("--cage", type=str, help="Process a single CAGE code")
        parser.add_argument("--limit", type=int, help="Max manufacturers to process")
        parser.add_argument("--delay", type=float, default=2.0, help="Seconds between requests")
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

        stats = {"extracted": 0, "no_website": 0, "no_logo": 0, "errors": 0}

        for i, mfr in enumerate(manufacturers, 1):
            label = f"[{i}/{total}] {mfr.display_name} ({mfr.cage_code})"
            self.stdout.write(f"\n{label}")

            if dry_run:
                self.stdout.write(f"  Website: {mfr.website or '(none)'}")
                if i < total:
                    time.sleep(delay)
                continue

            result = extract_logo_for_manufacturer(
                mfr, force=force, skip_sam=options["skip_sam"],
            )

            if result["ok"]:
                stats["extracted"] += 1
                self.stdout.write(self.style.SUCCESS(f"  {result['message']}"))
            else:
                msg = result["message"]
                if "No website" in msg:
                    stats["no_website"] += 1
                    self.stdout.write(self.style.WARNING(f"  Skipped: {msg}"))
                elif "No logo" in msg:
                    stats["no_logo"] += 1
                    self.stdout.write(self.style.WARNING(f"  {msg}"))
                else:
                    stats["errors"] += 1
                    self.stdout.write(self.style.ERROR(f"  {msg}"))

            if i < total:
                time.sleep(delay)

        # Summary
        self.stdout.write("\n=== Summary ===")
        self.stdout.write(f"  Total:         {total}")
        self.stdout.write(self.style.SUCCESS(f"  Logos saved:   {stats['extracted']}"))
        if stats["no_website"]:
            self.stdout.write(self.style.WARNING(f"  No website:    {stats['no_website']}"))
        if stats["no_logo"]:
            self.stdout.write(self.style.WARNING(f"  No logo found: {stats['no_logo']}"))
        if stats["errors"]:
            self.stdout.write(self.style.ERROR(f"  Errors:        {stats['errors']}"))
