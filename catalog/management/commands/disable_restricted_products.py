"""
Disable products in restricted categories that the site cannot legally or practically sell.

Uses multiple filtering approaches:
  A. Nomenclature keyword matching (weapons, aircraft, nuclear, naval, crypto, medical, clothing)
  B. FSC code matching (catches products with generic nomenclatures)
  C. Manufacturer-level disabling (defense-only contractors by name)
  D. Purge manufacturers — disable manufacturers where >N% of products are already disabled

Only touches NEUTRAL (is_active=0) products — never re-disables ENABLED ones.
New keyword/FSC categories can be added to the dicts at the top of the file.

Usage:
    python manage.py disable_restricted_products                  # dry-run stats
    python manage.py disable_restricted_products --stats          # current counts only
    python manage.py disable_restricted_products --dry-run        # preview per category
    python manage.py disable_restricted_products --execute        # apply all
    python manage.py disable_restricted_products --execute --category weapons
    python manage.py disable_restricted_products --execute --purge-manufacturers
    python manage.py disable_restricted_products --execute --purge-manufacturers --threshold 80
"""

from django.db import models
from django.db.models import Count, Q, F
from django.core.management.base import BaseCommand

from catalog.constants import FilterAction, FilterFieldType, PipelineStage
from catalog.models import PipelineFilter, Product
from catalog.models.entities import Manufacturer, ManufacturerProfile


# ── Nomenclature keywords by category ──────────────────────────────────────
#
# Two match modes per keyword:
#   "^KEYWORD"  → startswith (prefix match) — use for short words that appear
#                 as substrings in unrelated nomenclatures (SHELL, BOMB, TENT, etc.)
#   "KEYWORD"   → contains (substring match) — use for specific multi-word phrases
#                 or comma-qualified terms unlikely to cause false positives
#

KEYWORD_CATEGORIES = {
    "weapons": [
        # startswith — these are primary nouns in "NOUN,QUALIFIER" nomenclature format
        "^AMMUNITION", "^BOMB", "^BOMBLET", "^BULLET", "^CANNON",
        "^DETONATOR", "^EXPLOSIVE", "^FUZE", "^GRENADE",
        "^HOWITZER", "^MINE,ANTI", "^MISSILE", "^PISTOL",
        "^PROJECTILE", "^PROPELLANT", "^RIFLE",
        "^ROCKET,", "^ROCKET MOTOR", "^SHELL,", "^TORPEDO", "^WARHEAD",
        "^MORTAR,", "^MORTAR ASSEMBLY",
        # contains — specific compound terms safe from false positives
        "GUN,AUTOMATIC", "GUN,MACHINE", "GUN,AIRCRAFT",
        "GUN MOUNT",
        "LAUNCHER,GRENADE", "LAUNCHER,ROCKET", "LAUNCHER,MISSILE",
        "COUNTERMEASURE", "CHAFF", "DECOY",
        "ARMOR,SUPPLEMENTAL", "ARMOR,PILOT", "ARMOR,BODY",
        "^ARMOR PLATE,",
        "FIRING MECHANISM", "TRIGGER MECHANISM",
    ],
    "aircraft": [
        # contains — "AIRCRAFT" etc. appear as qualifiers after comma
        "AIRCRAFT", "AIRFRAME", "AILERON", "EJECTION SEAT", "HELICOPTER",
        "PROPELLER,AIRCRAFT", "PROPELLER BLADE", "ROTOR BLADE", "ROTOR,HELICOPTER",
        "WING,AIRCRAFT", "FUSELAGE", "NACELLE", "PYLON,AIRCRAFT",
        "LANDING GEAR", "COCKPIT", "AVIONICS",
        "ENGINE,AIRCRAFT", "ENGINE,JET", "TURBINE ENGINE,AIRCRAFT",
        "AFTERBURNER", "NOZZLE,TURBINE", "STATOR,TURBINE",
        "COMPRESSOR BLADE", "TURBINE BLADE",
    ],
    "nuclear": [
        "NUCLEAR", "RADIOACTIVE", "RADIATION SOURCE",
    ],
    "naval": [
        "SONAR", "SUBMARINE", "DEPTH CHARGE",
        "HULL,SHIP", "HULL,BOAT", "PROPELLER,MARINE", "PROPELLER,SHIP",
        "ANCHOR,SHIP", "RUDDER,SHIP",
    ],
    "crypto": [
        "CRYPTOGRAPHIC", "JAMMING", "ELECTRONIC WARFARE",
    ],
    "medical": [
        "^SURGICAL", "^MEDICAL", "^SYRINGE", "^SCALPEL", "^CATHETER",
        "^STETHOSCOPE", "^DEFIBRILLATOR", "^STERILIZER", "^DENTAL",
        "^PROSTHETIC", "^ORTHOPEDIC", "^X-RAY", "^ANESTHESIA",
        "CENTRIFUGE,BLOOD",
    ],
    "clothing": [
        "^HELMET,COMBAT", "^HELMET,FLIGHT", "^VEST,ARMORED", "BODY ARMOR",
        "^CAMOUFLAGE", "^PARACHUTE",
    ],
    "hazmat": [
        "^SEALING COMPOUND", "^COATING,", "^ADHESIVE,", "^PRIMER,",
        "^CAULKING COMPOUND", "^POTTING COMPOUND", "^INSULATING COMPOUND",
        "^PRESERVATIVE", "^CARTRIDGE,COMPRESSED GAS",
    ],
}

# ── FSC codes for additional filtering ──────────────────────────────────────

FSC_CODES = {
    "fsc": {
        "6515": "Medical/surgical instruments",
        "6545": "Medical sets/kits/outfits",
        "8465": "Individual equipment (body gear)",
        "8030": "Preservative and Sealing Compounds — chemical hazmat",
    },
}

# ── Defense-only manufacturer keywords ──────────────────────────────────────

MANUFACTURER_KEYWORDS = ["ORDNANCE", "MUNITIONS", "ARMAMENT", "WEAPONS"]


class Command(BaseCommand):
    help = "Disable products in restricted categories (weapons, aircraft, nuclear, etc.)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run", action="store_true",
            help="Preview what would be disabled without making changes.",
        )
        parser.add_argument(
            "--execute", action="store_true",
            help="Actually disable products (required to make changes).",
        )
        parser.add_argument(
            "--category",
            choices=[
                *KEYWORD_CATEGORIES.keys(), "fsc", "manufacturers",
            ],
            help="Only process one category.",
        )
        parser.add_argument(
            "--stats", action="store_true",
            help="Show current counts without changing anything.",
        )
        parser.add_argument(
            "--purge-manufacturers", action="store_true",
            help="Disable manufacturers where most products are already disabled.",
        )
        parser.add_argument(
            "--threshold", type=int, default=90,
            help="Percent of disabled products to trigger manufacturer purge (default: 90).",
        )
        parser.add_argument(
            "--min-products", type=int, default=10,
            help="Minimum product count for a manufacturer to be purge-eligible (default: 10).",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        execute = options["execute"]
        category = options["category"]
        stats_only = options["stats"]
        purge = options["purge_manufacturers"]
        threshold = options["threshold"]
        min_products = options["min_products"]

        if stats_only:
            self._show_stats()
            return

        if not execute and not dry_run:
            # Default to dry-run for safety
            dry_run = True
            self.stdout.write(self.style.WARNING(
                "No --execute flag; running in dry-run mode.\n"
            ))

        if execute and dry_run:
            self.stderr.write(self.style.ERROR(
                "Cannot use both --execute and --dry-run."
            ))
            return

        total_disabled = 0

        if not purge:
            # A. Keyword-based categories
            if category is None or category in KEYWORD_CATEGORIES:
                cats = {category: KEYWORD_CATEGORIES[category]} if category else KEYWORD_CATEGORIES
                for cat_name, keywords in cats.items():
                    count = self._disable_by_keywords(cat_name, keywords, execute)
                    total_disabled += count

            # B. FSC-based filtering
            if category is None or category == "fsc":
                count = self._disable_by_fsc(execute)
                total_disabled += count

            # C. Manufacturer-level disabling (by name keywords)
            if category is None or category == "manufacturers":
                count = self._disable_manufacturers(execute)
                total_disabled += count

            label = "Disabled" if execute else "Would disable"
            self.stdout.write(self.style.SUCCESS(
                f"\n{'=' * 50}\n{label} {total_disabled:,} total products."
            ))
        else:
            # D. Purge manufacturers with high disabled-product ratios
            count = self._purge_manufacturers(execute, threshold, min_products)
            label = "Disabled" if execute else "Would disable"
            self.stdout.write(self.style.SUCCESS(
                f"\n{'=' * 50}\n{label} {count} manufacturer(s)."
            ))

    def _show_stats(self):
        """Show current product counts by is_active status."""
        total = Product.objects.count()
        enabled = Product.objects.filter(is_active=Product.ENABLED).count()
        neutral = Product.objects.filter(is_active=Product.NEUTRAL).count()
        disabled = Product.objects.filter(is_active=Product.DISABLED).count()
        published = Product.objects.published().count()

        self.stdout.write(f"Total products:     {total:,}")
        self.stdout.write(f"  Enabled (1):      {enabled:,}")
        self.stdout.write(f"  Neutral (0):      {neutral:,}")
        self.stdout.write(f"  Disabled (-1):    {disabled:,}")
        self.stdout.write(f"  Published:        {published:,}")

    def _disable_by_keywords(self, cat_name, keywords, execute):
        """Disable NEUTRAL products whose nomenclature matches any keyword.

        Keywords prefixed with ^ use startswith matching (avoids substring
        false positives like DETENT matching TENT). All others use contains.
        """
        q = models.Q()
        for kw in keywords:
            if kw.startswith("^"):
                q |= models.Q(nsn__nomenclature__istartswith=kw[1:])
            else:
                q |= models.Q(nsn__nomenclature__icontains=kw)

        qs = Product.objects.filter(is_active=Product.NEUTRAL).filter(q)
        count = qs.count()

        label = "Disabling" if execute else "[DRY RUN] Would disable"
        self.stdout.write(f"  {cat_name:15s}: {count:>7,} products  {label}")

        if execute and count > 0:
            qs.update(is_active=Product.DISABLED)

            # Persist keywords as PipelineFilter entries for future imports
            # ^ prefix → startswith → fnmatch pattern "KEYWORD*"
            # no prefix → contains → fnmatch pattern "*KEYWORD*"
            for kw in keywords:
                if kw.startswith("^"):
                    pattern = kw[1:] + "*"
                else:
                    pattern = "*" + kw + "*"
                PipelineFilter.objects.get_or_create(
                    field_type=FilterFieldType.NOMENCLATURE,
                    field_value=pattern,
                    defaults={
                        "action": FilterAction.EXCLUDE,
                        "stage": PipelineStage.ALL,
                        "is_active": True,
                        "reason": f"Restricted category '{cat_name}' — auto-disabled by disable_restricted_products",
                    },
                )

        return count

    def _disable_by_fsc(self, execute):
        """Disable NEUTRAL products matching restricted FSC codes."""
        fsc_codes = list(FSC_CODES["fsc"].keys())

        # Exclude products already caught by keyword categories
        qs = Product.objects.filter(
            is_active=Product.NEUTRAL,
            nsn__fsc__code__in=fsc_codes,
        )
        count = qs.count()

        label = "Disabling" if execute else "[DRY RUN] Would disable"
        self.stdout.write(f"  {'fsc':15s}: {count:>7,} products  {label}")

        if execute and count > 0:
            qs.update(is_active=Product.DISABLED)

        # Show per-code breakdown
        for code, desc in FSC_CODES["fsc"].items():
            sub_count = Product.objects.filter(
                is_active=Product.NEUTRAL,
                nsn__fsc__code=code,
            ).count()
            if sub_count > 0:
                self.stdout.write(f"    FSC {code} ({desc}): {sub_count:,}")

        return count

    def _disable_manufacturers(self, execute):
        """Disable defense-only manufacturers and their products."""
        q = models.Q()
        for kw in MANUFACTURER_KEYWORDS:
            q |= models.Q(company_name__icontains=kw)

        mfrs = Manufacturer.objects.filter(q)
        mfr_count = mfrs.count()

        # Count NEUTRAL products under these manufacturers
        product_qs = Product.objects.filter(
            is_active=Product.NEUTRAL,
            manufacturer__in=mfrs,
        )
        product_count = product_qs.count()

        label = "Disabling" if execute else "[DRY RUN] Would disable"
        self.stdout.write(
            f"  {'manufacturers':15s}: {mfr_count:>4} manufacturers, "
            f"{product_count:>7,} products  {label}"
        )

        if execute:
            # Disable the products
            if product_count > 0:
                product_qs.update(is_active=Product.DISABLED)

            # Disable manufacturer profiles
            for mfr in mfrs:
                profile, created = ManufacturerProfile.objects.get_or_create(
                    organization=mfr,
                )
                if profile.status != Manufacturer.DISABLED:
                    profile.status = Manufacturer.DISABLED
                    profile.save(update_fields=["status"])
                    self.stdout.write(self.style.WARNING(
                        f"    Disabled manufacturer: {mfr.company_name} "
                        f"(CAGE {mfr.cage_code})"
                    ))

                # Create PipelineFilter entry to prevent future imports
                PipelineFilter.objects.get_or_create(
                    field_type=FilterFieldType.MANUFACTURER_NAME,
                    field_value=mfr.company_name,
                    defaults={
                        "action": FilterAction.EXCLUDE,
                        "stage": PipelineStage.ALL,
                        "is_active": True,
                        "reason": f"Defense-only contractor — auto-disabled by disable_restricted_products",
                    },
                )

        # List matched manufacturers
        for mfr in mfrs[:20]:
            neutral_count = Product.objects.filter(
                is_active=Product.NEUTRAL, manufacturer=mfr,
            ).count()
            self.stdout.write(
                f"    {mfr.company_name} (CAGE {mfr.cage_code}): "
                f"{neutral_count} neutral products"
            )

        return product_count

    def _purge_manufacturers(self, execute, threshold, min_products):
        """Disable manufacturers where >threshold% of products are already disabled."""
        threshold_fraction = threshold / 100.0

        mfrs = (
            Manufacturer.objects
            .annotate(
                total=Count("products"),
                disabled=Count("products", filter=Q(products__is_active=Product.DISABLED)),
            )
            .filter(total__gte=min_products)
            .filter(disabled__gt=F("total") * threshold_fraction)
            .order_by("-total")
        )

        # Exclude manufacturers already disabled
        already_disabled_ids = set(
            ManufacturerProfile.objects
            .filter(status=Manufacturer.DISABLED)
            .values_list("organization_id", flat=True)
        )
        mfrs_to_process = [m for m in mfrs if m.id not in already_disabled_ids]

        self.stdout.write(
            f"Manufacturers with >{threshold}% disabled products "
            f"(min {min_products} products): {len(mfrs_to_process)}"
        )
        self.stdout.write(
            f"{'Name':60s} {'CAGE':6s} {'Total':>6s} {'Disabled':>8s} {'Neutral':>8s} {'%':>5s}"
        )
        self.stdout.write("-" * 95)

        total_neutral_disabled = 0
        label = "Disabling" if execute else "[DRY RUN]"

        for m in mfrs_to_process:
            pct = m.disabled / m.total * 100
            neutral = m.total - m.disabled
            total_neutral_disabled += neutral
            self.stdout.write(
                f"  {m.company_name[:58]:58s} {m.cage_code or '':6s} "
                f"{m.total:>6,} {m.disabled:>8,} {neutral:>8,} {pct:>5.1f}  {label}"
            )

            if execute:
                # Disable manufacturer profile
                profile, _ = ManufacturerProfile.objects.get_or_create(
                    organization=m,
                )
                profile.status = Manufacturer.DISABLED
                profile.save(update_fields=["status"])

                # Disable remaining neutral products
                Product.objects.filter(
                    manufacturer=m, is_active=Product.NEUTRAL,
                ).update(is_active=Product.DISABLED)

                # Create PipelineFilter to prevent future imports
                PipelineFilter.objects.get_or_create(
                    field_type=FilterFieldType.MANUFACTURER_NAME,
                    field_value=m.company_name,
                    defaults={
                        "action": FilterAction.EXCLUDE,
                        "stage": PipelineStage.ALL,
                        "is_active": True,
                        "reason": f">{threshold}% restricted products — auto-disabled by disable_restricted_products",
                    },
                )

        self.stdout.write(
            f"\n  Remaining neutral products also disabled: {total_neutral_disabled:,}"
        )
        return len(mfrs_to_process)
