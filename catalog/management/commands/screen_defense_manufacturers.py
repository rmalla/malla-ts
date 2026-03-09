"""
Screen manufacturers for defense/military NAICS codes via SAM.gov.

Queries SAM.gov for each enabled (or all non-disabled) manufacturer's NAICS
codes and flags those whose primary NAICS or majority of NAICS codes fall in
defense-restricted categories.

Usage:
    python manage.py screen_defense_manufacturers                  # dry-run enabled only
    python manage.py screen_defense_manufacturers --all            # include NEUTRAL
    python manage.py screen_defense_manufacturers --execute        # disable matches
    python manage.py screen_defense_manufacturers --verbose        # show all NAICS
"""

import time

from django.core.management.base import BaseCommand

from catalog.constants import FilterAction, FilterFieldType, PipelineStage
from catalog.models import PipelineFilter, Product
from catalog.models.entities import Manufacturer, ManufacturerProfile
from catalog.services.sam_api import fetch_naics_by_cage


# Defense-specific NAICS codes (6-digit) and prefixes (for broader matching).
# Sources: census.gov NAICS definitions, defense acquisition categories.
DEFENSE_NAICS = {
    # Ammunition manufacturing
    "332992": "Small arms ammunition manufacturing",
    "332993": "Ammunition (except small arms) manufacturing",
    "332994": "Small arms, ordnance, and ordnance accessories manufacturing",
    # Aircraft & missile manufacturing
    "336411": "Aircraft manufacturing",
    "336412": "Aircraft engine and engine parts manufacturing",
    "336413": "Other aircraft parts and auxiliary equipment manufacturing",
    "336414": "Guided missile and space vehicle manufacturing",
    "336415": "Guided missile propulsion unit manufacturing",
    "336419": "Other guided missile and space vehicle parts manufacturing",
    # Military vehicles
    "336992": "Military armored vehicle, tank, and tank component manufacturing",
    # Ship building
    "336611": "Ship building and repairing",
    # Defense electronics & C4ISR
    "334511": "Search, detection, navigation, guidance, aeronautical systems",
    # National security
    "928110": "National security",
}

# NAICS prefixes that indicate defense when they are the PRIMARY code.
# These are broader categories where some companies are defense-focused.
DEFENSE_NAICS_PREFIXES = [
    "3364",   # Aerospace product and parts manufacturing
]


def is_defense_naics(code: str) -> bool:
    """Check if a NAICS code is defense-related."""
    if code in DEFENSE_NAICS:
        return True
    for prefix in DEFENSE_NAICS_PREFIXES:
        if code.startswith(prefix):
            return True
    return False


class Command(BaseCommand):
    help = "Screen manufacturers for defense NAICS codes via SAM.gov"

    def add_arguments(self, parser):
        parser.add_argument(
            "--execute", action="store_true",
            help="Actually disable flagged manufacturers (default is dry-run).",
        )
        parser.add_argument(
            "--all", action="store_true",
            help="Screen all non-disabled manufacturers (not just ENABLED).",
        )
        parser.add_argument(
            "--verbose", action="store_true",
            help="Show all NAICS codes for each manufacturer.",
        )
        parser.add_argument(
            "--delay", type=float, default=0.5,
            help="Seconds between SAM.gov API calls (default: 0.5).",
        )

    def handle(self, **options):
        execute = options["execute"]
        screen_all = options["all"]
        verbose = options["verbose"]
        delay = options["delay"]

        if screen_all:
            profiles = ManufacturerProfile.objects.exclude(
                status=Manufacturer.DISABLED,
            ).select_related("organization")
        else:
            profiles = ManufacturerProfile.objects.filter(
                status=Manufacturer.ENABLED,
            ).select_related("organization")

        profiles = profiles.order_by("organization__company_name")
        total = profiles.count()
        self.stdout.write(f"Screening {total} manufacturer(s) against SAM.gov NAICS data\n")

        flagged = []
        clean = []
        no_data = []

        for i, profile in enumerate(profiles, 1):
            mfr = profile.organization
            name = profile.display_name or mfr.company_name
            cage = mfr.cage_code

            self.stdout.write(f"  [{i}/{total}] {name} ({cage})...", ending="")

            naics_list = fetch_naics_by_cage(cage)
            if naics_list is None:
                self.stdout.write(" no SAM.gov data")
                no_data.append({"name": name, "cage": cage, "profile": profile})
                if i < total:
                    time.sleep(delay)
                continue

            defense_codes = []
            other_codes = []
            primary_is_defense = False

            for entry in naics_list:
                code = entry["code"]
                desc = entry["description"]
                is_primary = entry["primary"]
                if is_defense_naics(code):
                    defense_codes.append(entry)
                    if is_primary:
                        primary_is_defense = True
                else:
                    other_codes.append(entry)

            defense_count = len(defense_codes)
            total_codes = len(naics_list)
            defense_ratio = defense_count / total_codes if total_codes else 0

            # Flag if: primary NAICS is defense, OR majority of codes are defense
            is_flagged = primary_is_defense or defense_ratio > 0.5

            if is_flagged:
                self.stdout.write(
                    self.style.WARNING(
                        f" DEFENSE ({defense_count}/{total_codes} NAICS"
                        f"{', PRIMARY' if primary_is_defense else ''})"
                    )
                )
                flagged.append({
                    "name": name,
                    "cage": cage,
                    "profile": profile,
                    "defense_codes": defense_codes,
                    "other_codes": other_codes,
                    "primary_is_defense": primary_is_defense,
                })
            else:
                self.stdout.write(
                    self.style.SUCCESS(f" OK ({defense_count}/{total_codes} defense NAICS)")
                )
                clean.append({"name": name, "cage": cage})

            if verbose:
                for entry in naics_list:
                    marker = " **DEF**" if is_defense_naics(entry["code"]) else ""
                    primary_mark = " [PRIMARY]" if entry["primary"] else ""
                    self.stdout.write(
                        f"      {entry['code']} {entry['description']}"
                        f"{primary_mark}{marker}"
                    )

            if i < total:
                time.sleep(delay)

        # Summary
        self.stdout.write(f"\n{'=' * 60}")
        self.stdout.write(f"  Screened:   {total}")
        self.stdout.write(f"  Clean:      {len(clean)}")
        self.stdout.write(
            self.style.WARNING(f"  Flagged:    {len(flagged)}")
        )
        self.stdout.write(f"  No data:    {len(no_data)}")

        if flagged:
            self.stdout.write(f"\nFlagged manufacturers:")
            for f in flagged:
                codes_str = ", ".join(
                    c["code"] for c in f["defense_codes"]
                )
                self.stdout.write(
                    f"  {f['name']:50s} CAGE={f['cage']}  "
                    f"NAICS: {codes_str}"
                )

        if not flagged:
            self.stdout.write(self.style.SUCCESS("\nNo defense manufacturers found."))
            return

        if not execute:
            self.stdout.write(self.style.WARNING(
                "\nDry run — no changes made. Use --execute to disable flagged manufacturers."
            ))
            return

        # Execute: disable flagged manufacturers
        disabled_count = 0
        for f in flagged:
            profile = f["profile"]
            mfr = profile.organization

            profile.status = Manufacturer.DISABLED
            profile.save(update_fields=["status"])

            # Disable their NEUTRAL products too
            Product.objects.filter(
                manufacturer=mfr,
                is_active=Product.NEUTRAL,
            ).update(is_active=Product.DISABLED)

            # Create pipeline filter to prevent future activation
            codes_str = ", ".join(c["code"] for c in f["defense_codes"])
            PipelineFilter.objects.get_or_create(
                field_type=FilterFieldType.CAGE_CODE,
                field_value=mfr.cage_code,
                defaults={
                    "action": FilterAction.EXCLUDE,
                    "stage": PipelineStage.ALL,
                    "is_active": True,
                    "reason": (
                        f"Defense NAICS screening — codes: {codes_str}"
                    ),
                },
            )

            disabled_count += 1
            self.stdout.write(self.style.WARNING(
                f"  Disabled: {f['name']} (CAGE {f['cage']})"
            ))

        self.stdout.write(self.style.SUCCESS(
            f"\nDisabled {disabled_count} manufacturer(s)."
        ))
