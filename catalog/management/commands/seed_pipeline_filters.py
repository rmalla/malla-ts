"""
Seed all pipeline filter rules — FSC codes, manufacturer names, and
nomenclature keywords.  Safe to re-run; uses get_or_create.

Usage:
    python manage.py seed_pipeline_filters              # seed all
    python manage.py seed_pipeline_filters --dry-run    # preview only
"""

from django.core.management.base import BaseCommand

from catalog.constants import FilterFieldType, FilterAction, PipelineStage
from catalog.models import PipelineFilter


# ── FSC code filters ─────────────────────────────────────────────────────

FSC_FILTERS = [
    {"field_value": "6130", "reason": "Batteries, Nonrechargeable — not in product scope"},
    {"field_value": "6135", "reason": "Batteries, Rechargeable — not in product scope"},
    {"field_value": "6140", "reason": "Batteries, Thermal — not in product scope"},
    {"field_value": "6515", "reason": "Medical/surgical instruments"},
    {"field_value": "6545", "reason": "Medical sets/kits/outfits"},
    {"field_value": "8465", "reason": "Individual equipment (body gear)"},
    {"field_value": "8030", "reason": "Preservative and Sealing Compounds — chemical hazmat"},
]

# ── Manufacturer name filters ────────────────────────────────────────────

MANUFACTURER_NAME_FILTERS = [
    # Military branches
    {"field_value": "military", "reason": "Military entity — not a commercial supplier"},
    {"field_value": "navy", "reason": "Military branch — not a commercial supplier"},
    {"field_value": "army", "reason": "Military branch — not a commercial supplier"},
    {"field_value": "air force", "reason": "Military branch — not a commercial supplier"},
    {"field_value": "marine corps", "reason": "Military branch — not a commercial supplier"},
    # Defense primes
    {"field_value": "northrop", "reason": "Defense prime — excluded from supplier pipeline"},
    {"field_value": "boeing", "reason": "Defense prime — excluded from supplier pipeline"},
    {"field_value": "lockheed", "reason": "Defense prime — excluded from supplier pipeline"},
    {"field_value": "raytheon", "reason": "Defense prime — excluded from supplier pipeline"},
    {"field_value": "grumman", "reason": "Defense prime — excluded from supplier pipeline"},
    {"field_value": "general dynamics", "reason": "Defense prime — excluded from supplier pipeline"},
    {"field_value": "bae systems", "reason": "Defense prime — excluded from supplier pipeline"},
    {"field_value": "l3harris", "reason": "Defense prime — excluded from supplier pipeline"},
    # Defense-only contractor keywords
    {"field_value": "ordnance", "reason": "Defense-only contractor keyword"},
    {"field_value": "munitions", "reason": "Defense-only contractor keyword"},
    {"field_value": "armament", "reason": "Defense-only contractor keyword"},
    {"field_value": "weapons", "reason": "Defense-only contractor keyword"},
]

# ── Nomenclature keyword filters ─────────────────────────────────────────
#
# Convention: "^KEYWORD" in disable_restricted_products means startswith,
# which maps to fnmatch pattern "KEYWORD*".
# Plain "KEYWORD" means contains → fnmatch pattern "*KEYWORD*".

NOMENCLATURE_KEYWORDS = {
    "weapons": [
        "^AMMUNITION", "^BOMB", "^BOMBLET", "^BULLET", "^CANNON",
        "^DETONATOR", "^EXPLOSIVE", "^FUZE", "^GRENADE",
        "^HOWITZER", "^MINE,ANTI", "^MISSILE", "^PISTOL",
        "^PROJECTILE", "^PROPELLANT", "^RIFLE",
        "^ROCKET,", "^ROCKET MOTOR", "^SHELL,", "^TORPEDO", "^WARHEAD",
        "^MORTAR,", "^MORTAR ASSEMBLY",
        "GUN,AUTOMATIC", "GUN,MACHINE", "GUN,AIRCRAFT",
        "GUN MOUNT",
        "LAUNCHER,GRENADE", "LAUNCHER,ROCKET", "LAUNCHER,MISSILE",
        "COUNTERMEASURE", "CHAFF", "DECOY",
        "ARMOR,SUPPLEMENTAL", "ARMOR,PILOT", "ARMOR,BODY",
        "^ARMOR PLATE,",
        "FIRING MECHANISM", "TRIGGER MECHANISM",
    ],
    "aircraft": [
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


def _keyword_to_pattern(kw):
    """Convert a ^PREFIX or CONTAINS keyword to an fnmatch glob pattern."""
    if kw.startswith("^"):
        return kw[1:] + "*"
    return "*" + kw + "*"


class Command(BaseCommand):
    help = "Seed all pipeline filter rules (FSC, manufacturer names, nomenclature keywords)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run", action="store_true",
            help="Show what would be seeded without creating anything.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        created_count = 0

        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run mode.\n"))

        # ── FSC code filters ──
        self.stdout.write("FSC code filters:")
        for entry in FSC_FILTERS:
            created = self._seed(
                FilterFieldType.FSC_CODE,
                entry["field_value"],
                entry["reason"],
                dry_run,
            )
            if created:
                created_count += 1

        # ── Manufacturer name filters ──
        self.stdout.write("Manufacturer name filters:")
        for entry in MANUFACTURER_NAME_FILTERS:
            created = self._seed(
                FilterFieldType.MANUFACTURER_NAME,
                entry["field_value"],
                entry["reason"],
                dry_run,
            )
            if created:
                created_count += 1

        # ── Nomenclature keyword filters ──
        self.stdout.write("Nomenclature keyword filters:")
        for category, keywords in NOMENCLATURE_KEYWORDS.items():
            for kw in keywords:
                pattern = _keyword_to_pattern(kw)
                created = self._seed(
                    FilterFieldType.NOMENCLATURE,
                    pattern,
                    f"Restricted category '{category}'",
                    dry_run,
                )
                if created:
                    created_count += 1

        verb = "Would create" if dry_run else "Created"
        self.stdout.write(self.style.SUCCESS(
            f"\nDone — {verb} {created_count} new filter(s)."
        ))

    def _seed(self, field_type, field_value, reason, dry_run):
        """Create a filter if it doesn't exist. Returns True if new."""
        exists = PipelineFilter.objects.filter(
            field_type=field_type,
            field_value=field_value,
            stage=PipelineStage.ALL,
        ).exists()

        if exists:
            self.stdout.write(f"  exists: {field_type} = {field_value}")
            return False

        if dry_run:
            self.stdout.write(self.style.SUCCESS(
                f"  [DRY RUN] would create: {field_type} = {field_value}"
            ))
        else:
            PipelineFilter.objects.create(
                field_type=field_type,
                field_value=field_value,
                action=FilterAction.EXCLUDE,
                stage=PipelineStage.ALL,
                is_active=True,
                reason=reason,
            )
            self.stdout.write(self.style.SUCCESS(
                f"  created: {field_type} = {field_value}"
            ))
        return True
