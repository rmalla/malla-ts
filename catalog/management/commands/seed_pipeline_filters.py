from django.core.management.base import BaseCommand

from catalog.constants import FilterFieldType, FilterAction, PipelineStage
from catalog.models import PipelineFilter


BATTERY_FILTERS = [
    {
        "field_type": FilterFieldType.FSC_CODE,
        "field_value": "6130",
        "reason": "Batteries, Nonrechargeable — not in product scope",
    },
    {
        "field_type": FilterFieldType.FSC_CODE,
        "field_value": "6135",
        "reason": "Batteries, Rechargeable — not in product scope",
    },
    {
        "field_type": FilterFieldType.FSC_CODE,
        "field_value": "6140",
        "reason": "Batteries, Thermal — not in product scope",
    },
]

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
]


class Command(BaseCommand):
    help = "Seed default pipeline filter rules (battery and manufacturer name exclusions)."

    def handle(self, *args, **options):
        created_count = 0

        # Battery FSC filters
        self.stdout.write("Seeding battery FSC filters...")
        for entry in BATTERY_FILTERS:
            _, created = PipelineFilter.objects.get_or_create(
                field_type=entry["field_type"],
                field_value=entry["field_value"],
                stage=PipelineStage.ALL,
                defaults={
                    "action": FilterAction.EXCLUDE,
                    "reason": entry["reason"],
                    "is_active": True,
                },
            )
            if created:
                created_count += 1
                self.stdout.write(
                    self.style.SUCCESS(f"  Created filter: FSC {entry['field_value']}")
                )
            else:
                self.stdout.write(f"  Already exists: FSC {entry['field_value']}")

        # Manufacturer name filters
        self.stdout.write("Seeding manufacturer name filters...")
        for entry in MANUFACTURER_NAME_FILTERS:
            _, created = PipelineFilter.objects.get_or_create(
                field_type=FilterFieldType.MANUFACTURER_NAME,
                field_value=entry["field_value"],
                stage=PipelineStage.ALL,
                defaults={
                    "action": FilterAction.EXCLUDE,
                    "reason": entry["reason"],
                    "is_active": True,
                },
            )
            if created:
                created_count += 1
                self.stdout.write(
                    self.style.SUCCESS(f"  Created filter: MFR_NAME '{entry['field_value']}'")
                )
            else:
                self.stdout.write(f"  Already exists: MFR_NAME '{entry['field_value']}'")

        self.stdout.write(
            self.style.SUCCESS(f"\nDone — {created_count} new filter(s) seeded.")
        )
