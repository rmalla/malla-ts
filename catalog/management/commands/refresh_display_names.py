"""
Re-apply format_manufacturer_name() to all manufacturers, updating display names.

Usage:
    python manage.py refresh_display_names --dry-run   # preview changes
    python manage.py refresh_display_names              # apply changes
    python manage.py refresh_display_names --refresh-slugs --dry-run  # preview slug fixes
"""

from django.core.management.base import BaseCommand

from catalog.models import Manufacturer, ManufacturerProfile
from catalog.models.entities import slugify_manufacturer
from catalog.services.name_formatter import format_manufacturer_name


class Command(BaseCommand):
    help = 'Refresh display_name on all ManufacturerProfiles using format_manufacturer_name()'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='Show changes without writing')
        parser.add_argument('--batch-size', type=int, default=1000, help='bulk_update batch size')
        parser.add_argument('--refresh-slugs', action='store_true',
                            help='Regenerate slugs from formatted display names (destructive for SEO — use with care)')

    def handle(self, **options):
        dry_run = options['dry_run']
        batch_size = options['batch_size']
        refresh_slugs = options['refresh_slugs']

        manufacturers = Manufacturer.objects.select_related('profile').all()
        total = manufacturers.count()

        changed = []
        unchanged = 0
        created_profiles = 0
        to_update = []

        slug_changes = []
        slugs_to_update = []
        pending_slugs = set()  # track slugs claimed in this batch to avoid duplicates

        for mfr in manufacturers.iterator():
            new_name = format_manufacturer_name(mfr.company_name)

            try:
                profile = mfr.profile
            except ManufacturerProfile.DoesNotExist:
                if not dry_run:
                    profile = ManufacturerProfile.objects.create(
                        organization=mfr,
                        display_name=new_name,
                    )
                created_profiles += 1
                changed.append((mfr.company_name, '', new_name))
                if refresh_slugs:
                    self._check_slug(mfr, new_name, slug_changes, slugs_to_update, pending_slugs, dry_run)
                continue

            old_name = profile.display_name or ''
            if old_name != new_name:
                changed.append((mfr.company_name, old_name, new_name))
                if not dry_run:
                    profile.display_name = new_name
                    to_update.append(profile)
                    if len(to_update) >= batch_size:
                        ManufacturerProfile.objects.bulk_update(to_update, ['display_name'], batch_size=batch_size)
                        to_update.clear()
            else:
                unchanged += 1

            if refresh_slugs:
                self._check_slug(mfr, new_name, slug_changes, slugs_to_update, pending_slugs, dry_run)

        if to_update and not dry_run:
            ManufacturerProfile.objects.bulk_update(to_update, ['display_name'], batch_size=batch_size)

        if slugs_to_update and not dry_run:
            Manufacturer.objects.bulk_update(slugs_to_update, ['slug'], batch_size=batch_size)

        # Report
        self.stdout.write(f'\nTotal manufacturers: {total}')
        self.stdout.write(f'Changed: {len(changed)}')
        self.stdout.write(f'Unchanged: {unchanged}')
        if created_profiles:
            self.stdout.write(f'Profiles created: {created_profiles}')

        if changed:
            self.stdout.write(f'\n--- Sample display name changes (first 50) ---')
            for raw, old, new in changed[:50]:
                if old:
                    self.stdout.write(f'  {raw}')
                    self.stdout.write(f'    old: {old}')
                    self.stdout.write(f'    new: {new}')
                else:
                    self.stdout.write(f'  {raw}')
                    self.stdout.write(f'    new (created): {new}')

        if refresh_slugs:
            self.stdout.write(f'\nSlugs changed: {len(slug_changes)}')
            if slug_changes:
                self.stdout.write(f'\n--- Sample slug changes (first 50) ---')
                for company, old_slug, new_slug in slug_changes[:50]:
                    self.stdout.write(f'  {company}')
                    self.stdout.write(f'    {old_slug} -> {new_slug}')

        if dry_run:
            self.stdout.write(self.style.WARNING('\n[DRY RUN] No changes were written.'))
        else:
            self.stdout.write(self.style.SUCCESS(f'\nDone. Updated {len(changed)} display names.'))
            if refresh_slugs:
                self.stdout.write(self.style.SUCCESS(f'Updated {len(slug_changes)} slugs.'))

    def _check_slug(self, mfr, display_name, slug_changes, slugs_to_update, pending_slugs, dry_run):
        """Check if a manufacturer's slug needs updating and queue it."""
        ideal_slug = slugify_manufacturer(display_name, mfr.cage_code)
        if not ideal_slug or ideal_slug == mfr.slug:
            return

        # Check for conflicts in DB and in this batch's pending changes
        candidate = ideal_slug
        qs = Manufacturer.objects.exclude(pk=mfr.pk)
        if qs.filter(slug=candidate).exists() or candidate in pending_slugs:
            if mfr.cage_code:
                candidate = f"{ideal_slug}-{mfr.cage_code.lower()}"
            else:
                candidate = f"{ideal_slug}-{mfr.pk}"
            if qs.filter(slug=candidate).exists() or candidate in pending_slugs:
                return  # can't resolve conflict, skip

        if candidate == mfr.slug:
            return

        slug_changes.append((mfr.company_name, mfr.slug, candidate))
        pending_slugs.add(candidate)
        if not dry_run:
            mfr.slug = candidate
            slugs_to_update.append(mfr)
