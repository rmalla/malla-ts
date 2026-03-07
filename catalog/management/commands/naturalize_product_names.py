"""
Populate Product.display_name with natural-language names from nomenclature.

Groups by unique nomenclature (69K values) for efficiency across 800K+ products.
Uses batch CASE/WHEN SQL for speed on the long tail of small groups.

Usage:
    python manage.py naturalize_product_names --dry-run   # preview changes
    python manage.py naturalize_product_names              # apply changes
    python manage.py naturalize_product_names --force      # overwrite existing display_name
"""

from django.core.management.base import BaseCommand
from django.db.models import Count, Case, When, Value, CharField

from catalog.models import Product
from catalog.services.name_formatter import naturalize_nomenclature


class Command(BaseCommand):
    help = 'Populate Product.display_name from nomenclature using natural-language rules'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='Show changes without writing')
        parser.add_argument('--force', action='store_true', help='Overwrite existing display_name values')
        parser.add_argument('--batch-size', type=int, default=500, help='Nomenclatures per CASE/WHEN batch')

    def handle(self, **options):
        dry_run = options['dry_run']
        force = options['force']
        batch_size = options['batch_size']

        # Get unique nomenclatures with product counts
        qs = Product.objects.exclude(nomenclature='')
        if not force:
            qs = qs.filter(display_name='')

        nomenclatures = list(
            qs.values_list('nomenclature', flat=True)
            .distinct()
            .order_by('nomenclature')
        )

        total_noms = len(nomenclatures)
        self.stdout.write(f'Unique nomenclatures to process: {total_noms}')

        if total_noms == 0:
            self.stdout.write('Nothing to do.')
            return

        # Build mapping: nomenclature -> naturalized name
        mapping = {}
        samples = []
        for nom in nomenclatures:
            new_name = naturalize_nomenclature(nom)
            if new_name:
                mapping[nom] = new_name
                if len(samples) < 50 and new_name != nom.strip():
                    samples.append((nom, new_name))

        self.stdout.write(f'Mappings built: {len(mapping)}')

        if dry_run:
            count = qs.filter(nomenclature__in=list(mapping.keys())).count()
            self.stdout.write(f'Products would be updated: {count}')
            if samples:
                self.stdout.write(f'\n--- Sample transformations ---')
                for nom, new_name in samples:
                    self.stdout.write(f'  {nom}  ->  {new_name}')
            self.stdout.write(self.style.WARNING('\n[DRY RUN] No changes were written.'))
            return

        # Batch update using CASE/WHEN
        changed_count = 0
        noms_list = list(mapping.keys())

        for i in range(0, len(noms_list), batch_size):
            batch = noms_list[i:i + batch_size]
            whens = [
                When(nomenclature=nom, then=Value(mapping[nom]))
                for nom in batch
            ]

            batch_qs = Product.objects.filter(nomenclature__in=batch)
            if not force:
                batch_qs = batch_qs.filter(display_name='')

            updated = batch_qs.update(
                display_name=Case(*whens, output_field=CharField())
            )
            changed_count += updated

            self.stdout.write(
                f'  [{min(i + batch_size, len(noms_list))}/{len(noms_list)}] '
                f'{changed_count} products updated'
            )
            self.stdout.flush()

        self.stdout.write(self.style.SUCCESS(f'\nDone. Updated {changed_count} products.'))
