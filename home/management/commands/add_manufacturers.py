from django.core.management.base import BaseCommand
from django.utils.text import slugify

from catalog.models import Manufacturer


class Command(BaseCommand):
    help = 'Add sample manufacturer profiles (showcase brands with no CAGE code)'

    def handle(self, *args, **options):
        manufacturers_data = [
            {
                'name': 'Caterpillar',
                'website': 'https://www.caterpillar.com',
                'description': 'Heavy equipment manufacturer',
                'display_order': 1,
            },
            {
                'name': 'John Deere',
                'website': 'https://www.deere.com',
                'description': 'Agricultural and construction equipment',
                'display_order': 2,
            },
            {
                'name': 'Milwaukee Tool',
                'website': 'https://www.milwaukeetool.com',
                'description': 'Professional power tools',
                'display_order': 3,
            },
            {
                'name': 'DeWalt',
                'website': 'https://www.dewalt.com',
                'description': 'Power tools and hand tools',
                'display_order': 4,
            },
            {
                'name': 'Honeywell',
                'website': 'https://www.honeywell.com',
                'description': 'Industrial automation and safety equipment',
                'display_order': 5,
            },
            {
                'name': '3M',
                'website': 'https://www.3m.com',
                'description': 'Safety and industrial products',
                'display_order': 6,
            },
        ]

        for data in manufacturers_data:
            slug = slugify(data['name'])
            mfr, created = Manufacturer.objects.get_or_create(
                slug=slug,
                defaults={
                    'cage_code': None,
                    'company_name': data['name'],
                    'name': data['name'],
                    'website': data['website'],
                    'description': data['description'],
                    'display_order': data['display_order'],
                    'status': Manufacturer.ENABLED,
                },
            )
            if created:
                self.stdout.write(self.style.SUCCESS(f'Created: {mfr.name}'))
            else:
                self.stdout.write(self.style.WARNING(f'Already exists: {mfr.name}'))

        self.stdout.write(self.style.SUCCESS(
            '\nDone! Visit /django-admin/catalog/manufacturer/ to manage.'
        ))
