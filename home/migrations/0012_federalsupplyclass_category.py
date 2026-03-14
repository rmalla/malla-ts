from django.db import migrations, models


CATEGORY_MAP = {
    'vehicles': {
        'name': 'Vehicles & Engines',
        'groups': ['12', '23', '25', '28', '29'],
    },
    'mechanical': {
        'name': 'Mechanical & Industrial',
        'groups': ['30', '31', '34', '35', '36', '37', '38', '39'],
    },
    'fluid-systems': {
        'name': 'Fluid Systems & Valves',
        'groups': ['43', '44', '45', '46', '47', '48'],
    },
    'facilities': {
        'name': 'HVAC, Safety & Facilities',
        'groups': ['40', '41', '42', '49', '54', '56'],
    },
    'tools': {
        'name': 'Tools & Hardware',
        'groups': ['51', '52', '53'],
    },
    'electrical': {
        'name': 'Electrical & Electronics',
        'groups': ['58', '59', '61', '62', '63'],
    },
    'instruments': {
        'name': 'Instruments & Equipment',
        'groups': ['66', '67', '70'],
    },
    'supplies': {
        'name': 'Office, Supplies & Misc',
        'groups': ['73', '74', '75', '77', '80', '81', '83', '99'],
    },
}


def populate_categories(apps, schema_editor):
    FederalSupplyClass = apps.get_model('home', 'FederalSupplyClass')

    # Build reverse lookup: FSG -> (category, category_name)
    fsg_to_cat = {}
    for cat_code, info in CATEGORY_MAP.items():
        for fsg in info['groups']:
            fsg_to_cat[fsg] = (cat_code, info['name'])

    for fsc in FederalSupplyClass.objects.all():
        fsg = fsc.group or fsc.code[:2]
        cat_code, cat_name = fsg_to_cat.get(fsg, ('miscellaneous', 'Miscellaneous'))
        fsc.category = cat_code
        fsc.category_name = cat_name
        fsc.save(update_fields=['category', 'category_name'])


def reverse_categories(apps, schema_editor):
    FederalSupplyClass = apps.get_model('home', 'FederalSupplyClass')
    FederalSupplyClass.objects.all().update(category='', category_name='')


class Migration(migrations.Migration):

    dependencies = [
        ('home', '0011_delete_nsnitem'),
    ]

    operations = [
        migrations.AddField(
            model_name='federalsupplyclass',
            name='category',
            field=models.CharField(blank=True, db_index=True, max_length=50),
        ),
        migrations.AddField(
            model_name='federalsupplyclass',
            name='category_name',
            field=models.CharField(blank=True, max_length=255),
        ),
        migrations.RunPython(populate_categories, reverse_categories),
    ]
