# Delete old Manufacturer model (data already migrated to ManufacturerProfile).

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('home', '0009_manufacturer_cage_code'),
    ]

    operations = [
        migrations.DeleteModel(
            name='Manufacturer',
        ),
    ]
