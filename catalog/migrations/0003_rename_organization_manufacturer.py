from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("catalog", "0002_product_unique_manufacturer_part_number"),
    ]

    operations = [
        migrations.RenameModel(
            old_name="Organization",
            new_name="Manufacturer",
        ),
        migrations.AlterModelTable(
            name="Manufacturer",
            table="catalog_organization",
        ),
        migrations.RenameModel(
            old_name="OrganizationProfile",
            new_name="ManufacturerProfile",
        ),
        migrations.AlterModelTable(
            name="ManufacturerProfile",
            table="catalog_organizationprofile",
        ),
    ]
