from django.db import migrations


def copy_data_forward(apps, schema_editor):
    """Copy CatalogItem/CatalogPricing data onto Product rows."""
    cursor = schema_editor.connection.cursor()
    cursor.execute("""
        UPDATE catalog_product p
        SET
            nsn = ci.nsn,
            nomenclature = ci.nomenclature,
            fsc_id = ci.fsc_id,
            unit_of_issue = ci.unit_of_issue,
            price = cp.unit_price
        FROM catalog_catalogitem ci
        LEFT JOIN catalog_catalogpricing cp ON cp.catalog_item_id = ci.id
        WHERE p.catalog_item_id = ci.id
    """)


class Migration(migrations.Migration):

    dependencies = [
        ('catalog', '0005_add_denormalized_fields_to_product'),
    ]

    operations = [
        migrations.RunPython(copy_data_forward, migrations.RunPython.noop),
    ]
