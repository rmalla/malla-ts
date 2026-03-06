from django.db import migrations, models


def bool_to_tristate(apps, schema_editor):
    """Convert boolean is_active to smallint: true→1, false→-1."""
    schema_editor.execute("""
        ALTER TABLE catalog_product
        ALTER COLUMN is_active DROP DEFAULT,
        ALTER COLUMN is_active TYPE smallint
            USING CASE WHEN is_active THEN 1 ELSE -1 END,
        ALTER COLUMN is_active SET DEFAULT 1;
    """)


class Migration(migrations.Migration):

    dependencies = [
        ('catalog', '0009_replace_role_bools_with_manufacturer_tristate'),
    ]

    operations = [
        migrations.RunPython(bool_to_tristate, migrations.RunPython.noop),
        migrations.AlterField(
            model_name='product',
            name='is_active',
            field=models.SmallIntegerField(
                choices=[(-1, 'Disabled'), (0, 'Neutral'), (1, 'Enabled')],
                db_index=True, default=1,
            ),
        ),
    ]
