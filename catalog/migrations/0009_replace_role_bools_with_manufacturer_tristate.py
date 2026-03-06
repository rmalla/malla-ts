# Convert is_manufacturer from BooleanField to SmallIntegerField tri-state,
# drop is_distributor and is_awardee.
#
# Data conversion: True → 1, False → 0 (neutral, not yet evaluated)
# The evaluate_manufacturers command will set proper -1/0/1 values.

from django.db import migrations, models


def convert_field_type(apps, schema_editor):
    """Convert is_manufacturer from boolean to smallint via raw SQL."""
    schema_editor.execute("""
        ALTER TABLE catalog_organization
        ALTER COLUMN is_manufacturer
        TYPE smallint
        USING is_manufacturer::int::smallint
    """)
    schema_editor.execute("""
        ALTER TABLE catalog_organization
        ALTER COLUMN is_manufacturer
        SET DEFAULT 0
    """)


class Migration(migrations.Migration):

    dependencies = [
        ('catalog', '0008_drop_unused_tables'),
    ]

    operations = [
        # Step 1: Raw SQL to convert bool→smallint (Django can't do this cast)
        migrations.RunPython(convert_field_type, migrations.RunPython.noop),
        # Step 2: Tell Django the field definition changed (no DB change, already done)
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.AlterField(
                    model_name='manufacturer',
                    name='is_manufacturer',
                    field=models.SmallIntegerField(
                        choices=[(-1, 'No'), (0, 'Neutral'), (1, 'Yes')],
                        db_index=True,
                        default=0,
                        help_text='Verified manufacturer? -1=No, 0=Unverified, 1=Yes',
                    ),
                ),
            ],
            database_operations=[],
        ),
        # Step 3: Drop unused fields
        migrations.RemoveField(
            model_name='manufacturer',
            name='is_awardee',
        ),
        migrations.RemoveField(
            model_name='manufacturer',
            name='is_distributor',
        ),
    ]
