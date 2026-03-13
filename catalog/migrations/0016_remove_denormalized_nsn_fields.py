"""Remove denormalized NSN fields from Product; rename nsn_obj → nsn."""

import django.db.models.deletion
from django.db import migrations, models


def forwards(apps, schema_editor):
    """Drop old nsn CharField column, rename nsn_obj_id → nsn_id."""
    schema_editor.execute("ALTER TABLE catalog_product DROP COLUMN IF EXISTS nsn")
    schema_editor.execute("ALTER TABLE catalog_product RENAME COLUMN nsn_obj_id TO nsn_id")


def backwards(apps, schema_editor):
    """Reverse: rename nsn_id back to nsn_obj_id, re-add nsn CharField."""
    schema_editor.execute("ALTER TABLE catalog_product RENAME COLUMN nsn_id TO nsn_obj_id")
    schema_editor.execute(
        "ALTER TABLE catalog_product ADD COLUMN nsn VARCHAR(300) NOT NULL DEFAULT ''"
    )


class Migration(migrations.Migration):

    dependencies = [
        ('catalog', '0015_create_nsn_model'),
    ]

    operations = [
        # 1. Drop denormalized fields
        migrations.RemoveField(model_name='product', name='fsc'),
        migrations.RemoveField(model_name='product', name='nomenclature'),
        migrations.RemoveField(model_name='product', name='unit_of_issue'),

        # 2. Raw SQL: drop old nsn CharField, rename nsn_obj_id → nsn_id
        migrations.RunPython(forwards, backwards),

        # 3. Tell Django the field is now called 'nsn' (state-only rename)
        migrations.SeparateDatabaseAndState(
            state_operations=[
                migrations.RemoveField(model_name='product', name='nsn_obj'),
                migrations.AddField(
                    model_name='product',
                    name='nsn',
                    field=models.ForeignKey(
                        blank=True, null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name='products',
                        to='catalog.nationalstocknumber',
                    ),
                ),
            ],
            database_operations=[],
        ),
    ]
