from django.contrib.postgres.operations import TrigramExtension
from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ('catalog', '0018_schema_modernization'),
    ]

    operations = [
        TrigramExtension(),
        # NSN search indexes
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY catalog_nsn_nsn_trgm ON catalog_nsn USING gin (nsn gin_trgm_ops);",
            "DROP INDEX IF EXISTS catalog_nsn_nsn_trgm;",
        ),
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY catalog_nsn_nomenclature_trgm ON catalog_nsn USING gin (nomenclature gin_trgm_ops);",
            "DROP INDEX IF EXISTS catalog_nsn_nomenclature_trgm;",
        ),
        # Product search indexes
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY catalog_product_name_trgm ON catalog_product USING gin (name gin_trgm_ops);",
            "DROP INDEX IF EXISTS catalog_product_name_trgm;",
        ),
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY catalog_product_part_number_trgm ON catalog_product USING gin (part_number gin_trgm_ops);",
            "DROP INDEX IF EXISTS catalog_product_part_number_trgm;",
        ),
        # Manufacturer search indexes
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY catalog_organization_company_name_trgm ON catalog_organization USING gin (company_name gin_trgm_ops);",
            "DROP INDEX IF EXISTS catalog_organization_company_name_trgm;",
        ),
        migrations.RunSQL(
            "CREATE INDEX CONCURRENTLY catalog_organizationprofile_display_name_trgm ON catalog_organizationprofile USING gin (display_name gin_trgm_ops);",
            "DROP INDEX IF EXISTS catalog_organizationprofile_display_name_trgm;",
        ),
    ]
