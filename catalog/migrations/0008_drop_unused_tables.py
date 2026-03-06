from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('catalog', '0007_remove_product_catalog_item'),
    ]

    operations = [
        migrations.DeleteModel(name='DataProvenance'),
        migrations.DeleteModel(name='AwardHistory'),
        migrations.DeleteModel(name='SupplierLink'),
        migrations.DeleteModel(name='CatalogSpecifications'),
        migrations.DeleteModel(name='CatalogPricing'),
        migrations.DeleteModel(name='CatalogItem'),
        migrations.DeleteModel(name='PurchaseTransaction'),
        migrations.DeleteModel(name='Opportunity'),
        migrations.DeleteModel(name='MarketOpportunity'),
        migrations.DeleteModel(name='DistributorStats'),
    ]
