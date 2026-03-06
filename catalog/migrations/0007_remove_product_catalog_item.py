from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('catalog', '0006_copy_catalogitem_data_to_product'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='product',
            name='catalog_item',
        ),
    ]
