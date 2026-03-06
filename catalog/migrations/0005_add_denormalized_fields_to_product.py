import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('catalog', '0004_alter_manufacturerprofile_options'),
        ('home', '0011_delete_nsnitem'),
    ]

    operations = [
        migrations.AddField(
            model_name='product',
            name='fsc',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='home.federalsupplyclass'),
        ),
        migrations.AddField(
            model_name='product',
            name='nomenclature',
            field=models.CharField(blank=True, max_length=500),
        ),
        migrations.AddField(
            model_name='product',
            name='nsn',
            field=models.CharField(blank=True, db_index=True, max_length=300),
        ),
        migrations.AddField(
            model_name='product',
            name='price',
            field=models.DecimalField(blank=True, db_index=True, decimal_places=2, max_digits=12, null=True),
        ),
        migrations.AddField(
            model_name='product',
            name='unit_of_issue',
            field=models.CharField(blank=True, max_length=20),
        ),
        migrations.AlterField(
            model_name='product',
            name='name',
            field=models.CharField(blank=True, help_text='Product name. Falls back to nomenclature if blank', max_length=500),
        ),
    ]
