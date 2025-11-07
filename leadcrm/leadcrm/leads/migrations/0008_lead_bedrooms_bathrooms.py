from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("leads", "0007_skiptracerecord"),
    ]

    operations = [
        migrations.AddField(
            model_name="lead",
            name="bathrooms",
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=5, null=True),
        ),
        migrations.AddField(
            model_name="lead",
            name="bedrooms",
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=5, null=True),
        ),
    ]
