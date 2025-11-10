from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("leads", "0013_rename_leads_gener_town_id_0b9642_idx_leads_gener_town_id_0a356d_idx"),
    ]

    operations = [
        migrations.AddField(
            model_name="savedparcellist",
            name="archived_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
