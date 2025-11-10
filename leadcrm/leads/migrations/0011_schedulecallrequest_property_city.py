from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("leads", "0010_rename_leads_sched_town_id_d49fdb_idx_leads_sched_town_id_7037a5_idx"),
    ]

    operations = [
        migrations.AddField(
            model_name="schedulecallrequest",
            name="property_city",
            field=models.CharField(blank=True, max_length=100),
        ),
    ]
