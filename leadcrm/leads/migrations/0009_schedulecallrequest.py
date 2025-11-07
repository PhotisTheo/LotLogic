from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("leads", "0008_lead_bedrooms_bathrooms"),
    ]

    operations = [
        migrations.CreateModel(
            name="ScheduleCallRequest",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("town_id", models.IntegerField(blank=True, null=True)),
                ("loc_id", models.CharField(blank=True, max_length=200)),
                ("property_address", models.CharField(blank=True, max_length=255)),
                ("recipient_name", models.CharField(blank=True, max_length=255)),
                ("contact_phone", models.CharField(max_length=50)),
                ("preferred_call_time", models.DateTimeField(blank=True, null=True)),
                ("notes", models.TextField(blank=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
        migrations.AddIndex(
            model_name="schedulecallrequest",
            index=models.Index(fields=["town_id", "loc_id"], name="leads_sched_town_id_d49fdb_idx"),
        ),
    ]
