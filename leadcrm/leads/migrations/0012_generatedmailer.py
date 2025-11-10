from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("leads", "0011_schedulecallrequest_property_city"),
    ]

    operations = [
        migrations.CreateModel(
            name="GeneratedMailer",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("town_id", models.IntegerField()),
                ("loc_id", models.CharField(max_length=200)),
                ("prompt_id", models.CharField(blank=True, max_length=100)),
                ("payload", models.JSONField(blank=True, default=dict)),
                ("html", models.TextField(blank=True)),
                ("ai_generated", models.BooleanField(default=False)),
                ("ai_model", models.CharField(blank=True, max_length=100)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["-updated_at"],
                "unique_together": {("town_id", "loc_id")},
            },
        ),
        migrations.AddIndex(
            model_name="generatedmailer",
            index=models.Index(fields=["town_id", "loc_id"], name="leads_gener_town_id_0b9642_idx"),
        ),
    ]
