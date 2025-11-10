# Generated manually for lead stage and archive functionality

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('leads', '0008_massogisparcellcache_legalaction_lienrecord_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='schedulecallrequest',
            name='stage',
            field=models.CharField(
                choices=[
                    ('new', 'New Lead'),
                    ('contacted', 'Contacted'),
                    ('appointment', 'Listing Appointment'),
                    ('listed', 'Listed'),
                    ('under_contract', 'Under Contract'),
                    ('closed', 'Closed/Sold')
                ],
                default='new',
                max_length=20
            ),
        ),
        migrations.AddField(
            model_name='schedulecallrequest',
            name='is_archived',
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name='schedulecallrequest',
            name='archived_at',
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name='schedulecallrequest',
            name='updated_at',
            field=models.DateTimeField(auto_now=True),
        ),
        migrations.AddIndex(
            model_name='schedulecallrequest',
            index=models.Index(fields=['stage', 'is_archived'], name='leads_sched_stage_is_ar_idx'),
        ),
    ]
