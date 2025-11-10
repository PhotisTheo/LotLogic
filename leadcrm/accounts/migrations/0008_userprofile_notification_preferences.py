# Generated manually

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('accounts', '0007_userprofile_email_confirmed_at_emailverification'),
    ]

    operations = [
        migrations.AddField(
            model_name='userprofile',
            name='notify_qr_scan',
            field=models.BooleanField(default=True, help_text='Email when owner scans QR code'),
        ),
        migrations.AddField(
            model_name='userprofile',
            name='notify_call_request',
            field=models.BooleanField(default=True, help_text='Email when owner submits call request'),
        ),
        migrations.AddField(
            model_name='userprofile',
            name='notify_lead_activity',
            field=models.BooleanField(default=True, help_text='Email for lead status changes'),
        ),
        migrations.AddField(
            model_name='userprofile',
            name='notify_team_activity',
            field=models.BooleanField(default=True, help_text='Email for team collaboration'),
        ),
    ]
