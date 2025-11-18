# Generated manually for adding state field support

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('leads', '0031_alter_massgisparcel_building_value_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='savedparcellist',
            name='state',
            field=models.CharField(default='MA', max_length=2),
        ),
        migrations.AddField(
            model_name='skiptracerecord',
            name='state',
            field=models.CharField(default='MA', max_length=2),
        ),
        migrations.AlterUniqueTogether(
            name='skiptracerecord',
            unique_together={('created_by', 'state', 'town_id', 'loc_id')},
        ),
        migrations.RemoveIndex(
            model_name='skiptracerecord',
            name='leads_skipt_created_225aa9_idx',
        ),
        migrations.AddIndex(
            model_name='skiptracerecord',
            index=models.Index(fields=['created_by', 'state', 'town_id', 'loc_id'], name='leads_skipt_created_state_idx'),
        ),
    ]
