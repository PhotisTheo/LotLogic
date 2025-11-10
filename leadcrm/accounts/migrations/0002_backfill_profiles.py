from django.db import migrations


def create_profiles(apps, schema_editor):
    UserProfile = apps.get_model("accounts", "UserProfile")
    User = apps.get_model("auth", "User")
    for user in User.objects.all():
        UserProfile.objects.get_or_create(user=user)


def remove_profiles(apps, schema_editor):
    UserProfile = apps.get_model("accounts", "UserProfile")
    UserProfile.objects.filter(account_type="individual", team_lead__isnull=True).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("accounts", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(create_profiles, reverse_code=remove_profiles),
    ]
