from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0007_acolyteprofile_experience_level_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="eventseries",
            name="is_active",
            field=models.BooleanField(default=True),
        ),
    ]
