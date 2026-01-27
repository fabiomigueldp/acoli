from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("scheduler", "0002_alter_schedulejobrequest_status"),
    ]

    operations = [
        migrations.AddField(
            model_name="schedulejobrequest",
            name="job_type",
            field=models.CharField(
                choices=[("schedule", "Escalonamento"), ("replacement", "Substituicao")],
                default="schedule",
                max_length=20,
            ),
        ),
        migrations.AddField(
            model_name="schedulejobrequest",
            name="payload_json",
            field=models.JSONField(blank=True, default=dict),
        ),
    ]
