# Generated manually

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0010_acolyte_scheduling_mode'),
    ]

    operations = [
        migrations.AlterField(
            model_name='acolytecreditledger',
            name='reason_code',
            field=models.CharField(
                choices=[
                    ('served_unpopular_slot', 'Serviu em horario pouco procurado'),
                    ('accepted_last_minute_substitution', 'Aceitou substituicao de ultima hora'),
                    ('high_attendance_streak', 'Boa sequencia de presencas'),
                    ('received_high_demand_assignment', 'Recebeu funcao disputada'),
                    ('manual_adjustment', 'Ajuste manual'),
                ],
                max_length=40
            ),
        ),
        migrations.AddField(
            model_name='acolytecreditledger',
            name='notes',
            field=models.TextField(blank=True),
        ),
        migrations.AddField(
            model_name='acolytecreditledger',
            name='created_by',
            field=models.ForeignKey(
                null=True,
                blank=True,
                on_delete=models.SET_NULL,
                to='accounts.user',
            ),
        ),
    ]
