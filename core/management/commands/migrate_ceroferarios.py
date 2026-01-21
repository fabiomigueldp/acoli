from django.core.management.base import BaseCommand
from django.db import transaction

from core.models import (
    AcolyteQualification,
    FunctionType,
    Parish,
    PositionType,
    PositionTypeFunction,
    RequirementProfile,
    RequirementProfilePosition,
)


class Command(BaseCommand):
    help = "Migrar CER_1/CER_2 para o modelo com quantidade (CER)."

    def handle(self, *args, **options):
        for parish in Parish.objects.all():
            with transaction.atomic():
                cer_function, _ = FunctionType.objects.get_or_create(
                    parish=parish,
                    code="CER",
                    defaults={"name": "Ceroferario", "active": True},
                )
                cer_position, _ = PositionType.objects.get_or_create(
                    parish=parish,
                    code="CER",
                    defaults={"name": "Ceroferario", "active": True},
                )
                if cer_position.name != "Ceroferario":
                    cer_position.name = "Ceroferario"
                    cer_position.save(update_fields=["name", "updated_at"])
                if not cer_position.active:
                    cer_position.active = True
                    cer_position.save(update_fields=["active", "updated_at"])

                PositionTypeFunction.objects.get_or_create(
                    position_type=cer_position, function_type=cer_function
                )

                old_positions = PositionType.objects.filter(parish=parish, code__in=["CER_1", "CER_2"])
                old_position_ids = list(old_positions.values_list("id", flat=True))
                old_functions = FunctionType.objects.filter(parish=parish, code__in=["CER_1", "CER_2"])
                if old_position_ids:
                    profiles = RequirementProfile.objects.filter(
                        positions__position_type_id__in=old_position_ids
                    ).distinct()
                    for profile in profiles:
                        RequirementProfilePosition.objects.filter(
                            profile=profile, position_type_id__in=old_position_ids
                        ).delete()
                        rp, created = RequirementProfilePosition.objects.get_or_create(
                            profile=profile, position_type=cer_position, defaults={"quantity": 2}
                        )
                        if not created and rp.quantity != 2:
                            rp.quantity = 2
                            rp.save(update_fields=["quantity"])

                    for qualification in AcolyteQualification.objects.filter(
                        position_type_id__in=old_position_ids
                    ):
                        AcolyteQualification.objects.get_or_create(
                            parish=qualification.parish,
                            acolyte=qualification.acolyte,
                            position_type=cer_position,
                            defaults={"qualified": True},
                        )

                    old_positions.update(active=False)
                    old_functions.update(active=False)

                self.stdout.write(
                    self.style.SUCCESS(
                        f"[{parish.name}] Migracao concluida para Ceroferario."
                    )
                )
