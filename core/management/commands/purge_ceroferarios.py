from django.core.management.base import BaseCommand
from django.db import transaction

from core.models import (
    AcolytePreference,
    AcolyteQualification,
    AssignmentSlot,
    FunctionType,
    MassInstance,
    Parish,
    PositionType,
    PositionTypeFunction,
    RequirementProfile,
    RequirementProfilePosition,
)
from core.services.slots import sync_slots_for_instance


class Command(BaseCommand):
    help = "Remover CER_1/CER_2 do banco e consolidar tudo em CER com quantidade."

    def add_arguments(self, parser):
        parser.add_argument("--parish-id", type=int, default=None)

    def handle(self, *args, **options):
        parish_id = options.get("parish_id")
        parishes = Parish.objects.all()
        if parish_id:
            parishes = parishes.filter(id=parish_id)

        for parish in parishes:
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

                if not old_position_ids:
                    self.stdout.write(self.style.SUCCESS(f"[{parish.name}] Nada a corrigir."))
                    continue

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

                AcolytePreference.objects.filter(
                    parish=parish, target_position_id__in=old_position_ids
                ).update(target_position=cer_position)

                for qualification in AcolyteQualification.objects.filter(
                    parish=parish, position_type_id__in=old_position_ids
                ):
                    AcolyteQualification.objects.get_or_create(
                        parish=qualification.parish,
                        acolyte=qualification.acolyte,
                        position_type=cer_position,
                        defaults={"qualified": True},
                    )
                AcolyteQualification.objects.filter(
                    parish=parish, position_type_id__in=old_position_ids
                ).delete()

                affected_instances = MassInstance.objects.filter(
                    parish=parish,
                    slots__position_type_id__in=old_position_ids,
                ).distinct()
                for instance in affected_instances:
                    sync_slots_for_instance(instance)

                AssignmentSlot.objects.filter(position_type_id__in=old_position_ids).delete()
                PositionTypeFunction.objects.filter(position_type_id__in=old_position_ids).delete()
                old_positions.delete()
                old_functions.delete()

                self.stdout.write(
                    self.style.SUCCESS(
                        f"[{parish.name}] CER_1/CER_2 removidos e consolidados em CER."
                    )
                )
