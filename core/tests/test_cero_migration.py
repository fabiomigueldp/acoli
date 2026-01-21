from django.core.management import call_command
from django.test import TestCase

from core.models import (
    AcolyteProfile,
    AcolyteQualification,
    FunctionType,
    Parish,
    PositionType,
    PositionTypeFunction,
    RequirementProfile,
    RequirementProfilePosition,
)


class CeroferarioMigrationTests(TestCase):
    def test_migrate_ceroferarios(self):
        parish = Parish.objects.create(name="Parish")
        cer1 = PositionType.objects.create(parish=parish, code="CER_1", name="Ceroferario 1", active=True)
        cer2 = PositionType.objects.create(parish=parish, code="CER_2", name="Ceroferario 2", active=True)
        FunctionType.objects.create(parish=parish, code="CER_1", name="Ceroferario 1", active=True)
        FunctionType.objects.create(parish=parish, code="CER_2", name="Ceroferario 2", active=True)
        profile = RequirementProfile.objects.create(parish=parish, name="Solenidade")
        RequirementProfilePosition.objects.create(profile=profile, position_type=cer1, quantity=1)
        RequirementProfilePosition.objects.create(profile=profile, position_type=cer2, quantity=1)
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="A1", active=True)
        AcolyteQualification.objects.create(parish=parish, acolyte=acolyte, position_type=cer1, qualified=True)

        call_command("migrate_ceroferarios")

        cer = PositionType.objects.get(parish=parish, code="CER")
        self.assertTrue(cer.active)
        self.assertFalse(PositionType.objects.get(id=cer1.id).active)
        self.assertFalse(PositionType.objects.get(id=cer2.id).active)
        self.assertTrue(FunctionType.objects.filter(parish=parish, code="CER").exists())
        self.assertFalse(FunctionType.objects.get(parish=parish, code="CER_1").active)
        self.assertFalse(FunctionType.objects.get(parish=parish, code="CER_2").active)
        self.assertTrue(
            PositionTypeFunction.objects.filter(position_type=cer, function_type__code="CER").exists()
        )

        profile_positions = RequirementProfilePosition.objects.filter(profile=profile)
        self.assertEqual(profile_positions.count(), 1)
        self.assertEqual(profile_positions.first().position_type_id, cer.id)
        self.assertEqual(profile_positions.first().quantity, 2)
        self.assertTrue(
            AcolyteQualification.objects.filter(parish=parish, acolyte=acolyte, position_type=cer).exists()
        )
