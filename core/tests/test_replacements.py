from datetime import timedelta

from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteProfile,
    AcolyteQualification,
    Assignment,
    AssignmentSlot,
    Community,
    MassInstance,
    Parish,
    PositionType,
    ReplacementRequest,
)
from core.services.replacements import assign_replacement, create_replacement_request


class ReplacementServiceTests(TestCase):
    def test_assign_replacement_creates_new_assignment(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte_a = AcolyteProfile.objects.create(parish=parish, display_name="Acolito A")
        acolyte_b = AcolyteProfile.objects.create(parish=parish, display_name="Acolito B")
        AcolyteQualification.objects.create(parish=parish, acolyte=acolyte_a, position_type=position, qualified=True)
        AcolyteQualification.objects.create(parish=parish, acolyte=acolyte_b, position_type=position, qualified=True)

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(
            parish=parish,
            mass_instance=instance,
            position_type=position,
            slot_index=1,
            required=True,
            status="assigned",
        )
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte_a)
        create_replacement_request(parish, slot)

        new_assignment = assign_replacement(parish, slot, acolyte_b)

        assignment.refresh_from_db()
        slot.refresh_from_db()
        self.assertFalse(assignment.is_active)
        self.assertTrue(new_assignment.is_active)
        self.assertEqual(new_assignment.acolyte_id, acolyte_b.id)
        self.assertEqual(ReplacementRequest.objects.filter(parish=parish, slot=slot, status="assigned").count(), 1)
