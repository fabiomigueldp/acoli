from django.test import TestCase
from django.utils import timezone

from core.models import AcolyteProfile, Assignment, AssignmentSlot, Community, MassInstance, Parish, PositionType
from core.services.assignments import assign_manual


class AssignmentServiceTests(TestCase):
    def test_assign_manual_idempotent_for_same_acolyte(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now(),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(
            parish=parish,
            mass_instance=instance,
            position_type=position,
            slot_index=1,
            required=True,
            status="open",
        )

        first = assign_manual(slot, acolyte)
        second = assign_manual(slot, acolyte)

        self.assertEqual(first.id, second.id)
        self.assertEqual(Assignment.objects.filter(slot=slot, is_active=True).count(), 1)
