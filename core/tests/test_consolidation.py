from datetime import timedelta

from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteProfile,
    Assignment,
    AssignmentSlot,
    Community,
    MassInstance,
    Parish,
    PositionType,
)


class ConsolidationLockTests(TestCase):
    def test_lock_consolidation_window_sets_flags(self):
        parish = Parish.objects.create(name="Parish", consolidation_days=14)
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")

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
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte, assignment_state="published")

        call_command("lock_consolidation_window", parish_id=parish.id)

        slot.refresh_from_db()
        assignment.refresh_from_db()
        self.assertTrue(slot.is_locked)
        self.assertEqual(slot.status, "finalized")
        self.assertEqual(assignment.assignment_state, "locked")
