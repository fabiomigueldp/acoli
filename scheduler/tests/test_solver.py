from datetime import timedelta

from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteProfile,
    AcolyteQualification,
    AssignmentSlot,
    Community,
    MassInstance,
    Parish,
    PositionType,
)
from scheduler.services.solver import solve_schedule


class SolverTests(TestCase):
    def test_solver_assigns_slots(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolyte")
        AcolyteQualification.objects.create(parish=parish, acolyte=acolyte, position_type=position, qualified=True)
        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(parish=parish, mass_instance=instance, position_type=position)

        result = solve_schedule(parish, [instance], parish.consolidation_days, {}, allow_changes=True)
        slot.refresh_from_db()
        self.assertIsNotNone(slot.assignment)
        self.assertEqual(result.coverage, 1)

