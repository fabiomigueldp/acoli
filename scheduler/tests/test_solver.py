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
        self.assertIsNotNone(slot.get_active_assignment())
        self.assertEqual(result.coverage, 1)

    def test_rotation_considers_historical_assignments(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte_a = AcolyteProfile.objects.create(parish=parish, display_name="Acolito A")
        acolyte_b = AcolyteProfile.objects.create(parish=parish, display_name="Acolito B")
        AcolyteQualification.objects.create(parish=parish, acolyte=acolyte_a, position_type=position, qualified=True)
        AcolyteQualification.objects.create(parish=parish, acolyte=acolyte_b, position_type=position, qualified=True)

        past_instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() - timedelta(days=7),
            status="scheduled",
        )
        past_slot = AssignmentSlot.objects.create(parish=parish, mass_instance=past_instance, position_type=position)
        past_assignment = Assignment.objects.create(
            parish=parish,
            slot=past_slot,
            acolyte=acolyte_a,
            assignment_state="published",
            is_active=False,
        )
        Assignment.objects.filter(id=past_assignment.id).update(
            created_at=past_instance.starts_at - timedelta(hours=1),
            ended_at=past_instance.starts_at + timedelta(hours=1),
        )

        future_instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )
        future_slot = AssignmentSlot.objects.create(parish=parish, mass_instance=future_instance, position_type=position)

        weights = {"rotation_penalty": 1000, "rotation_days": 60, "fairness_penalty": 0, "stability_penalty": 0}
        solve_schedule(parish, [future_instance], parish.consolidation_days, weights, allow_changes=True)

        future_slot.refresh_from_db()
        self.assertEqual(future_slot.get_active_assignment().acolyte_id, acolyte_b.id)

    def test_solver_ignores_optional_slots(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")
        AcolyteQualification.objects.create(parish=parish, acolyte=acolyte, position_type=position, qualified=True)

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=2),
            status="scheduled",
        )
        required_slot = AssignmentSlot.objects.create(
            parish=parish, mass_instance=instance, position_type=position, required=True, slot_index=1
        )
        optional_slot = AssignmentSlot.objects.create(
            parish=parish, mass_instance=instance, position_type=position, required=False, slot_index=2
        )

        result = solve_schedule(parish, [instance], parish.consolidation_days, {}, allow_changes=True)

        required_slot.refresh_from_db()
        optional_slot.refresh_from_db()
        self.assertIsNotNone(required_slot.get_active_assignment())
        self.assertIsNone(optional_slot.get_active_assignment())
        self.assertEqual(result.required_slots_count, 1)
        self.assertEqual(result.coverage, 1)

