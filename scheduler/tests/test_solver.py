from collections import defaultdict
from datetime import timedelta

from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteAvailabilityRule,
    AcolyteProfile,
    AcolyteQualification,
    Assignment,
    AssignmentSlot,
    Community,
    FamilyGroup,
    MassInstance,
    Parish,
    PositionType,
    RequirementProfile,
    RequirementProfilePosition,
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

    def test_solver_ignores_canceled_instances(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")
        AcolyteQualification.objects.create(parish=parish, acolyte=acolyte, position_type=position, qualified=True)

        canceled_instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=1),
            status="canceled",
        )
        canceled_slot = AssignmentSlot.objects.create(
            parish=parish, mass_instance=canceled_instance, position_type=position, required=True, slot_index=1
        )

        scheduled_instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=2),
            status="scheduled",
        )
        scheduled_slot = AssignmentSlot.objects.create(
            parish=parish, mass_instance=scheduled_instance, position_type=position, required=True, slot_index=1
        )

        solve_schedule(parish, [canceled_instance, scheduled_instance], parish.consolidation_days, {}, allow_changes=True)

        canceled_slot.refresh_from_db()
        scheduled_slot.refresh_from_db()
        self.assertIsNone(canceled_slot.get_active_assignment())
        self.assertIsNotNone(scheduled_slot.get_active_assignment())

    def test_solver_limits_candidates_per_slot(self):
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
            parish=parish, mass_instance=instance, position_type=position, required=True, slot_index=1
        )
        AcolyteAvailabilityRule.objects.create(
            parish=parish,
            acolyte=acolyte_b,
            rule_type="unavailable",
            day_of_week=None,
        )

        result = solve_schedule(
            parish,
            [instance],
            parish.consolidation_days,
            {"max_candidates_per_slot": 1},
            allow_changes=True,
        )

        slot.refresh_from_db()
        self.assertEqual(result.coverage, 1)
        self.assertEqual(slot.get_active_assignment().acolyte_id, acolyte_a.id)

    def test_solver_requires_senior_when_configured(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        profile = RequirementProfile.objects.create(parish=parish, name="Solenidade", min_senior_per_mass=1)
        RequirementProfilePosition.objects.create(profile=profile, position_type=position, quantity=1)
        senior = AcolyteProfile.objects.create(
            parish=parish, display_name="Senior", experience_level="senior"
        )
        junior = AcolyteProfile.objects.create(
            parish=parish, display_name="Junior", experience_level="intermediate"
        )
        AcolyteQualification.objects.create(parish=parish, acolyte=senior, position_type=position, qualified=True)
        AcolyteQualification.objects.create(parish=parish, acolyte=junior, position_type=position, qualified=True)

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            requirement_profile=profile,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )

        solve_schedule(parish, [instance], parish.consolidation_days, {"fairness_penalty": 0}, allow_changes=True)

        slot = AssignmentSlot.objects.filter(parish=parish, mass_instance=instance).first()
        self.assertEqual(slot.get_active_assignment().acolyte_id, senior.id)

    def test_solver_family_bonus_groups_members(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        profile = RequirementProfile.objects.create(parish=parish, name="Dupla")
        RequirementProfilePosition.objects.create(profile=profile, position_type=position, quantity=2)

        family = FamilyGroup.objects.create(parish=parish, name="Familia A")
        fam_a = AcolyteProfile.objects.create(parish=parish, display_name="A1", family_group=family)
        fam_b = AcolyteProfile.objects.create(parish=parish, display_name="A2", family_group=family)
        other_a = AcolyteProfile.objects.create(parish=parish, display_name="B1")
        other_b = AcolyteProfile.objects.create(parish=parish, display_name="B2")
        for acolyte in (fam_a, fam_b, other_a, other_b):
            AcolyteQualification.objects.create(parish=parish, acolyte=acolyte, position_type=position, qualified=True)

        mass_a = MassInstance.objects.create(
            parish=parish,
            community=community,
            requirement_profile=profile,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )
        mass_b = MassInstance.objects.create(
            parish=parish,
            community=community,
            requirement_profile=profile,
            starts_at=timezone.now() + timedelta(days=4),
            status="scheduled",
        )

        solve_schedule(
            parish,
            [mass_a, mass_b],
            parish.consolidation_days,
            {"family_group_bonus": 10, "fairness_penalty": 0, "stability_penalty": 0},
            allow_changes=True,
        )

        fam_assignments = Assignment.objects.filter(
            parish=parish, is_active=True, acolyte_id__in=[fam_a.id, fam_b.id]
        ).select_related("slot__mass_instance")
        members_by_mass = defaultdict(set)
        for assignment in fam_assignments:
            members_by_mass[assignment.slot.mass_instance_id].add(assignment.acolyte_id)
        self.assertTrue(any(len(members) == 2 for members in members_by_mass.values()))
        for members in members_by_mass.values():
            self.assertNotEqual(len(members), 1)

    def test_solver_penalizes_reserve_acolyte(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        normal = AcolyteProfile.objects.create(parish=parish, display_name="Normal")
        reserve = AcolyteProfile.objects.create(
            parish=parish, display_name="Reserva", scheduling_mode="reserve"
        )
        AcolyteQualification.objects.create(parish=parish, acolyte=normal, position_type=position, qualified=True)
        AcolyteQualification.objects.create(parish=parish, acolyte=reserve, position_type=position, qualified=True)

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )
        AssignmentSlot.objects.create(
            parish=parish, mass_instance=instance, position_type=position, required=True, slot_index=1
        )

        solve_schedule(
            parish,
            [instance],
            parish.consolidation_days,
            {"reserve_penalty": 1000, "fairness_penalty": 0, "stability_penalty": 0},
            allow_changes=True,
        )

        slot = AssignmentSlot.objects.filter(parish=parish, mass_instance=instance).first()
        self.assertEqual(slot.get_active_assignment().acolyte_id, normal.id)

