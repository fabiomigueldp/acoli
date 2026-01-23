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
from scheduler.services.quick_fill import quick_fill_slot


class QuickFillSlotTests(TestCase):
    def setUp(self):
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        self.position = PositionType.objects.create(parish=self.parish, code="LIB", name="Libriferario")
        self.position2 = PositionType.objects.create(parish=self.parish, code="TUR", name="Turiferario")

        self.acolyte_a = AcolyteProfile.objects.create(parish=self.parish, display_name="Acolito A")
        self.acolyte_b = AcolyteProfile.objects.create(parish=self.parish, display_name="Acolito B")
        self.acolyte_c = AcolyteProfile.objects.create(parish=self.parish, display_name="Acolito C")

        # All qualified for both positions
        for acolyte in [self.acolyte_a, self.acolyte_b, self.acolyte_c]:
            AcolyteQualification.objects.create(
                parish=self.parish, acolyte=acolyte, position_type=self.position, qualified=True
            )
            AcolyteQualification.objects.create(
                parish=self.parish, acolyte=acolyte, position_type=self.position2, qualified=True
            )

        self.instance = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )
        self.slot1 = AssignmentSlot.objects.create(
            parish=self.parish, mass_instance=self.instance, position_type=self.position
        )
        self.slot2 = AssignmentSlot.objects.create(
            parish=self.parish, mass_instance=self.instance, position_type=self.position2
        )

    def test_quick_fill_returns_candidates(self):
        """quick_fill_slot returns qualified acolytes"""
        candidates = quick_fill_slot(self.slot1, self.parish)
        self.assertEqual(len(candidates), 3)
        candidate_ids = {c.id for c in candidates}
        self.assertIn(self.acolyte_a.id, candidate_ids)
        self.assertIn(self.acolyte_b.id, candidate_ids)
        self.assertIn(self.acolyte_c.id, candidate_ids)

    def test_quick_fill_excludes_specified_acolytes(self):
        """quick_fill_slot excludes acolytes in exclude_acolyte_ids"""
        exclude_ids = {self.acolyte_a.id, self.acolyte_b.id}
        candidates = quick_fill_slot(self.slot1, self.parish, exclude_acolyte_ids=exclude_ids)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].id, self.acolyte_c.id)

    def test_quick_fill_excludes_already_assigned_in_same_mass(self):
        """quick_fill_slot should not suggest acolytes already assigned to another slot in the same mass"""
        # Assign acolyte_a to slot1
        Assignment.objects.create(
            parish=self.parish,
            slot=self.slot1,
            acolyte=self.acolyte_a,
            is_active=True,
        )
        # When getting suggestions for slot2, exclude acolyte_a
        exclude_ids = {self.acolyte_a.id}
        candidates = quick_fill_slot(self.slot2, self.parish, exclude_acolyte_ids=exclude_ids)

        candidate_ids = {c.id for c in candidates}
        self.assertNotIn(self.acolyte_a.id, candidate_ids)
        self.assertIn(self.acolyte_b.id, candidate_ids)
        self.assertIn(self.acolyte_c.id, candidate_ids)

    def test_quick_fill_empty_exclude_set(self):
        """quick_fill_slot with empty exclude set returns all candidates"""
        candidates = quick_fill_slot(self.slot1, self.parish, exclude_acolyte_ids=set())
        self.assertEqual(len(candidates), 3)

    def test_quick_fill_none_exclude_set(self):
        """quick_fill_slot with None exclude set returns all candidates"""
        candidates = quick_fill_slot(self.slot1, self.parish, exclude_acolyte_ids=None)
        self.assertEqual(len(candidates), 3)

    def test_quick_fill_deprioritizes_reserve(self):
        """reserve acolytes should be ranked after normal candidates"""
        self.acolyte_b.scheduling_mode = "reserve"
        self.acolyte_b.save(update_fields=["scheduling_mode"])
        candidates = quick_fill_slot(self.slot1, self.parish)
        self.assertEqual(candidates[0].id, self.acolyte_a.id)
