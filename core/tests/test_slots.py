from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteProfile,
    Assignment,
    Community,
    MassInstance,
    Parish,
    PositionType,
    RequirementProfile,
    RequirementProfilePosition,
)
from core.services.slots import sync_slots_for_instance


class SlotSyncTests(TestCase):
    def test_sync_slots_creates_required_positions(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        profile = RequirementProfile.objects.create(parish=parish, name="Dominical")
        RequirementProfilePosition.objects.create(profile=profile, position_type=position, quantity=2)

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now(),
            requirement_profile=profile,
            status="scheduled",
        )

        created = sync_slots_for_instance(instance)
        self.assertEqual(len(created), 2)
        self.assertEqual(instance.slots.count(), 2)

    def test_sync_slots_finalizes_removed_positions(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        profile_a = RequirementProfile.objects.create(parish=parish, name="Dominical")
        profile_b = RequirementProfile.objects.create(parish=parish, name="Simples")
        RequirementProfilePosition.objects.create(profile=profile_a, position_type=position, quantity=2)
        RequirementProfilePosition.objects.create(profile=profile_b, position_type=position, quantity=1)

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now(),
            requirement_profile=profile_a,
            status="scheduled",
        )

        sync_slots_for_instance(instance)

        instance.requirement_profile = profile_b
        instance.save(update_fields=["requirement_profile", "updated_at"])
        sync_slots_for_instance(instance)

        removed_slot = instance.slots.get(slot_index=2)
        self.assertFalse(removed_slot.required)
        self.assertEqual(removed_slot.status, "finalized")

    def test_sync_slots_handles_missing_profile(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        profile = RequirementProfile.objects.create(parish=parish, name="Dominical")
        RequirementProfilePosition.objects.create(profile=profile, position_type=position, quantity=1)

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now(),
            requirement_profile=profile,
            status="scheduled",
        )

        sync_slots_for_instance(instance)

        instance.requirement_profile = None
        instance.save(update_fields=["requirement_profile", "updated_at"])
        sync_slots_for_instance(instance)

        slot = instance.slots.get(slot_index=1)
        self.assertFalse(slot.required)
        self.assertEqual(slot.status, "finalized")

    def test_sync_slots_reactivates_slot_status(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        profile_a = RequirementProfile.objects.create(parish=parish, name="Dominical")
        profile_b = RequirementProfile.objects.create(parish=parish, name="Simples")
        RequirementProfilePosition.objects.create(profile=profile_a, position_type=position, quantity=2)
        RequirementProfilePosition.objects.create(profile=profile_b, position_type=position, quantity=1)

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now(),
            requirement_profile=profile_a,
            status="scheduled",
        )
        sync_slots_for_instance(instance)

        instance.requirement_profile = profile_b
        instance.save(update_fields=["requirement_profile", "updated_at"])
        sync_slots_for_instance(instance)

        instance.requirement_profile = profile_a
        instance.save(update_fields=["requirement_profile", "updated_at"])
        sync_slots_for_instance(instance)

        slot = instance.slots.get(slot_index=2)
        self.assertTrue(slot.required)
        self.assertEqual(slot.status, "open")

    def test_sync_slots_clears_external_coverage_on_removal(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        profile_a = RequirementProfile.objects.create(parish=parish, name="Dominical")
        profile_b = RequirementProfile.objects.create(parish=parish, name="Simples")
        RequirementProfilePosition.objects.create(profile=profile_a, position_type=position, quantity=2)
        RequirementProfilePosition.objects.create(profile=profile_b, position_type=position, quantity=1)

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now(),
            requirement_profile=profile_a,
            status="scheduled",
        )
        sync_slots_for_instance(instance)

        slot = instance.slots.get(slot_index=2)
        slot.externally_covered = True
        slot.external_coverage_notes = "Coberto"
        slot.save(update_fields=["externally_covered", "external_coverage_notes", "updated_at"])

        instance.requirement_profile = profile_b
        instance.save(update_fields=["requirement_profile", "updated_at"])
        sync_slots_for_instance(instance)

        slot.refresh_from_db()
        self.assertFalse(slot.externally_covered)
        self.assertEqual(slot.external_coverage_notes, "")

    def test_sync_slots_deactivates_assignment_on_removal(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        profile_a = RequirementProfile.objects.create(parish=parish, name="Dominical")
        profile_b = RequirementProfile.objects.create(parish=parish, name="Simples")
        RequirementProfilePosition.objects.create(profile=profile_a, position_type=position, quantity=2)
        RequirementProfilePosition.objects.create(profile=profile_b, position_type=position, quantity=1)
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now(),
            requirement_profile=profile_a,
            status="scheduled",
        )
        sync_slots_for_instance(instance)
        slot = instance.slots.get(slot_index=2)
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte, assignment_state="published")

        instance.requirement_profile = profile_b
        instance.save(update_fields=["requirement_profile", "updated_at"])
        sync_slots_for_instance(instance)

        assignment.refresh_from_db()
        self.assertFalse(assignment.is_active)
