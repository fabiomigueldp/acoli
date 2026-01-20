from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteProfile,
    Assignment,
    Community,
    MassInstance,
    MassOverride,
    MembershipRole,
    Parish,
    ParishMembership,
    PositionType,
    AssignmentSlot,
    ReplacementRequest,
)


class ReplacementResolveTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(email="admin@example.com", full_name="Admin", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        self.position = PositionType.objects.create(parish=self.parish, code="LIB", name="Libriferario")
        role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Parish admin")
        membership = ParishMembership.objects.create(parish=self.parish, user=self.user, active=True)
        membership.roles.add(role)

    def _login(self):
        self.client.login(email="admin@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()

    def test_resolve_cancel_mass_creates_override(self):
        instance = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=timezone.now() + timedelta(days=2),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance,
            position_type=self.position,
            slot_index=1,
            required=True,
            status="open",
        )
        other_slot = AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance,
            position_type=self.position,
            slot_index=2,
            required=True,
            status="assigned",
        )
        acolyte = AcolyteProfile.objects.create(parish=self.parish, display_name="Acolito")
        assignment = Assignment.objects.create(parish=self.parish, slot=other_slot, acolyte=acolyte)
        replacement = ReplacementRequest.objects.create(parish=self.parish, slot=slot, status="pending")
        other_replacement = ReplacementRequest.objects.create(parish=self.parish, slot=other_slot, status="assigned")

        self._login()
        response = self.client.post(
            f"/replacements/{replacement.id}/resolve/",
            {"resolution_type": "mass_canceled", "notes": "Teste", "confirm_cancel_mass": "on"},
        )
        self.assertEqual(response.status_code, 302)

        instance.refresh_from_db()
        slot.refresh_from_db()
        other_slot.refresh_from_db()
        assignment.refresh_from_db()
        replacement.refresh_from_db()
        other_replacement.refresh_from_db()
        self.assertEqual(instance.status, "canceled")
        self.assertFalse(slot.required)
        self.assertFalse(other_slot.required)
        self.assertEqual(slot.status, "finalized")
        self.assertEqual(other_slot.status, "finalized")
        self.assertFalse(assignment.is_active)
        self.assertEqual(replacement.status, "resolved")
        self.assertEqual(other_replacement.status, "resolved")
        self.assertTrue(MassOverride.objects.filter(parish=self.parish, instance=instance, override_type="cancel_instance").exists())

    def test_resolve_slot_not_required_updates_slot(self):
        instance = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=timezone.now() + timedelta(days=2),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance,
            position_type=self.position,
            slot_index=1,
            required=True,
            status="open",
        )
        replacement = ReplacementRequest.objects.create(parish=self.parish, slot=slot, status="pending")

        self._login()
        response = self.client.post(
            f"/replacements/{replacement.id}/resolve/",
            {"resolution_type": "slot_not_required", "notes": "Sem necessidade"},
        )
        self.assertEqual(response.status_code, 302)

        slot.refresh_from_db()
        replacement.refresh_from_db()
        self.assertFalse(slot.required)
        self.assertEqual(slot.status, "finalized")
        self.assertEqual(replacement.status, "resolved")

    def test_resolve_covered_externally_marks_slot(self):
        instance = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=timezone.now() + timedelta(days=2),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance,
            position_type=self.position,
            slot_index=1,
            required=True,
            status="open",
        )
        replacement = ReplacementRequest.objects.create(parish=self.parish, slot=slot, status="pending")

        self._login()
        response = self.client.post(
            f"/replacements/{replacement.id}/resolve/",
            {"resolution_type": "covered_externally", "notes": "Coberto pelo coral"},
        )
        self.assertEqual(response.status_code, 302)

        slot.refresh_from_db()
        replacement.refresh_from_db()
        self.assertTrue(slot.externally_covered)
        self.assertFalse(slot.required)
        self.assertEqual(slot.status, "finalized")
        self.assertEqual(slot.external_coverage_notes, "Coberto pelo coral")
        self.assertEqual(replacement.status, "resolved")
