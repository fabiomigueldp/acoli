from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteProfile,
    Assignment,
    AssignmentSlot,
    Confirmation,
    Community,
    MassInstance,
    Parish,
    ParishMembership,
    PositionType,
    ReplacementRequest,
)


class AssignmentLifecycleTests(TestCase):
    def test_decline_deactivates_assignment(self):
        User = get_user_model()
        user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, user=user, display_name="Acolito")
        ParishMembership.objects.create(parish=parish, user=user, active=True)
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
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte)

        self.client.login(email="user@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        response = self.client.post(f"/assignments/{assignment.id}/decline/")
        self.assertEqual(response.status_code, 302)
        assignment.refresh_from_db()
        slot.refresh_from_db()
        self.assertFalse(assignment.is_active)
        self.assertEqual(slot.status, "open")

    def test_confirm_inactive_assignment_is_blocked(self):
        User = get_user_model()
        user = User.objects.create_user(email="user2@example.com", full_name="User", password="pass")
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, user=user, display_name="Acolito")
        ParishMembership.objects.create(parish=parish, user=user, active=True)
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
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte, is_active=False)

        self.client.login(email="user2@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        response = self.client.post(f"/assignments/{assignment.id}/confirm/")
        self.assertEqual(response.status_code, 302)
        self.assertFalse(Confirmation.objects.filter(parish=parish, assignment=assignment).exists())

    def test_decline_inactive_assignment_is_blocked(self):
        User = get_user_model()
        user = User.objects.create_user(email="user3@example.com", full_name="User", password="pass")
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, user=user, display_name="Acolito")
        ParishMembership.objects.create(parish=parish, user=user, active=True)
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
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte, is_active=False)

        self.client.login(email="user3@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        response = self.client.post(f"/assignments/{assignment.id}/decline/")
        self.assertEqual(response.status_code, 302)
        self.assertFalse(Confirmation.objects.filter(parish=parish, assignment=assignment).exists())

    def test_cancel_assignment_blocked_when_mass_canceled(self):
        User = get_user_model()
        user = User.objects.create_user(email="user4@example.com", full_name="User", password="pass")
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, user=user, display_name="Acolito")
        ParishMembership.objects.create(parish=parish, user=user, active=True)
        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=3),
            status="canceled",
        )
        slot = AssignmentSlot.objects.create(
            parish=parish,
            mass_instance=instance,
            position_type=position,
            slot_index=1,
            required=True,
            status="assigned",
        )
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte, is_active=True)

        self.client.login(email="user4@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        response = self.client.post(f"/assignments/{assignment.id}/cancel/")
        self.assertEqual(response.status_code, 302)
        assignment.refresh_from_db()
        self.assertTrue(assignment.is_active)
        self.assertFalse(ReplacementRequest.objects.filter(parish=parish, slot=slot).exists())
