from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteProfile,
    Assignment,
    AssignmentSlot,
    Community,
    Confirmation,
    MassInstance,
    MembershipRole,
    Parish,
    ParishMembership,
    PositionType,
)


class SchedulingDashboardTests(TestCase):
    def test_pending_confirmations_ignore_inactive(self):
        User = get_user_model()
        user = User.objects.create_user(email="admin@example.com", full_name="Admin", password="pass")
        parish = Parish.objects.create(name="Parish", consolidation_days=14)
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Parish admin")
        membership = ParishMembership.objects.create(parish=parish, user=user, active=True)
        membership.roles.add(role)

        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")
        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=2),
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
        active_assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte, is_active=True)
        Confirmation.objects.create(parish=parish, assignment=active_assignment, status="pending")

        inactive_slot = AssignmentSlot.objects.create(
            parish=parish,
            mass_instance=instance,
            position_type=position,
            slot_index=2,
            required=True,
            status="assigned",
        )
        inactive_assignment = Assignment.objects.create(parish=parish, slot=inactive_slot, acolyte=acolyte, is_active=False)
        Confirmation.objects.create(parish=parish, assignment=inactive_assignment, status="pending")

        self.client.login(email="admin@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        response = self.client.get("/scheduling/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["pending_confirmations"], 1)
