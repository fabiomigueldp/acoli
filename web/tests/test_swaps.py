from datetime import timedelta

from django.contrib.auth import get_user_model
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
    SwapRequest,
    ParishMembership,
)


class SwapAuthorizationTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        self.position = PositionType.objects.create(parish=self.parish, code="LIB", name="Libriferario")
        self.acolyte = AcolyteProfile.objects.create(parish=self.parish, user=self.user, display_name="Acolito")
        ParishMembership.objects.create(parish=self.parish, user=self.user, active=True)
        instance = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )
        self.slot = AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance,
            position_type=self.position,
            slot_index=1,
            required=True,
            status="assigned",
        )
        self.assignment = Assignment.objects.create(parish=self.parish, slot=self.slot, acolyte=self.acolyte)
        self.swap = SwapRequest.objects.create(
            parish=self.parish,
            swap_type="acolyte_swap",
            requestor_acolyte=self.acolyte,
            mass_instance=instance,
            from_slot=self.slot,
            status="pending",
            open_to_admin=True,
        )

    def test_non_admin_cannot_accept_open_swap(self):
        self.client.login(email="user@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()

        response = self.client.post(f"/swap-requests/{self.swap.id}/accept/")
        self.assertEqual(response.status_code, 302)
        self.swap.refresh_from_db()
        self.assertTrue(self.swap.open_to_admin)
        self.assertEqual(self.swap.status, "pending")
