from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import AssignmentSlot, Community, MassInstance, Parish, ParishMembership, PositionType


class DashboardCountsTests(TestCase):
    def test_unfilled_ignores_optional_slots(self):
        User = get_user_model()
        user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        ParishMembership.objects.create(parish=parish, user=user, active=True)

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=2),
            status="scheduled",
        )
        AssignmentSlot.objects.create(
            parish=parish,
            mass_instance=instance,
            position_type=position,
            status="open",
            required=False,
            slot_index=1,
        )
        AssignmentSlot.objects.create(
            parish=parish,
            mass_instance=instance,
            position_type=position,
            status="open",
            required=True,
            slot_index=2,
        )

        self.client.login(email="user@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["unfilled"], 1)
