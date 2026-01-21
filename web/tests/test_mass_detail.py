from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteProfile,
    AcolyteQualification,
    Assignment,
    AssignmentSlot,
    Community,
    MassInstance,
    MembershipRole,
    Parish,
    ParishMembership,
    PositionType,
)
from core.services.replacements import assign_replacement_request, create_replacement_request


class MassDetailHistoryTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(email="admin@example.com", full_name="Admin", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Admin")
        membership = ParishMembership.objects.create(parish=self.parish, user=self.user, active=True)
        membership.roles.add(role)

        self.client.login(email="admin@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()

    def test_mass_detail_shows_replacement_history(self):
        position = PositionType.objects.create(parish=self.parish, code="LIB", name="Libriferario")
        acolyte_a = AcolyteProfile.objects.create(parish=self.parish, display_name="Acolito A")
        acolyte_b = AcolyteProfile.objects.create(parish=self.parish, display_name="Acolito B")
        AcolyteQualification.objects.create(parish=self.parish, acolyte=acolyte_a, position_type=position, qualified=True)
        AcolyteQualification.objects.create(parish=self.parish, acolyte=acolyte_b, position_type=position, qualified=True)

        instance = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=timezone.now() + timedelta(days=2),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance,
            position_type=position,
            slot_index=1,
            required=True,
            status="assigned",
        )
        Assignment.objects.create(parish=self.parish, slot=slot, acolyte=acolyte_a, assignment_state="published")
        replacement = create_replacement_request(self.parish, slot)
        assign_replacement_request(self.parish, replacement.id, acolyte_b)

        response = self.client.get(f"/calendar/{instance.id}/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Acolito A")
        self.assertContains(response, "Acolito B")
