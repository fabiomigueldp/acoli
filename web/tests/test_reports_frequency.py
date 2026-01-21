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
    MembershipRole,
    Parish,
    ParishMembership,
    PositionType,
)


class FrequencyReportTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(email="admin@example.com", full_name="Admin", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        self.position = PositionType.objects.create(parish=self.parish, code="LIB", name="Libriferario")
        role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Admin")
        membership = ParishMembership.objects.create(parish=self.parish, user=self.user, active=True)
        membership.roles.add(role)

        self.acolyte_a = AcolyteProfile.objects.create(parish=self.parish, display_name="Acolito A")
        self.acolyte_b = AcolyteProfile.objects.create(parish=self.parish, display_name="Acolito B")

        self.client.login(email="admin@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()

    def test_frequency_counts_assignments(self):
        now = timezone.now()
        instance_a = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=now + timedelta(days=1),
            status="scheduled",
        )
        slot_a = AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance_a,
            position_type=self.position,
            slot_index=1,
            required=True,
            status="assigned",
        )
        Assignment.objects.create(
            parish=self.parish,
            slot=slot_a,
            acolyte=self.acolyte_a,
            assignment_state="published",
        )

        instance_b = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=now + timedelta(days=2),
            status="scheduled",
        )
        slot_b = AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance_b,
            position_type=self.position,
            slot_index=1,
            required=True,
            status="assigned",
        )
        Assignment.objects.create(
            parish=self.parish,
            slot=slot_b,
            acolyte=self.acolyte_a,
            assignment_state="published",
        )

        instance_c = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=now + timedelta(days=3),
            status="scheduled",
        )
        slot_c = AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance_c,
            position_type=self.position,
            slot_index=1,
            required=True,
            status="assigned",
        )
        Assignment.objects.create(
            parish=self.parish,
            slot=slot_c,
            acolyte=self.acolyte_b,
            assignment_state="published",
        )

        start = timezone.localdate()
        end = start + timedelta(days=10)
        response = self.client.get(f"/reports/frequency/?start={start.isoformat()}&end={end.isoformat()}")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Acolito A")
        self.assertContains(response, "Acolito B")
