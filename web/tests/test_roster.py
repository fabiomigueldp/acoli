from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteProfile,
    AssignmentSlot,
    Community,
    MassInstance,
    MembershipRole,
    Parish,
    ParishMembership,
    PositionType,
)


class RosterViewTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(email="admin@example.com", full_name="Admin", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Admin")
        membership = ParishMembership.objects.create(parish=self.parish, user=self.user, active=True)
        membership.roles.add(role)
        AcolyteProfile.objects.create(parish=self.parish, display_name="Acolito")

        self.client.login(email="admin@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()

    def test_roster_marks_open_and_external_and_na(self):
        position_open = PositionType.objects.create(parish=self.parish, code="LIB", name="Libriferario")
        position_external = PositionType.objects.create(parish=self.parish, code="TUR", name="Turiferario")
        position_na = PositionType.objects.create(parish=self.parish, code="CER", name="Cerimoniario")

        instance = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=timezone.now() + timedelta(days=1),
            status="scheduled",
        )
        AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance,
            position_type=position_open,
            slot_index=1,
            required=True,
            status="open",
        )
        AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance,
            position_type=position_external,
            slot_index=1,
            required=False,
            externally_covered=True,
            status="finalized",
        )
        AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance,
            position_type=position_na,
            slot_index=1,
            required=False,
            status="finalized",
        )

        start = timezone.localdate()
        end = start + timedelta(days=2)
        response = self.client.get(f"/roster/?start={start.isoformat()}&end={end.isoformat()}")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "ABERTO")
        self.assertContains(response, "Externo")
        self.assertContains(response, "N/A")

    def test_roster_requires_admin(self):
        User = get_user_model()
        user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        role = MembershipRole.objects.create(code="ACOLYTE", name="Acolyte")
        membership = ParishMembership.objects.create(parish=self.parish, user=user, active=True)
        membership.roles.add(role)

        client = self.client
        client.logout()
        client.login(email="user@example.com", password="pass")
        session = client.session
        session["active_parish_id"] = self.parish.id
        session.save()

        response = client.get("/roster/")
        self.assertEqual(response.status_code, 403)


class RosterExportTests(TestCase):
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

        position = PositionType.objects.create(parish=self.parish, code="LIB", name="Libriferario")
        instance = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=timezone.now() + timedelta(days=1),
            status="scheduled",
        )
        AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance,
            position_type=position,
            slot_index=1,
            required=True,
            status="open",
        )

    def test_export_pdf_returns_pdf(self):
        start = timezone.localdate()
        end = start + timedelta(days=2)
        response = self.client.get(f"/roster/export/pdf/?start={start.isoformat()}&end={end.isoformat()}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("application/pdf", response["Content-Type"])

    def test_export_whatsapp_returns_text(self):
        start = timezone.localdate()
        end = start + timedelta(days=2)
        response = self.client.get(f"/roster/export/whatsapp/?start={start.isoformat()}&end={end.isoformat()}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("ABERTO", response.content.decode("utf-8"))
