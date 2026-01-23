from datetime import date

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import Community, EventSeries, MembershipRole, Parish, ParishMembership


class EventSeriesViewTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.admin = User.objects.create_user(email="admin@example.com", full_name="Admin", password="pass")
        self.user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")

        admin_role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Admin")
        acolyte_role = MembershipRole.objects.create(code="ACOLYTE", name="Acolyte")
        admin_membership = ParishMembership.objects.create(parish=self.parish, user=self.admin, active=True)
        admin_membership.roles.add(admin_role)
        user_membership = ParishMembership.objects.create(parish=self.parish, user=self.user, active=True)
        user_membership.roles.add(acolyte_role)

        self.series = EventSeries.objects.create(
            parish=self.parish,
            series_type="Festa",
            title="Festa",
            start_date=date.today(),
            end_date=date.today(),
            default_community=self.community,
            candidate_pool="all",
            is_active=True,
        )

    def _login_as(self, user):
        self.client.login(email=user.email, password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()

    def test_event_series_list_requires_admin(self):
        self._login_as(self.user)
        response = self.client.get("/events/")
        self.assertEqual(response.status_code, 403)

    def test_event_series_list_admin_ok(self):
        self._login_as(self.admin)
        response = self.client.get("/events/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Festa")

    def test_event_series_detail_admin_ok(self):
        self._login_as(self.admin)
        response = self.client.get(f"/events/{self.series.id}/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Festa")

    def test_event_series_detail_requires_admin(self):
        self._login_as(self.user)
        response = self.client.get(f"/events/{self.series.id}/")
        self.assertEqual(response.status_code, 403)

    def test_event_series_archive_hides_series(self):
        self._login_as(self.admin)
        response = self.client.post(f"/events/{self.series.id}/archive/")
        self.assertEqual(response.status_code, 302)
        self.series.refresh_from_db()
        self.assertFalse(self.series.is_active)
        list_response = self.client.get("/events/")
        self.assertNotContains(list_response, "Festa")
        archived_response = self.client.get("/events/?status=archived")
        self.assertContains(archived_response, "Festa")

    def test_event_series_unarchive_restores_visibility(self):
        self._login_as(self.admin)
        self.series.is_active = False
        self.series.save(update_fields=["is_active"])
        response = self.client.post(f"/events/{self.series.id}/unarchive/")
        self.assertEqual(response.status_code, 302)
        self.series.refresh_from_db()
        self.assertTrue(self.series.is_active)
        list_response = self.client.get("/events/")
        self.assertContains(list_response, "Festa")

    def test_event_series_delete_removes_masses(self):
        self._login_as(self.admin)
        self.series.is_active = False
        self.series.save(update_fields=["is_active"])
        from core.models import MassInstance

        MassInstance.objects.create(
            parish=self.parish,
            event_series=self.series,
            community=self.community,
            starts_at=timezone.now(),
            status="scheduled",
        )
        response = self.client.post(f"/events/{self.series.id}/delete/")
        self.assertEqual(response.status_code, 302)
        self.assertFalse(EventSeries.objects.filter(id=self.series.id).exists())
        self.assertEqual(MassInstance.objects.filter(parish=self.parish).count(), 0)
