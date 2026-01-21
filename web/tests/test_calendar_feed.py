from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteProfile,
    Assignment,
    AssignmentSlot,
    CalendarFeedToken,
    Community,
    MassInstance,
    Parish,
    ParishMembership,
    PositionType,
)


class CalendarFeedTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        self.position = PositionType.objects.create(parish=self.parish, code="LIB", name="Libriferario")
        ParishMembership.objects.create(parish=self.parish, user=self.user, active=True)
        self.acolyte = AcolyteProfile.objects.create(parish=self.parish, user=self.user, display_name="Acolito")

        instance = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(
            parish=self.parish,
            mass_instance=instance,
            position_type=self.position,
            slot_index=1,
            required=True,
            status="assigned",
        )
        Assignment.objects.create(
            parish=self.parish,
            slot=slot,
            acolyte=self.acolyte,
            assignment_state="published",
        )

    def test_calendar_feed_requires_token(self):
        response = self.client.get("/calendar/my.ics")
        self.assertEqual(response.status_code, 404)

    def test_calendar_feed_returns_ics(self):
        token = CalendarFeedToken.objects.create(parish=self.parish, user=self.user, token="tok123")
        response = self.client.get(f"/calendar/my.ics?token={token.token}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("text/calendar", response["Content-Type"])
        self.assertIn("BEGIN:VEVENT", response.content.decode("utf-8"))

    def test_calendar_feed_token_creation(self):
        self.client.login(email="user@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()
        response = self.client.post("/calendar/feed/token/")
        self.assertEqual(response.status_code, 302)
        self.assertTrue(CalendarFeedToken.objects.filter(parish=self.parish, user=self.user).exists())
