from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import AcolyteAvailabilityRule, AcolyteProfile, Community, MembershipRole, Parish, ParishMembership


class PreferencesViewTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        role = MembershipRole.objects.create(code="ACOLYTE", name="Acolyte")
        membership = ParishMembership.objects.create(parish=self.parish, user=self.user, active=True)
        membership.roles.add(role)
        self.acolyte = AcolyteProfile.objects.create(parish=self.parish, user=self.user, display_name="Acolito")

        self.client.login(email="user@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()

    def test_weekly_deduplicates_rules(self):
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="unavailable",
            day_of_week=1,
            community=self.community,
        )
        response = self.client.post(
            "/preferences/",
            {
                "form_type": "weekly_availability",
                "rule_type": "unavailable",
                "day_of_week": 1,
                "community": self.community.id,
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Voce ja possui uma regra igual.")
        self.assertEqual(
            AcolyteAvailabilityRule.objects.filter(parish=self.parish, acolyte=self.acolyte).count(), 1
        )

    def test_weekly_create_redirects(self):
        response = self.client.post(
            "/preferences/",
            {
                "form_type": "weekly_availability",
                "rule_type": "unavailable",
                "day_of_week": 1,
                "community": self.community.id,
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            AcolyteAvailabilityRule.objects.filter(parish=self.parish, acolyte=self.acolyte).count(), 1
        )

    def test_date_absence_deduplicates_rules(self):
        start_date = timezone.now().date()
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="unavailable",
            start_date=start_date,
            end_date=start_date,
        )
        response = self.client.post(
            "/preferences/",
            {
                "form_type": "date_absence",
                "start_date": start_date.isoformat(),
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Voce ja possui uma regra igual.")
        self.assertEqual(
            AcolyteAvailabilityRule.objects.filter(parish=self.parish, acolyte=self.acolyte).count(), 1
        )

    def test_weekly_overlap_shows_message(self):
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="unavailable",
            day_of_week=1,
            start_time=timezone.datetime.strptime("09:00", "%H:%M").time(),
            end_time=timezone.datetime.strptime("11:00", "%H:%M").time(),
        )
        response = self.client.post(
            "/preferences/",
            {
                "form_type": "weekly_availability",
                "rule_type": "unavailable",
                "day_of_week": 1,
                "start_time": "10:00",
                "end_time": "12:00",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Ja existe uma regra semelhante nesse dia/horario.")
