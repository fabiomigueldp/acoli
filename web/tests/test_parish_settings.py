from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import MembershipRole, Parish, ParishMembership, AuditEvent
from web.forms import ParishSettingsForm


class ParishSettingsTests(TestCase):
    def test_update_parish_settings(self):
        User = get_user_model()
        user = User.objects.create_user(email="admin@example.com", full_name="Admin", password="pass")
        parish = Parish.objects.create(name="Parish", consolidation_days=14, horizon_days=60)
        role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Parish admin")
        membership = ParishMembership.objects.create(parish=parish, user=user, active=True)
        membership.roles.add(role)

        self.client.login(email="admin@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        response = self.client.post(
            "/settings/",
            {
                "consolidation_days": 10,
                "horizon_days": 45,
                "default_mass_duration_minutes": 60,
                "min_rest_minutes_between_masses": 0,
                "swap_requires_approval": "on",
                "notify_on_cancellation": "on",
                "auto_assign_on_decline": "",
                "stability_penalty": 10,
                "fairness_penalty": 2,
                "credit_weight": 1,
                "max_solve_seconds": 10,
                "max_services_per_week": 3,
                "max_consecutive_weekends": 2,
                "rotation_penalty": 3,
                "rotation_days": 60,
            },
        )
        self.assertEqual(response.status_code, 302)
        parish.refresh_from_db()
        self.assertEqual(parish.consolidation_days, 10)
        self.assertEqual(parish.horizon_days, 45)
        self.assertTrue(AuditEvent.objects.filter(parish=parish, entity_type="Parish").exists())

    def test_horizon_must_be_after_consolidation(self):
        parish = Parish.objects.create(name="Parish", consolidation_days=14, horizon_days=60)
        form = ParishSettingsForm(
            data={
                "consolidation_days": 40,
                "horizon_days": 20,
                "default_mass_duration_minutes": 60,
                "min_rest_minutes_between_masses": 0,
            },
            parish=parish,
        )
        form.fields["consolidation_days"].validators = []
        form.fields["horizon_days"].validators = []
        form.fields["consolidation_days"].min_value = None
        form.fields["consolidation_days"].max_value = None
        form.fields["horizon_days"].min_value = None
        self.assertFalse(form.is_valid())
        self.assertIn("O horizonte precisa ser maior ou igual a consolidacao.", form.errors.get("horizon_days", []))

    def test_reset_defaults(self):
        User = get_user_model()
        user = User.objects.create_user(email="admin3@example.com", full_name="Admin", password="pass")
        parish = Parish.objects.create(name="Parish", consolidation_days=14, horizon_days=60, schedule_weights={"stability_penalty": 99})
        role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Parish admin")
        membership = ParishMembership.objects.create(parish=parish, user=user, active=True)
        membership.roles.add(role)

        self.client.login(email="admin3@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        response = self.client.post("/settings/", {"action": "reset"})
        self.assertEqual(response.status_code, 302)
        parish.refresh_from_db()
        self.assertEqual(parish.schedule_weights, ParishSettingsForm.DEFAULT_SCHEDULE_WEIGHTS)

    def test_update_parish_settings_extended_weights(self):
        User = get_user_model()
        user = User.objects.create_user(email="admin4@example.com", full_name="Admin", password="pass")
        parish = Parish.objects.create(name="Parish", consolidation_days=14, horizon_days=60, schedule_weights={})
        role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Parish admin")
        membership = ParishMembership.objects.create(parish=parish, user=user, active=True)
        membership.roles.add(role)

        self.client.login(email="admin4@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        response = self.client.post(
            "/settings/",
            {
                "consolidation_days": 10,
                "horizon_days": 45,
                "default_mass_duration_minutes": 60,
                "min_rest_minutes_between_masses": 0,
                "home_community_bonus": 55,
                "community_recent_penalty": 7,
                "community_recent_window_days": 20,
                "scarcity_bonus": 12,
                "event_series_community_factor": 0.3,
                "single_mass_community_policy": "special",
                "interest_deadline_hours": 24,
                "interested_pool_fallback": "strict",
            },
        )
        self.assertEqual(response.status_code, 302)
        parish.refresh_from_db()
        weights = parish.schedule_weights
        self.assertEqual(weights.get("home_community_bonus"), 55)
        self.assertEqual(weights.get("community_recent_penalty"), 7)
        self.assertEqual(weights.get("community_recent_window_days"), 20)
        self.assertEqual(weights.get("scarcity_bonus"), 12)
        self.assertAlmostEqual(weights.get("event_series_community_factor"), 0.3)
        self.assertEqual(weights.get("single_mass_community_policy"), "special")
        self.assertEqual(weights.get("interest_deadline_hours"), 24)
        self.assertEqual(weights.get("interested_pool_fallback"), "strict")
