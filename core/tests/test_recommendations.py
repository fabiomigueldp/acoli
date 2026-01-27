from datetime import timedelta

from django.test import TestCase
from django.utils import timezone

from core.models import Community, EventSeries, MassInstance, Parish
from core.services.recommendations import get_mass_context


class RecommendationContextTests(TestCase):
    def setUp(self):
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        today = timezone.now().date()
        self.series = EventSeries.objects.create(
            parish=self.parish,
            series_type="Novena",
            title="Serie",
            start_date=today,
            end_date=today,
            default_community=self.community,
            candidate_pool="interested_only",
        )

    def _create_instance(self):
        return MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            event_series=self.series,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )

    def test_interested_only_pool_before_deadline(self):
        instance = self._create_instance()
        now = instance.starts_at - timedelta(hours=60)
        context = get_mass_context(
            instance,
            {"interest_deadline_hours": 48, "interested_pool_fallback": "relax_to_all"},
            interest_map={},
            now=now,
        )
        self.assertEqual(context["pool_mode"], "empty")

    def test_interested_only_pool_after_deadline_relaxes(self):
        instance = self._create_instance()
        now = instance.starts_at - timedelta(hours=10)
        context = get_mass_context(
            instance,
            {"interest_deadline_hours": 48, "interested_pool_fallback": "relax_to_all"},
            interest_map={},
            now=now,
        )
        self.assertEqual(context["pool_mode"], "all")

    def test_interested_only_pool_after_deadline_strict(self):
        instance = self._create_instance()
        now = instance.starts_at - timedelta(hours=10)
        context = get_mass_context(
            instance,
            {"interest_deadline_hours": 48, "interested_pool_fallback": "strict"},
            interest_map={},
            now=now,
        )
        self.assertEqual(context["pool_mode"], "empty")

    def test_series_deadline_overrides_hours(self):
        instance = self._create_instance()
        deadline = instance.starts_at - timedelta(hours=12)
        self.series.interest_deadline_at = deadline
        self.series.save(update_fields=["interest_deadline_at", "updated_at"])

        context = get_mass_context(
            instance,
            {"interest_deadline_hours": 48},
            interest_map={},
            now=instance.starts_at - timedelta(hours=20),
        )
        self.assertEqual(context["interest_deadline_at"], deadline)
