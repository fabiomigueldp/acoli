from datetime import datetime, time, timedelta

from django.test import TestCase
from django.utils import timezone

from core.models import AcolyteAvailabilityRule, AcolyteProfile, Community, MassInstance, Parish
from core.services.availability import is_acolyte_available


class AvailabilityRuleTests(TestCase):
    def setUp(self):
        self.parish = Parish.objects.create(name="Parish")
        self.community_a = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        self.community_b = Community.objects.create(parish=self.parish, code="STM", name="Comunidade")
        self.acolyte = AcolyteProfile.objects.create(parish=self.parish, display_name="Acolito")
        self.tz = timezone.get_current_timezone()
        self.monday = datetime(2026, 2, 9, 10, 0, tzinfo=self.tz)
        self.tuesday = datetime(2026, 2, 10, 10, 0, tzinfo=self.tz)

    def _instance_at(self, when, community):
        return MassInstance.objects.create(
            parish=self.parish,
            community=community,
            starts_at=when,
            status="scheduled",
        )

    def test_no_rules_defaults_to_available(self):
        instance = self._instance_at(self.monday, self.community_a)
        self.assertTrue(is_acolyte_available(self.acolyte, instance))

    def test_unavailable_blocks(self):
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="unavailable",
            day_of_week=self.monday.weekday(),
        )
        instance = self._instance_at(self.monday, self.community_a)
        self.assertFalse(is_acolyte_available(self.acolyte, instance))

    def test_available_only_requires_match(self):
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="available_only",
            day_of_week=self.monday.weekday(),
            start_time=time(9, 0),
            end_time=time(11, 0),
        )
        instance = self._instance_at(self.tuesday, self.community_a)
        self.assertFalse(is_acolyte_available(self.acolyte, instance))

    def test_available_only_allows_match(self):
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="available_only",
            day_of_week=self.monday.weekday(),
            start_time=time(9, 0),
            end_time=time(11, 0),
        )
        instance = self._instance_at(self.monday, self.community_a)
        self.assertTrue(is_acolyte_available(self.acolyte, instance))

    def test_available_only_end_time_exclusive(self):
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="available_only",
            day_of_week=self.monday.weekday(),
            start_time=time(9, 0),
            end_time=time(11, 0),
        )
        boundary = datetime(2026, 2, 9, 11, 0, tzinfo=self.tz)
        instance = self._instance_at(boundary, self.community_a)
        self.assertFalse(is_acolyte_available(self.acolyte, instance))

    def test_available_only_end_only_exclusive(self):
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="available_only",
            end_time=time(10, 0),
        )
        boundary = datetime(2026, 2, 9, 10, 0, tzinfo=self.tz)
        instance = self._instance_at(boundary, self.community_a)
        self.assertFalse(is_acolyte_available(self.acolyte, instance))

    def test_unavailable_overrides_available_only(self):
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="available_only",
            day_of_week=self.monday.weekday(),
            start_time=time(9, 0),
            end_time=time(11, 0),
        )
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="unavailable",
            day_of_week=self.monday.weekday(),
            start_time=time(9, 30),
            end_time=time(10, 30),
        )
        instance = self._instance_at(self.monday, self.community_a)
        self.assertFalse(is_acolyte_available(self.acolyte, instance))

    def test_invalid_available_only_does_not_restrict(self):
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="available_only",
            start_time=time(11, 0),
            end_time=time(10, 0),
        )
        instance = self._instance_at(self.monday, self.community_a)
        self.assertTrue(is_acolyte_available(self.acolyte, instance))

    def test_available_only_respects_date_window(self):
        rule_start = self.monday.date()
        rule_end = rule_start + timedelta(days=2)
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="available_only",
            day_of_week=self.monday.weekday(),
            start_time=time(10, 0),
            end_time=time(12, 0),
            start_date=rule_start,
            end_date=rule_end,
        )

        outside_date = rule_end + timedelta(days=5)
        outside_instance = self._instance_at(datetime.combine(outside_date, time(10, 30), tzinfo=self.tz), self.community_a)
        self.assertFalse(is_acolyte_available(self.acolyte, outside_instance))

        inside_instance = self._instance_at(datetime.combine(rule_start, time(10, 30), tzinfo=self.tz), self.community_a)
        self.assertTrue(is_acolyte_available(self.acolyte, inside_instance))

    def test_available_only_respects_community(self):
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="available_only",
            community=self.community_a,
        )
        instance = self._instance_at(self.monday, self.community_b)
        self.assertFalse(is_acolyte_available(self.acolyte, instance))

    def test_available_only_any_day_applies(self):
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="available_only",
            start_time=time(9, 0),
            end_time=time(11, 0),
            day_of_week=None,
        )
        instance = self._instance_at(self.tuesday, self.community_a)
        self.assertTrue(is_acolyte_available(self.acolyte, instance))

    def test_unavailable_any_day_blocks(self):
        AcolyteAvailabilityRule.objects.create(
            parish=self.parish,
            acolyte=self.acolyte,
            rule_type="unavailable",
            start_time=time(9, 0),
            end_time=time(11, 0),
            day_of_week=None,
        )
        instance = self._instance_at(self.tuesday, self.community_a)
        self.assertFalse(is_acolyte_available(self.acolyte, instance))
