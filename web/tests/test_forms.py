from datetime import time

from django.test import TestCase
from core.models import (
    AcolyteAvailabilityRule,
    AcolyteProfile,
    Community,
    FunctionType,
    MassTemplate,
    Parish,
    PositionType,
)
from web.forms import AcolytePreferenceForm, DateAbsenceForm, MassTemplateForm, WeeklyAvailabilityForm


class AvailabilityFormTests(TestCase):
    def test_weekly_accepts_any_day(self):
        form = WeeklyAvailabilityForm(data={"rule_type": "unavailable", "day_of_week": ""})
        self.assertTrue(form.is_valid())
        self.assertIsNone(form.cleaned_data["day_of_week"])

    def test_weekly_valid_with_day(self):
        form = WeeklyAvailabilityForm(data={"rule_type": "unavailable", "day_of_week": 1})
        self.assertTrue(form.is_valid())

    def test_weekly_rejects_invalid_time_range(self):
        form = WeeklyAvailabilityForm(
            data={"rule_type": "unavailable", "day_of_week": 1, "start_time": "12:00", "end_time": "10:00"}
        )
        self.assertFalse(form.is_valid())
        self.assertIn("end_time", form.errors)

    def test_date_absence_requires_start_date(self):
        form = DateAbsenceForm(data={})
        self.assertFalse(form.is_valid())
        self.assertIn("start_date", form.errors)

    def test_date_absence_sets_end_date(self):
        form = DateAbsenceForm(data={"start_date": "2026-02-10"})
        self.assertTrue(form.is_valid())
        self.assertEqual(form.cleaned_data["end_date"], form.cleaned_data["start_date"])

    def test_weekly_rejects_overlap(self):
        parish = Parish.objects.create(name="Parish")
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")
        AcolyteAvailabilityRule.objects.create(
            parish=parish,
            acolyte=acolyte,
            rule_type="unavailable",
            day_of_week=1,
            start_time=time(9, 0),
            end_time=time(11, 0),
        )
        form = WeeklyAvailabilityForm(
            data={
                "rule_type": "unavailable",
                "day_of_week": 1,
                "start_time": "10:00",
                "end_time": "12:00",
            },
            acolyte=acolyte,
        )
        self.assertFalse(form.is_valid())
        self.assertIn("__all__", form.errors)

    def test_weekly_allows_touching_intervals(self):
        parish = Parish.objects.create(name="Parish")
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")
        AcolyteAvailabilityRule.objects.create(
            parish=parish,
            acolyte=acolyte,
            rule_type="unavailable",
            day_of_week=1,
            start_time=time(9, 0),
            end_time=time(11, 0),
        )
        form = WeeklyAvailabilityForm(
            data={
                "rule_type": "unavailable",
                "day_of_week": 1,
                "start_time": "11:00",
                "end_time": "12:00",
            },
            acolyte=acolyte,
        )
        self.assertTrue(form.is_valid())

    def test_weekly_rejects_overlap_with_any_day(self):
        parish = Parish.objects.create(name="Parish")
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")
        AcolyteAvailabilityRule.objects.create(
            parish=parish,
            acolyte=acolyte,
            rule_type="unavailable",
            day_of_week=None,
            start_time=time(9, 0),
            end_time=time(11, 0),
        )
        form = WeeklyAvailabilityForm(
            data={
                "rule_type": "unavailable",
                "day_of_week": 1,
                "start_time": "10:00",
                "end_time": "12:00",
            },
            acolyte=acolyte,
        )
        self.assertFalse(form.is_valid())
        self.assertIn("__all__", form.errors)

    def test_weekly_rejects_any_day_over_specific(self):
        parish = Parish.objects.create(name="Parish")
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")
        AcolyteAvailabilityRule.objects.create(
            parish=parish,
            acolyte=acolyte,
            rule_type="unavailable",
            day_of_week=2,
            start_time=time(9, 0),
            end_time=time(11, 0),
        )
        form = WeeklyAvailabilityForm(
            data={
                "rule_type": "unavailable",
                "day_of_week": "",
                "start_time": "10:00",
                "end_time": "12:00",
            },
            acolyte=acolyte,
        )
        self.assertFalse(form.is_valid())
        self.assertIn("__all__", form.errors)

    def test_weekly_rejects_duplicate_rule(self):
        parish = Parish.objects.create(name="Parish")
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")
        AcolyteAvailabilityRule.objects.create(
            parish=parish,
            acolyte=acolyte,
            rule_type="unavailable",
            day_of_week=3,
            start_time=time(9, 0),
            end_time=time(11, 0),
        )
        form = WeeklyAvailabilityForm(
            data={
                "rule_type": "unavailable",
                "day_of_week": 3,
                "start_time": "09:00",
                "end_time": "11:00",
            },
            acolyte=acolyte,
        )
        self.assertFalse(form.is_valid())
        self.assertIn("__all__", form.errors)

    def test_weekly_rejects_overlap_with_full_day_rule(self):
        parish = Parish.objects.create(name="Parish")
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")
        AcolyteAvailabilityRule.objects.create(
            parish=parish,
            acolyte=acolyte,
            rule_type="unavailable",
            day_of_week=4,
        )
        form = WeeklyAvailabilityForm(
            data={
                "rule_type": "unavailable",
                "day_of_week": 4,
                "start_time": "10:00",
                "end_time": "12:00",
            },
            acolyte=acolyte,
        )
        self.assertFalse(form.is_valid())
        self.assertIn("__all__", form.errors)


class PreferenceFormTests(TestCase):
    def setUp(self):
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        self.position = PositionType.objects.create(parish=self.parish, code="LIB", name="Libriferario")
        self.function = FunctionType.objects.create(parish=self.parish, code="LIB", name="Libriferario")
        self.acolyte = AcolyteProfile.objects.create(parish=self.parish, display_name="Acolito")
        self.template = MassTemplate.objects.create(
            parish=self.parish,
            title="Missa",
            community=self.community,
            weekday=0,
            time=time(9, 0),
        )

    def _build_form(self, data):
        return AcolytePreferenceForm(data=data, parish=self.parish)

    def test_preference_requires_target_community(self):
        form = self._build_form({"preference_type": "preferred_community", "weight": 50})
        self.assertFalse(form.is_valid())
        self.assertIn("target_community", form.errors)

    def test_preference_requires_target_position(self):
        form = self._build_form({"preference_type": "preferred_position", "weight": 50})
        self.assertFalse(form.is_valid())
        self.assertIn("target_position", form.errors)

    def test_preference_requires_target_function(self):
        form = self._build_form({"preference_type": "preferred_function", "weight": 50})
        self.assertFalse(form.is_valid())
        self.assertIn("target_function", form.errors)

    def test_preference_requires_target_template(self):
        form = self._build_form({"preference_type": "preferred_mass_template", "weight": 50})
        self.assertFalse(form.is_valid())
        self.assertIn("target_template", form.errors)

    def test_preference_requires_target_partner(self):
        form = self._build_form({"preference_type": "preferred_partner", "weight": 50})
        self.assertFalse(form.is_valid())
        self.assertIn("target_acolyte", form.errors)

    def test_preference_timeslot_requires_criteria(self):
        form = self._build_form({"preference_type": "preferred_timeslot", "weight": 50})
        self.assertFalse(form.is_valid())
        self.assertIn("__all__", form.errors)

    def test_preference_timeslot_accepts_weekday(self):
        form = self._build_form({"preference_type": "preferred_timeslot", "weekday": 2, "weight": 50})
        self.assertTrue(form.is_valid())

    def test_preference_cleans_irrelevant_fields(self):
        form = self._build_form(
            {
                "preference_type": "preferred_community",
                "target_community": self.community.id,
                "target_function": self.function.id,
                "weekday": 1,
                "weight": 50,
            }
        )
        self.assertTrue(form.is_valid())
        self.assertIsNone(form.cleaned_data["target_function"])
        self.assertIsNone(form.cleaned_data["weekday"])

    def test_preference_rejects_foreign_parish_target(self):
        other_parish = Parish.objects.create(name="Other")
        other_community = Community.objects.create(parish=other_parish, code="OUT", name="Outra")
        form = self._build_form(
            {
                "preference_type": "preferred_community",
                "target_community": other_community.id,
                "weight": 50,
            }
        )
        self.assertFalse(form.is_valid())
        self.assertIn("target_community", form.errors)


class MassTemplateFormTests(TestCase):
    def test_mass_template_rejects_foreign_parish(self):
        parish_a = Parish.objects.create(name="Parish A")
        parish_b = Parish.objects.create(name="Parish B")
        community_b = Community.objects.create(parish=parish_b, code="B", name="B")
        form = MassTemplateForm(
            data={
                "title": "Missa",
                "community": community_b.id,
                "weekday": 0,
                "time": "09:00",
                "active": True,
            },
            parish=parish_a,
        )
        self.assertFalse(form.is_valid())
        self.assertIn("community", form.errors)
