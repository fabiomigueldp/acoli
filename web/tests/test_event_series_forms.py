from datetime import date, time

from django.test import TestCase

from core.models import Community, Parish, RequirementProfile
from web.forms import EventOccurrenceForm, EventSeriesBasicsForm


class EventSeriesFormTests(TestCase):
    def setUp(self):
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        self.profile = RequirementProfile.objects.create(parish=self.parish, name="Simple")

    def test_series_type_other_requires_custom_value(self):
        form = EventSeriesBasicsForm(
            data={
                "series_type": "Outro",
                "title": "Festa",
                "start_date": date.today(),
                "end_date": date.today(),
                "default_time": "19:00",
                "candidate_pool": "all",
            },
            parish=self.parish,
        )
        self.assertFalse(form.is_valid())
        self.assertIn("series_type_other", form.errors)

        form = EventSeriesBasicsForm(
            data={
                "series_type": "Outro",
                "series_type_other": "Festa local",
                "title": "Festa",
                "start_date": date.today(),
                "end_date": date.today(),
                "default_time": "19:00",
                "candidate_pool": "all",
            },
            parish=self.parish,
        )
        self.assertTrue(form.is_valid())
        self.assertEqual(form.cleaned_data["series_type"], "Festa local")

    def test_move_existing_requires_move_fields(self):
        form = EventOccurrenceForm(
            data={
                "date": date.today(),
                "time": time(19, 0),
                "community": self.community.id,
                "requirement_profile": self.profile.id,
                "label": "Festa",
                "conflict_action": "move_existing",
            },
            parish=self.parish,
        )
        self.assertFalse(form.is_valid())
        self.assertIn("move_to_date", form.errors)
        self.assertIn("move_to_time", form.errors)
        self.assertIn("move_to_community", form.errors)
