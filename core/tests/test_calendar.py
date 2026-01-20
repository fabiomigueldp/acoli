from datetime import date, time, timedelta

from django.test import TestCase
from django.utils import timezone

from core.models import Community, MassTemplate, Parish, RequirementProfile
from core.services.calendar_generation import generate_instances_for_parish


class CalendarGenerationTests(TestCase):
    def test_generate_instances(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        profile = RequirementProfile.objects.create(parish=parish, name="Simple")
        MassTemplate.objects.create(
            parish=parish,
            title="Saturday Mass",
            community=community,
            weekday=5,
            time=time(17, 0),
            default_requirement_profile=profile,
        )
        start = date.today()
        end = start + timedelta(days=7)
        created = generate_instances_for_parish(parish, start, end)
        self.assertTrue(created)

