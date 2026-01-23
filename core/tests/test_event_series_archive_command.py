from datetime import date, timedelta

from django.core.management import call_command
from django.test import TestCase

from core.models import Community, EventSeries, Parish


class ArchiveEventSeriesCommandTests(TestCase):
    def test_archives_past_series_only(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        past = EventSeries.objects.create(
            parish=parish,
            series_type="Festa",
            title="Past",
            start_date=date.today() - timedelta(days=3),
            end_date=date.today() - timedelta(days=1),
            default_community=community,
            candidate_pool="all",
            is_active=True,
        )
        future = EventSeries.objects.create(
            parish=parish,
            series_type="Festa",
            title="Future",
            start_date=date.today(),
            end_date=date.today() + timedelta(days=1),
            default_community=community,
            candidate_pool="all",
            is_active=True,
        )

        call_command("archive_event_series")

        past.refresh_from_db()
        future.refresh_from_db()
        self.assertFalse(past.is_active)
        self.assertTrue(future.is_active)
