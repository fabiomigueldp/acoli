from datetime import date, datetime, time

from django.test import TestCase
from django.utils import timezone

from core.models import Community, EventSeries, MassInstance, Parish, RequirementProfile
from core.services.event_series import apply_event_occurrences


class EventSeriesConflictTests(TestCase):
    def setUp(self):
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        self.profile = RequirementProfile.objects.create(parish=self.parish, name="Simple")
        self.series = EventSeries.objects.create(
            parish=self.parish,
            series_type="novena",
            title="Novena",
            start_date=date.today(),
            end_date=date.today(),
            default_community=self.community,
        )

    def test_keep_conflict_does_not_duplicate(self):
        starts_at = timezone.make_aware(datetime.combine(date.today(), time(19, 0)))
        existing = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=starts_at,
            status="scheduled",
        )
        apply_event_occurrences(
            self.series,
            [
                {
                    "date": date.today(),
                    "time": time(19, 0),
                    "community_id": self.community.id,
                    "requirement_profile_id": self.profile.id,
                    "label": "Novena",
                    "conflict_action": "keep",
                }
            ],
        )
        self.assertEqual(MassInstance.objects.filter(parish=self.parish).count(), 1)
        existing.refresh_from_db()
        self.assertEqual(existing.event_series_id, self.series.id)

    def test_cancel_existing_creates_new(self):
        starts_at = timezone.make_aware(datetime.combine(date.today(), time(19, 0)))
        existing = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=starts_at,
            status="scheduled",
        )
        apply_event_occurrences(
            self.series,
            [
                {
                    "date": date.today(),
                    "time": time(19, 0),
                    "community_id": self.community.id,
                    "requirement_profile_id": self.profile.id,
                    "label": "Novena",
                    "conflict_action": "cancel_existing",
                }
            ],
        )
        existing.refresh_from_db()
        self.assertEqual(existing.status, "canceled")
        self.assertEqual(MassInstance.objects.filter(parish=self.parish).count(), 2)

    def test_skip_does_not_create(self):
        apply_event_occurrences(
            self.series,
            [
                {
                    "date": date.today(),
                    "time": time(19, 0),
                    "community_id": self.community.id,
                    "requirement_profile_id": self.profile.id,
                    "label": "Novena",
                    "conflict_action": "skip",
                }
            ],
        )
        self.assertEqual(MassInstance.objects.filter(parish=self.parish).count(), 0)

    def test_blank_label_defaults_to_series_title(self):
        apply_event_occurrences(
            self.series,
            [
                {
                    "date": date.today(),
                    "time": time(19, 0),
                    "community_id": self.community.id,
                    "requirement_profile_id": self.profile.id,
                    "label": "",
                    "conflict_action": "keep",
                }
            ],
        )
        instance = MassInstance.objects.get(parish=self.parish)
        self.assertEqual(instance.liturgy_label, self.series.title)

    def test_move_existing_creates_new_and_moves_original(self):
        starts_at = timezone.make_aware(datetime.combine(date.today(), time(19, 0)))
        move_to_date = date.today()
        existing = MassInstance.objects.create(
            parish=self.parish,
            community=self.community,
            starts_at=starts_at,
            status="scheduled",
        )
        apply_event_occurrences(
            self.series,
            [
                {
                    "date": date.today(),
                    "time": time(19, 0),
                    "community_id": self.community.id,
                    "requirement_profile_id": self.profile.id,
                    "label": "Novena",
                    "conflict_action": "move_existing",
                    "move_to_date": move_to_date,
                    "move_to_time": time(20, 0),
                    "move_to_community_id": self.community.id,
                }
            ],
        )
        self.assertEqual(MassInstance.objects.filter(parish=self.parish).count(), 2)
        existing.refresh_from_db()
        self.assertEqual(existing.starts_at, timezone.make_aware(datetime.combine(move_to_date, time(20, 0))))

