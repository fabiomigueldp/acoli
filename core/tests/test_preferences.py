from datetime import datetime, time

from django.test import TestCase
from django.utils import timezone

from core.models import AcolytePreference, AcolyteProfile, AssignmentSlot, Community, MassInstance, Parish, PositionType
from core.services.preferences import preference_score


class PreferenceScoreTests(TestCase):
    def test_preferred_timeslot_end_exclusive(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        tz = timezone.get_current_timezone()
        starts_at_inside = datetime(2026, 2, 9, 10, 59, tzinfo=tz)
        weekday = starts_at_inside.weekday()
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")

        pref = AcolytePreference.objects.create(
            parish=parish,
            acolyte=acolyte,
            preference_type="preferred_timeslot",
            weekday=weekday,
            start_time=time(9, 0),
            end_time=time(11, 0),
            weight=50,
        )

        inside_instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=starts_at_inside,
            status="scheduled",
        )
        boundary_instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=datetime(2026, 2, 9, 11, 0, tzinfo=tz),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(parish=parish, mass_instance=inside_instance, position_type=position)

        score_inside = preference_score(None, inside_instance, slot, [pref])
        score_boundary = preference_score(None, boundary_instance, slot, [pref])

        self.assertEqual(score_inside, 50)
        self.assertEqual(score_boundary, 0)

    def test_preferred_timeslot_end_only_exclusive(self):
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        tz = timezone.get_current_timezone()

        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Acolito")
        pref = AcolytePreference.objects.create(
            parish=parish,
            acolyte=acolyte,
            preference_type="preferred_timeslot",
            end_time=time(10, 0),
            weight=40,
        )

        inside_instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=datetime(2026, 2, 9, 9, 59, tzinfo=tz),
            status="scheduled",
        )
        boundary_instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=datetime(2026, 2, 9, 10, 0, tzinfo=tz),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(parish=parish, mass_instance=inside_instance, position_type=position)

        score_inside = preference_score(None, inside_instance, slot, [pref])
        score_boundary = preference_score(None, boundary_instance, slot, [pref])

        self.assertEqual(score_inside, 40)
        self.assertEqual(score_boundary, 0)
