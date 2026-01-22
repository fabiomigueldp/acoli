from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteProfile,
    Assignment,
    AssignmentSlot,
    Community,
    MassInstance,
    Parish,
    PositionType,
)
from core.services.publishing import publish_assignments
from notifications.models import Notification


class PublishingTests(TestCase):
    def test_publish_creates_confirmations_and_notifications(self):
        User = get_user_model()
        user = User.objects.create_user(email="acolyte@example.com", full_name="Acolito", password="pass")
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, user=user, display_name="Acolito")

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(
            parish=parish,
            mass_instance=instance,
            position_type=position,
            slot_index=1,
            required=True,
            status="open",
        )
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte, assignment_state="proposed")

        published = publish_assignments(parish, instance.starts_at.date(), instance.starts_at.date(), actor=user)
        self.assertEqual(published, 1)

        assignment.refresh_from_db()
        self.assertEqual(assignment.assignment_state, "published")
        self.assertIsNotNone(assignment.published_at)
        self.assertEqual(assignment.confirmation.status, "pending")
        self.assertEqual(Notification.objects.filter(user=user, parish=parish).count(), 2)

    def test_publish_is_idempotent(self):
        User = get_user_model()
        user = User.objects.create_user(email="acolyte2@example.com", full_name="Acolito", password="pass")
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, user=user, display_name="Acolito")

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(
            parish=parish,
            mass_instance=instance,
            position_type=position,
            slot_index=1,
            required=True,
            status="open",
        )
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte, assignment_state="proposed")

        first = publish_assignments(parish, instance.starts_at.date(), instance.starts_at.date(), actor=user)
        second = publish_assignments(parish, instance.starts_at.date(), instance.starts_at.date(), actor=user)

        assignment.refresh_from_db()
        self.assertEqual(first, 1)
        self.assertEqual(second, 0)
        self.assertEqual(assignment.assignment_state, "published")
        self.assertEqual(Notification.objects.filter(user=user, parish=parish).count(), 2)
