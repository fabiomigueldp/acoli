from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
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
from notifications.models import Notification
from notifications.services import enqueue_notification


class NotificationIdempotencyTests(TestCase):
    def test_idempotency_is_scoped_by_parish(self):
        User = get_user_model()
        user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        parish_a = Parish.objects.create(name="Parish A")
        parish_b = Parish.objects.create(name="Parish B")

        enqueue_notification(parish_a, user, "ASSIGNMENT_PUBLISHED", {"assignment_id": 1}, "publish:1")
        enqueue_notification(parish_b, user, "ASSIGNMENT_PUBLISHED", {"assignment_id": 1}, "publish:1")

        self.assertEqual(Notification.objects.count(), 2)
        keys = set(Notification.objects.values_list("idempotency_key", flat=True))
        self.assertTrue(any(key.startswith(f"parish:{parish_a.id}:") for key in keys))
        self.assertTrue(any(key.startswith(f"parish:{parish_b.id}:") for key in keys))


class NotificationTemplateTests(TestCase):
    @override_settings(APP_BASE_URL="https://example.com")
    def test_assignment_payload_uses_absolute_url(self):
        User = get_user_model()
        user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, user=user, display_name="Acolito")

        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now(),
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
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte)

        notification = enqueue_notification(
            parish,
            user,
            "ASSIGNMENT_PUBLISHED",
            {"assignment_id": assignment.id},
            "publish:1",
        )
        self.assertIn("https://example.com", notification.payload.get("body", ""))
