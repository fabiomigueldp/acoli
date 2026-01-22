from datetime import timedelta

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
from notifications.services import NotificationService, enqueue_notification


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


class NotificationProcessingTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        self.parish = Parish.objects.create(name="Parish")

    def test_claim_pending_is_exclusive(self):
        Notification.objects.create(
            parish=self.parish,
            user=self.user,
            channel="email",
            template_code="ASSIGNMENT_PUBLISHED",
            payload={"subject": "Teste", "body": "Teste"},
            idempotency_key="parish:1:claim:1",
        )
        Notification.objects.create(
            parish=self.parish,
            user=self.user,
            channel="email",
            template_code="ASSIGNMENT_PUBLISHED",
            payload={"subject": "Teste", "body": "Teste"},
            idempotency_key="parish:1:claim:2",
        )
        service = NotificationService()
        first = set(service.claim_pending_ids(limit=1))
        second = set(service.claim_pending_ids(limit=1))
        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertTrue(first.isdisjoint(second))

    def test_claim_pending_returns_only_updated_batch(self):
        Notification.objects.create(
            parish=self.parish,
            user=self.user,
            channel="email",
            template_code="ASSIGNMENT_PUBLISHED",
            payload={"subject": "Teste", "body": "Teste"},
            idempotency_key="parish:1:claim:3",
        )
        Notification.objects.create(
            parish=self.parish,
            user=self.user,
            channel="email",
            template_code="ASSIGNMENT_PUBLISHED",
            payload={"subject": "Teste", "body": "Teste"},
            idempotency_key="parish:1:claim:4",
        )
        service = NotificationService()
        now = timezone.now()
        claimed = service.claim_pending_ids(limit=10, now=now)
        self.assertEqual(len(claimed), 2)
        self.assertEqual(
            Notification.objects.filter(id__in=claimed, last_attempt_at=now).count(),
            2,
        )
        self.assertEqual(service.claim_pending_ids(limit=10), [])

    def test_failed_delivery_does_not_set_sent_at(self):
        class FailingService(NotificationService):
            def deliver(self, notification):
                raise RuntimeError("fail")

        notification = Notification.objects.create(
            parish=self.parish,
            user=self.user,
            channel="email",
            template_code="ASSIGNMENT_PUBLISHED",
            payload={"subject": "Teste", "body": "Teste"},
            idempotency_key="parish:1:fail:1",
        )
        service = FailingService()
        service.send_pending()
        notification.refresh_from_db()
        self.assertEqual(notification.status, "failed")
        self.assertIsNone(notification.sent_at)
        self.assertTrue(notification.error_message)

    def test_reclaim_stuck_processing(self):
        now = timezone.now()
        stuck = Notification.objects.create(
            parish=self.parish,
            user=self.user,
            channel="email",
            template_code="ASSIGNMENT_PUBLISHED",
            payload={"subject": "Teste", "body": "Teste"},
            idempotency_key="parish:1:stuck:1",
            status="processing",
            last_attempt_at=now - timedelta(minutes=20),
        )
        service = NotificationService()
        claimed = service.claim_pending_ids(limit=10, now=now)
        self.assertIn(stuck.id, claimed)
        stuck.refresh_from_db()
        self.assertEqual(stuck.status, "processing")
        self.assertEqual(stuck.last_attempt_at, now)
