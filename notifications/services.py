from django.conf import settings
from django.core.mail import send_mail
from django.utils import timezone

from notifications.models import Notification, NotificationPreference


class WhatsAppProvider:
    def send(self, notification):
        return True


class NotificationService:
    def __init__(self):
        self.whatsapp_provider = WhatsAppProvider()

    def deliver(self, notification):
        if notification.channel == "email":
            subject = notification.payload.get("subject", "Acoli notification")
            body = notification.payload.get("body", "")
            send_mail(subject, body, settings.DEFAULT_FROM_EMAIL, [notification.user.email])
            return True
        if notification.channel == "whatsapp":
            return self.whatsapp_provider.send(notification)
        return False

    def send_pending(self):
        pending = Notification.objects.filter(status="pending")
        for notification in pending:
            try:
                delivered = self.deliver(notification)
                notification.status = "sent" if delivered else "failed"
                notification.sent_at = timezone.now()
            except Exception as exc:
                notification.status = "failed"
                notification.error_message = str(exc)
            notification.save(update_fields=["status", "sent_at", "error_message"])


def enqueue_notification(parish, user, template_code, payload, idempotency_key, channel="email"):
    if channel == "email":
        pref = NotificationPreference.objects.filter(parish=parish, user=user).first()
        if pref and not pref.email_enabled:
            return None
    notification, _ = Notification.objects.get_or_create(
        channel=channel,
        idempotency_key=idempotency_key,
        defaults={
            "parish": parish,
            "user": user,
            "template_code": template_code,
            "payload": payload,
            "status": "pending",
        },
    )
    return notification

