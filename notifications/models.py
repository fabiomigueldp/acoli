from django.conf import settings
from django.db import models

from core.models import Parish


class NotificationPreference(models.Model):
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    email_enabled = models.BooleanField(default=True)  # type: ignore[arg-type]
    email_digest = models.BooleanField(default=False)  # type: ignore[arg-type]
    whatsapp_enabled = models.BooleanField(default=False)  # type: ignore[arg-type]

    class Meta:
        unique_together = ("parish", "user")


class Notification(models.Model):
    CHANNEL_CHOICES = [
        ("email", "Email"),
        ("whatsapp", "WhatsApp"),
    ]
    STATUS_CHOICES = [
        ("pending", "Pendente"),
        ("processing", "Processando"),
        ("sent", "Enviada"),
        ("failed", "Falhou"),
        ("skipped", "Ignorada"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    channel = models.CharField(max_length=20, choices=CHANNEL_CHOICES)
    template_code = models.CharField(max_length=100)
    payload = models.JSONField(default=dict, blank=True)
    idempotency_key = models.CharField(max_length=120)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending")
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    last_attempt_at = models.DateTimeField(null=True, blank=True)
    sent_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = ("channel", "idempotency_key")


class PushSubscription(models.Model):
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    endpoint = models.URLField(max_length=500)
    auth_key = models.CharField(max_length=255)
    p256dh_key = models.CharField(max_length=255)
    user_agent = models.CharField(max_length=255, blank=True)
    is_active = models.BooleanField(default=True)  # type: ignore[arg-type]
    last_seen_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("parish", "user", "endpoint")

