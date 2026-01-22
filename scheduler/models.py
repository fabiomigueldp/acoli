from django.conf import settings
from django.db import models

from core.models import Parish


class ScheduleJobRequest(models.Model):
    STATUS_CHOICES = [
        ("pending", "Pendente"),
        ("running", "Executando"),
        ("success", "Concluido"),
        ("failed", "Falhou"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    requested_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending")
    horizon_days = models.PositiveIntegerField(default=60)
    force_republish = models.BooleanField(default=False)
    summary_json = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    error_message = models.TextField(blank=True)

