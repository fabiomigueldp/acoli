from datetime import timedelta

from django.utils import timezone

from core.models import MassInstance


def build_horizon_instances(parish, horizon_days):
    start = timezone.now()
    end = start + timedelta(days=horizon_days)
    qs = MassInstance.objects.filter(parish=parish, starts_at__range=(start, end), status="scheduled")
    return qs.select_related("community", "requirement_profile", "template")

