from datetime import timedelta

from django.utils import timezone

from core.models import MassInstance


def build_horizon_instances(parish, horizon_days):
    start = timezone.now()
    end = start + timedelta(days=horizon_days)
    qs = MassInstance.objects.filter(parish=parish, starts_at__range=(start, end), status="scheduled")
    weekend_dates = set()
    for instance in qs:
        if instance.starts_at.weekday() in (5, 6):
            base_date = instance.starts_at.date()
            if instance.starts_at.weekday() == 6:
                base_date = base_date - timedelta(days=1)
            weekend_dates.add(base_date)
            weekend_dates.add(base_date + timedelta(days=1))
    if weekend_dates:
        qs = MassInstance.objects.filter(parish=parish, starts_at__date__in=weekend_dates, status="scheduled")
    series_ids = qs.exclude(event_series=None).values_list("event_series_id", flat=True)
    if series_ids:
        qs = MassInstance.objects.filter(parish=parish, status="scheduled").filter(event_series_id__in=series_ids)
    return qs.select_related("community", "requirement_profile", "template")

