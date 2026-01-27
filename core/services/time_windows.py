from django.utils import timezone


def filter_upcoming(queryset, now=None, field="slot__mass_instance__starts_at"):
    now = now or timezone.now()
    return queryset.filter(**{f"{field}__gte": now})


def filter_past(queryset, now=None, field="slot__mass_instance__starts_at"):
    now = now or timezone.now()
    return queryset.filter(**{f"{field}__lt": now})
