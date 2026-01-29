from datetime import timedelta

from django.db import models
from django.db.models import F, Q
from django.utils import timezone

from core.models import AcolyteCreditLedger, AcolyteProfile, AcolyteStats, Assignment, Confirmation


def recompute_stats(parish):
    now = timezone.now()
    start_30 = now - timedelta(days=30)
    start_90 = now - timedelta(days=90)
    assignments = Assignment.objects.filter(parish=parish, assignment_state__in=["published", "locked"])
    active_at_service = assignments.filter(
        created_at__lte=F("slot__mass_instance__starts_at")
    ).filter(
        Q(ended_at__isnull=True) | Q(ended_at__gte=F("slot__mass_instance__starts_at"))
    )
    acolyte_ids = list(
        AcolyteProfile.objects.filter(parish=parish, active=True).values_list("id", flat=True)
    )
    for acolyte_id in acolyte_ids:
        recent_30 = active_at_service.filter(
            acolyte_id=acolyte_id, slot__mass_instance__starts_at__gte=start_30, slot__mass_instance__starts_at__lte=now
        ).count()
        recent_90 = active_at_service.filter(
            acolyte_id=acolyte_id, slot__mass_instance__starts_at__gte=start_90, slot__mass_instance__starts_at__lte=now
        ).count()
        confirmations = Confirmation.objects.filter(
            parish=parish,
            assignment__acolyte_id=acolyte_id,
            assignment__assignment_state__in=["published", "locked"],
            assignment__slot__mass_instance__starts_at__gte=start_90,
            assignment__slot__mass_instance__starts_at__lte=now,
        )
        total_confirmations = confirmations.count()
        confirmed = confirmations.filter(status="confirmed").count()
        canceled = confirmations.filter(status__in=["declined", "canceled_by_acolyte"]).count()
        no_show = confirmations.filter(status="no_show").count()
        confirmation_rate = (confirmed / total_confirmations) if total_confirmations else 0.0
        cancellations_rate = (canceled / total_confirmations) if total_confirmations else 0.0
        credit_balance = (
            AcolyteCreditLedger.objects.filter(parish=parish, acolyte_id=acolyte_id)
            .aggregate(total=models.Sum("delta"))
            .get("total")
            or 0
        )
        reliability_score = max(0.0, 100.0 - (cancellations_rate * 50.0) - (no_show * 5.0))
        AcolyteStats.objects.update_or_create(
            parish=parish,
            acolyte_id=acolyte_id,
            defaults={
                "services_last_30_days": recent_30,
                "services_last_90_days": recent_90,
                "confirmation_rate": confirmation_rate,
                "cancellations_rate": cancellations_rate,
                "no_show_count": no_show,
                "credit_balance": credit_balance,
                "reliability_score": reliability_score,
                "last_served_at": active_at_service.filter(acolyte_id=acolyte_id)
                .order_by("-slot__mass_instance__starts_at")
                .values_list("slot__mass_instance__starts_at", flat=True)
                .first(),
            },
        )
    return True

