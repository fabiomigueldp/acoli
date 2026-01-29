from datetime import timedelta

from django.utils import timezone

from core.models import Assignment
from core.services.assignments import deactivate_assignment
from core.services.replacements import create_replacement_request, should_create_replacement


def deactivate_future_assignments_for_acolyte(acolyte, actor=None, now=None):
    if not acolyte:
        return 0
    now = now or timezone.now()
    parish = acolyte.parish
    assignments = (
        Assignment.objects.filter(
            parish=parish,
            acolyte=acolyte,
            is_active=True,
            slot__mass_instance__starts_at__gt=now,
            slot__mass_instance__status="scheduled",
        )
        .select_related("slot__mass_instance", "slot")
        .order_by("slot__mass_instance__starts_at")
    )
    count = assignments.count()
    for assignment in assignments:
        slot = assignment.slot
        deactivate_assignment(assignment, "manual_unassign", actor=actor)
        if slot.required and not slot.externally_covered:
            slot.status = "open"
            slot.save(update_fields=["status", "updated_at"])
            if should_create_replacement(parish, slot, now=now):
                create_replacement_request(parish, slot, actor=actor, notes="Acolito desativado")
    return count
