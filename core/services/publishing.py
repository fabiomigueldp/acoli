from django.utils import timezone

from core.models import Assignment, Confirmation
from core.services.audit import log_audit
from notifications.services import enqueue_notification


ASSIGNMENT_PUBLISHED = "ASSIGNMENT_PUBLISHED"
CONFIRMATION_REQUESTED = "CONFIRMATION_REQUESTED"


def publish_assignments(parish, start_date, end_date, actor=None):
    assignments = (
        Assignment.objects.filter(
            parish=parish,
            slot__mass_instance__starts_at__date__gte=start_date,
            slot__mass_instance__starts_at__date__lte=end_date,
        )
        .select_related("slot__mass_instance", "acolyte__user")
        .order_by("slot__mass_instance__starts_at")
    )

    published = 0
    for assignment in assignments:
        if assignment.assignment_state == "proposed":
            assignment.assignment_state = "published"
            assignment.published_at = timezone.now()
            assignment.save(update_fields=["assignment_state", "published_at", "updated_at"])
            published += 1
        slot = assignment.slot
        if slot.status == "open":
            slot.status = "assigned"
            slot.save(update_fields=["status", "updated_at"])
        confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
        if confirmation.status == "pending":
            confirmation.updated_by = actor
            confirmation.save(update_fields=["updated_by", "timestamp"])
        log_audit(parish, actor, "Assignment", assignment.id, "publish", {"assignment_id": assignment.id})
        if assignment.acolyte.user:
            payload = {
                "subject": "Escala publicada",
                "body": f"Voce recebeu uma escala em {assignment.slot.mass_instance.starts_at:%d/%m %H:%M}.",
            }
            enqueue_notification(
                parish,
                assignment.acolyte.user,
                ASSIGNMENT_PUBLISHED,
                payload,
                idempotency_key=f"publish:{assignment.id}",
            )
            enqueue_notification(
                parish,
                assignment.acolyte.user,
                CONFIRMATION_REQUESTED,
                payload,
                idempotency_key=f"confirm:{assignment.id}",
            )
    return published

