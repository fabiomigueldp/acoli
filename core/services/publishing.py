from django.db import connection, transaction
from django.utils import timezone

from core.models import Assignment, AssignmentSlot, Confirmation
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
            is_active=True,
        )
        .select_related("slot__mass_instance", "acolyte__user")
        .order_by("slot__mass_instance__starts_at")
    )

    published = 0
    for assignment in assignments:
        with transaction.atomic():
            if connection.features.has_select_for_update:
                assignment = (
                    Assignment.objects.select_for_update()
                    .select_related("slot__mass_instance", "acolyte__user")
                    .get(id=assignment.id)
                )
            updated = Assignment.objects.filter(id=assignment.id, assignment_state="proposed").update(
                assignment_state="published",
                published_at=timezone.now(),
                updated_at=timezone.now(),
            )
            if updated:
                published += 1
            slot = assignment.slot
            if connection.features.has_select_for_update:
                slot = AssignmentSlot.objects.select_for_update().get(id=slot.id)
            if slot.status == "open":
                slot.status = "assigned"
                slot.save(update_fields=["status", "updated_at"])
            confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
            if confirmation.status == "pending":
                confirmation.updated_by = actor
                confirmation.save(update_fields=["updated_by", "timestamp"])
            log_audit(parish, actor, "Assignment", assignment.id, "publish", {"assignment_id": assignment.id})
            if assignment.acolyte.user:
                enqueue_notification(
                    parish,
                    assignment.acolyte.user,
                    ASSIGNMENT_PUBLISHED,
                    {"assignment_id": assignment.id},
                    idempotency_key=f"publish:{assignment.id}",
                )
                enqueue_notification(
                    parish,
                    assignment.acolyte.user,
                    CONFIRMATION_REQUESTED,
                    {"assignment_id": assignment.id},
                    idempotency_key=f"confirm:{assignment.id}",
                )
    return published

