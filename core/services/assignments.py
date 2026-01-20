from django.utils import timezone

from core.models import Assignment, Confirmation
from core.services.audit import log_audit


def deactivate_assignment(assignment, reason, actor=None):
    if not assignment or not assignment.is_active:
        return assignment
    assignment.is_active = False
    assignment.ended_at = timezone.now()
    assignment.end_reason = reason
    assignment.save(update_fields=["is_active", "ended_at", "end_reason", "updated_at"])
    log_audit(
        assignment.parish,
        actor,
        "Assignment",
        assignment.id,
        "deactivate",
        {"reason": reason, "slot_id": assignment.slot_id},
    )
    return assignment


def create_assignment(slot, acolyte, actor=None, assignment_state="proposed"):
    assignment = Assignment.objects.create(
        parish=slot.parish,
        slot=slot,
        acolyte=acolyte,
        assigned_by=actor,
        assignment_state=assignment_state,
    )
    log_audit(
        slot.parish,
        actor,
        "Assignment",
        assignment.id,
        "create",
        {"acolyte_id": acolyte.id, "slot_id": slot.id},
    )
    return assignment


def assign_acolyte_to_slot(
    slot,
    acolyte,
    actor=None,
    assignment_state="proposed",
    end_reason="replaced",
    create_confirmation=False,
):
    current = slot.get_active_assignment()
    if current and current.acolyte_id == acolyte.id:
        return current
    if current:
        deactivate_assignment(current, end_reason, actor=actor)
    assignment = create_assignment(slot, acolyte, actor=actor, assignment_state=assignment_state)
    if create_confirmation:
        Confirmation.objects.get_or_create(parish=slot.parish, assignment=assignment)
    return assignment


def assign_manual(slot, acolyte, actor=None):
    assignment_state = "locked" if slot.is_locked else "published"
    assignment = assign_acolyte_to_slot(
        slot,
        acolyte,
        actor=actor,
        assignment_state=assignment_state,
        end_reason="manual_unassign",
        create_confirmation=True,
    )
    slot.status = "finalized" if slot.is_locked else "assigned"
    slot.save(update_fields=["status", "updated_at"])
    log_audit(
        slot.parish,
        actor,
        "Assignment",
        assignment.id,
        "manual_assign",
        {"slot_id": slot.id, "acolyte_id": acolyte.id},
    )
    return assignment
