from django.db import IntegrityError, connection, transaction
from django.utils import timezone

from core.models import Assignment, AssignmentSlot, Confirmation
from core.services.audit import log_audit


class ConcurrentUpdateError(RuntimeError):
    pass


def _ensure_same_parish(slot, acolyte):
    if slot.parish_id != acolyte.parish_id:
        raise ValueError("Paroquia invalida para atribuicao.")


def _lock_slot(slot_id):
    qs = AssignmentSlot.objects
    if connection.features.has_select_for_update:
        qs = qs.select_for_update()
    return qs.select_related("mass_instance").get(id=slot_id)


def _assign_acolyte_to_slot_locked(
    slot,
    acolyte,
    actor=None,
    assignment_state="proposed",
    end_reason="replaced",
    create_confirmation=False,
):
    _ensure_same_parish(slot, acolyte)
    current = slot.get_active_assignment()
    if current and current.acolyte_id == acolyte.id:
        return current
    if current:
        deactivate_assignment(current, end_reason, actor=actor)
    try:
        assignment = create_assignment(slot, acolyte, actor=actor, assignment_state=assignment_state)
    except IntegrityError:
        existing = Assignment.objects.filter(slot=slot, is_active=True).first()
        if existing and existing.acolyte_id == acolyte.id:
            return existing
        raise ConcurrentUpdateError("Slot atualizado por outra acao.")
    if create_confirmation:
        Confirmation.objects.get_or_create(parish=slot.parish, assignment=assignment)
    return assignment


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
    if reason != "claim_transfer":
        from core.services.claims import expire_claims_for_assignment

        expire_claims_for_assignment(assignment, "assignment_changed", actor=actor)
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
    with transaction.atomic():
        locked_slot = _lock_slot(slot.id)
        return _assign_acolyte_to_slot_locked(
            locked_slot,
            acolyte,
            actor=actor,
            assignment_state=assignment_state,
            end_reason=end_reason,
            create_confirmation=create_confirmation,
        )


def _validate_no_conflict_in_mass(slot, acolyte):
    from core.models import Assignment
    existing_assignments = Assignment.objects.filter(
        slot__mass_instance=slot.mass_instance,
        acolyte=acolyte,
        is_active=True
    ).exclude(slot=slot).select_related("slot__position_type")
    if existing_assignments.exists():
        current_slot = existing_assignments.first().slot
        raise ValueError(
            f"O acólito {acolyte.display_name} já está atribuído à posição {current_slot.position_type.name} nesta missa.",
            "conflict",
            current_slot
        )


def move_acolyte_to_slot(current_slot, new_slot, acolyte, actor=None):
    with transaction.atomic():
        # Valida se o acólito está no current_slot
        current_assignment = current_slot.assignments.filter(acolyte=acolyte, is_active=True).first()
        if not current_assignment:
            raise ValueError("Acólito não está atribuído ao slot atual.")

        # Desativa a atribuição anterior
        deactivate_assignment(current_assignment, "moved_to_another_slot", actor=actor)

        # Atribui ao novo slot
        return assign_manual(new_slot, acolyte, actor=actor)


def assign_manual(slot, acolyte, actor=None):
    _validate_no_conflict_in_mass(slot, acolyte)
    with transaction.atomic():
        locked_slot = _lock_slot(slot.id)
        assignment_state = "locked" if locked_slot.is_locked else "published"
        assignment = _assign_acolyte_to_slot_locked(
            locked_slot,
            acolyte,
            actor=actor,
            assignment_state=assignment_state,
            end_reason="manual_unassign",
            create_confirmation=True,
        )
        locked_slot.status = "finalized" if locked_slot.is_locked else "assigned"
        locked_slot.save(update_fields=["status", "updated_at"])
        log_audit(
            locked_slot.parish,
            actor,
            "Assignment",
            assignment.id,
            "manual_assign",
            {"slot_id": locked_slot.id, "acolyte_id": acolyte.id},
        )
        return assignment
