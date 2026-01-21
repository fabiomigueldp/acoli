from django.db import connection, transaction

from core.models import AcolyteQualification, AssignmentSlot, Confirmation
from core.services.assignments import _assign_acolyte_to_slot_locked, _lock_slot, deactivate_assignment
from core.services.audit import log_audit
from core.services.availability import is_acolyte_available


def _is_qualified(parish, acolyte, position_type):
    return AcolyteQualification.objects.filter(
        parish=parish, acolyte=acolyte, position_type=position_type, qualified=True
    ).exists()


def _lock_slots(parish, slot_ids):
    qs = AssignmentSlot.objects.filter(parish=parish, id__in=slot_ids)
    if connection.features.has_select_for_update:
        qs = qs.select_for_update()
    slots = qs.select_related("mass_instance", "position_type")
    return {slot.id: slot for slot in slots}


def apply_swap_request(swap, actor=None):
    parish = swap.parish
    if swap.swap_type == "acolyte_swap":
        if not swap.target_acolyte or not swap.from_slot:
            return False
        if swap.target_acolyte.parish_id != parish.id or swap.from_slot.parish_id != parish.id:
            return False
        with transaction.atomic():
            locked_slot = _lock_slot(swap.from_slot_id)
            assignment = locked_slot.get_active_assignment()
            if not assignment:
                return False
            target = swap.target_acolyte
            if not _is_qualified(parish, target, locked_slot.position_type):
                return False
            if not is_acolyte_available(target, swap.mass_instance):
                return False
            deactivate_assignment(assignment, "swap", actor=actor)
            assignment = _assign_acolyte_to_slot_locked(
                locked_slot,
                target,
                actor=actor,
                assignment_state=assignment.assignment_state,
                end_reason="swap",
            )
            confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
            confirmation.status = "pending"
            confirmation.updated_by = actor
            confirmation.save(update_fields=["status", "updated_by", "timestamp"])
            log_audit(parish, actor, "SwapRequest", swap.id, "apply", {"assignment_id": assignment.id})
            return True

    if swap.swap_type == "role_swap":
        if not swap.from_slot or not swap.to_slot:
            return False
        if swap.from_slot.parish_id != parish.id or swap.to_slot.parish_id != parish.id:
            return False
        with transaction.atomic():
            slot_ids = sorted({swap.from_slot_id, swap.to_slot_id})
            slots = _lock_slots(parish, slot_ids)
            from_slot = slots.get(swap.from_slot_id)
            to_slot = slots.get(swap.to_slot_id)
            if not from_slot or not to_slot:
                return False
            from_assignment = from_slot.get_active_assignment()
            to_assignment = to_slot.get_active_assignment()
            if not from_assignment or not to_assignment:
                return False
            if not _is_qualified(parish, from_assignment.acolyte, to_slot.position_type):
                return False
            if not _is_qualified(parish, to_assignment.acolyte, from_slot.position_type):
                return False
            if not is_acolyte_available(from_assignment.acolyte, swap.mass_instance):
                return False
            if not is_acolyte_available(to_assignment.acolyte, swap.mass_instance):
                return False
            deactivate_assignment(from_assignment, "swap", actor=actor)
            deactivate_assignment(to_assignment, "swap", actor=actor)
            new_from = _assign_acolyte_to_slot_locked(
                from_slot,
                to_assignment.acolyte,
                actor=actor,
                assignment_state=from_assignment.assignment_state,
                end_reason="swap",
            )
            new_to = _assign_acolyte_to_slot_locked(
                to_slot,
                from_assignment.acolyte,
                actor=actor,
                assignment_state=to_assignment.assignment_state,
                end_reason="swap",
            )
            for assignment in (new_from, new_to):
                confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
                confirmation.status = "pending"
                confirmation.updated_by = actor
                confirmation.save(update_fields=["status", "updated_by", "timestamp"])
            log_audit(
                parish,
                actor,
                "SwapRequest",
                swap.id,
                "apply",
                {"from_slot": swap.from_slot_id, "to_slot": swap.to_slot_id},
            )
            return True
    return False

