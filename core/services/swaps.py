from core.models import AcolyteQualification, Confirmation
from core.services.audit import log_audit
from core.services.availability import is_acolyte_available


def _is_qualified(parish, acolyte, position_type):
    return AcolyteQualification.objects.filter(
        parish=parish, acolyte=acolyte, position_type=position_type, qualified=True
    ).exists()


def apply_swap_request(swap, actor=None):
    parish = swap.parish
    if swap.swap_type == "acolyte_swap":
        if not swap.target_acolyte or not swap.from_slot:
            return False
        assignment = swap.from_slot.assignment
        target = swap.target_acolyte
        if not _is_qualified(parish, target, swap.from_slot.position_type):
            return False
        if not is_acolyte_available(target, swap.mass_instance):
            return False
        assignment.acolyte = target
        assignment.save(update_fields=["acolyte", "updated_at"])
        confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
        confirmation.status = "pending"
        confirmation.updated_by = actor
        confirmation.save(update_fields=["status", "updated_by", "timestamp"])
        log_audit(parish, actor, "SwapRequest", swap.id, "apply", {"assignment_id": assignment.id})
        return True

    if swap.swap_type == "role_swap":
        if not swap.from_slot or not swap.to_slot:
            return False
        from_assignment = swap.from_slot.assignment
        to_assignment = swap.to_slot.assignment
        if not from_assignment or not to_assignment:
            return False
        if not _is_qualified(parish, from_assignment.acolyte, swap.to_slot.position_type):
            return False
        if not _is_qualified(parish, to_assignment.acolyte, swap.from_slot.position_type):
            return False
        if not is_acolyte_available(from_assignment.acolyte, swap.mass_instance):
            return False
        if not is_acolyte_available(to_assignment.acolyte, swap.mass_instance):
            return False
        from_assignment.acolyte, to_assignment.acolyte = to_assignment.acolyte, from_assignment.acolyte
        from_assignment.save(update_fields=["acolyte", "updated_at"])
        to_assignment.save(update_fields=["acolyte", "updated_at"])
        for assignment in (from_assignment, to_assignment):
            confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
            confirmation.status = "pending"
            confirmation.updated_by = actor
            confirmation.save(update_fields=["status", "updated_by", "timestamp"])
        log_audit(parish, actor, "SwapRequest", swap.id, "apply", {"from_slot": swap.from_slot_id, "to_slot": swap.to_slot_id})
        return True
    return False

