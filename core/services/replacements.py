from django.utils import timezone

from django.db.models import Prefetch

from core.models import (
    AcolyteQualification,
    Assignment,
    AssignmentSlot,
    Confirmation,
    MassOverride,
    ReplacementRequest,
)
from core.services.assignments import assign_acolyte_to_slot, deactivate_assignment
from core.services.audit import log_audit
from core.services.availability import is_acolyte_available


def create_replacement_request(parish, slot, actor=None, proposed_acolyte=None, notes=""):
    request = (
        ReplacementRequest.objects.filter(parish=parish, slot=slot, status="pending").first()
    )
    if request:
        return request
    request = ReplacementRequest.objects.create(
        parish=parish,
        slot=slot,
        requested_by=actor,
        proposed_acolyte=proposed_acolyte,
        status="pending",
        notes=notes or "",
    )
    log_audit(parish, actor, "ReplacementRequest", request.id, "create", {"slot_id": slot.id})
    return request


def mark_replacement_assigned(parish, slot, actor=None):
    request = ReplacementRequest.objects.filter(parish=parish, slot=slot, status="pending").first()
    if request:
        request.status = "assigned"
        request.save(update_fields=["status", "updated_at"])
        log_audit(parish, actor, "ReplacementRequest", request.id, "update", {"status": "assigned"})
        return request
    return None


def assign_replacement(parish, slot, acolyte, actor=None):
    if not AcolyteQualification.objects.filter(
        parish=parish, acolyte=acolyte, position_type=slot.position_type, qualified=True
    ).exists():
        return None
    if not is_acolyte_available(acolyte, slot.mass_instance):
        return None
    assignment = slot.get_active_assignment()
    if assignment:
        deactivate_assignment(assignment, "replaced", actor=actor)
    assignment_state = "locked" if slot.is_locked else "published"
    new_assignment = assign_acolyte_to_slot(
        slot,
        acolyte,
        actor=actor,
        assignment_state=assignment_state,
        end_reason="replaced",
        create_confirmation=True,
    )
    confirmation = Confirmation.objects.get(parish=parish, assignment=new_assignment)
    confirmation.status = "pending"
    confirmation.updated_by = actor
    confirmation.timestamp = timezone.now()
    confirmation.save(update_fields=["status", "updated_by", "timestamp"])
    request = mark_replacement_assigned(parish, slot, actor=actor)
    slot.status = "finalized" if slot.is_locked else "assigned"
    slot.save(update_fields=["status", "updated_at"])
    if request:
        log_audit(parish, actor, "ReplacementRequest", request.id, "assign", {"acolyte_id": acolyte.id})
    else:
        log_audit(parish, actor, "Assignment", new_assignment.id, "assign", {"acolyte_id": acolyte.id})
    return new_assignment


def cancel_mass_and_resolve_dependents(parish, instance, actor=None, notes="", reason_code="replacement_resolve"):
    if instance.status != "canceled":
        instance.status = "canceled"
        instance.save(update_fields=["status", "updated_at"])
        MassOverride.objects.create(
            parish=parish,
            instance=instance,
            override_type="cancel_instance",
            payload={"reason": reason_code, "notes": notes},
            created_by=actor,
        )
        log_audit(parish, actor, "MassInstance", instance.id, "cancel", {"reason": reason_code})

    slots = (
        AssignmentSlot.objects.filter(parish=parish, mass_instance=instance)
        .prefetch_related(
            Prefetch("assignments", queryset=Assignment.objects.filter(is_active=True), to_attr="active_assignments")
        )
        .select_related("mass_instance")
    )
    for slot in slots:
        assignment = slot.get_active_assignment()
        if assignment:
            deactivate_assignment(assignment, "canceled", actor=actor)
        slot.required = False
        slot.externally_covered = False
        slot.external_coverage_notes = ""
        slot.status = "finalized"
        slot.save(update_fields=["required", "externally_covered", "external_coverage_notes", "status", "updated_at"])

    now = timezone.now()
    replacements = ReplacementRequest.objects.filter(
        parish=parish, slot__mass_instance=instance, status__in=["pending", "assigned"]
    )
    for replacement in replacements:
        replacement.status = "resolved"
        replacement.resolved_reason = "mass_canceled"
        replacement.resolved_notes = notes
        replacement.resolved_at = now
        replacement.save(update_fields=["status", "resolved_reason", "resolved_notes", "resolved_at", "updated_at"])
        log_audit(parish, actor, "ReplacementRequest", replacement.id, "update", {"status": "resolved"})

