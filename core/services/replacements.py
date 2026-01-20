from core.models import ReplacementRequest
from core.services.audit import log_audit


def create_replacement_request(parish, slot, actor=None, proposed_acolyte=None):
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

