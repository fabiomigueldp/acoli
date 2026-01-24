from collections import defaultdict
from datetime import timedelta

from django.db import transaction
from django.utils import timezone

from core.models import AcolyteQualification, PositionClaimRequest
from core.services.assignments import _lock_slot, _assign_acolyte_to_slot_locked
from core.services.audit import log_audit
from core.services.availability import is_acolyte_available
from core.services.assignments import _validate_no_conflict_in_mass
from core.services.permissions import ADMIN_ROLE_CODES, users_with_roles
from notifications.services import enqueue_notification


PENDING_STATUSES = {"pending_target", "scheduled_auto_approve", "pending_coordination"}


def _is_qualified(parish, acolyte, position_type):
    return AcolyteQualification.objects.filter(
        parish=parish, acolyte=acolyte, position_type=position_type, qualified=True
    ).exists()


def _get_auto_approve_at(parish, slot):
    if not parish.claim_auto_approve_enabled:
        return None
    hours = int(parish.claim_auto_approve_hours or 0)
    if hours <= 0:
        return None
    return slot.mass_instance.starts_at - timedelta(hours=hours)


def create_position_claim(parish, slot, requestor, actor=None):
    if slot.parish_id != parish.id:
        return None, "Paroquia invalida."
    if slot.mass_instance.status != "scheduled":
        return None, "Missa cancelada."
    assignment = slot.get_active_assignment()
    if not assignment:
        return None, "Nao ha acolito atribuido para solicitar."
    if assignment.acolyte_id == requestor.id:
        return None, "Voce ja esta nesta posicao."
    if not _is_qualified(parish, requestor, slot.position_type):
        return None, "Voce nao esta qualificado para esta funcao."
    if not is_acolyte_available(requestor, slot.mass_instance):
        return None, "Voce nao esta disponivel para esta missa."
    try:
        _validate_no_conflict_in_mass(slot, requestor)
    except ValueError:
        return None, "Voce ja esta escalado nesta missa."

    existing = PositionClaimRequest.objects.filter(
        parish=parish,
        slot=slot,
        requestor_acolyte=requestor,
        status__in=PENDING_STATUSES,
    ).exists()
    if existing:
        return None, "Solicitacao ja registrada."

    auto_approve_at = None
    status = "pending_target"
    confirmation = getattr(assignment, "confirmation", None)
    if confirmation is None or confirmation.status != "confirmed":
        auto_approve_at = _get_auto_approve_at(parish, slot)
        if auto_approve_at:
            status = "scheduled_auto_approve"
            if auto_approve_at <= timezone.now():
                auto_approve_at = timezone.now()

    claim = PositionClaimRequest.objects.create(
        parish=parish,
        slot=slot,
        requestor_acolyte=requestor,
        target_assignment=assignment,
        status=status,
        auto_approve_at=auto_approve_at,
    )
    log_audit(parish, actor, "PositionClaimRequest", claim.id, "create", {
        "slot_id": slot.id,
        "requestor_id": requestor.id,
    })
    if assignment.acolyte.user:
        enqueue_notification(
            parish,
            assignment.acolyte.user,
            "POSITION_CLAIM_REQUESTED",
            {"claim_id": claim.id},
            idempotency_key=f"claim:{claim.id}:requested",
        )
    return claim, None


def reject_claim(claim, actor=None, reason="holder_rejected", approval_mode="target"):
    if claim.status not in PENDING_STATUSES:
        return False
    claim.status = "rejected"
    claim.resolution_reason = reason
    claim.resolved_at = timezone.now()
    claim.approval_mode = approval_mode
    claim.approved_by = actor
    claim.save(update_fields=[
        "status",
        "resolution_reason",
        "resolved_at",
        "approval_mode",
        "approved_by",
        "updated_at",
    ])
    log_audit(claim.parish, actor, "PositionClaimRequest", claim.id, "reject", {"reason": reason})
    if claim.requestor_acolyte.user:
        enqueue_notification(
            claim.parish,
            claim.requestor_acolyte.user,
            "POSITION_CLAIM_REJECTED",
            {"claim_id": claim.id},
            idempotency_key=f"claim:{claim.id}:rejected",
        )
    return True


def expire_claims_for_assignment(assignment, reason, actor=None, exclude_claim_id=None):
    if not assignment:
        return 0
    claims = PositionClaimRequest.objects.filter(
        parish=assignment.parish,
        slot=assignment.slot,
        status__in=PENDING_STATUSES,
    )
    if exclude_claim_id:
        claims = claims.exclude(id=exclude_claim_id)
    now = timezone.now()
    updated = claims.update(
        status="expired",
        resolution_reason=reason,
        resolved_at=now,
        updated_at=now,
    )
    if updated:
        log_audit(
            assignment.parish,
            actor,
            "PositionClaimRequest",
            assignment.slot_id,
            "expire",
            {"reason": reason, "count": updated},
        )
    return updated


def _resolve_other_claims(claim, actor=None, status="rejected", reason="holder_selected_other"):
    claims = PositionClaimRequest.objects.filter(
        parish=claim.parish,
        slot=claim.slot,
        status__in=PENDING_STATUSES,
    ).exclude(id=claim.id)
    now = timezone.now()
    count = claims.update(
        status=status,
        resolution_reason=reason,
        resolved_at=now,
        updated_at=now,
    )
    if count:
        log_audit(
            claim.parish,
            actor,
            "PositionClaimRequest",
            claim.slot_id,
            "resolve_others",
            {"count": count, "status": status, "reason": reason},
        )
    return count


def choose_claim(claim, actor=None, require_coordination=False):
    if claim.status not in PENDING_STATUSES:
        return False
    if require_coordination:
        claim.status = "pending_coordination"
        claim.approval_mode = "coordination"
        claim.auto_approve_at = None
        claim.save(update_fields=["status", "approval_mode", "auto_approve_at", "updated_at"])
        _resolve_other_claims(claim, actor=actor)
        log_audit(claim.parish, actor, "PositionClaimRequest", claim.id, "select", {"mode": "coordination"})
        for user in users_with_roles(claim.parish, ADMIN_ROLE_CODES):
            enqueue_notification(
                claim.parish,
                user,
                "POSITION_CLAIM_COORDINATION_REQUIRED",
                {"claim_id": claim.id},
                idempotency_key=f"claim:{claim.id}:coordination:{user.id}",
            )
        return True
    return approve_claim(
        claim,
        actor=actor,
        approval_mode="target",
        resolution_reason="holder_approved",
        other_status="rejected",
        other_reason="holder_selected_other",
    )


def approve_claim(
    claim,
    actor=None,
    approval_mode="target",
    resolution_reason="coordination_approved",
    other_status=None,
    other_reason=None,
):
    if claim.status not in PENDING_STATUSES:
        return False
    parish = claim.parish
    requestor = claim.requestor_acolyte
    with transaction.atomic():
        locked_slot = _lock_slot(claim.slot_id)
        assignment = locked_slot.get_active_assignment()
        if not assignment:
            expire_claims_for_assignment(claim.target_assignment, "assignment_changed", actor=actor)
            return False
        if claim.target_assignment_id and assignment.id != claim.target_assignment_id:
            expire_claims_for_assignment(assignment, "assignment_changed", actor=actor)
            return False
        if not _is_qualified(parish, requestor, locked_slot.position_type):
            reject_claim(claim, actor=actor, reason="holder_rejected")
            return False
        if not is_acolyte_available(requestor, locked_slot.mass_instance):
            reject_claim(claim, actor=actor, reason="holder_rejected")
            return False
        try:
            _validate_no_conflict_in_mass(locked_slot, requestor)
        except ValueError:
            reject_claim(claim, actor=actor, reason="holder_rejected")
            return False

        new_assignment = _assign_acolyte_to_slot_locked(
            locked_slot,
            requestor,
            actor=actor,
            assignment_state=assignment.assignment_state,
            end_reason="claim_transfer",
            create_confirmation=True,
        )

        claim.status = "approved"
        claim.approval_mode = approval_mode
        claim.resolution_reason = resolution_reason
        claim.resolved_at = timezone.now()
        claim.approved_by = actor
        claim.auto_approve_at = None
        claim.target_assignment = assignment
        claim.save(update_fields=[
            "status",
            "approval_mode",
            "resolution_reason",
            "resolved_at",
            "approved_by",
            "auto_approve_at",
            "target_assignment",
            "updated_at",
        ])
        if other_status:
            _resolve_other_claims(claim, actor=actor, status=other_status, reason=other_reason or "holder_selected_other")
        log_audit(parish, actor, "PositionClaimRequest", claim.id, "approve", {"assignment_id": new_assignment.id})
        if claim.requestor_acolyte.user:
            enqueue_notification(
                parish,
                claim.requestor_acolyte.user,
                "POSITION_CLAIM_APPROVED",
                {"claim_id": claim.id},
                idempotency_key=f"claim:{claim.id}:approved",
            )
    return True


def cancel_claim(claim, actor=None):
    if claim.status not in PENDING_STATUSES:
        return False
    claim.status = "expired"
    claim.resolution_reason = "requestor_canceled"
    claim.resolved_at = timezone.now()
    claim.save(update_fields=["status", "resolution_reason", "resolved_at", "updated_at"])
    log_audit(claim.parish, actor, "PositionClaimRequest", claim.id, "cancel", {})
    return True


def process_due_claims(parish=None):
    now = timezone.now()
    claims = PositionClaimRequest.objects.filter(
        status="scheduled_auto_approve",
        auto_approve_at__lte=now,
    ).select_related("slot__mass_instance", "target_assignment", "requestor_acolyte")
    if parish:
        claims = claims.filter(parish=parish)

    claims_by_slot = defaultdict(list)
    for claim in claims.order_by("created_at"):
        claims_by_slot[claim.slot_id].append(claim)

    for slot_id, slot_claims in claims_by_slot.items():
        with transaction.atomic():
            locked_slot = _lock_slot(slot_id)
            assignment = locked_slot.get_active_assignment()
            if not assignment:
                for claim in slot_claims:
                    if claim.status in PENDING_STATUSES:
                        claim.status = "expired"
                        claim.resolution_reason = "assignment_changed"
                        claim.resolved_at = now
                        claim.save(update_fields=["status", "resolution_reason", "resolved_at", "updated_at"])
                continue
            confirmation = getattr(assignment, "confirmation", None)
            if confirmation and confirmation.status == "confirmed":
                expire_claims_for_assignment(assignment, "holder_confirmed")
                continue

            selected = slot_claims[0]
            approved = approve_claim(
                selected,
                actor=None,
                approval_mode="auto",
                resolution_reason="auto_approved",
                other_status="expired",
                other_reason="auto_expired",
            )
            if not approved:
                continue
