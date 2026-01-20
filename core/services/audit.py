from core.models import AuditEvent


def log_audit(parish, actor, entity_type, entity_id, action_type, diff=None):
    AuditEvent.objects.create(
        parish=parish,
        actor_user=actor,
        entity_type=entity_type,
        entity_id=str(entity_id),
        action_type=action_type,
        diff_json=diff or {},
    )

