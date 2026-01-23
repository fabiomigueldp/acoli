from datetime import datetime

from django.utils import timezone

from django.db import transaction

from core.models import AuditEvent, EventInterest, EventOccurrence, MassInstance, MassOverride
from core.services.audit import log_audit
from core.services.slots import sync_slots_for_instance


def _build_datetime(date_value, time_value):
    return timezone.make_aware(datetime.combine(date_value, time_value))


def apply_event_occurrences(event_series, occurrences, actor=None):
    created = []
    for occ in occurrences:
        if isinstance(occ, EventOccurrence):
            date_value = occ.date
            time_value = occ.time
            community_id = occ.community_id
            profile_id = occ.requirement_profile_id
            label = occ.label
            conflict_action = occ.conflict_action
            move_to_date = occ.move_to_date
            move_to_time = occ.move_to_time
            move_to_community_id = occ.move_to_community_id
        else:
            date_value = occ["date"]
            time_value = occ["time"]
            community_id = occ["community_id"]
            profile_id = occ.get("requirement_profile_id")
            label = occ.get("label", "")
            conflict_action = occ.get("conflict_action", "keep")
            move_to_date = occ.get("move_to_date")
            move_to_time = occ.get("move_to_time")
            move_to_community_id = occ.get("move_to_community_id")

        if not label:
            label = event_series.title
        starts_at = _build_datetime(date_value, time_value)
        if conflict_action == "skip":
            continue
        existing = MassInstance.objects.filter(
            parish=event_series.parish,
            community_id=community_id,
            starts_at=starts_at,
            status="scheduled",
        ).first()

        if existing and conflict_action == "keep":
            updated_fields = []
            if existing.event_series_id != event_series.id:
                existing.event_series = event_series
                updated_fields.append("event_series")
            if label and existing.liturgy_label != label:
                existing.liturgy_label = label
                updated_fields.append("liturgy_label")
            if profile_id and existing.requirement_profile_id != profile_id:
                existing.requirement_profile_id = profile_id
                updated_fields.append("requirement_profile")
            if updated_fields:
                existing.save(update_fields=updated_fields + ["updated_at"])
                log_audit(event_series.parish, actor, "MassInstance", existing.id, "update", {"event_series_id": event_series.id})
                sync_slots_for_instance(existing)
            continue

        if existing and conflict_action == "cancel_existing":
            existing.status = "canceled"
            existing.save(update_fields=["status", "updated_at"])
            MassOverride.objects.create(
                parish=event_series.parish,
                instance=existing,
                override_type="cancel_instance",
                payload={"reason": "event_series", "event_series_id": event_series.id},
                created_by=actor,
            )
            log_audit(event_series.parish, actor, "MassInstance", existing.id, "cancel", {"event_series_id": event_series.id})

        if existing and conflict_action == "move_existing":
            if move_to_date and move_to_time and move_to_community_id:
                move_to = _build_datetime(move_to_date, move_to_time)
                conflict = MassInstance.objects.filter(
                    parish=event_series.parish,
                    community_id=move_to_community_id,
                    starts_at=move_to,
                    status="scheduled",
                ).exclude(id=existing.id).exists()
                if conflict:
                    raise ValueError("Conflito ao mover missa existente para o novo horario/comunidade.")
                payload = {
                    "from": {"starts_at": existing.starts_at.isoformat(), "community_id": existing.community_id},
                    "to": {"starts_at": move_to.isoformat(), "community_id": move_to_community_id},
                }
                existing.starts_at = move_to
                existing.community_id = move_to_community_id
                existing.save(update_fields=["starts_at", "community", "updated_at"])
                MassOverride.objects.create(
                    parish=event_series.parish,
                    instance=existing,
                    override_type="move_instance",
                    payload=payload,
                    created_by=actor,
                )
                log_audit(event_series.parish, actor, "MassInstance", existing.id, "move", payload)
            else:
                continue

        instance = MassInstance.objects.create(
            parish=event_series.parish,
            event_series=event_series,
            community_id=community_id,
            starts_at=starts_at,
            liturgy_label=label,
            requirement_profile_id=profile_id,
            status="scheduled",
            created_by=actor,
            updated_by=actor,
        )
        created.append(instance)
        log_audit(event_series.parish, actor, "MassInstance", instance.id, "create", {"event_series_id": event_series.id})
        sync_slots_for_instance(instance)
    return created


def generate_instances_for_event_series(event_series, actor=None):
    rules = event_series.ruleset_json or {}
    days = rules.get("days", [])
    occurrences = []
    for day in days:
        date_str = day.get("date")
        time_str = day.get("time")
        community_id = day.get("community_id") or (event_series.default_community_id)
        profile_id = day.get("requirement_profile_id")
        label = day.get("label", "")
        conflict_action = day.get("conflict_action", "keep")
        move_to_date = day.get("move_to_date")
        move_to_time = day.get("move_to_time")
        move_to_community_id = day.get("move_to_community_id")
        if not (date_str and time_str and community_id):
            continue
        occurrences.append(
            {
                "date": datetime.fromisoformat(date_str).date(),
                "time": datetime.fromisoformat(f"{date_str}T{time_str}").time(),
                "community_id": community_id,
                "requirement_profile_id": profile_id,
                "label": label,
                "conflict_action": conflict_action,
                "move_to_date": datetime.fromisoformat(move_to_date).date() if move_to_date else None,
                "move_to_time": datetime.fromisoformat(f"{move_to_date}T{move_to_time}").time() if move_to_date and move_to_time else None,
                "move_to_community_id": move_to_community_id,
            }
        )
    return apply_event_occurrences(event_series, occurrences, actor=actor)


def delete_event_series_with_masses(parish, series, actor=None):
    if series.parish_id != parish.id:
        raise ValueError("Serie nao pertence a esta paroquia.")
    with transaction.atomic():
        mass_ids = list(
            MassInstance.objects.filter(parish=parish, event_series=series).values_list("id", flat=True)
        )
        slot_ids = []
        assignment_ids = []
        replacement_ids = []
        swap_ids = []
        if mass_ids:
            from core.models import Assignment, AssignmentSlot, ReplacementRequest, SwapRequest

            slot_ids = list(
                AssignmentSlot.objects.filter(parish=parish, mass_instance_id__in=mass_ids).values_list("id", flat=True)
            )
            assignment_ids = list(
                Assignment.objects.filter(parish=parish, slot_id__in=slot_ids).values_list("id", flat=True)
            )
            replacement_ids = list(
                ReplacementRequest.objects.filter(parish=parish, slot_id__in=slot_ids).values_list("id", flat=True)
            )
            swap_ids = list(
                SwapRequest.objects.filter(parish=parish, mass_instance_id__in=mass_ids).values_list("id", flat=True)
            )
            AuditEvent.objects.filter(
                parish=parish,
                entity_type="MassInstance",
                entity_id__in=[str(mass_id) for mass_id in mass_ids],
            ).delete()
            if assignment_ids:
                AuditEvent.objects.filter(
                    parish=parish,
                    entity_type="Assignment",
                    entity_id__in=[str(assignment_id) for assignment_id in assignment_ids],
                ).delete()
            if replacement_ids:
                AuditEvent.objects.filter(
                    parish=parish,
                    entity_type="ReplacementRequest",
                    entity_id__in=[str(req_id) for req_id in replacement_ids],
                ).delete()
            if swap_ids:
                AuditEvent.objects.filter(
                    parish=parish,
                    entity_type="SwapRequest",
                    entity_id__in=[str(swap_id) for swap_id in swap_ids],
                ).delete()
        EventInterest.objects.filter(parish=parish, event_series=series).delete()
        EventOccurrence.objects.filter(parish=parish, event_series=series).delete()
        MassInstance.objects.filter(parish=parish, id__in=mass_ids).delete()
        AuditEvent.objects.filter(parish=parish, entity_type="EventSeries", entity_id=str(series.id)).delete()
        series.delete()
