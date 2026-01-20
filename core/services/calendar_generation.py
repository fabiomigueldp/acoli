from datetime import datetime, timedelta

from dateutil import rrule
from django.utils import timezone

from core.models import MassInstance, MassTemplate
from core.services.audit import log_audit
from core.services.slots import sync_slots_for_instance


def generate_instances_for_parish(parish, start_date, end_date, actor=None):
    templates = MassTemplate.objects.filter(parish=parish, active=True)
    created = []
    for template in templates:
        if template.rrule_text:
            rule = rrule.rrulestr(template.rrule_text, dtstart=datetime.combine(start_date, template.time))
            occurrences = rule.between(datetime.combine(start_date, template.time), datetime.combine(end_date, template.time), inc=True)
        else:
            occurrences = []
            current = start_date
            while current <= end_date:
                if current.weekday() == template.weekday:
                    occurrences.append(datetime.combine(current, template.time))
                current += timedelta(days=1)
        for occ in occurrences:
            starts_at = timezone.make_aware(occ)
            existing = MassInstance.objects.filter(
                parish=parish,
                community=template.community,
                starts_at=starts_at,
                status="scheduled",
            ).first()
            if existing:
                continue
            instance = MassInstance.objects.create(
                parish=parish,
                community=template.community,
                starts_at=starts_at,
                template=template,
                requirement_profile=template.default_requirement_profile,
                status="scheduled",
                created_by=actor,
                updated_by=actor,
            )
            created.append(instance)
            log_audit(parish, actor, "MassInstance", instance.id, "create", {"template_id": template.id})
            sync_slots_for_instance(instance)
    return created

