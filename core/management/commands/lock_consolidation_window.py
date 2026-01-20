from datetime import timedelta

from django.core.management.base import BaseCommand
from django.utils import timezone

from core.models import AssignmentSlot, Parish
from core.services.audit import log_audit


class Command(BaseCommand):
    help = "Lock assignments inside the consolidation window."

    def add_arguments(self, parser):
        parser.add_argument("--parish-id", type=int)

    def handle(self, *args, **options):
        parish_id = options.get("parish_id")
        parishes = Parish.objects.all()
        if parish_id:
            parishes = parishes.filter(id=parish_id)

        for parish in parishes:
            end = timezone.now() + timedelta(days=parish.consolidation_days)
            slots = AssignmentSlot.objects.filter(
                parish=parish,
                mass_instance__starts_at__lte=end,
                mass_instance__starts_at__gte=timezone.now(),
            ).select_related("assignment")
            locked = 0
            for slot in slots:
                if not slot.is_locked:
                    slot.is_locked = True
                    slot.locked_at = timezone.now()
                    slot.save(update_fields=["is_locked", "locked_at", "updated_at"])
                if hasattr(slot, "assignment") and slot.assignment:
                    if slot.assignment.assignment_state != "locked":
                        slot.assignment.assignment_state = "locked"
                        slot.assignment.save(update_fields=["assignment_state", "updated_at"])
                    if slot.status != "finalized":
                        slot.status = "finalized"
                        slot.save(update_fields=["status", "updated_at"])
                locked += 1
            log_audit(parish, None, "Consolidation", parish.id, "lock", {"locked_slots": locked})
            self.stdout.write(self.style.SUCCESS(f"Parish {parish.id}: locked {locked} slots"))

