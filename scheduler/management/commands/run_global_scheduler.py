from django.core.management.base import BaseCommand
from django.utils import timezone

from scheduler.models import ScheduleJobRequest
from core.models import AssignmentSlot
from core.services.replacements import assign_replacement, assign_replacement_request
from notifications.services import enqueue_notification
from scheduler.services.jobs import claim_job
from scheduler.services.horizon import build_horizon_instances
from scheduler.services.solver import solve_open_slots, solve_schedule


class Command(BaseCommand):
    help = "Run pending schedule jobs."

    def add_arguments(self, parser):
        parser.add_argument("--parish-id", type=int)

    def handle(self, *args, **options):
        parish_id = options.get("parish_id")
        jobs = ScheduleJobRequest.objects.filter(status="pending").order_by("created_at")
        if parish_id:
            jobs = jobs.filter(parish_id=parish_id)
        for job in jobs:
            if not claim_job(job.id):
                continue
            job.refresh_from_db()
            try:
                parish = job.parish
                weights = parish.schedule_weights or {}
                if job.job_type == "replacement":
                    payload = job.payload_json or {}
                    slot_ids = payload.get("slot_ids") or []
                    if payload.get("slot_id"):
                        slot_ids.append(payload.get("slot_id"))
                    slots = AssignmentSlot.objects.filter(parish=parish, id__in=slot_ids)
                    result = solve_open_slots(parish, slots, weights)
                    assigned = 0
                    replacement_id = payload.get("replacement_request_id")
                    for slot_id, acolyte in result.assigned_map.items():
                        try:
                            if replacement_id:
                                assignment = assign_replacement_request(parish, replacement_id, acolyte, actor=None)
                            else:
                                slot = AssignmentSlot.objects.filter(parish=parish, id=slot_id).first()
                                assignment = assign_replacement(parish, slot, acolyte, actor=None) if slot else None
                            if assignment:
                                assigned += 1
                                if assignment.acolyte.user:
                                    enqueue_notification(
                                        parish,
                                        assignment.acolyte.user,
                                        "REPLACEMENT_ASSIGNED",
                                        {"assignment_id": assignment.id},
                                        idempotency_key=f"replacement:{assignment.id}",
                                    )
                        except Exception:
                            continue
                    job.status = "success" if result.feasible else "failed"
                    job.summary_json = {
                        "coverage": result.coverage,
                        "preference_score": result.preference_score,
                        "fairness_std": result.fairness_std,
                        "changes": assigned,
                        "required_slots_count": result.required_slots_count,
                        "unfilled_slots_count": result.unfilled_slots_count,
                        "unfilled_details": result.unfilled_details,
                    }
                    if not result.feasible:
                        job.error_message = "Slots sem candidatos para cobertura."
                else:
                    instances = build_horizon_instances(parish, job.horizon_days)
                    result = solve_schedule(parish, instances, parish.consolidation_days, weights, allow_changes=job.force_republish)
                    job.status = "success" if result.feasible else "failed"
                    job.summary_json = {
                        "coverage": result.coverage,
                        "preference_score": result.preference_score,
                        "fairness_std": result.fairness_std,
                        "changes": result.changes,
                        "required_slots_count": result.required_slots_count,
                        "unfilled_slots_count": result.unfilled_slots_count,
                        "unfilled_details": result.unfilled_details,
                    }
                    if not result.feasible:
                        job.error_message = "Slots sem candidatos para cobertura."
                job.finished_at = timezone.now()
                job.save(update_fields=["status", "summary_json", "finished_at", "error_message"])
                self.stdout.write(self.style.SUCCESS(f"Job {job.id} completed"))
            except Exception as exc:
                job.status = "failed"
                job.error_message = str(exc)
                job.finished_at = timezone.now()
                job.save(update_fields=["status", "error_message", "finished_at"])
                self.stderr.write(self.style.ERROR(f"Job {job.id} failed: {exc}"))

