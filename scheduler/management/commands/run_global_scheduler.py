from django.core.management.base import BaseCommand
from django.utils import timezone

from scheduler.models import ScheduleJobRequest
from scheduler.services.jobs import claim_job
from scheduler.services.horizon import build_horizon_instances
from scheduler.services.solver import solve_schedule


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
                instances = build_horizon_instances(parish, job.horizon_days)
                weights = parish.schedule_weights or {}
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

