from django.utils import timezone

from scheduler.models import ScheduleJobRequest


def claim_job(job_id, now=None):
    now = now or timezone.now()
    updated = ScheduleJobRequest.objects.filter(id=job_id, status="pending").update(
        status="running",
        started_at=now,
    )
    return updated == 1
