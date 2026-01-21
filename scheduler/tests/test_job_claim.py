from django.test import TestCase

from core.models import Parish
from scheduler.models import ScheduleJobRequest
from scheduler.services.jobs import claim_job


class JobClaimTests(TestCase):
    def test_claim_job_only_once(self):
        parish = Parish.objects.create(name="Parish")
        job = ScheduleJobRequest.objects.create(parish=parish, status="pending")
        first = claim_job(job.id)
        second = claim_job(job.id)
        self.assertTrue(first)
        self.assertFalse(second)
