from django.core.management.base import BaseCommand
from django.utils import timezone

from core.models import EventSeries
from core.services.audit import log_audit


class Command(BaseCommand):
    help = "Archive event series that ended before today."

    def handle(self, *args, **options):
        today = timezone.localdate()
        to_archive = EventSeries.objects.filter(is_active=True, end_date__lt=today)
        archived = 0
        for series in to_archive:
            series.is_active = False
            series.save(update_fields=["is_active", "updated_at"])
            log_audit(series.parish, None, "EventSeries", series.id, "archive", {"auto": True})
            archived += 1
        self.stdout.write(self.style.SUCCESS(f"Archived {archived} event series."))
