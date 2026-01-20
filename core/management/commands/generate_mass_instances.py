from datetime import date, timedelta

from django.core.management.base import BaseCommand

from core.models import Parish
from core.services.calendar_generation import generate_instances_for_parish


class Command(BaseCommand):
    help = "Generate rolling mass instances for the next N days."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=60)
        parser.add_argument("--parish-id", type=int)

    def handle(self, *args, **options):
        days = options["days"]
        parish_id = options.get("parish_id")
        start_date = date.today()
        end_date = start_date + timedelta(days=days)
        parishes = Parish.objects.all()
        if parish_id:
            parishes = parishes.filter(id=parish_id)
        for parish in parishes:
            created = generate_instances_for_parish(parish, start_date, end_date)
            self.stdout.write(self.style.SUCCESS(f"Parish {parish.id}: created {len(created)} instances"))

