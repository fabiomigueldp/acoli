from datetime import date, timedelta

from django.core.management.base import BaseCommand

from core.models import Parish
from core.services.slots import sync_slots_for_parish


class Command(BaseCommand):
    help = "Ensure assignment slots exist for mass instances in a range."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=60)
        parser.add_argument("--parish-id", type=int)
        parser.add_argument("--start-date", type=str)
        parser.add_argument("--end-date", type=str)

    def handle(self, *args, **options):
        parish_id = options.get("parish_id")
        start_date = options.get("start_date")
        end_date = options.get("end_date")

        if start_date and end_date:
            start = date.fromisoformat(start_date)
            end = date.fromisoformat(end_date)
        else:
            start = date.today()
            end = start + timedelta(days=options["days"])

        parishes = Parish.objects.all()
        if parish_id:
            parishes = parishes.filter(id=parish_id)
        for parish in parishes:
            count = sync_slots_for_parish(parish, start, end)
            self.stdout.write(self.style.SUCCESS(f"Parish {parish.id}: synced {count} instances"))

