from django.core.management.base import BaseCommand

from core.models import Parish
from core.services.stats import recompute_stats


class Command(BaseCommand):
    help = "Recompute acolyte statistics and reliability metrics."

    def add_arguments(self, parser):
        parser.add_argument("--parish-id", type=int)

    def handle(self, *args, **options):
        parish_id = options.get("parish_id")
        parishes = Parish.objects.all()
        if parish_id:
            parishes = parishes.filter(id=parish_id)
        for parish in parishes:
            recompute_stats(parish)
            self.stdout.write(self.style.SUCCESS(f"Parish {parish.id}: stats recomputed"))

