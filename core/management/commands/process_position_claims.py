from django.core.management.base import BaseCommand

from core.models import Parish
from core.services.claims import process_due_claims


class Command(BaseCommand):
    help = "Processa solicitacoes de posicao vencidas para auto-aprovacao."

    def add_arguments(self, parser):
        parser.add_argument("--parish-id", type=int)

    def handle(self, *args, **options):
        parish_id = options.get("parish_id")
        parishes = Parish.objects.all()
        if parish_id:
            parishes = parishes.filter(id=parish_id)

        processed = 0
        for parish in parishes:
            process_due_claims(parish=parish)
            processed += 1
        self.stdout.write(self.style.SUCCESS(f"Processadas solicitacoes em {processed} paroquias"))
