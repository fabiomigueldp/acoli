from django.core.management.base import BaseCommand

from notifications.services import NotificationService


class Command(BaseCommand):
    help = "Send pending notifications (email/whatsapp)."

    def handle(self, *args, **options):
        service = NotificationService()
        service.send_pending()
        self.stdout.write(self.style.SUCCESS("Notifications processed"))

