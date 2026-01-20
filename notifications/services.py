from django.conf import settings
from django.core.mail import send_mail
from django.urls import reverse
from django.utils import timezone

from core.models import Assignment, AssignmentSlot, SwapRequest
from notifications.models import Notification, NotificationPreference


class WhatsAppProvider:
    def send(self, notification):
        return True


class NotificationService:
    def __init__(self):
        self.whatsapp_provider = WhatsAppProvider()

    def deliver(self, notification):
        if notification.channel == "email":
            subject = notification.payload.get("subject", "Acoli notification")
            body = notification.payload.get("body", "")
            send_mail(subject, body, settings.DEFAULT_FROM_EMAIL, [notification.user.email])
            return True
        if notification.channel == "whatsapp":
            return self.whatsapp_provider.send(notification)
        return False

    def send_pending(self):
        pending = Notification.objects.filter(status="pending")
        for notification in pending:
            try:
                delivered = self.deliver(notification)
                notification.status = "sent" if delivered else "failed"
                notification.sent_at = timezone.now()
            except Exception as exc:
                notification.status = "failed"
                notification.error_message = str(exc)
            notification.save(update_fields=["status", "sent_at", "error_message"])


def _format_datetime(value):
    return timezone.localtime(value).strftime("%d/%m %H:%M")


def _absolute_url(path):
    base = getattr(settings, "APP_BASE_URL", "")
    if base:
        return f"{base}{path}"
    return path


def _render_payload(template_code, parish, payload):
    if payload.get("subject") and payload.get("body"):
        return payload

    assignment = payload.get("assignment")
    if not assignment and payload.get("assignment_id"):
        assignment = Assignment.objects.select_related(
            "slot__mass_instance__community", "slot__position_type"
        ).filter(parish=parish, id=payload["assignment_id"]).first()

    slot = payload.get("slot")
    if not slot and payload.get("slot_id"):
        slot = AssignmentSlot.objects.select_related(
            "mass_instance__community", "position_type"
        ).filter(parish=parish, id=payload["slot_id"]).first()

    swap = payload.get("swap")
    if not swap and payload.get("swap_id"):
        swap = SwapRequest.objects.select_related(
            "mass_instance__community", "from_slot__position_type"
        ).filter(parish=parish, id=payload["swap_id"]).first()

    if assignment:
        mass = assignment.slot.mass_instance
        context = {
            "date": _format_datetime(mass.starts_at),
            "community": mass.community.code,
            "position": assignment.slot.position_type.name,
            "url": _absolute_url(reverse("mass_detail", args=[mass.id])),
        }
        if template_code == "ASSIGNMENT_PUBLISHED":
            return {
                "subject": "Escala publicada",
                "body": f"Sua escala foi publicada para {context['date']} ({context['community']}). Ver detalhes: {context['url']}",
            }
        if template_code == "CONFIRMATION_REQUESTED":
            return {
                "subject": "Confirmacao de escala",
                "body": f"Confirme sua escala em {context['date']} ({context['community']}). Ver detalhes: {context['url']}",
            }
        if template_code == "REPLACEMENT_ASSIGNED":
            return {
                "subject": "Nova escala",
                "body": f"Voce foi atribuido em {context['date']} ({context['community']}). Ver detalhes: {context['url']}",
            }

    if slot:
        mass = slot.mass_instance
        context = {
            "date": _format_datetime(mass.starts_at),
            "community": mass.community.code,
            "position": slot.position_type.name,
            "url": _absolute_url(reverse("mass_detail", args=[mass.id])),
        }
        if template_code == "ASSIGNMENT_CANCELED_ALERT_ADMIN":
            return {
                "subject": "Vaga aberta",
                "body": f"Vaga aberta em {context['date']} ({context['community']}) - {context['position']}. Ver detalhes: {context['url']}",
            }

    if swap:
        mass = swap.mass_instance
        context = {
            "date": _format_datetime(mass.starts_at),
            "community": mass.community.code,
            "position": swap.from_slot.position_type.name if swap.from_slot else "Funcao",
            "url": _absolute_url(reverse("swap_requests")),
        }
        if template_code == "SWAP_REQUESTED":
            return {
                "subject": "Solicitacao de troca",
                "body": f"Voce recebeu uma solicitacao de troca para {context['date']} ({context['community']}). Ver detalhes: {context['url']}",
            }
        if template_code == "SWAP_ACCEPTED":
            return {
                "subject": "Troca aprovada",
                "body": f"A troca para {context['date']} ({context['community']}) foi aprovada. Ver detalhes: {context['url']}",
            }
        if template_code == "SWAP_REJECTED":
            return {
                "subject": "Troca recusada",
                "body": f"A troca para {context['date']} ({context['community']}) foi recusada. Ver detalhes: {context['url']}",
            }

    return {"subject": "Acoli", "body": "Voce tem uma nova atualizacao no sistema."}


def enqueue_notification(parish, user, template_code, payload, idempotency_key, channel="email"):
    if not idempotency_key.startswith(f"parish:{parish.id}:"):
        idempotency_key = f"parish:{parish.id}:{idempotency_key}"
    payload = _render_payload(template_code, parish, payload)
    if channel == "email":
        if not user.is_active:
            notification, _ = Notification.objects.get_or_create(
                channel=channel,
                idempotency_key=idempotency_key,
                defaults={
                    "parish": parish,
                    "user": user,
                    "template_code": template_code,
                    "payload": payload,
                    "status": "skipped",
                    "error_message": "user_inactive",
                },
            )
            return notification
        if not user.email:
            notification, _ = Notification.objects.get_or_create(
                channel=channel,
                idempotency_key=idempotency_key,
                defaults={
                    "parish": parish,
                    "user": user,
                    "template_code": template_code,
                    "payload": payload,
                    "status": "skipped",
                    "error_message": "missing_email",
                },
            )
            return notification
        pref = NotificationPreference.objects.filter(parish=parish, user=user).first()
        if pref and not pref.email_enabled:
            notification, _ = Notification.objects.get_or_create(
                channel=channel,
                idempotency_key=idempotency_key,
                defaults={
                    "parish": parish,
                    "user": user,
                    "template_code": template_code,
                    "payload": payload,
                    "status": "skipped",
                    "error_message": "email_disabled",
                },
            )
            return notification
    notification, _ = Notification.objects.get_or_create(
        channel=channel,
        idempotency_key=idempotency_key,
        defaults={
            "parish": parish,
            "user": user,
            "template_code": template_code,
            "payload": payload,
            "status": "pending",
        },
    )
    return notification

