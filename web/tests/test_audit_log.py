from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import AuditEvent, MembershipRole, Parish, ParishMembership


class AuditLogViewTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.admin_user = User.objects.create_user(email="admin@example.com", full_name="Admin", password="pass")
        self.user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Parish admin")
        membership = ParishMembership.objects.create(parish=self.parish, user=self.admin_user, active=True)
        membership.roles.add(role)
        ParishMembership.objects.create(parish=self.parish, user=self.user, active=True)
        AuditEvent.objects.create(
            parish=self.parish,
            actor_user=self.admin_user,
            entity_type="Parish",
            entity_id=str(self.parish.id),
            action_type="update",
            diff_json={},
        )

    def _set_active_parish(self, client):
        session = client.session
        session["active_parish_id"] = self.parish.id
        session.save()

    def test_admin_can_view_audit_log(self):
        self.client.login(email="admin@example.com", password="pass")
        self._set_active_parish(self.client)
        response = self.client.get("/audit/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Configuracoes atualizadas")

    def test_non_admin_cannot_view_audit_log(self):
        self.client.login(email="user@example.com", password="pass")
        self._set_active_parish(self.client)
        response = self.client.get("/audit/")
        self.assertEqual(response.status_code, 403)
