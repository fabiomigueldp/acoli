from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import AcolyteProfile, Community, MembershipRole, Parish, ParishMembership


class AcolyteLinkTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.admin = User.objects.create_user(email="admin@example.com", full_name="Admin", password="pass")
        self.target = User.objects.create_user(email="target@example.com", full_name="Target", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        self.role_admin = MembershipRole.objects.create(code="PARISH_ADMIN", name="Parish admin")
        self.role_acolyte = MembershipRole.objects.create(code="ACOLYTE", name="Acolyte")
        self.role_system = MembershipRole.objects.create(code="SYSTEM_ADMIN", name="System admin")
        membership = ParishMembership.objects.create(parish=self.parish, user=self.admin, active=True)
        membership.roles.add(self.role_admin)
        self.acolyte = AcolyteProfile.objects.create(parish=self.parish, display_name="Acolito")

    def _login(self):
        self.client.login(email="admin@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()

    def test_link_reactivates_membership(self):
        ParishMembership.objects.create(parish=self.parish, user=self.target, active=False)
        self._login()
        response = self.client.post(
            "/acolytes/link/",
            {
                "email": "target@example.com",
                "acolyte": self.acolyte.id,
                "roles": [self.role_acolyte.id],
            },
        )
        self.assertEqual(response.status_code, 302)
        membership = ParishMembership.objects.get(parish=self.parish, user=self.target)
        self.assertTrue(membership.active)

    def test_system_admin_role_is_not_assigned(self):
        self._login()
        response = self.client.post(
            "/acolytes/link/",
            {
                "email": "target@example.com",
                "acolyte": self.acolyte.id,
                "roles": [self.role_system.id, self.role_acolyte.id],
            },
        )
        self.assertEqual(response.status_code, 302)
        membership = ParishMembership.objects.get(parish=self.parish, user=self.target)
        self.assertTrue(membership.roles.filter(code="ACOLYTE").exists())
        self.assertFalse(membership.roles.filter(code="SYSTEM_ADMIN").exists())
