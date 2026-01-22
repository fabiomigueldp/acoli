from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import MembershipRole, Parish, ParishMembership


class AcolyteLinkRedirectTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.admin = User.objects.create_user(email="admin@example.com", full_name="Admin", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        role_admin = MembershipRole.objects.create(code="PARISH_ADMIN", name="Parish admin")
        membership = ParishMembership.objects.create(parish=self.parish, user=self.admin, active=True)
        membership.roles.add(role_admin)

    def _login(self):
        self.client.login(email="admin@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()

    def test_acolyte_link_redirects_to_people_create(self):
        self._login()
        response = self.client.get("/acolytes/link/")
        self.assertEqual(response.status_code, 302)
        self.assertIn("/people/new/", response["Location"])
