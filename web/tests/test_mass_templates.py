from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import Community, MembershipRole, Parish, ParishMembership


class MassTemplateFormTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(email="admin@example.com", full_name="Admin", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Admin")
        membership = ParishMembership.objects.create(parish=self.parish, user=self.user, active=True)
        membership.roles.add(role)

        self.client.login(email="admin@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()

    def test_mass_template_form_shows_profile_callout_when_empty(self):
        response = self.client.get("/templates/new/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Nenhum perfil cadastrado.")
