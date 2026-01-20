from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import Community, Parish, ParishMembership


class WebPermissionsTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(email="user@example.com", full_name="User", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        ParishMembership.objects.create(parish=self.parish, user=self.user)

    def test_acolyte_cannot_access_templates(self):
        self.client.login(email="user@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()
        response = self.client.get("/templates/")
        self.assertEqual(response.status_code, 403)

