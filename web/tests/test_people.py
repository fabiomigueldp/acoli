from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import (
    AcolyteProfile,
    Community,
    MembershipRole,
    Parish,
    ParishMembership,
)


class PeopleTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.parish = Parish.objects.create(name="Parish")
        self.community = Community.objects.create(parish=self.parish, code="MAT", name="Matriz")
        self.role_admin = MembershipRole.objects.create(code="PARISH_ADMIN", name="Parish admin")
        self.role_acolyte = MembershipRole.objects.create(code="ACOLYTE", name="Acolyte")
        self.role_system = MembershipRole.objects.create(code="SYSTEM_ADMIN", name="System admin")
        self.admin = User.objects.create_user(email="admin@example.com", full_name="Admin", password="pass")
        membership = ParishMembership.objects.create(parish=self.parish, user=self.admin, active=True)
        membership.roles.add(self.role_admin)

    def _login(self, user=None):
        if not user:
            user = self.admin
        self.client.login(email=user.email, password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()

    def test_people_directory_requires_admin(self):
        User = get_user_model()
        user = User.objects.create_user(email="acolyte@example.com", full_name="Acolyte", password="pass")
        membership = ParishMembership.objects.create(parish=self.parish, user=user, active=True)
        membership.roles.add(self.role_acolyte)
        self._login(user)
        response = self.client.get("/people/")
        self.assertEqual(response.status_code, 403)

    def test_people_directory_lists_user_and_acolyte(self):
        User = get_user_model()
        user_only = User.objects.create_user(email="member@example.com", full_name="Member", password="pass")
        ParishMembership.objects.create(parish=self.parish, user=user_only, active=True)
        AcolyteProfile.objects.create(parish=self.parish, display_name="Acolito sem login")
        self._login()
        response = self.client.get("/people/")
        self.assertContains(response, "Member")
        self.assertContains(response, "Acolito sem login")

    def test_people_create_acolyte_only(self):
        self._login()
        response = self.client.post(
            "/people/new/",
            {
                "full_name": "Novo Acolito",
                "phone": "",
                "is_acolyte": "on",
                "community_of_origin": self.community.id,
                "experience_level": "intermediate",
            },
        )
        self.assertEqual(response.status_code, 302)
        acolyte = AcolyteProfile.objects.filter(parish=self.parish, display_name="Novo Acolito").first()
        self.assertIsNotNone(acolyte)
        self.assertIsNone(acolyte.user)

    def test_people_create_admin_user(self):
        self._login()
        response = self.client.post(
            "/people/new/",
            {
                "full_name": "Novo Admin",
                "has_login": "on",
                "email": "novo@example.com",
                "password": "pass1234",
                "has_admin_access": "on",
                "roles": [self.role_admin.id],
            },
        )
        self.assertEqual(response.status_code, 302)
        user = get_user_model().objects.get(email="novo@example.com")
        membership = ParishMembership.objects.get(parish=self.parish, user=user)
        self.assertTrue(membership.roles.filter(code="PARISH_ADMIN").exists())

    def test_people_create_blocks_system_admin(self):
        self._login()
        response = self.client.post(
            "/people/new/",
            {
                "full_name": "Novo Admin",
                "has_login": "on",
                "email": "forbidden@example.com",
                "password": "pass1234",
                "has_admin_access": "on",
                "roles": [self.role_system.id],
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertFalse(get_user_model().objects.filter(email="forbidden@example.com").exists())
