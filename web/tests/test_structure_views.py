from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import (
    Community,
    FunctionType,
    MembershipRole,
    Parish,
    ParishMembership,
    PositionType,
    PositionTypeFunction,
    RequirementProfile,
    RequirementProfilePosition,
)


class StructureViewsTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(email="admin@example.com", full_name="Admin", password="pass")
        self.parish = Parish.objects.create(name="Parish")
        role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Admin")
        membership = ParishMembership.objects.create(parish=self.parish, user=self.user, active=True)
        membership.roles.add(role)
        self.client.login(email="admin@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()

    def test_community_create(self):
        response = self.client.post(
            "/structure/communities/new/",
            {"code": "MAT", "name": "Matriz", "address": "Rua A", "active": True},
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(Community.objects.filter(parish=self.parish, code="MAT").exists())

    def test_role_create_creates_position_and_function(self):
        response = self.client.post(
            "/structure/roles/new/",
            {"name": "Libriferario", "code": "LIB", "active": True},
        )
        self.assertEqual(response.status_code, 302)
        position = PositionType.objects.get(parish=self.parish, code="LIB")
        function = FunctionType.objects.get(parish=self.parish, code="LIB")
        self.assertEqual(position.name, "Libriferario")
        self.assertTrue(PositionTypeFunction.objects.filter(position_type=position, function_type=function).exists())

    def test_requirement_profile_create(self):
        position = PositionType.objects.create(parish=self.parish, code="LIB", name="Libriferario", active=True)
        payload = {
            "name": "Dominical",
            "notes": "",
            "min_senior_per_mass": 0,
            "active": True,
            "positions-TOTAL_FORMS": 1,
            "positions-INITIAL_FORMS": 0,
            "positions-MIN_NUM_FORMS": 0,
            "positions-MAX_NUM_FORMS": 1000,
            "positions-0-position_type": position.id,
            "positions-0-quantity": 1,
        }
        response = self.client.post("/structure/requirement-profiles/new/", payload)
        self.assertEqual(response.status_code, 302)
        profile = RequirementProfile.objects.get(parish=self.parish, name="Dominical")
        self.assertTrue(
            RequirementProfilePosition.objects.filter(profile=profile, position_type=position, quantity=1).exists()
        )

    def test_structure_requires_admin(self):
        User = get_user_model()
        user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        role = MembershipRole.objects.create(code="ACOLYTE", name="Acolyte")
        membership = ParishMembership.objects.create(parish=self.parish, user=user, active=True)
        membership.roles.add(role)

        self.client.logout()
        self.client.login(email="user@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = self.parish.id
        session.save()

        response = self.client.get("/structure/communities/")
        self.assertEqual(response.status_code, 403)
