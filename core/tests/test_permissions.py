from datetime import timedelta
import base64

from django.contrib.auth import get_user_model
from django.test import TestCase

from django.utils import timezone
from rest_framework.test import APIClient

from core.models import Community, MassInstance, Parish, ParishMembership


class ParishIsolationTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(email="user@example.com", full_name="User", password="pass")
        self.parish1 = Parish.objects.create(name="Parish 1")
        self.parish2 = Parish.objects.create(name="Parish 2")
        self.community1 = Community.objects.create(parish=self.parish1, code="A", name="A")
        self.community2 = Community.objects.create(parish=self.parish2, code="B", name="B")
        ParishMembership.objects.create(parish=self.parish1, user=self.user)

        MassInstance.objects.create(
            parish=self.parish1,
            community=self.community1,
            starts_at=timezone.now() + timedelta(days=1),
            status="scheduled",
        )
        MassInstance.objects.create(
            parish=self.parish2,
            community=self.community2,
            starts_at=timezone.now() + timedelta(days=2),
            status="scheduled",
        )

    def test_api_is_scoped_by_active_parish(self):
        client = APIClient()
        client.login(email="user@example.com", password="pass")
        session = client.session
        session["active_parish_id"] = self.parish1.id
        session.save()
        response = client.get("/api/masses/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()), 1)

    def test_api_parish_header_scopes_results(self):
        client = APIClient()
        credentials = base64.b64encode(b"user@example.com:pass").decode("utf-8")
        client.credentials(HTTP_AUTHORIZATION=f"Basic {credentials}", HTTP_X_PARISH_ID=str(self.parish1.id))
        response = client.get("/api/masses/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()), 1)

    def test_api_parish_header_blocks_other_parish(self):
        client = APIClient()
        credentials = base64.b64encode(b"user@example.com:pass").decode("utf-8")
        client.credentials(HTTP_AUTHORIZATION=f"Basic {credentials}", HTTP_X_PARISH_ID=str(self.parish2.id))
        response = client.get("/api/masses/")
        self.assertEqual(response.status_code, 403)

    def test_api_requires_parish_header_when_missing(self):
        client = APIClient()
        credentials = base64.b64encode(b"user@example.com:pass").decode("utf-8")
        client.credentials(HTTP_AUTHORIZATION=f"Basic {credentials}")
        response = client.get("/api/masses/")
        self.assertEqual(response.status_code, 400)
        self.assertIn("X-Parish-ID", response.content.decode("utf-8"))

