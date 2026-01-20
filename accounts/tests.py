from django.test import TestCase

from .models import User


class UserModelTests(TestCase):
    def test_create_user(self):
        user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        self.assertTrue(user.check_password("pass"))

