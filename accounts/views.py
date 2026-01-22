from django.contrib.auth.views import LoginView, LogoutView
from django.views.generic import TemplateView

from .forms import LoginForm


class UserLoginView(LoginView):
    template_name = "login.html"
    authentication_form = LoginForm


class UserLogoutView(LogoutView):
    template_name = "logout.html"


class LogoutConfirmView(TemplateView):
    """Página de confirmação de logout (GET request)."""

    template_name = "logout_confirm.html"

