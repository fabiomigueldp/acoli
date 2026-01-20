from django.contrib.auth.views import LoginView, LogoutView

from .forms import LoginForm


class UserLoginView(LoginView):
    template_name = "login.html"
    authentication_form = LoginForm


class UserLogoutView(LogoutView):
    pass

