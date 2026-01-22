from django.urls import path

from .views import LogoutConfirmView, UserLoginView, UserLogoutView

urlpatterns = [
    path("login/", UserLoginView.as_view(), name="login"),
    path("logout/", UserLogoutView.as_view(), name="logout"),
    path("logout/confirm/", LogoutConfirmView.as_view(), name="logout_confirm"),
]

