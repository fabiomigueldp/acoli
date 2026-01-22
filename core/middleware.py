from django.http import HttpResponseBadRequest, HttpResponseForbidden
from rest_framework.authentication import BasicAuthentication
from rest_framework.exceptions import AuthenticationFailed

from core.models import Parish, ParishMembership


class ActiveParishMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        request.active_parish = None
        if request.user.is_authenticated:
            parish_id = request.session.get("active_parish_id")
            if parish_id:
                if request.user.is_system_admin:
                    request.active_parish = Parish.objects.filter(id=parish_id).first()
                else:
                    membership = ParishMembership.objects.filter(user=request.user, parish_id=parish_id, active=True).select_related("parish").first()
                    if membership:
                        request.active_parish = membership.parish
            if request.active_parish is None:
                membership = ParishMembership.objects.filter(user=request.user, active=True).select_related("parish").first()
                if membership:
                    request.active_parish = membership.parish
                    request.session["active_parish_id"] = membership.parish_id
            if request.active_parish is None and request.user.is_system_admin:
                request.active_parish = Parish.objects.first()
                if request.active_parish:
                    request.session["active_parish_id"] = request.active_parish.id
        return self.get_response(request)


class ApiParishHeaderMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        self.basic_auth = BasicAuthentication()

    def __call__(self, request):
        if request.path.startswith("/api/"):
            if not request.user.is_authenticated and request.META.get("HTTP_AUTHORIZATION"):
                try:
                    auth_result = self.basic_auth.authenticate(request)
                except AuthenticationFailed:
                    return HttpResponseForbidden("Paroquia invalida.")
                if auth_result:
                    request.user, request.auth = auth_result

            parish_id = request.headers.get("X-Parish-ID") or request.GET.get("parish_id")
            if parish_id:
                if not request.user.is_authenticated:
                    return HttpResponseForbidden("Paroquia invalida.")
                try:
                    parish_id = int(parish_id)
                except (TypeError, ValueError):
                    return HttpResponseForbidden("Paroquia invalida.")
                if request.user.is_system_admin:
                    parish = Parish.objects.filter(id=parish_id).first()
                else:
                    membership = ParishMembership.objects.filter(
                        user=request.user, parish_id=parish_id, active=True
                    ).select_related("parish").first()
                    parish = membership.parish if membership else None
                if parish is None:
                    return HttpResponseForbidden("Paroquia invalida.")
                request.active_parish = parish
            if request.active_parish is None:
                return HttpResponseBadRequest("Informe X-Parish-ID ou parish_id.")

        return self.get_response(request)

