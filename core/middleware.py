from django.http import HttpResponseBadRequest, HttpResponseForbidden
from rest_framework.authentication import BasicAuthentication
from rest_framework.exceptions import AuthenticationFailed

from core.models import Parish, ParishMembership


class NoStoreHtmlMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        content_type = response.get("Content-Type", "")
        if content_type.startswith("text/html"):
            response["Cache-Control"] = "no-store"
            response["Pragma"] = "no-cache"
            response["Expires"] = "0"
        return response


class BackUrlMiddleware:
    """Middleware to track the last navigable URL for back navigation."""
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)

        # Only save GET requests that are navigable and successful
        if (request.method == "GET" and
            response.status_code < 400 and
            not request.headers.get("HX-Request") and  # Ignore HTMX requests
            not request.path.startswith(("/static/", "/media/", "/api/", "/admin/", "/logout/")) and
            not any(ext in request.path for ext in [".css", ".js", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".woff", ".woff2"])):
            current_url = request.build_absolute_uri()
            last_url = request.session.get("last_url")
            if current_url != last_url:
                if last_url:
                    request.session["back_url"] = last_url
                request.session["last_url"] = current_url

        return response


class ActiveParishMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        request.active_parish = None
        if request.user.is_authenticated:
            parish_id = request.session.get("active_parish_id")
            if parish_id:
                if request.user.is_system_admin:
                    request.active_parish = Parish.objects.filter(id=parish_id).first()  # type: ignore[attr-defined]
                else:
                    membership = ParishMembership.objects.filter(user=request.user, parish_id=parish_id, active=True).select_related("parish").first()  # type: ignore[attr-defined]
                    if membership:
                        request.active_parish = membership.parish
            if request.active_parish is None:
                membership = ParishMembership.objects.filter(user=request.user, active=True).select_related("parish").first()  # type: ignore[attr-defined]
                if membership:
                    request.active_parish = membership.parish
                    request.session["active_parish_id"] = membership.parish_id
            if request.active_parish is None and request.user.is_system_admin:
                request.active_parish = Parish.objects.first()  # type: ignore[attr-defined]
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
                    return HttpResponseForbidden(b"Paroquia invalida.")
                if auth_result:
                    request.user, request.auth = auth_result

            parish_id = request.headers.get("X-Parish-ID") or request.GET.get("parish_id")
            if parish_id:
                if not request.user.is_authenticated:
                    return HttpResponseForbidden(b"Paroquia invalida.")
                try:
                    parish_id = int(parish_id)
                except (TypeError, ValueError):
                    return HttpResponseForbidden(b"Paroquia invalida.")
                if request.user.is_system_admin:
                    parish = Parish.objects.filter(id=parish_id).first()  # type: ignore[attr-defined]
                else:
                    membership = ParishMembership.objects.filter(  # type: ignore[attr-defined]
                        user=request.user, parish_id=parish_id, active=True
                    ).select_related("parish").first()
                    parish = membership.parish if membership else None
                if parish is None:
                    return HttpResponseForbidden(b"Paroquia invalida.")
                request.active_parish = parish
            if request.active_parish is None:
                return HttpResponseBadRequest(b"Informe X-Parish-ID ou parish_id.")

        return self.get_response(request)

