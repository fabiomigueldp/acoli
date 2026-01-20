from functools import wraps

from django.contrib import messages
from django.http import HttpResponseForbidden
from django.shortcuts import redirect

from core.models import ParishMembership

ADMIN_ROLE_CODES = ["PARISH_ADMIN", "ACOLYTE_COORDINATOR", "PASTOR", "SECRETARY"]


def _get_membership_roles(request):
    if hasattr(request, "_parish_roles"):
        return request._parish_roles
    if not request.user.is_authenticated or not request.active_parish:
        request._parish_roles = []
        return request._parish_roles
    membership = ParishMembership.objects.filter(
        user=request.user, parish=request.active_parish, active=True
    ).prefetch_related("roles").first()
    if not membership:
        request._parish_roles = []
        return request._parish_roles
    request._parish_roles = list(membership.roles.values_list("code", flat=True))
    return request._parish_roles


def user_has_role(user, parish, role_codes):
    if user.is_system_admin:
        return True
    membership = ParishMembership.objects.filter(user=user, parish=parish, active=True).first()
    if not membership:
        return False
    return membership.roles.filter(code__in=role_codes).exists()


def request_has_role(request, role_codes):
    if request.user.is_system_admin:
        return True
    roles = _get_membership_roles(request)
    return any(role in roles for role in role_codes)


def users_with_roles(parish, role_codes):
    memberships = (
        ParishMembership.objects.filter(parish=parish, active=True, roles__code__in=role_codes)
        .select_related("user")
        .distinct()
    )
    return [membership.user for membership in memberships]


def require_parish_roles(role_codes):
    def decorator(view_func):
        @wraps(view_func)
        def _wrapped(request, *args, **kwargs):
            if request_has_role(request, role_codes):
                return view_func(request, *args, **kwargs)
            return HttpResponseForbidden("Sem permissao para acessar esta area.")

        return _wrapped

    return decorator


def require_active_parish(view_func):
    @wraps(view_func)
    def _wrapped(request, *args, **kwargs):
        if not getattr(request, "active_parish", None):
            messages.info(request, "Selecione uma paroquia para continuar.")
            return redirect("dashboard")
        return view_func(request, *args, **kwargs)

    return _wrapped

