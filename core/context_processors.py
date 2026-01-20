from types import SimpleNamespace

from core.models import Parish, ParishMembership
from core.services.permissions import ADMIN_ROLE_CODES, request_has_role


def active_parish(request):
    parish = getattr(request, "active_parish", None)
    memberships = []
    if request.user.is_authenticated:
        if request.user.is_system_admin:
            memberships = [
                SimpleNamespace(parish=parish, parish_id=parish.id)
                for parish in Parish.objects.all()
            ]
        else:
            memberships = ParishMembership.objects.filter(user=request.user, active=True).select_related("parish")
    return {
        "active_parish": parish,
        "parish_memberships": memberships,
        "can_manage_parish": request.user.is_authenticated
        and parish is not None
        and request_has_role(request, ADMIN_ROLE_CODES),
    }

