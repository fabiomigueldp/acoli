from types import SimpleNamespace

from core.models import Parish, ParishMembership
from core.services.permissions import ADMIN_ROLE_CODES, request_has_role


def active_parish(request):
    active = getattr(request, "active_parish", None)
    memberships = []
    no_parishes = False
    if request.user.is_authenticated:
        if request.user.is_system_admin:
            parishes = list(Parish.objects.all())
            memberships = [
                SimpleNamespace(parish=parish_obj, parish_id=parish_obj.id)
                for parish_obj in parishes
            ]
            if not parishes:
                no_parishes = True
        else:
            memberships = ParishMembership.objects.filter(user=request.user, active=True).select_related("parish")
    return {
        "active_parish": active,
        "parish_memberships": memberships,
        "can_manage_parish": request.user.is_authenticated
        and active is not None
        and request_has_role(request, ADMIN_ROLE_CODES),
        "no_parishes": no_parishes,
    }

