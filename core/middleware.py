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

