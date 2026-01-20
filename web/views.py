import calendar as cal
from datetime import date, datetime, timedelta

from django.contrib.auth.decorators import login_required
from django.forms import formset_factory
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone

from core.models import (
    Assignment,
    AssignmentSlot,
    Confirmation,
    EventOccurrence,
    EventSeries,
    EventInterest,
    MassInstance,
    MassTemplate,
    MassOverride,
    ParishMembership,
    SwapRequest,
)
from core.services.audit import log_audit
from core.services.calendar_generation import generate_instances_for_parish
from core.services.event_series import apply_event_occurrences
from core.services.publishing import publish_assignments
from core.services.slots import sync_slots_for_instance
from core.services.permissions import ADMIN_ROLE_CODES, require_parish_roles, user_has_role, users_with_roles
from core.services.replacements import create_replacement_request, mark_replacement_assigned
from core.services.swaps import apply_swap_request
from scheduler.models import ScheduleJobRequest
from notifications.services import enqueue_notification
from scheduler.services.quick_fill import quick_fill_slot
from web.forms import (
    AcolyteAvailabilityRuleForm,
    AcolytePreferenceForm,
    EventOccurrenceForm,
    EventSeriesBasicsForm,
    MassInstanceCancelForm,
    MassInstanceMoveForm,
    MassInstanceUpdateForm,
    MassTemplateForm,
    SwapRequestForm,
)


@login_required
def dashboard(request):
    parish = request.active_parish
    if not parish:
        return render(request, "dashboard.html", {"missing_parish": True})
    upcoming = MassInstance.objects.filter(parish=parish, starts_at__gte=timezone.now()).order_by("starts_at")[:5]
    unfilled = AssignmentSlot.objects.filter(parish=parish, status="open").count()
    return render(
        request,
        "dashboard.html",
        {
            "parish": parish,
            "upcoming": upcoming,
            "unfilled": unfilled,
        },
    )


@login_required
def calendar_month(request):
    parish = request.active_parish
    today = date.today()
    year = int(request.GET.get("year", today.year))
    month = int(request.GET.get("month", today.month))
    community_id = request.GET.get("community")
    status = request.GET.get("status")
    kind = request.GET.get("kind")

    month_start = date(year, month, 1)
    last_day = cal.monthrange(year, month)[1]
    month_end = date(year, month, last_day)

    instances = (
        MassInstance.objects.filter(parish=parish, starts_at__date__range=(month_start, month_end))
        .select_related("community", "requirement_profile", "template", "event_series")
        .order_by("starts_at")
    )
    if community_id:
        instances = instances.filter(community_id=community_id)
    if status:
        instances = instances.filter(status=status)
    if kind == "event":
        instances = instances.exclude(event_series=None)
    elif kind == "template":
        instances = instances.exclude(template=None)

    instances_by_day = {}
    for instance in instances:
        instances_by_day.setdefault(instance.starts_at.date(), []).append(instance)

    month_grid = cal.Calendar(firstweekday=6).monthdatescalendar(year, month)
    prev_month = month_start - timedelta(days=1)
    next_month = month_end + timedelta(days=1)

    communities = parish.community_set.filter(active=True).order_by("code")

    return render(
        request,
        "calendar/month.html",
        {
            "today": today,
            "year": year,
            "month": month,
            "month_grid": month_grid,
            "instances_by_day": instances_by_day,
            "instances": instances,
            "communities": communities,
            "filters": {"community": community_id or "", "status": status or "", "kind": kind or ""},
            "prev_month": {"year": prev_month.year, "month": prev_month.month},
            "next_month": {"year": next_month.year, "month": next_month.month},
        },
    )


@login_required
def mass_detail(request, instance_id):
    parish = request.active_parish
    instance = get_object_or_404(MassInstance, parish=parish, id=instance_id)
    sync_slots_for_instance(instance)
    slots = instance.slots.select_related("position_type", "assignment__acolyte")
    update_form = MassInstanceUpdateForm(instance=instance)
    move_form = MassInstanceMoveForm(
        parish=parish,
        initial={
            "starts_at": timezone.localtime(instance.starts_at).replace(tzinfo=None),
            "community": instance.community,
        },
    )
    cancel_form = MassInstanceCancelForm()
    return render(
        request,
        "calendar/detail.html",
        {
            "instance": instance,
            "slots": slots,
            "update_form": update_form,
            "move_form": move_form,
            "cancel_form": cancel_form,
        },
    )


@login_required
@require_parish_roles(ADMIN_ROLE_CODES)
def mass_update(request, instance_id):
    parish = request.active_parish
    instance = get_object_or_404(MassInstance, parish=parish, id=instance_id)
    if request.method == "POST":
        form = MassInstanceUpdateForm(request.POST, instance=instance)
        if form.is_valid():
            old_label = instance.liturgy_label
            old_profile = instance.requirement_profile_id
            instance = form.save()
            payload = {
                "from": {"liturgy_label": old_label, "requirement_profile_id": old_profile},
                "to": {"liturgy_label": instance.liturgy_label, "requirement_profile_id": instance.requirement_profile_id},
            }
            log_audit(parish, request.user, "MassInstance", instance.id, "update", payload)
            if old_label != instance.liturgy_label:
                MassOverride.objects.create(
                    parish=parish,
                    instance=instance,
                    override_type="change_display_fields",
                    payload=payload,
                    created_by=request.user,
                )
            if old_profile != instance.requirement_profile_id:
                MassOverride.objects.create(
                    parish=parish,
                    instance=instance,
                    override_type="change_requirements",
                    payload=payload,
                    created_by=request.user,
                )
                sync_slots_for_instance(instance)
            return redirect("mass_detail", instance_id=instance.id)
    return redirect("mass_detail", instance_id=instance.id)


@login_required
@require_parish_roles(ADMIN_ROLE_CODES)
def mass_move(request, instance_id):
    parish = request.active_parish
    instance = get_object_or_404(MassInstance, parish=parish, id=instance_id)
    if request.method == "POST":
        form = MassInstanceMoveForm(request.POST, parish=parish)
        if form.is_valid():
            new_starts_at = timezone.make_aware(form.cleaned_data["starts_at"])
            new_community = form.cleaned_data["community"]
            conflict = MassInstance.objects.filter(
                parish=parish,
                community=new_community,
                starts_at=new_starts_at,
                status="scheduled",
            ).exclude(id=instance.id)
            if conflict.exists():
                return redirect("mass_detail", instance_id=instance.id)
            payload = {
                "from": {"starts_at": instance.starts_at.isoformat(), "community_id": instance.community_id},
                "to": {"starts_at": new_starts_at.isoformat(), "community_id": new_community.id},
            }
            instance.starts_at = new_starts_at
            instance.community = new_community
            instance.save(update_fields=["starts_at", "community", "updated_at"])
            MassOverride.objects.create(
                parish=parish,
                instance=instance,
                override_type="move_instance",
                payload=payload,
                created_by=request.user,
            )
            log_audit(parish, request.user, "MassInstance", instance.id, "move", payload)
    return redirect("mass_detail", instance_id=instance.id)


@login_required
@require_parish_roles(ADMIN_ROLE_CODES)
def mass_cancel(request, instance_id):
    parish = request.active_parish
    instance = get_object_or_404(MassInstance, parish=parish, id=instance_id)
    if request.method == "POST":
        form = MassInstanceCancelForm(request.POST)
        if form.is_valid():
            reason = form.cleaned_data.get("reason", "")
            instance.status = "canceled"
            instance.save(update_fields=["status", "updated_at"])
            MassOverride.objects.create(
                parish=parish,
                instance=instance,
                override_type="cancel_instance",
                payload={"reason": reason},
                created_by=request.user,
            )
            log_audit(parish, request.user, "MassInstance", instance.id, "cancel", {"reason": reason})
    return redirect("mass_detail", instance_id=instance.id)


@login_required
@require_parish_roles(ADMIN_ROLE_CODES)
def template_list(request):
    parish = request.active_parish
    templates = MassTemplate.objects.filter(parish=parish)
    return render(request, "mass_templates/list.html", {"templates": templates})


@login_required
@require_parish_roles(ADMIN_ROLE_CODES)
def template_create(request):
    parish = request.active_parish
    if request.method == "POST":
        form = MassTemplateForm(request.POST)
        if form.is_valid():
            template = form.save(commit=False)
            template.parish = parish
            template.save()
            return redirect("template_list")
    else:
        form = MassTemplateForm()
    return render(request, "mass_templates/form.html", {"form": form})


@login_required
@require_parish_roles(ADMIN_ROLE_CODES)
def generate_instances(request):
    parish = request.active_parish
    if request.method == "POST":
        days = int(request.POST.get("days", parish.horizon_days))
        start = date.today()
        end = start + timedelta(days=days)
        generate_instances_for_parish(parish, start, end, actor=request.user)
    return redirect("template_list")


@login_required
@require_parish_roles(ADMIN_ROLE_CODES)
def event_series_list(request):
    parish = request.active_parish
    series = EventSeries.objects.filter(parish=parish)
    return render(request, "events/list.html", {"series": series})


@login_required
def event_interest(request):
    parish = request.active_parish
    acolyte = parish.acolytes.filter(user=request.user).first()
    series = EventSeries.objects.filter(parish=parish).order_by("start_date")
    interests = {
        interest.event_series_id: interest
        for interest in EventInterest.objects.filter(parish=parish, acolyte=acolyte)
    }
    if request.method == "POST" and acolyte:
        selected = set(request.POST.getlist("series"))
        for item in series:
            interested = str(item.id) in selected
            obj, _ = EventInterest.objects.get_or_create(
                parish=parish, event_series=item, acolyte=acolyte, defaults={"interested": interested}
            )
            if obj.interested != interested:
                obj.interested = interested
                obj.save(update_fields=["interested", "updated_at"])
        return redirect("event_interest")
    return render(
        request,
        "events/interest.html",
        {"series": series, "interests": interests, "acolyte": acolyte},
    )


@login_required
@require_parish_roles(ADMIN_ROLE_CODES)
def event_series_create(request):
    parish = request.active_parish
    if request.method == "POST":
        form = EventSeriesBasicsForm(request.POST, parish=parish)
        if form.is_valid():
            cleaned = form.cleaned_data
            request.session["event_series_draft"] = {
                "series_type": cleaned["series_type"],
                "title": cleaned["title"],
                "start_date": cleaned["start_date"].isoformat(),
                "end_date": cleaned["end_date"].isoformat(),
                "default_time": cleaned["default_time"].strftime("%H:%M"),
                "default_community_id": cleaned["default_community"].id if cleaned["default_community"] else None,
                "default_requirement_profile_id": cleaned["default_requirement_profile"].id if cleaned["default_requirement_profile"] else None,
                "candidate_pool": cleaned["candidate_pool"],
            }
            return redirect("event_series_days")
    else:
        form = EventSeriesBasicsForm(parish=parish)
    return render(request, "events/basics.html", {"form": form})


@login_required
@require_parish_roles(ADMIN_ROLE_CODES)
def event_series_days(request):
    parish = request.active_parish
    draft = request.session.get("event_series_draft")
    if not draft:
        return redirect("event_series_create")

    start_date = date.fromisoformat(draft["start_date"])
    end_date = date.fromisoformat(draft["end_date"])
    default_time = datetime.strptime(draft["default_time"], "%H:%M").time()
    default_community = draft.get("default_community_id")
    default_profile = draft.get("default_requirement_profile_id")

    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    OccurrenceFormSet = formset_factory(EventOccurrenceForm, extra=0)
    if request.method == "POST":
        formset = OccurrenceFormSet(request.POST, form_kwargs={"parish": parish})
        if formset.is_valid():
            series = EventSeries.objects.create(
                parish=parish,
                series_type=draft["series_type"],
                title=draft["title"],
                start_date=start_date,
                end_date=end_date,
                default_community_id=default_community,
                candidate_pool=draft.get("candidate_pool", "all"),
                ruleset_json={},
                created_by=request.user,
                updated_by=request.user,
            )
            occurrences = []
            for form in formset:
                data = form.cleaned_data
                occurrence = EventOccurrence.objects.create(
                    parish=parish,
                    event_series=series,
                    date=data["date"],
                    time=data["time"],
                    community=data["community"],
                    requirement_profile=data["requirement_profile"],
                    label=data.get("label", ""),
                    conflict_action=data["conflict_action"],
                    move_to_date=data.get("move_to_date"),
                    move_to_time=data.get("move_to_time"),
                    move_to_community=data.get("move_to_community"),
                )
                occurrences.append(occurrence)
            apply_event_occurrences(series, occurrences, actor=request.user)
            request.session.pop("event_series_draft", None)
            return redirect("event_series_list")
    else:
        initial = []
        for date_value in dates:
            initial.append(
                {
                    "date": date_value,
                    "time": default_time,
                    "community": default_community,
                    "requirement_profile": default_profile,
                    "conflict_action": "keep",
                }
            )
        formset = OccurrenceFormSet(initial=initial, form_kwargs={"parish": parish})

    conflicts = []
    for form in formset:
        data = form.initial
        conflict = MassInstance.objects.filter(
            parish=parish,
            community_id=data.get("community"),
            starts_at__date=data.get("date"),
            starts_at__time=data.get("time"),
            status="scheduled",
        ).first()
        conflicts.append(conflict)

    return render(
        request,
        "events/days.html",
        {
            "formset": formset,
            "conflicts": conflicts,
            "draft": draft,
        },
    )


@login_required
@require_parish_roles(ADMIN_ROLE_CODES)
def scheduling_dashboard(request):
    parish = request.active_parish
    jobs = ScheduleJobRequest.objects.filter(parish=parish).order_by("-created_at")[:10]
    if request.method == "POST":
        if request.POST.get("action") == "publish":
            start_date = date.fromisoformat(request.POST.get("start_date"))
            end_date = date.fromisoformat(request.POST.get("end_date"))
            publish_assignments(parish, start_date, end_date, actor=request.user)
        else:
            ScheduleJobRequest.objects.create(parish=parish, requested_by=request.user, horizon_days=parish.horizon_days)
        return redirect("scheduling_dashboard")

    consolidation_end = timezone.now() + timedelta(days=parish.consolidation_days)
    urgent_open = AssignmentSlot.objects.filter(
        parish=parish, status="open", mass_instance__starts_at__lte=consolidation_end
    ).count()
    pending_confirmations = Confirmation.objects.filter(
        parish=parish, status="pending", assignment__slot__mass_instance__starts_at__lte=consolidation_end
    ).count()
    return render(
        request,
        "scheduling/dashboard.html",
        {
            "jobs": jobs,
            "urgent_open": urgent_open,
            "pending_confirmations": pending_confirmations,
            "now": timezone.now(),
        },
    )


@login_required
@require_parish_roles(ADMIN_ROLE_CODES)
def acolyte_list(request):
    parish = request.active_parish
    acolytes = parish.acolytes.filter(active=True)
    return render(request, "acolytes/list.html", {"acolytes": acolytes})


@login_required
def my_assignments(request):
    parish = request.active_parish
    assignments = Assignment.objects.filter(parish=parish, acolyte__user=request.user).select_related(
        "slot__mass_instance", "slot__position_type", "confirmation"
    )
    return render(request, "acolytes/assignments.html", {"assignments": assignments})


@login_required
def my_preferences(request):
    parish = request.active_parish
    acolyte = parish.acolytes.filter(user=request.user).first()
    if not acolyte:
        return render(request, "acolytes/preferences.html", {"acolyte": None})

    availability = acolyte.acolyteavailabilityrule_set.select_related("community")
    preferences = acolyte.acolytepreference_set.select_related(
        "target_community", "target_position", "target_function", "target_template", "target_acolyte"
    )

    if request.method == "POST":
        if request.POST.get("form_type") == "availability":
            form = AcolyteAvailabilityRuleForm(request.POST)
            form.fields["community"].queryset = parish.community_set.filter(active=True)
            if form.is_valid():
                rule = form.save(commit=False)
                rule.parish = parish
                rule.acolyte = acolyte
                rule.save()
                return redirect("my_preferences")
        elif request.POST.get("form_type") == "preference":
            form = AcolytePreferenceForm(request.POST)
            form.fields["target_community"].queryset = parish.community_set.filter(active=True)
            form.fields["target_position"].queryset = parish.positiontype_set.filter(active=True)
            form.fields["target_function"].queryset = parish.functiontype_set.filter(active=True)
            form.fields["target_template"].queryset = parish.masstemplate_set.filter(active=True)
            form.fields["target_acolyte"].queryset = parish.acolytes.filter(active=True)
            if form.is_valid():
                pref = form.save(commit=False)
                pref.parish = parish
                pref.acolyte = acolyte
                pref.save()
                return redirect("my_preferences")

    availability_form = AcolyteAvailabilityRuleForm()
    availability_form.fields["community"].queryset = parish.community_set.filter(active=True)
    preference_form = AcolytePreferenceForm()
    preference_form.fields["target_community"].queryset = parish.community_set.filter(active=True)
    preference_form.fields["target_position"].queryset = parish.positiontype_set.filter(active=True)
    preference_form.fields["target_function"].queryset = parish.functiontype_set.filter(active=True)
    preference_form.fields["target_template"].queryset = parish.masstemplate_set.filter(active=True)
    preference_form.fields["target_acolyte"].queryset = parish.acolytes.filter(active=True)

    return render(
        request,
        "acolytes/preferences.html",
        {
            "acolyte": acolyte,
            "availability": availability,
            "preferences": preferences,
            "availability_form": availability_form,
            "preference_form": preference_form,
        },
    )


@login_required
def delete_availability(request, rule_id):
    parish = request.active_parish
    acolyte = parish.acolytes.filter(user=request.user).first()
    if not acolyte:
        return redirect("my_preferences")
    rule = get_object_or_404(acolyte.acolyteavailabilityrule_set, id=rule_id)
    if request.method == "POST":
        rule.delete()
    return redirect("my_preferences")


@login_required
def delete_preference(request, pref_id):
    parish = request.active_parish
    acolyte = parish.acolytes.filter(user=request.user).first()
    if not acolyte:
        return redirect("my_preferences")
    pref = get_object_or_404(acolyte.acolytepreference_set, id=pref_id)
    if request.method == "POST":
        pref.delete()
    return redirect("my_preferences")


@login_required
def swap_requests(request):
    parish = request.active_parish
    swaps = SwapRequest.objects.filter(parish=parish).select_related("mass_instance", "requestor_acolyte", "target_acolyte")
    if not user_has_role(request.user, parish, ADMIN_ROLE_CODES):
        swaps = (
            swaps.filter(requestor_acolyte__user=request.user)
            | swaps.filter(target_acolyte__user=request.user)
        ).distinct()
    return render(request, "acolytes/swaps.html", {"swaps": swaps})


@login_required
def swap_request_create(request, assignment_id):
    parish = request.active_parish
    assignment = get_object_or_404(Assignment, parish=parish, id=assignment_id)
    if assignment.acolyte.user_id != request.user.id and not user_has_role(
        request.user, parish, ADMIN_ROLE_CODES
    ):
        return redirect("my_assignments")
    slots_qs = assignment.slot.mass_instance.slots.exclude(id=assignment.slot_id).filter(assignment__isnull=False)
    acolytes_qs = parish.acolytes.filter(active=True)
    if request.method == "POST":
        form = SwapRequestForm(request.POST, acolytes_qs=acolytes_qs, slots_qs=slots_qs)
        if form.is_valid():
            swap_type = form.cleaned_data["swap_type"]
            target_acolyte = form.cleaned_data.get("target_acolyte")
            to_slot = form.cleaned_data.get("to_slot")
            if swap_type == "role_swap" and not to_slot:
                return redirect("swap_request_create", assignment_id=assignment.id)
            if swap_type == "role_swap" and to_slot and to_slot.assignment:
                target_acolyte = to_slot.assignment.acolyte
            swap = SwapRequest.objects.create(
                parish=parish,
                swap_type=swap_type,
                requestor_acolyte=assignment.acolyte,
                target_acolyte=target_acolyte,
                mass_instance=assignment.slot.mass_instance,
                from_slot=assignment.slot,
                to_slot=to_slot,
                status="pending",
                notes=form.cleaned_data.get("notes", ""),
            )
            log_audit(parish, request.user, "SwapRequest", swap.id, "create", {"swap_type": swap.swap_type})
            if target_acolyte and target_acolyte.user:
                enqueue_notification(
                    parish,
                    target_acolyte.user,
                    "SWAP_REQUESTED",
                    {"subject": "Solicitacao de troca", "body": "Voce recebeu um pedido de troca."},
                    idempotency_key=f"swap:{swap.id}:request",
                )
            return redirect("swap_requests")
    else:
        form = SwapRequestForm(acolytes_qs=acolytes_qs, slots_qs=slots_qs)
    return render(request, "acolytes/swap_form.html", {"form": form, "assignment": assignment})


@login_required
def swap_request_accept(request, swap_id):
    if request.method != "POST":
        return redirect("swap_requests")
    parish = request.active_parish
    swap = get_object_or_404(SwapRequest, parish=parish, id=swap_id)
    if swap.target_acolyte and swap.target_acolyte.user_id != request.user.id and not user_has_role(
        request.user, parish, ADMIN_ROLE_CODES
    ):
        return redirect("swap_requests")
    if parish.swap_requires_approval:
        swap.status = "awaiting_approval"
        swap.save(update_fields=["status", "updated_at"])
        for user in users_with_roles(parish, ADMIN_ROLE_CODES):
            enqueue_notification(
                parish,
                user,
                "SWAP_REQUESTED",
                {"subject": "Troca aguardando aprovacao", "body": "Ha uma troca aguardando aprovacao."},
                idempotency_key=f"swap:{swap.id}:approval:{user.id}",
            )
        if swap.requestor_acolyte.user:
            enqueue_notification(
                parish,
                swap.requestor_acolyte.user,
                "SWAP_REQUESTED",
                {"subject": "Troca enviada", "body": "Sua solicitacao aguarda aprovacao."},
                idempotency_key=f"swap:{swap.id}:awaiting",
            )
        return redirect("swap_requests")
    if apply_swap_request(swap, actor=request.user):
        swap.status = "accepted"
        swap.save(update_fields=["status", "updated_at"])
        if swap.requestor_acolyte.user:
            enqueue_notification(
                parish,
                swap.requestor_acolyte.user,
                "SWAP_ACCEPTED",
                {"subject": "Troca aprovada", "body": "Sua troca foi aprovada."},
                idempotency_key=f"swap:{swap.id}:accepted",
            )
    return redirect("swap_requests")


@login_required
def swap_request_reject(request, swap_id):
    if request.method != "POST":
        return redirect("swap_requests")
    parish = request.active_parish
    swap = get_object_or_404(SwapRequest, parish=parish, id=swap_id)
    if swap.target_acolyte and swap.target_acolyte.user_id != request.user.id and not user_has_role(
        request.user, parish, ADMIN_ROLE_CODES
    ):
        return redirect("swap_requests")
    swap.status = "rejected"
    swap.save(update_fields=["status", "updated_at"])
    if swap.requestor_acolyte.user:
        enqueue_notification(
            parish,
            swap.requestor_acolyte.user,
            "SWAP_REJECTED",
            {"subject": "Troca recusada", "body": "Sua troca foi recusada."},
            idempotency_key=f"swap:{swap.id}:rejected",
        )
    return redirect("swap_requests")


@login_required
@require_parish_roles(ADMIN_ROLE_CODES)
def swap_request_approve(request, swap_id):
    if request.method != "POST":
        return redirect("swap_requests")
    parish = request.active_parish
    swap = get_object_or_404(SwapRequest, parish=parish, id=swap_id)
    if swap.status != "awaiting_approval":
        return redirect("swap_requests")
    if apply_swap_request(swap, actor=request.user):
        swap.status = "accepted"
        swap.save(update_fields=["status", "updated_at"])
        if swap.requestor_acolyte.user:
            enqueue_notification(
                parish,
                swap.requestor_acolyte.user,
                "SWAP_ACCEPTED",
                {"subject": "Troca aprovada", "body": "Sua troca foi aprovada."},
                idempotency_key=f"swap:{swap.id}:accepted",
            )
    return redirect("swap_requests")


@login_required
@require_parish_roles(ADMIN_ROLE_CODES)
def parish_settings(request):
    parish = request.active_parish
    return render(request, "settings/parish.html", {"parish": parish})


@login_required
def switch_parish(request, parish_id):
    if request.user.is_system_admin:
        request.session["active_parish_id"] = parish_id
    else:
        membership = ParishMembership.objects.filter(user=request.user, parish_id=parish_id, active=True).first()
        if membership:
            request.session["active_parish_id"] = membership.parish_id
    return redirect("dashboard")


def _can_manage_assignment(user, assignment):
    if user.is_system_admin:
        return True
    if assignment.acolyte.user_id == user.id:
        return True
    return user_has_role(user, assignment.parish, ["PARISH_ADMIN", "ACOLYTE_COORDINATOR", "PASTOR", "SECRETARY"])


@login_required
def confirm_assignment(request, assignment_id):
    if request.method != "POST":
        return redirect("my_assignments")
    parish = request.active_parish
    assignment = get_object_or_404(Assignment, parish=parish, id=assignment_id)
    if not _can_manage_assignment(request.user, assignment):
        return redirect("my_assignments")
    confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
    confirmation.status = "confirmed"
    confirmation.updated_by = request.user
    confirmation.save(update_fields=["status", "updated_by", "timestamp"])
    log_audit(parish, request.user, "Confirmation", confirmation.id, "update", {"status": "confirmed"})
    return redirect("my_assignments")


@login_required
def decline_assignment(request, assignment_id):
    if request.method != "POST":
        return redirect("my_assignments")
    parish = request.active_parish
    assignment = get_object_or_404(Assignment, parish=parish, id=assignment_id)
    if not _can_manage_assignment(request.user, assignment):
        return redirect("my_assignments")
    confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
    confirmation.status = "declined"
    confirmation.updated_by = request.user
    confirmation.save(update_fields=["status", "updated_by", "timestamp"])
    slot = assignment.slot
    slot.status = "open"
    slot.save(update_fields=["status", "updated_at"])
    log_audit(parish, request.user, "Confirmation", confirmation.id, "update", {"status": "declined"})
    replacement = create_replacement_request(parish, slot, actor=request.user)
    if parish.notify_on_cancellation:
        for user in users_with_roles(parish, ADMIN_ROLE_CODES):
            enqueue_notification(
                parish,
                user,
                "ASSIGNMENT_CANCELED_ALERT_ADMIN",
                {"subject": "Escala cancelada", "body": f"Vaga aberta em {slot.mass_instance.starts_at:%d/%m %H:%M}."},
                idempotency_key=f"cancel:{assignment.id}:{user.id}",
            )
    if parish.auto_assign_on_decline:
        candidates = quick_fill_slot(slot, parish, max_candidates=1)
        if candidates:
            assignment.acolyte = candidates[0]
            assignment.assignment_state = "proposed"
            assignment.save(update_fields=["acolyte", "assignment_state", "updated_at"])
            confirmation.status = "pending"
            confirmation.updated_by = request.user
            confirmation.save(update_fields=["status", "updated_by", "timestamp"])
            mark_replacement_assigned(parish, slot, actor=request.user)
            if assignment.acolyte.user:
                enqueue_notification(
                    parish,
                    assignment.acolyte.user,
                    "REPLACEMENT_ASSIGNED",
                    {"subject": "Nova escala", "body": f"Voce foi escalado para {slot.mass_instance.starts_at:%d/%m %H:%M}."},
                    idempotency_key=f"replacement:{assignment.id}",
                )
    return redirect("my_assignments")


@login_required
def cancel_assignment(request, assignment_id):
    if request.method != "POST":
        return redirect("my_assignments")
    parish = request.active_parish
    assignment = get_object_or_404(Assignment, parish=parish, id=assignment_id)
    if not _can_manage_assignment(request.user, assignment):
        return redirect("my_assignments")
    confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
    confirmation.status = "canceled_by_acolyte"
    confirmation.updated_by = request.user
    confirmation.save(update_fields=["status", "updated_by", "timestamp"])
    slot = assignment.slot
    slot.status = "open"
    slot.save(update_fields=["status", "updated_at"])
    log_audit(parish, request.user, "Confirmation", confirmation.id, "update", {"status": "canceled_by_acolyte"})
    create_replacement_request(parish, slot, actor=request.user)
    if parish.notify_on_cancellation:
        for user in users_with_roles(parish, ADMIN_ROLE_CODES):
            enqueue_notification(
                parish,
                user,
                "ASSIGNMENT_CANCELED_ALERT_ADMIN",
                {"subject": "Escala cancelada", "body": f"Vaga aberta em {slot.mass_instance.starts_at:%d/%m %H:%M}."},
                idempotency_key=f"cancel:{assignment.id}:{user.id}",
            )
    return redirect("my_assignments")
