import calendar as cal
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone as dt_timezone
from uuid import uuid4
from urllib.parse import urlencode

from django.contrib import messages
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core.mail import send_mail
from django.core.paginator import Paginator
from django.db.models import Count, F, Prefetch, Q
from django.forms import formset_factory
from django.http import HttpResponse, HttpResponseNotFound
from django.shortcuts import get_object_or_404, redirect, render
from django.template.loader import render_to_string
from django.urls import reverse
from django.urls import reverse
from django.utils.http import url_has_allowed_host_and_scheme
from django.utils import timezone

from core.models import (
    AcolyteAvailabilityRule,
    AcolyteQualification,
    AcolyteProfile,
    AcolyteStats,
    AuditEvent,
    Assignment,
    AssignmentSlot,
    CalendarFeedToken,
    Community,
    Confirmation,
    EventOccurrence,
    EventSeries,
    EventInterest,
    FamilyGroup,
    FunctionType,
    MassInstance,
    MassTemplate,
    MassOverride,
    MembershipRole,
    ParishMembership,
    PositionType,
    PositionTypeFunction,
    RequirementProfile,
    RequirementProfilePosition,
    ReplacementRequest,
    SwapRequest,
)
from core.services.audit import log_audit
from core.services.calendar_generation import generate_instances_for_parish
from core.services.event_series import apply_event_occurrences
from core.services.assignments import ConcurrentUpdateError, assign_manual, deactivate_assignment
from core.services.publishing import publish_assignments
from core.services.slots import sync_slots_for_instance
from core.services.permissions import (
    ADMIN_ROLE_CODES,
    require_active_parish,
    require_parish_roles,
    user_has_role,
    users_with_roles,
)
from core.services.replacements import (
    assign_replacement_request,
    cancel_mass_and_resolve_dependents,
    create_replacement_request,
)
from core.services.swaps import apply_swap_request
from core.services.availability import is_acolyte_available, is_acolyte_available_with_rules
from core.services.acolytes import deactivate_future_assignments_for_acolyte
from scheduler.models import ScheduleJobRequest
from notifications.services import enqueue_notification
from scheduler.services.quick_fill import build_quick_fill_cache, quick_fill_slot
from web.forms import (
    AcolyteAvailabilityRuleForm,
    AcolyteLinkForm,
    AcolytePreferenceForm,
    CommunityForm,
    DateAbsenceForm,
    EventOccurrenceForm,
    EventSeriesBasicsForm,
    MassInstanceCancelForm,
    MassInstanceMoveForm,
    MassInstanceUpdateForm,
    MassTemplateForm,
    ParishSettingsForm,
    PeopleAcolyteForm,
    PeopleCreateForm,
    PeopleMembershipForm,
    PeopleQualificationsForm,
    PeopleUserForm,
    RequirementProfileForm,
    RequirementProfilePositionFormSet,
    ReplacementResolveForm,
    RoleForm,
    WEEKDAY_CHOICES,
    WeeklyAvailabilityForm,
    SwapAssignForm,
    SwapRequestForm,
    generate_unique_code,
)


@login_required
def dashboard(request):
    parish = request.active_parish
    if not parish:
        return render(request, "dashboard.html", {"missing_parish": True})
    upcoming = MassInstance.objects.filter(parish=parish, starts_at__gte=timezone.now()).order_by("starts_at")[:5]
    unfilled = AssignmentSlot.objects.filter(parish=parish, status="open", required=True).count()
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
@require_active_parish
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


def _parse_date(value, fallback):
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        return fallback


def _safe_next_url(request, fallback):
    next_url = request.GET.get("next") or request.POST.get("next")
    if next_url and url_has_allowed_host_and_scheme(next_url, allowed_hosts={request.get_host()}):
        return next_url
    return fallback


def _ensure_roster_slots(instances):
    instance_ids = [instance.id for instance in instances if instance.requirement_profile_id]
    if not instance_ids:
        return
    existing_keys = set(
        AssignmentSlot.objects.filter(mass_instance_id__in=instance_ids).values_list(
            "mass_instance_id",
            "position_type_id",
            "slot_index",
        )
    )
    to_create = []
    for instance in instances:
        if not instance.requirement_profile_id:
            continue
        for position in instance.requirement_profile.positions.all():
            for idx in range(1, position.quantity + 1):
                key = (instance.id, position.position_type_id, idx)
                if key in existing_keys:
                    continue
                to_create.append(
                    AssignmentSlot(
                        parish=instance.parish,
                        mass_instance=instance,
                        position_type=position.position_type,
                        slot_index=idx,
                        required=True,
                        status="open",
                    )
                )
                existing_keys.add(key)
    if to_create:
        AssignmentSlot.objects.bulk_create(to_create, ignore_conflicts=True)


def _build_roster_context(parish, start_date, end_date, community_id=None, kind=None):
    base_qs = (
        MassInstance.objects.filter(parish=parish, starts_at__date__range=(start_date, end_date))
        .select_related("community", "template", "event_series", "requirement_profile")
        .prefetch_related("requirement_profile__positions__position_type")
        .order_by("starts_at")
    )
    if community_id:
        base_qs = base_qs.filter(community_id=community_id)
    if kind == "event":
        base_qs = base_qs.exclude(event_series=None)
    elif kind == "template":
        base_qs = base_qs.exclude(template=None)

    instances = list(base_qs)
    _ensure_roster_slots(instances)
    instances = list(
        base_qs.prefetch_related(
            Prefetch(
                "slots",
                queryset=AssignmentSlot.objects.select_related("position_type").prefetch_related(
                    Prefetch(
                        "assignments",
                        queryset=Assignment.objects.filter(is_active=True).select_related("acolyte"),
                        to_attr="active_assignments",
                    )
                ),
            )
        )
    )

    column_keys = {}
    column_max_index = {}
    for instance in instances:
        for slot in instance.slots.all():
            key = (slot.position_type_id, slot.slot_index)
            column_keys[key] = slot.position_type
            column_max_index[slot.position_type_id] = max(
                column_max_index.get(slot.position_type_id, 0), slot.slot_index
            )

    def _col_sort(item):
        position = item[1]
        return (position.code or position.name, item[0][1])

    columns = []
    for key, position in sorted(column_keys.items(), key=_col_sort):
        suffix = ""
        if column_max_index.get(position.id, 0) > 1:
            suffix = f" {key[1]}"
        label = f"{position.code}{suffix}" if position.code else f"{position.name}{suffix}"
        columns.append({"key": key, "label": label, "title": position.name})

    days = []
    current_day = None
    day_bucket = None
    for instance in instances:
        instance_day = instance.starts_at.date()
        if instance_day != current_day:
            day_bucket = {"date": instance_day, "items": []}
            days.append(day_bucket)
            current_day = instance_day
        slot_map = {(slot.position_type_id, slot.slot_index): slot for slot in instance.slots.all()}
        day_bucket["items"].append({"instance": instance, "slot_map": slot_map})

    return {"days": days, "columns": columns, "instances": instances}


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def roster_view(request):
    parish = request.active_parish
    today = timezone.localdate()
    start_date = _parse_date(request.GET.get("start"), today)
    end_date = _parse_date(request.GET.get("end"), today + timedelta(days=13))
    community_id = request.GET.get("community") or ""
    kind = request.GET.get("kind") or ""

    saturday_offset = (5 - today.weekday()) % 7
    next_saturday = today + timedelta(days=saturday_offset)
    next_sunday = next_saturday + timedelta(days=1)

    presets = [
        {"label": "Proximo fim de semana", "start": next_saturday, "end": next_sunday},
        {"label": "Proximos 7 dias", "start": today, "end": today + timedelta(days=6)},
        {"label": "Proximos 14 dias", "start": today, "end": today + timedelta(days=13)},
        {
            "label": "Mes atual",
            "start": today.replace(day=1),
            "end": (today.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1),
        },
    ]

    context = _build_roster_context(
        parish,
        start_date,
        end_date,
        community_id=community_id or None,
        kind=kind or None,
    )
    communities = parish.community_set.filter(active=True).order_by("code")
    return render(
        request,
        "roster/index.html",
        {
            "parish": parish,
            "start_date": start_date,
            "end_date": end_date,
            "community_id": community_id,
            "kind": kind,
            "communities": communities,
            "presets": presets,
            "days": context["days"],
            "columns": context["columns"],
        },
    )


def _build_roster_lines(days, columns):
    lines = []
    for day in days:
        lines.append(day["date"].strftime("%a %d/%m"))
        for item in day["items"]:
            instance = item["instance"]
            line = f"{instance.starts_at.strftime('%H:%M')} {instance.community.code}"
            if instance.liturgy_label:
                line = f"{line} - {instance.liturgy_label}"
            cells = []
            for column in columns:
                slot = item["slot_map"].get(column["key"])
                if not slot:
                    cells.append(f"{column['label']}: N/A")
                    continue
                if instance.status == "canceled":
                    cells.append(f"{column['label']}: CANCELADA")
                    continue
                active = slot.get_active_assignment()
                if slot.externally_covered:
                    cells.append(f"{column['label']}: EXTERNO")
                elif not slot.required:
                    cells.append(f"{column['label']}: N/A")
                elif active:
                    cells.append(f"{column['label']}: {active.acolyte.display_name}")
                else:
                    cells.append(f"{column['label']}: ABERTO")
            lines.append(f"{line} — " + " | ".join(cells))
        lines.append("")
    return "\n".join(lines).strip()


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def roster_export_whatsapp(request):
    parish = request.active_parish
    today = timezone.localdate()
    start_date = _parse_date(request.GET.get("start"), today)
    end_date = _parse_date(request.GET.get("end"), today + timedelta(days=13))
    community_id = request.GET.get("community") or ""
    kind = request.GET.get("kind") or ""
    context = _build_roster_context(
        parish,
        start_date,
        end_date,
        community_id=community_id or None,
        kind=kind or None,
    )
    text = _build_roster_lines(context["days"], context["columns"])
    response = HttpResponse(text, content_type="text/plain; charset=utf-8")
    response["Content-Disposition"] = "inline; filename=escala.txt"
    return response


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def roster_export_pdf(request):
    parish = request.active_parish
    today = timezone.localdate()
    start_date = _parse_date(request.GET.get("start"), today)
    end_date = _parse_date(request.GET.get("end"), today + timedelta(days=13))
    community_id = request.GET.get("community") or ""
    kind = request.GET.get("kind") or ""
    context = _build_roster_context(
        parish,
        start_date,
        end_date,
        community_id=community_id or None,
        kind=kind or None,
    )
    lines = _build_roster_lines(context["days"], context["columns"]).splitlines()

    from io import BytesIO
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    y = height - 40
    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(40, y, parish.name)
    pdf.setFont("Helvetica", 10)
    pdf.drawString(40, y - 14, f"Periodo: {start_date.strftime('%d/%m/%Y')} a {end_date.strftime('%d/%m/%Y')}")
    pdf.drawString(40, y - 28, f"Gerado em {timezone.localtime(timezone.now()).strftime('%d/%m/%Y %H:%M')}")
    y -= 50

    for line in lines:
        if y < 60:
            pdf.showPage()
            y = height - 40
            pdf.setFont("Helvetica", 10)
        if not line.strip():
            y -= 8
            continue
        pdf.drawString(40, y, line)
        y -= 14

    pdf.save()
    buffer.seek(0)
    response = HttpResponse(buffer.read(), content_type="application/pdf")
    response["Content-Disposition"] = "inline; filename=escala.pdf"
    return response


@login_required
@require_active_parish
def mass_detail(request, instance_id):
    parish = request.active_parish
    instance = get_object_or_404(MassInstance, parish=parish, id=instance_id)
    sync_slots_for_instance(instance)
    slots_context = _build_slots_context(parish, instance)
    update_form = MassInstanceUpdateForm(instance=instance, parish=parish)
    move_form = MassInstanceMoveForm(
        parish=parish,
        initial={
            "starts_at": timezone.localtime(instance.starts_at).replace(tzinfo=None),
            "community": instance.community,
        },
    )
    cancel_form = MassInstanceCancelForm()
    show_conflict_modal = False
    conflict_acolyte = None
    current_slot = None
    new_slot = None
    if request.GET.get('show_conflict') == '1':
        acolyte_id = request.GET.get('acolyte_id')
        current_slot_id = request.GET.get('current_slot_id')
        new_slot_id = request.GET.get('new_slot_id')
        if acolyte_id and current_slot_id and new_slot_id:
            try:
                conflict_acolyte = parish.acolytes.get(id=acolyte_id)
                current_slot = instance.slots.get(id=current_slot_id)
                new_slot = instance.slots.get(id=new_slot_id)
                show_conflict_modal = True
            except:
                pass
    return render(
        request,
        "calendar/detail.html",
        {
            **slots_context,
            "update_form": update_form,
            "move_form": move_form,
            "cancel_form": cancel_form,
            "show_conflict_modal": show_conflict_modal,
            "conflict_acolyte": conflict_acolyte,
            "current_slot": current_slot,
            "new_slot": new_slot,
        },
    )


def _slot_candidate_list(parish, slot, query=None):
    qualified_ids = AcolyteQualification.objects.filter(
        parish=parish,
        position_type=slot.position_type,
        qualified=True,
    ).values_list("acolyte_id", flat=True)
    acolytes = parish.acolytes.filter(active=True, id__in=qualified_ids)
    if query:
        acolytes = acolytes.filter(display_name__icontains=query)
    return [acolyte for acolyte in acolytes if is_acolyte_available(acolyte, slot.mass_instance)]


def _build_slots_context(parish, instance):
    """Build the context needed for rendering the slots section."""
    slots = list(
        instance.slots.select_related("position_type").prefetch_related(
            Prefetch(
                "assignments",
                queryset=Assignment.objects.filter(is_active=True).select_related("acolyte"),
                to_attr="active_assignments",
            ),
        )
    )
    recent_assignments = (
        Assignment.objects.filter(slot__mass_instance=instance)
        .select_related("acolyte", "slot")
        .order_by("slot_id", "-created_at")
    )
    recent_by_slot = defaultdict(list)
    for assignment in recent_assignments:
        if len(recent_by_slot[assignment.slot_id]) < 5:
            recent_by_slot[assignment.slot_id].append(assignment)
    for slot in slots:
        slot.recent_assignments = recent_by_slot.get(slot.id, [])
    position_ids = {slot.position_type_id for slot in slots}
    quick_fill_cache = build_quick_fill_cache(parish, position_type_ids=position_ids)
    # Collect acolyte IDs already assigned in this mass to exclude from suggestions
    assigned_acolyte_ids = set()
    for slot in slots:
        for assignment in slot.active_assignments:
            assigned_acolyte_ids.add(assignment.acolyte_id)
    slot_suggestions = {
        slot.id: quick_fill_slot(
            slot, parish, max_candidates=3, cache=quick_fill_cache, exclude_acolyte_ids=assigned_acolyte_ids
        )
        for slot in slots
    }
    return {"instance": instance, "slots": slots, "slot_suggestions": slot_suggestions}


def _handle_slot_assign(request, instance_id, slot_id, action_label):
    parish = request.active_parish
    instance = get_object_or_404(MassInstance, parish=parish, id=instance_id)
    slot = get_object_or_404(AssignmentSlot, parish=parish, id=slot_id, mass_instance=instance)

    def _htmx_or_redirect():
        """Return partial for HTMX or redirect for normal requests."""
        if request.htmx:
            slots_context = _build_slots_context(parish, instance)
            # Ensure can_manage_parish is available (normally comes from context processor)
            slots_context["can_manage_parish"] = True  # User already passed role check via decorator
            return render(request, "calendar/_slots_section.html", slots_context)
        return redirect("mass_detail", instance_id=instance.id)

    if request.method == "POST":
        acolyte_id = request.POST.get("acolyte_id")
        if not acolyte_id:
            messages.error(request, "Selecione um acolito para atribuir.")
            return _htmx_or_redirect()
        acolyte = get_object_or_404(parish.acolytes, id=acolyte_id, active=True)
        if not AcolyteQualification.objects.filter(
            parish=parish, acolyte=acolyte, position_type=slot.position_type, qualified=True
        ).exists():
            messages.error(request, "Este acolito nao e qualificado para esta funcao.")
            return _htmx_or_redirect()
        if not is_acolyte_available(acolyte, slot.mass_instance):
            messages.error(request, "Este acolito nao esta disponivel para este horario.")
            return _htmx_or_redirect()
        try:
            assignment = assign_manual(slot, acolyte, actor=request.user)
        except ValueError as e:
            if len(e.args) > 1 and e.args[1] == "conflict":
                current_slot = e.args[2]
                url = reverse('mass_detail', kwargs={'instance_id': instance.id})
                url += f'?show_conflict=1&acolyte_id={acolyte.id}&current_slot_id={current_slot.id}&new_slot_id={slot.id}'
                if request.htmx:
                    response = HttpResponse()
                    response["HX-Redirect"] = url
                    return response
                return redirect(url)
            else:
                messages.error(request, str(e))
                return _htmx_or_redirect()
        except ConcurrentUpdateError:
            messages.error(request, "Esta vaga foi atualizada por outra acao. Recarregue a pagina e tente novamente.")
            return _htmx_or_redirect()
        if assignment.acolyte.user:
            enqueue_notification(
                parish,
                assignment.acolyte.user,
                "ASSIGNMENT_PUBLISHED",
                {"assignment_id": assignment.id},
                idempotency_key=f"manual:{assignment.id}",
            )
        messages.success(request, "Escala atribuida com sucesso.")
        return _htmx_or_redirect()

    query = request.GET.get("q", "").strip()
    candidates = _slot_candidate_list(parish, slot, query=query)
    return render(
        request,
        "calendar/assign_slot.html",
        {
            "instance": instance,
            "slot": slot,
            "candidates": candidates,
            "action_label": action_label,
            "query": query,
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def slot_assign(request, instance_id, slot_id):
    return _handle_slot_assign(request, instance_id, slot_id, "Atribuir")


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def slot_replace(request, instance_id, slot_id):
    return _handle_slot_assign(request, instance_id, slot_id, "Substituir")


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def mass_update(request, instance_id):
    parish = request.active_parish
    instance = get_object_or_404(MassInstance, parish=parish, id=instance_id)
    if request.method == "POST":
        form = MassInstanceUpdateForm(request.POST, instance=instance, parish=parish)
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
@require_active_parish
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
                messages.error(request, "Ja existe uma missa agendada nesse horario e comunidade.")
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
        else:
            messages.error(request, "Nao foi possivel mover esta missa. Verifique os campos.")
    return redirect("mass_detail", instance_id=instance.id)


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def mass_cancel(request, instance_id):
    parish = request.active_parish
    instance = get_object_or_404(MassInstance, parish=parish, id=instance_id)
    if request.method == "POST":
        form = MassInstanceCancelForm(request.POST)
        if form.is_valid():
            reason = form.cleaned_data.get("reason", "")
            cancel_mass_and_resolve_dependents(
                parish,
                instance,
                actor=request.user,
                notes=reason,
                reason_code="manual_cancel",
            )
    return redirect("mass_detail", instance_id=instance.id)


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def move_acolyte(request, instance_id, slot_id):
    """Move an acolyte from their current slot to a new slot in the same mass."""
    parish = request.active_parish
    instance = get_object_or_404(MassInstance, parish=parish, id=instance_id)
    slot = get_object_or_404(AssignmentSlot, parish=parish, mass_instance=instance, id=slot_id)
    
    # Only POST is allowed
    if request.method != "POST":
        messages.error(request, "Método não permitido.")
        return redirect("mass_detail", instance_id=instance.id)
    
    current_slot_id = request.POST.get("current_slot_id")
    acolyte_id = request.POST.get("acolyte_id")
    
    if not current_slot_id or not acolyte_id:
        messages.error(request, "Dados inválidos.")
        return redirect("mass_detail", instance_id=instance.id)
    
    current_slot = get_object_or_404(AssignmentSlot, parish=parish, id=current_slot_id)
    acolyte = get_object_or_404(parish.acolytes, id=acolyte_id, active=True)
    
    # Validate that both slots belong to the same mass
    if current_slot.mass_instance_id != instance.id:
        messages.error(request, "Slot atual não pertence a esta missa.")
        return redirect("mass_detail", instance_id=instance.id)
    
    # Validate that we're not moving to the same slot
    if current_slot.id == slot.id:
        messages.error(request, "Não é possível mover para o mesmo slot.")
        return redirect("mass_detail", instance_id=instance.id)
    
    try:
        from core.services.assignments import move_acolyte_to_slot
        assignment = move_acolyte_to_slot(current_slot, slot, acolyte, actor=request.user)
        
        # Send notification if acolyte has a user account
        if assignment.acolyte.user:
            enqueue_notification(
                parish,
                assignment.acolyte.user,
                "ASSIGNMENT_PUBLISHED",
                {"assignment_id": assignment.id},
                idempotency_key=f"move:{assignment.id}",
            )
        
        messages.success(request, "Acólito movido com sucesso.")
        
        # Return appropriate response based on request type
        if request.headers.get('HX-Request'):
            # HTMX request
            response = HttpResponse()
            response["HX-Redirect"] = redirect("mass_detail", instance_id=instance.id).url
            return response
        else:
            # Normal form submission
            return redirect("mass_detail", instance_id=instance.id)
            
    except ValueError as e:
        messages.error(request, str(e))
        return redirect("mass_detail", instance_id=instance.id)


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def template_list(request):
    parish = request.active_parish
    templates = MassTemplate.objects.filter(parish=parish)
    return render(request, "mass_templates/list.html", {"templates": templates})


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def template_create(request):
    parish = request.active_parish
    profiles_exist = RequirementProfile.objects.filter(parish=parish, active=True).exists()
    profile_create_url = f"{reverse('requirement_profile_create')}?{urlencode({'next': request.get_full_path()})}"
    if request.method == "POST":
        form = MassTemplateForm(request.POST, parish=parish)
        if form.is_valid():
            template = form.save(commit=False)
            template.parish = parish
            template.save()
            return redirect("template_list")
    else:
        form = MassTemplateForm(parish=parish)
    return render(
        request,
        "mass_templates/form.html",
        {"form": form, "profiles_exist": profiles_exist, "profile_create_url": profile_create_url},
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def structure_communities_list(request):
    parish = request.active_parish
    communities = Community.objects.filter(parish=parish).order_by("code")
    return render(request, "structure/communities_list.html", {"communities": communities})


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def structure_community_create(request):
    parish = request.active_parish
    if request.method == "POST":
        form = CommunityForm(request.POST, parish=parish)
        if form.is_valid():
            community = form.save(commit=False)
            community.parish = parish
            community.save()
            log_audit(parish, request.user, "Community", community.id, "create", {"code": community.code})
            return redirect(_safe_next_url(request, reverse("structure_communities_list")))
    else:
        form = CommunityForm(parish=parish)
    return render(
        request,
        "structure/communities_form.html",
        {"form": form, "next_url": _safe_next_url(request, "")},
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def structure_community_edit(request, community_id):
    parish = request.active_parish
    community = get_object_or_404(Community, parish=parish, id=community_id)
    if request.method == "POST":
        form = CommunityForm(request.POST, instance=community, parish=parish)
        if form.is_valid():
            form.save()
            log_audit(parish, request.user, "Community", community.id, "update", {"code": community.code})
            return redirect(_safe_next_url(request, reverse("structure_communities_list")))
    else:
        form = CommunityForm(instance=community, parish=parish)
    return render(
        request,
        "structure/communities_form.html",
        {"form": form, "community": community, "next_url": _safe_next_url(request, "")},
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def structure_community_toggle(request, community_id):
    parish = request.active_parish
    community = get_object_or_404(Community, parish=parish, id=community_id)
    if request.method == "POST":
        community.active = not community.active
        community.save(update_fields=["active", "updated_at"])
        log_audit(parish, request.user, "Community", community.id, "update", {"active": community.active})
    return redirect("structure_communities_list")


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def structure_roles_list(request):
    parish = request.active_parish
    show_inactive = request.GET.get("show_inactive") == "1"
    roles_qs = PositionType.objects.filter(parish=parish)
    if not show_inactive:
        roles_qs = roles_qs.filter(active=True)
    roles = (
        roles_qs
        .prefetch_related("functions")
        .order_by("code")
    )
    inactive_count = PositionType.objects.filter(parish=parish, active=False).count()
    return render(
        request,
        "structure/roles_list.html",
        {
            "roles": roles,
            "show_inactive": show_inactive,
            "inactive_count": inactive_count,
        },
    )


def _save_role_from_form(parish, form, position=None):
    name = form.cleaned_data["name"]
    code = form.cleaned_data.get("code") or generate_unique_code(
        parish, name, PositionType, max_len=10, exclude_id=position.id if position else None
    )
    active = bool(form.cleaned_data.get("active"))
    if position is None:
        position = PositionType.objects.create(parish=parish, code=code, name=name, active=active)
    else:
        position.code = code
        position.name = name
        position.active = active
        position.save(update_fields=["code", "name", "active", "updated_at"])

    primary, _ = FunctionType.objects.get_or_create(
        parish=parish,
        code=code,
        defaults={"name": name, "active": active},
    )
    update_fields = []
    if primary.name != name:
        primary.name = name
        update_fields.append("name")
    if primary.active != active:
        primary.active = active
        update_fields.append("active")
    if update_fields:
        primary.save(update_fields=update_fields + ["updated_at"])

    extra_ids = list(form.cleaned_data.get("extra_functions").values_list("id", flat=True))
    function_ids = list({primary.id, *extra_ids})
    PositionTypeFunction.objects.filter(position_type=position).exclude(function_type_id__in=function_ids).delete()
    for func_id in function_ids:
        PositionTypeFunction.objects.get_or_create(position_type=position, function_type_id=func_id)
    return position


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def structure_role_create(request):
    parish = request.active_parish
    if request.method == "POST":
        form = RoleForm(request.POST, parish=parish)
        if form.is_valid():
            role = _save_role_from_form(parish, form)
            log_audit(parish, request.user, "PositionType", role.id, "create", {"code": role.code})
            return redirect(_safe_next_url(request, reverse("structure_roles_list")))
    else:
        form = RoleForm(parish=parish)
    return render(
        request,
        "structure/roles_form.html",
        {"form": form, "next_url": _safe_next_url(request, "")},
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def structure_role_edit(request, role_id):
    parish = request.active_parish
    role = get_object_or_404(PositionType, parish=parish, id=role_id)
    if request.method == "POST":
        form = RoleForm(request.POST, parish=parish, position_type=role)
        if form.is_valid():
            role = _save_role_from_form(parish, form, position=role)
            log_audit(parish, request.user, "PositionType", role.id, "update", {"code": role.code})
            return redirect(_safe_next_url(request, reverse("structure_roles_list")))
    else:
        form = RoleForm(parish=parish, position_type=role)
    return render(
        request,
        "structure/roles_form.html",
        {"form": form, "role": role, "next_url": _safe_next_url(request, "")},
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def structure_role_toggle(request, role_id):
    parish = request.active_parish
    role = get_object_or_404(PositionType, parish=parish, id=role_id)
    if request.method == "POST":
        role.active = not role.active
        role.save(update_fields=["active", "updated_at"])
        log_audit(parish, request.user, "PositionType", role.id, "update", {"active": role.active})
    return redirect("structure_roles_list")


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def structure_role_delete(request, role_id):
    parish = request.active_parish
    role = get_object_or_404(PositionType, parish=parish, id=role_id)
    if request.method == "POST":
        if AssignmentSlot.objects.filter(position_type=role).exists():
            messages.error(
                request,
                "Nao e possivel excluir esta funcao porque existem escalas historicas. Desative a funcao se nao for mais usada.",
            )
            return redirect("structure_roles_list")
        role_id = role.id
        role.delete()
        log_audit(parish, request.user, "PositionType", role_id, "delete", {"code": role.code})
        messages.success(request, "Funcao excluida com sucesso.")
    return redirect("structure_roles_list")


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def structure_requirement_profiles_list(request):
    parish = request.active_parish
    profiles = (
        RequirementProfile.objects.filter(parish=parish)
        .annotate(position_count=Count("positions"))
        .order_by("name")
    )
    return render(request, "structure/requirement_profiles_list.html", {"profiles": profiles})


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def structure_requirement_profile_create(request):
    parish = request.active_parish
    profile = RequirementProfile(parish=parish)
    if request.method == "POST":
        form = RequirementProfileForm(request.POST, instance=profile)
        formset = RequirementProfilePositionFormSet(request.POST, instance=profile, parish=parish)
        if form.is_valid() and formset.is_valid():
            saved = form.save(commit=False)
            saved.parish = parish
            saved.save()
            formset.instance = saved
            formset.save()
            log_audit(parish, request.user, "RequirementProfile", saved.id, "create", {"name": saved.name})
            return redirect(_safe_next_url(request, reverse("structure_requirement_profiles_list")))
    else:
        form = RequirementProfileForm(instance=profile)
        formset = RequirementProfilePositionFormSet(instance=profile, parish=parish)
    return render(
        request,
        "structure/requirement_profiles_form.html",
        {
            "form": form,
            "formset": formset,
            "next_url": _safe_next_url(request, ""),
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def structure_requirement_profile_edit(request, profile_id):
    parish = request.active_parish
    profile = get_object_or_404(RequirementProfile, parish=parish, id=profile_id)
    if request.method == "POST":
        form = RequirementProfileForm(request.POST, instance=profile)
        formset = RequirementProfilePositionFormSet(request.POST, instance=profile, parish=parish)
        if form.is_valid() and formset.is_valid():
            saved = form.save()
            formset.save()
            log_audit(parish, request.user, "RequirementProfile", saved.id, "update", {"name": saved.name})
            return redirect(_safe_next_url(request, reverse("structure_requirement_profiles_list")))
    else:
        form = RequirementProfileForm(instance=profile)
        formset = RequirementProfilePositionFormSet(instance=profile, parish=parish)
    return render(
        request,
        "structure/requirement_profiles_form.html",
        {
            "form": form,
            "formset": formset,
            "profile": profile,
            "next_url": _safe_next_url(request, ""),
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def structure_requirement_profile_toggle(request, profile_id):
    parish = request.active_parish
    profile = get_object_or_404(RequirementProfile, parish=parish, id=profile_id)
    if request.method == "POST":
        profile.active = not profile.active
        profile.save(update_fields=["active", "updated_at"])
        log_audit(parish, request.user, "RequirementProfile", profile.id, "update", {"active": profile.active})
    return redirect("structure_requirement_profiles_list")


@login_required
@require_active_parish
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
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def event_series_list(request):
    parish = request.active_parish
    status = request.GET.get("status") or "active"
    base_qs = EventSeries.objects.filter(parish=parish)
    if status == "archived":
        base_qs = base_qs.filter(is_active=False)
    else:
        status = "active"
        base_qs = base_qs.filter(is_active=True)
    series = (
        base_qs
        .annotate(
            occurrence_count=Count("occurrences", distinct=True),
            mass_count=Count("massinstance", distinct=True),
        )
        .order_by("start_date")
    )
    counts = EventSeries.objects.filter(parish=parish).aggregate(
        active_count=Count("id", filter=Q(is_active=True)),
        archived_count=Count("id", filter=Q(is_active=False)),
    )
    return render(
        request,
        "events/list.html",
        {
            "series": series,
            "status": status,
            "active_count": counts["active_count"],
            "archived_count": counts["archived_count"],
        },
    )


@login_required
@require_active_parish
def event_interest(request):
    parish = request.active_parish
    acolyte = parish.acolytes.filter(user=request.user).first()
    series = EventSeries.objects.filter(parish=parish, is_active=True).order_by("start_date")
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
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def event_series_create(request):
    parish = request.active_parish
    profiles_exist = RequirementProfile.objects.filter(parish=parish, active=True).exists()
    profile_create_url = f"{reverse('requirement_profile_create')}?{urlencode({'next': request.get_full_path()})}"
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
    return render(
        request,
        "events/basics.html",
        {
            "form": form,
            "default_cancel_url": reverse("event_series_list"),
            "profiles_exist": profiles_exist,
            "profile_create_url": profile_create_url,
        },
    )


@login_required
@require_active_parish
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
    default_label = draft["title"] if start_date == end_date else ""
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
                ruleset_json={
                    "default_time": draft["default_time"],
                    "default_requirement_profile_id": draft.get("default_requirement_profile_id"),
                },
                created_by=request.user,
                updated_by=request.user,
                is_active=True,
            )
            log_audit(parish, request.user, "EventSeries", series.id, "create", {"title": series.title})
            occurrences = []
            for form in formset:
                data = form.cleaned_data
                label = data.get("label") or draft["title"]
                occurrence = EventOccurrence.objects.create(
                    parish=parish,
                    event_series=series,
                    date=data["date"],
                    time=data["time"],
                    community=data["community"],
                    requirement_profile=data["requirement_profile"],
                    label=label,
                    conflict_action=data["conflict_action"],
                    move_to_date=data.get("move_to_date"),
                    move_to_time=data.get("move_to_time"),
                    move_to_community=data.get("move_to_community"),
                )
                occurrences.append(occurrence)
            try:
                apply_event_occurrences(series, occurrences, actor=request.user)
            except ValueError as exc:
                messages.error(request, str(exc))
                conflicts = []
                for form in formset:
                    data = form.cleaned_data
                    conflict = MassInstance.objects.filter(
                        parish=parish,
                        community=data.get("community"),
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
                    "label": default_label,
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
            "series": None,
            "default_label": default_label,
            "default_time": draft.get("default_time"),
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def event_series_detail(request, series_id):
    parish = request.active_parish
    series = get_object_or_404(EventSeries, parish=parish, id=series_id)
    occurrences = (
        series.occurrences.select_related("community", "requirement_profile")
        .order_by("date", "time")
    )
    masses = (
        MassInstance.objects.filter(parish=parish, event_series=series)
        .select_related("community", "requirement_profile")
        .order_by("starts_at")
    )
    default_profile_id = (series.ruleset_json or {}).get("default_requirement_profile_id")
    default_profile = None
    if default_profile_id:
        default_profile = RequirementProfile.objects.filter(parish=parish, id=default_profile_id).first()
    default_time = (series.ruleset_json or {}).get("default_time")
    return render(
        request,
        "events/detail.html",
        {
            "series": series,
            "occurrences": occurrences,
            "masses": masses,
            "default_time": default_time,
            "default_profile": default_profile,
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def event_series_edit(request, series_id):
    parish = request.active_parish
    series = get_object_or_404(EventSeries, parish=parish, id=series_id)
    type_values = {choice[0] for choice in EventSeriesBasicsForm.SERIES_TYPE_CHOICES}

    # Verificar se existem perfis de requisitos
    profiles_exist = RequirementProfile.objects.filter(parish=parish, active=True).exists()
    profile_create_url = f"{reverse('requirement_profile_create')}?{urlencode({'next': request.get_full_path()})}"

    initial = {
        "title": series.title,
        "start_date": series.start_date,
        "end_date": series.end_date,
        "default_community": series.default_community_id,
        "candidate_pool": series.candidate_pool,
    }
    default_time = (series.ruleset_json or {}).get("default_time")
    if default_time:
        initial["default_time"] = datetime.strptime(default_time, "%H:%M").time()
    default_profile_id = (series.ruleset_json or {}).get("default_requirement_profile_id")
    if default_profile_id:
        initial["default_requirement_profile"] = default_profile_id

    if series.series_type in type_values:
        initial["series_type"] = series.series_type
    else:
        initial["series_type"] = "Outro"
        initial["series_type_other"] = series.series_type

    if request.method == "POST":
        form = EventSeriesBasicsForm(request.POST, parish=parish)
        if form.is_valid():
            cleaned = form.cleaned_data
            series.series_type = cleaned["series_type"]
            series.title = cleaned["title"]
            series.start_date = cleaned["start_date"]
            series.end_date = cleaned["end_date"]
            series.default_community = cleaned.get("default_community")
            series.candidate_pool = cleaned["candidate_pool"]
            ruleset = series.ruleset_json or {}
            ruleset["default_time"] = cleaned["default_time"].strftime("%H:%M")
            ruleset["default_requirement_profile_id"] = (
                cleaned["default_requirement_profile"].id if cleaned.get("default_requirement_profile") else None
            )
            series.ruleset_json = ruleset
            series.updated_by = request.user
            series.save()
            log_audit(parish, request.user, "EventSeries", series.id, "update", {"title": series.title})
            messages.success(request, "Serie atualizada com sucesso.")
            return redirect("event_series_detail", series_id=series.id)
    else:
        form = EventSeriesBasicsForm(initial=initial, parish=parish)
    return render(
        request,
        "events/basics.html",
        {
            "form": form,
            "page_title": "Editar celebracao",
            "heading": "Editar celebracao",
            "submit_label": "Salvar",
            "cancel_url": reverse("event_series_detail", args=[series.id]),
            "default_cancel_url": reverse("event_series_list"),
            "profiles_exist": profiles_exist,
            "profile_create_url": profile_create_url,
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def event_series_occurrences(request, series_id):
    parish = request.active_parish
    series = get_object_or_404(EventSeries, parish=parish, id=series_id)
    default_time = (series.ruleset_json or {}).get("default_time")
    default_time_value = datetime.strptime(default_time, "%H:%M").time() if default_time else time(19, 0)
    default_profile_id = (series.ruleset_json or {}).get("default_requirement_profile_id")

    dates = []
    current = series.start_date
    while current <= series.end_date:
        dates.append(current)
        current += timedelta(days=1)

    occurrence_map = {occ.date: occ for occ in series.occurrences.all()}
    OccurrenceFormSet = formset_factory(EventOccurrenceForm, extra=0)
    if request.method == "POST":
        formset = OccurrenceFormSet(request.POST, form_kwargs={"parish": parish})
        if formset.is_valid():
            occurrences = []
            for form in formset:
                data = form.cleaned_data
                label = data.get("label") or series.title
                existing = occurrence_map.get(data["date"])
                if existing:
                    existing.time = data["time"]
                    existing.community = data["community"]
                    existing.requirement_profile = data["requirement_profile"]
                    existing.label = label
                    existing.conflict_action = data["conflict_action"]
                    existing.move_to_date = data.get("move_to_date")
                    existing.move_to_time = data.get("move_to_time")
                    existing.move_to_community = data.get("move_to_community")
                    existing.save()
                    occurrences.append(existing)
                else:
                    occurrences.append(
                        EventOccurrence.objects.create(
                            parish=parish,
                            event_series=series,
                            date=data["date"],
                            time=data["time"],
                            community=data["community"],
                            requirement_profile=data["requirement_profile"],
                            label=label,
                            conflict_action=data["conflict_action"],
                            move_to_date=data.get("move_to_date"),
                            move_to_time=data.get("move_to_time"),
                            move_to_community=data.get("move_to_community"),
                        )
                    )
            try:
                apply_event_occurrences(series, occurrences, actor=request.user)
            except ValueError as exc:
                messages.error(request, str(exc))
                conflicts = []
                for form in formset:
                    data = form.cleaned_data
                    conflict = MassInstance.objects.filter(
                        parish=parish,
                        community=data.get("community"),
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
                        "draft": None,
                        "series": series,
                        "default_label": series.title,
                        "default_time": default_time_value.strftime("%H:%M"),
                    },
                )
            log_audit(parish, request.user, "EventSeries", series.id, "update", {"occurrences": len(occurrences)})
            messages.success(request, "Ocorrencias atualizadas.")
            return redirect("event_series_detail", series_id=series.id)
    else:
        initial = []
        for date_value in dates:
            existing = occurrence_map.get(date_value)
            initial.append(
                {
                    "date": date_value,
                    "time": existing.time if existing else default_time_value,
                    "community": existing.community_id if existing else series.default_community_id,
                    "requirement_profile": existing.requirement_profile_id if existing else default_profile_id,
                    "label": existing.label if existing else series.title,
                    "conflict_action": existing.conflict_action if existing else "keep",
                    "move_to_date": existing.move_to_date if existing else None,
                    "move_to_time": existing.move_to_time if existing else None,
                    "move_to_community": existing.move_to_community_id if existing else None,
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
            "draft": None,
            "series": series,
            "default_label": series.title,
            "default_time": default_time_value.strftime("%H:%M"),
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def event_series_archive(request, series_id):
    parish = request.active_parish
    series = get_object_or_404(EventSeries, parish=parish, id=series_id)
    if request.method != "POST":
        return redirect("event_series_detail", series_id=series.id)
    series.is_active = False
    series.updated_by = request.user
    series.save(update_fields=["is_active", "updated_by", "updated_at"])
    log_audit(parish, request.user, "EventSeries", series.id, "archive", {"title": series.title})
    messages.success(request, "Serie arquivada. As missas geradas permanecem no historico.")
    return redirect(f"{reverse('event_series_list')}?status=archived")


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def event_series_unarchive(request, series_id):
    parish = request.active_parish
    series = get_object_or_404(EventSeries, parish=parish, id=series_id)
    if request.method != "POST":
        return redirect("event_series_detail", series_id=series.id)
    series.is_active = True
    series.updated_by = request.user
    series.save(update_fields=["is_active", "updated_by", "updated_at"])
    log_audit(parish, request.user, "EventSeries", series.id, "unarchive", {"title": series.title})
    messages.success(request, "Serie reativada.")
    return redirect("event_series_detail", series_id=series.id)


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def event_series_delete(request, series_id):
    parish = request.active_parish
    series = get_object_or_404(EventSeries, parish=parish, id=series_id)
    if request.method != "POST":
        return redirect("event_series_detail", series_id=series.id)
    from core.services.event_series import delete_event_series_with_masses

    delete_event_series_with_masses(parish, series, actor=request.user)
    messages.success(request, "Serie excluida. Missas e escalas relacionadas foram removidas.")
    return redirect("event_series_list")


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def scheduling_dashboard(request):
    parish = request.active_parish
    jobs = ScheduleJobRequest.objects.filter(parish=parish).order_by("-created_at")[:10]
    if request.method == "POST":
        if request.POST.get("action") == "run":
            ScheduleJobRequest.objects.create(parish=parish, requested_by=request.user, horizon_days=parish.horizon_days)
        return redirect("scheduling_dashboard")

    today = timezone.now().date()
    default_end = today + timedelta(days=parish.consolidation_days - 1)
    start_date = date.fromisoformat(request.GET.get("start_date")) if request.GET.get("start_date") else today
    end_date = date.fromisoformat(request.GET.get("end_date")) if request.GET.get("end_date") else default_end

    publish_presets = [
        {"label": "Proximos 14 dias", "start": today, "end": today + timedelta(days=13)},
        {"label": "Proximos 30 dias", "start": today, "end": today + timedelta(days=29)},
        {"label": "Horizonte completo", "start": today, "end": today + timedelta(days=parish.horizon_days - 1)},
    ]
    consolidation_end = timezone.now() + timedelta(days=parish.consolidation_days)
    urgent_open = AssignmentSlot.objects.filter(
        parish=parish,
        status="open",
        required=True,
        mass_instance__starts_at__lte=consolidation_end,
    ).count()
    pending_confirmations = Confirmation.objects.filter(
        parish=parish,
        status="pending",
        assignment__is_active=True,
        assignment__slot__mass_instance__starts_at__lte=consolidation_end,
    ).count()
    return render(
        request,
        "scheduling/dashboard.html",
        {
            "jobs": jobs,
            "urgent_open": urgent_open,
            "pending_confirmations": pending_confirmations,
            "now": timezone.now(),
            "start_date": start_date,
            "end_date": end_date,
            "publish_presets": publish_presets,
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def scheduling_publish_preview(request):
    if request.method != "POST":
        return redirect("scheduling_dashboard")
    parish = request.active_parish
    try:
        start_date = date.fromisoformat(request.POST.get("start_date"))
        end_date = date.fromisoformat(request.POST.get("end_date"))
    except (TypeError, ValueError):
        messages.error(request, "Informe um intervalo valido para publicacao.")
        return redirect("scheduling_dashboard")
    if end_date < start_date:
        messages.error(request, "A data final precisa ser maior ou igual a data inicial.")
        return redirect("scheduling_dashboard")

    assignments = Assignment.objects.filter(
        parish=parish,
        is_active=True,
        slot__mass_instance__starts_at__date__gte=start_date,
        slot__mass_instance__starts_at__date__lte=end_date,
    )
    to_publish = assignments.filter(assignment_state="proposed")
    open_slots = AssignmentSlot.objects.filter(
        parish=parish,
        status="open",
        required=True,
        mass_instance__starts_at__date__gte=start_date,
        mass_instance__starts_at__date__lte=end_date,
    )
    acolytes_count = to_publish.values("acolyte_id").distinct().count()
    emails_count = to_publish.filter(acolyte__user__email__isnull=False).exclude(acolyte__user__email="").count()

    return render(
        request,
        "scheduling/publish_preview.html",
        {
            "start_date": start_date,
            "end_date": end_date,
            "assignments_count": to_publish.count(),
            "open_slots_count": open_slots.count(),
            "acolytes_count": acolytes_count,
            "emails_count": emails_count,
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def scheduling_publish_apply(request):
    if request.method != "POST":
        return redirect("scheduling_dashboard")
    parish = request.active_parish
    try:
        start_date = date.fromisoformat(request.POST.get("start_date"))
        end_date = date.fromisoformat(request.POST.get("end_date"))
    except (TypeError, ValueError):
        messages.error(request, "Informe um intervalo valido para publicacao.")
        return redirect("scheduling_dashboard")
    if end_date < start_date:
        messages.error(request, "A data final precisa ser maior ou igual a data inicial.")
        return redirect("scheduling_dashboard")
    published = publish_assignments(parish, start_date, end_date, actor=request.user)
    messages.success(request, f"{published} escalas publicadas.")
    return redirect("scheduling_dashboard")


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def schedule_job_detail(request, job_id):
    parish = request.active_parish
    job = get_object_or_404(ScheduleJobRequest, parish=parish, id=job_id)
    summary = job.summary_json or {}
    unfilled_details = summary.get("unfilled_details") or []
    community_map = {
        community.id: community
        for community in parish.community_set.filter(id__in=[item.get("community_id") for item in unfilled_details])
    }
    position_ids = [item.get("position_type_id") for item in unfilled_details]
    position_map = {
        position.id: position
        for position in parish.positiontype_set.filter(id__in=position_ids)
    }
    formatted_unfilled = []
    for item in unfilled_details:
        starts_at = item.get("starts_at")
        try:
            starts_at = timezone.localtime(datetime.fromisoformat(starts_at))
            starts_label = starts_at.strftime("%d/%m %H:%M")
        except (TypeError, ValueError):
            starts_label = starts_at or "-"
        formatted_unfilled.append(
            {
                "starts_at": starts_label,
                "community": community_map.get(item.get("community_id")),
                "position": position_map.get(item.get("position_type_id")),
                "slot_index": item.get("slot_index"),
            }
        )
    return render(
        request,
        "scheduling/job_detail.html",
        {"job": job, "summary": summary, "unfilled": formatted_unfilled},
    )


def _audit_summary(event):
    if event.entity_type == "MassInstance":
        if event.action_type == "create":
            return "Missa criada"
        if event.action_type == "cancel":
            return "Missa cancelada"
        if event.action_type == "move":
            return "Missa movida"
        if event.action_type == "update":
            return "Missa atualizada"
    if event.entity_type == "Assignment":
        if event.action_type == "create":
            return "Escala criada"
        if event.action_type == "assign":
            return "Escala atribuida"
        if event.action_type == "manual_assign":
            return "Escala atribuida manualmente"
        if event.action_type == "publish":
            return "Escala publicada"
        if event.action_type == "deactivate":
            return "Escala desativada"
        if event.action_type == "replace":
            return "Escala substituida"
    if event.entity_type == "SwapRequest":
        if event.action_type == "create":
            return "Troca solicitada"
        if event.action_type == "apply":
            return "Troca aplicada"
        if event.action_type == "update":
            return "Troca atualizada"
    if event.entity_type == "ReplacementRequest":
        if event.action_type == "create":
            return "Substituicao aberta"
        if event.action_type == "assign":
            return "Substituicao atribuida"
        if event.action_type == "update":
            return "Substituicao atualizada"
    if event.entity_type == "AcolyteProfile" and event.action_type == "link_user":
        return "Acolito vinculado a usuario"
    if event.entity_type == "Consolidation" and event.action_type == "lock":
        return "Consolidacao aplicada"
    if event.entity_type == "Confirmation" and event.action_type == "update":
        return "Confirmacao atualizada"
    if event.entity_type == "Parish" and event.action_type == "update":
        return "Configuracoes atualizadas"
    return f"{event.entity_type} {event.action_type}"


def _audit_link(event):
    if event.entity_type == "MassInstance":
        try:
            return "mass_detail", int(event.entity_id)
        except (TypeError, ValueError):
            return None
    if event.entity_type == "SwapRequest":
        return "swap_requests", None
    if event.entity_type == "ReplacementRequest":
        return "replacement_center", None
    if event.entity_type == "Parish":
        return "parish_settings", None
    return None


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def audit_log(request):
    parish = request.active_parish
    entity_type = request.GET.get("entity") or ""
    action_type = request.GET.get("action") or ""
    start_date = request.GET.get("start") or ""
    end_date = request.GET.get("end") or ""

    events = AuditEvent.objects.filter(parish=parish).select_related("actor_user").order_by("-timestamp")
    if entity_type:
        events = events.filter(entity_type=entity_type)
    if action_type:
        events = events.filter(action_type=action_type)
    if start_date:
        try:
            start = timezone.make_aware(datetime.combine(date.fromisoformat(start_date), time.min))
            events = events.filter(timestamp__gte=start)
        except ValueError:
            messages.error(request, "Data inicial invalida.")
    if end_date:
        try:
            end = timezone.make_aware(datetime.combine(date.fromisoformat(end_date), time.max))
            events = events.filter(timestamp__lte=end)
        except ValueError:
            messages.error(request, "Data final invalida.")

    paginator = Paginator(events, 50)
    page_obj = paginator.get_page(request.GET.get("page"))
    rows = []
    for event in page_obj:
        link = _audit_link(event)
        rows.append(
            {
                "event": event,
                "summary": _audit_summary(event),
                "link": link,
            }
        )
    return render(
        request,
        "audit/list.html",
        {
            "page_obj": page_obj,
            "rows": rows,
            "entity_type": entity_type,
            "action_type": action_type,
            "start_date": start_date,
            "end_date": end_date,
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def reports_frequency(request):
    parish = request.active_parish
    today = timezone.localdate()
    start_date = _parse_date(request.GET.get("start"), today - timedelta(days=29))
    end_date = _parse_date(request.GET.get("end"), today)

    assignments = (
        Assignment.objects.filter(
            parish=parish,
            assignment_state__in=["published", "locked"],
            slot__mass_instance__starts_at__date__range=(start_date, end_date),
        )
        .filter(created_at__lte=F("slot__mass_instance__starts_at"))
        .filter(Q(ended_at__isnull=True) | Q(ended_at__gte=F("slot__mass_instance__starts_at")))
    )
    counts = {row["acolyte_id"]: row["total"] for row in assignments.values("acolyte_id").annotate(total=Count("id"))}
    stats_map = {stat.acolyte_id: stat for stat in AcolyteStats.objects.filter(parish=parish)}

    rows = []
    for acolyte in parish.acolytes.filter(active=True).order_by("display_name"):
        stat = stats_map.get(acolyte.id)
        total = counts.get(acolyte.id, 0)
        rows.append(
            {
                "acolyte": acolyte,
                "total": total,
                "services_30": stat.services_last_30_days if stat else 0,
                "services_90": stat.services_last_90_days if stat else 0,
                "reliability": stat.reliability_score if stat else 0,
                "credit": stat.credit_balance if stat else 0,
                "has_user": bool(acolyte.user_id),
            }
        )

    max_total = max([row["total"] for row in rows], default=0) or 1
    avg_total = sum(row["total"] for row in rows) / len(rows) if rows else 0

    return render(
        request,
        "reports/frequency.html",
        {
            "rows": rows,
            "start_date": start_date,
            "end_date": end_date,
            "max_total": max_total,
            "avg_total": avg_total,
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def replacement_center(request):
    parish = request.active_parish
    now = timezone.now()
    consolidation_end = now + timedelta(days=parish.consolidation_days)
    replacements_qs = (
        ReplacementRequest.objects.filter(parish=parish, status="pending")
        .select_related("slot__mass_instance__community", "slot__mass_instance", "slot__position_type")
        .order_by("slot__mass_instance__starts_at")
    )
    pending_in_window = replacements_qs.filter(slot__mass_instance__starts_at__lte=consolidation_end).count()
    replacements = list(replacements_qs)
    open_slots = AssignmentSlot.objects.filter(
        parish=parish,
        status="open",
        required=True,
        mass_instance__starts_at__lte=consolidation_end,
    ).count()
    pending_confirmations = Confirmation.objects.filter(
        parish=parish,
        status="pending",
        assignment__is_active=True,
        assignment__slot__mass_instance__starts_at__lte=consolidation_end,
    ).count()
    if replacements:
        position_ids = {item.slot.position_type_id for item in replacements}
        quick_fill_cache = build_quick_fill_cache(parish, position_type_ids=position_ids)
        suggestions = {
            item.id: quick_fill_slot(item.slot, parish, max_candidates=3, cache=quick_fill_cache)
            for item in replacements
        }
    else:
        suggestions = {}
    return render(
        request,
        "replacements/center.html",
        {
            "replacements": replacements,
            "pending_in_window": pending_in_window,
            "open_slots": open_slots,
            "pending_confirmations": pending_confirmations,
            "suggestions": suggestions,
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def replacement_assign(request, request_id):
    parish = request.active_parish
    replacement = get_object_or_404(ReplacementRequest, parish=parish, id=request_id, status="pending")
    if request.method == "POST":
        acolyte_id = request.POST.get("acolyte_id")
        acolyte = get_object_or_404(parish.acolytes, id=acolyte_id, active=True)
        try:
            assignment = assign_replacement_request(parish, replacement.id, acolyte, actor=request.user)
        except (ConcurrentUpdateError, ValueError):
            messages.error(request, "Esta vaga foi atualizada por outra acao. Recarregue a pagina e tente novamente.")
            return redirect("replacement_center")
        if assignment:
            if assignment.acolyte.user:
                enqueue_notification(
                    parish,
                    assignment.acolyte.user,
                    "REPLACEMENT_ASSIGNED",
                    {"assignment_id": assignment.id},
                    idempotency_key=f"replacement:{assignment.id}",
                )
            messages.success(request, "Substituicao atribuida com sucesso.")
        else:
            messages.error(request, "Nao foi possivel atribuir este acolito.")
        return redirect("replacement_center")
    return redirect("replacement_center")


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def replacement_resolve(request, request_id):
    parish = request.active_parish
    replacement = get_object_or_404(ReplacementRequest, parish=parish, id=request_id, status="pending")
    if request.method == "POST":
        form = ReplacementResolveForm(request.POST)
        if form.is_valid():
            reason = form.cleaned_data["resolution_type"]
            notes = form.cleaned_data.get("notes", "")
            slot = replacement.slot
            instance = slot.mass_instance
            if reason == "mass_canceled":
                if not form.cleaned_data.get("confirm_cancel_mass"):
                    messages.error(request, "Confirme o cancelamento da missa para continuar.")
                    return render(request, "replacements/resolve.html", {"replacement": replacement, "form": form})
                cancel_mass_and_resolve_dependents(
                    parish,
                    instance,
                    actor=request.user,
                    notes=notes,
                    reason_code="replacement_resolve",
                )
            elif reason == "slot_not_required":
                slot.required = False
                slot.externally_covered = False
                slot.external_coverage_notes = ""
                slot.status = "finalized"
                slot.save(
                    update_fields=["required", "externally_covered", "external_coverage_notes", "status", "updated_at"]
                )
            elif reason == "covered_externally":
                slot.required = False
                slot.externally_covered = True
                slot.external_coverage_notes = notes
                slot.status = "finalized"
                slot.save(
                    update_fields=["required", "externally_covered", "external_coverage_notes", "status", "updated_at"]
                )
            else:
                slot.required = True
                slot.externally_covered = False
                slot.external_coverage_notes = ""
                slot.status = "open"
                slot.save(
                    update_fields=[
                        "required",
                        "externally_covered",
                        "external_coverage_notes",
                        "status",
                        "updated_at",
                    ]
                )

            replacement.status = "resolved"
            replacement.resolved_reason = reason
            replacement.resolved_notes = notes
            replacement.resolved_at = timezone.now()
            replacement.save(update_fields=["status", "resolved_reason", "resolved_notes", "resolved_at", "updated_at"])
            log_audit(parish, request.user, "ReplacementRequest", replacement.id, "update", {"status": "resolved", "reason": reason})
            messages.success(request, "Substituicao marcada como resolvida.")
            return redirect("replacement_center")
    else:
        form = ReplacementResolveForm()
    return render(request, "replacements/resolve.html", {"replacement": replacement, "form": form})


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def replacement_pick(request, request_id):
    parish = request.active_parish
    replacement = get_object_or_404(ReplacementRequest, parish=parish, id=request_id, status="pending")
    query = request.GET.get("q", "").strip()
    candidates = _slot_candidate_list(parish, replacement.slot, query=query)
    return render(
        request,
        "replacements/pick.html",
        {
            "replacement": replacement,
            "candidates": candidates,
            "query": query,
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def acolyte_list(request):
    return redirect("people_directory")


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def acolyte_link(request):
    return redirect("people_create")


def _build_people_members(parish, filters):
    search = filters.get("q", "").strip()
    community_id = filters.get("community")
    experience = filters.get("experience")
    status = filters.get("status")
    role = filters.get("role")

    memberships = ParishMembership.objects.filter(parish=parish).select_related("user").prefetch_related("roles")
    if role:
        memberships = memberships.filter(roles__code=role)
    if status in {"active", "inactive"}:
        memberships = memberships.filter(user__is_active=(status == "active"))
    if search:
        memberships = memberships.filter(
            Q(user__full_name__icontains=search)
            | Q(user__email__icontains=search)
            | Q(user__phone__icontains=search)
        )

    acolytes = (
        AcolyteProfile.objects.filter(parish=parish)
        .select_related("user", "community_of_origin", "family_group")
        .prefetch_related(
            Prefetch(
                "acolytequalification_set",
                queryset=AcolyteQualification.objects.select_related("position_type").filter(qualified=True),
            )
        )
    )
    if community_id:
        acolytes = acolytes.filter(community_of_origin_id=community_id)
    if experience:
        acolytes = acolytes.filter(experience_level=experience)
    if status in {"active", "inactive"}:
        acolytes = acolytes.filter(active=(status == "active"))
    if search:
        acolytes = acolytes.filter(
            Q(display_name__icontains=search)
            | Q(user__full_name__icontains=search)
            | Q(user__email__icontains=search)
            | Q(user__phone__icontains=search)
        )

    stats_map = {}
    acolyte_ids = list(acolytes.values_list("id", flat=True))
    if acolyte_ids:
        stats_map = {
            stat.acolyte_id: stat
            for stat in AcolyteStats.objects.filter(parish=parish, acolyte_id__in=acolyte_ids)
        }

    acolytes_by_user = {ac.user_id: ac for ac in acolytes if ac.user_id}
    member_rows = []
    membership_user_ids = set(memberships.values_list("user_id", flat=True))
    for membership in memberships:
        user = membership.user
        acolyte = acolytes_by_user.get(user.id)
        qualifications = []
        stats = None
        if acolyte:
            quals = list(acolyte.acolytequalification_set.all())
            qualifications = [qual.position_type.name for qual in quals]
            stats = stats_map.get(acolyte.id)
        member_rows.append(
            {
                "user": user,
                "acolyte": acolyte,
                "name": acolyte.display_name if acolyte else user.full_name,
                "roles": list(membership.roles.all()),
                "experience": acolyte.experience_level if acolyte else None,
                "qualifications": qualifications,
                "stats": stats,
                "membership_active": membership.active,
                "detail_url": reverse("people_acolyte_detail", args=[acolyte.id])
                if acolyte
                else reverse("people_user_detail", args=[user.id]),
            }
        )

    for acolyte in acolytes:
        if acolyte.user_id and acolyte.user_id in membership_user_ids:
            continue
        stats = stats_map.get(acolyte.id)
        quals = list(acolyte.acolytequalification_set.all())
        member_rows.append(
            {
                "user": acolyte.user,
                "acolyte": acolyte,
                "name": acolyte.display_name,
                "roles": [],
                "experience": acolyte.experience_level,
                "qualifications": [qual.position_type.name for qual in quals],
                "stats": stats,
                "membership_active": False,
                "detail_url": reverse("people_acolyte_detail", args=[acolyte.id]),
            }
        )

    member_rows.sort(key=lambda row: (row["name"] or "").lower())
    return member_rows


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def people_directory(request):
    parish = request.active_parish
    filters = {
        "q": request.GET.get("q", ""),
        "community": request.GET.get("community"),
        "experience": request.GET.get("experience"),
        "status": request.GET.get("status"),
        "role": request.GET.get("role"),
    }
    members = _build_people_members(parish, filters)
    communities = Community.objects.filter(parish=parish, active=True).order_by("name")
    roles = MembershipRole.objects.filter(code__in=AcolyteLinkForm.ALLOWED_ROLE_CODES).order_by("code")
    return render(
        request,
        "people/directory.html",
        {
            "members": members,
            "communities": communities,
            "roles": roles,
            "experience_choices": AcolyteProfile.EXPERIENCE_CHOICES,
            "filters": filters,
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def people_create(request):
    parish = request.active_parish
    User = get_user_model()
    if request.method == "POST":
        form = PeopleCreateForm(request.POST, parish=parish)
        if form.is_valid():
            full_name = form.cleaned_data["full_name"]
            phone = form.cleaned_data.get("phone", "")
            has_login = form.cleaned_data.get("has_login")
            email = form.cleaned_data.get("email")
            password = form.cleaned_data.get("password")
            send_invite = form.cleaned_data.get("send_invite")
            is_acolyte = form.cleaned_data.get("is_acolyte")
            community = form.cleaned_data.get("community_of_origin")
            experience = form.cleaned_data.get("experience_level")
            scheduling_mode = form.cleaned_data.get("scheduling_mode") or "normal"
            family_group = form.cleaned_data.get("family_group")
            notes = form.cleaned_data.get("notes")
            acolyte_active = form.cleaned_data.get("acolyte_active")
            has_admin_access = form.cleaned_data.get("has_admin_access")
            roles = form.cleaned_data.get("roles") or []
            qualifications = form.cleaned_data.get("qualifications") or []

            user = None
            generated_password = None
            if has_login and email:
                user = User.objects.filter(email=email).first()
                if not user:
                    if not password:
                        generated_password = User.objects.make_random_password()
                        password = generated_password
                    user = User.objects.create_user(email=email, full_name=full_name, password=password, phone=phone)
                else:
                    updates = []
                    if full_name and user.full_name != full_name:
                        user.full_name = full_name
                        updates.append("full_name")
                    if phone and user.phone != phone:
                        user.phone = phone
                        updates.append("phone")
                    if updates:
                        user.save(update_fields=updates + ["updated_at"])

            membership = None
            if user:
                membership, _ = ParishMembership.objects.get_or_create(
                    parish=parish, user=user, defaults={"active": True}
                )
                if not membership.active:
                    membership.active = True
                    membership.save(update_fields=["active", "updated_at"])
                if has_admin_access and roles:
                    allowed_ids = set(
                        MembershipRole.objects.filter(code__in=AcolyteLinkForm.ALLOWED_ROLE_CODES).values_list(
                            "id", flat=True
                        )
                    )
                    membership.roles.add(*[role for role in roles if role.id in allowed_ids])

            acolyte = None
            if is_acolyte:
                if user and parish.acolytes.filter(user=user).exists():
                    acolyte = parish.acolytes.filter(user=user).first()
                else:
                    acolyte = AcolyteProfile(parish=parish)
                acolyte.display_name = full_name
                acolyte.user = user if user else None
                acolyte.community_of_origin = community
                acolyte.experience_level = experience
                acolyte.scheduling_mode = scheduling_mode
                acolyte.family_group = family_group
                acolyte.notes = notes or ""
                acolyte.active = True if acolyte_active is None else bool(acolyte_active)
                acolyte.save()

                if user:
                    acolyte_role = MembershipRole.objects.filter(code="ACOLYTE").first()
                    if membership and acolyte_role:
                        membership.roles.add(acolyte_role)

                existing_ids = set(
                    AcolyteQualification.objects.filter(parish=parish, acolyte=acolyte).values_list(
                        "position_type_id", flat=True
                    )
                )
                for position in qualifications:
                    if position.id not in existing_ids:
                        AcolyteQualification.objects.create(
                            parish=parish,
                            acolyte=acolyte,
                            position_type=position,
                            qualified=True,
                        )

            if user and (send_invite or generated_password):
                if password:
                    send_mail(
                        "Acesso ao Acoli",
                        f"Seu acesso foi criado.\\nEmail: {user.email}\\nSenha: {password}\\n",
                        settings.DEFAULT_FROM_EMAIL,
                        [user.email],
                    )

            messages.success(request, "Pessoa criada com sucesso.")
            return redirect("people_directory")
    else:
        form = PeopleCreateForm(parish=parish)
    return render(request, "people/create.html", {"form": form})


def _people_member_context(parish, user=None, acolyte=None):
    membership = None
    if user:
        membership = ParishMembership.objects.filter(parish=parish, user=user).prefetch_related("roles").first()

    stats = None
    if acolyte:
        stats = AcolyteStats.objects.filter(parish=parish, acolyte=acolyte).first()

    user_form = PeopleUserForm(instance=user) if user else None
    membership_form = PeopleMembershipForm(
        parish=parish,
        initial={
            "active": membership.active if membership else True,
            "roles": membership.roles.all() if membership else [],
        },
    )
    acolyte_form = PeopleAcolyteForm(instance=acolyte, parish=parish) if acolyte else PeopleAcolyteForm(parish=parish)
    qual_form = PeopleQualificationsForm(
        parish=parish,
        initial={
            "qualifications": AcolyteQualification.objects.filter(
                parish=parish, acolyte=acolyte, qualified=True
            ).values_list("position_type_id", flat=True)
            if acolyte
            else []
        },
    )

    availability = acolyte.acolyteavailabilityrule_set.select_related("community") if acolyte else []
    weekly_rules = (
        availability.filter(start_date__isnull=True, end_date__isnull=True).order_by("day_of_week", "start_time")
        if acolyte
        else []
    )
    date_absences = (
        availability.filter(Q(start_date__isnull=False) | Q(end_date__isnull=False)).order_by("start_date")
        if acolyte
        else []
    )
    preferences = (
        acolyte.acolytepreference_set.select_related(
            "target_community", "target_position", "target_function", "target_template", "target_acolyte"
        )
        if acolyte
        else []
    )

    upcoming = []
    recent = []
    if acolyte:
        upcoming = (
            Assignment.objects.filter(
                parish=parish, acolyte=acolyte, is_active=True, slot__mass_instance__starts_at__gte=timezone.now()
            )
            .select_related("slot__mass_instance__community", "slot__position_type", "confirmation")
            .order_by("slot__mass_instance__starts_at")[:5]
        )
        recent = (
            Assignment.objects.filter(
                parish=parish, acolyte=acolyte, slot__mass_instance__starts_at__lt=timezone.now()
            )
            .select_related("slot__mass_instance__community", "slot__position_type", "confirmation")
            .order_by("-slot__mass_instance__starts_at")[:5]
        )

    return {
        "membership": membership,
        "stats": stats,
        "user_form": user_form,
        "membership_form": membership_form,
        "acolyte_form": acolyte_form,
        "qual_form": qual_form,
        "weekly_rules": weekly_rules,
        "date_absences": date_absences,
        "preferences": preferences,
        "weekly_form": WeeklyAvailabilityForm(acolyte=acolyte) if acolyte else None,
        "date_absence_form": DateAbsenceForm() if acolyte else None,
        "preference_form": AcolytePreferenceForm(parish=parish) if acolyte else None,
        "upcoming": upcoming,
        "recent": recent,
    }


def _handle_member_post(request, parish, user=None, acolyte=None):
    form_type = request.POST.get("form_type")
    if form_type == "user" and user:
        form = PeopleUserForm(request.POST, instance=user)
        if form.is_valid():
            form.save()
            messages.success(request, "Identidade atualizada.")
        else:
            messages.error(request, "Revise os dados de identidade.")
    elif form_type == "membership" and user:
        form = PeopleMembershipForm(request.POST, parish=parish)
        if form.is_valid():
            membership, _ = ParishMembership.objects.get_or_create(parish=parish, user=user, defaults={"active": True})
            membership.active = bool(form.cleaned_data.get("active"))
            membership.save(update_fields=["active", "updated_at"])
            membership.roles.clear()
            if form.cleaned_data.get("roles"):
                membership.roles.add(*form.cleaned_data["roles"])
            messages.success(request, "Papeis atualizados.")
        else:
            messages.error(request, "Revise os papeis informados.")
    elif form_type == "acolyte":
        was_active = acolyte.active if acolyte else None
        form = PeopleAcolyteForm(request.POST, instance=acolyte, parish=parish)
        if form.is_valid():
            profile = form.save(commit=False)
            profile.parish = parish
            if user:
                profile.user = user
            profile.save()
            if was_active and not profile.active:
                removed = deactivate_future_assignments_for_acolyte(profile, actor=request.user)
                if removed:
                    messages.info(request, f"{removed} escalas futuras foram removidas.")
            messages.success(request, "Perfil de acolito atualizado.")
        else:
            messages.error(request, "Revise os dados do acolito.")
    elif form_type == "qualifications" and acolyte:
        form = PeopleQualificationsForm(request.POST, parish=parish)
        if form.is_valid():
            selected = set(form.cleaned_data.get("qualifications").values_list("id", flat=True))
            existing = set(
                AcolyteQualification.objects.filter(parish=parish, acolyte=acolyte).values_list(
                    "position_type_id", flat=True
                )
            )
            for to_add in selected - existing:
                AcolyteQualification.objects.create(
                    parish=parish, acolyte=acolyte, position_type_id=to_add, qualified=True
                )
            if existing - selected:
                AcolyteQualification.objects.filter(
                    parish=parish, acolyte=acolyte, position_type_id__in=existing - selected
                ).delete()
            messages.success(request, "Qualificacoes atualizadas.")
        else:
            messages.error(request, "Revise as qualificacoes.")
    elif form_type == "weekly_availability" and acolyte:
        form = WeeklyAvailabilityForm(request.POST, acolyte=acolyte)
        if form.is_valid():
            cleaned = form.cleaned_data
            existing = AcolyteAvailabilityRule.objects.filter(
                parish=parish,
                acolyte=acolyte,
                rule_type=cleaned.get("rule_type"),
                day_of_week=cleaned.get("day_of_week"),
                start_time=cleaned.get("start_time"),
                end_time=cleaned.get("end_time"),
                community=cleaned.get("community"),
            ).exists()
            if existing:
                messages.info(request, "Voce ja possui uma regra igual.")
            else:
                rule = form.save(commit=False)
                rule.parish = parish
                rule.acolyte = acolyte
                rule.save()
                messages.success(request, "Regra semanal adicionada.")
        else:
            if form.non_field_errors():
                messages.info(request, form.non_field_errors()[0])
            else:
                messages.error(request, "Revise a regra semanal.")
    elif form_type == "date_absence" and acolyte:
        form = DateAbsenceForm(request.POST)
        if form.is_valid():
            absence = form.save(commit=False)
            absence.parish = parish
            absence.acolyte = acolyte
            absence.rule_type = "unavailable"
            absence.day_of_week = None
            absence.save()
            messages.success(request, "Ausencia cadastrada.")
        else:
            messages.error(request, "Revise a ausencia.")
    elif form_type == "preference" and acolyte:
        form = AcolytePreferenceForm(request.POST, parish=parish)
        if form.is_valid():
            pref = form.save(commit=False)
            pref.parish = parish
            pref.acolyte = acolyte
            pref.save()
            messages.success(request, "Preferencia adicionada.")
        else:
            messages.error(request, "Revise a preferencia.")
    elif form_type == "delete_availability" and acolyte:
        rule_id = request.POST.get("rule_id")
        rule = get_object_or_404(AcolyteAvailabilityRule, parish=parish, acolyte=acolyte, id=rule_id)
        rule.delete()
        messages.success(request, "Regra removida.")
    elif form_type == "delete_preference" and acolyte:
        pref_id = request.POST.get("pref_id")
        pref = get_object_or_404(AcolytePreference, parish=parish, acolyte=acolyte, id=pref_id)
        pref.delete()
        messages.success(request, "Preferencia removida.")


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def people_user_detail(request, user_id):
    parish = request.active_parish
    user = get_object_or_404(get_user_model(), id=user_id)
    membership = ParishMembership.objects.filter(parish=parish, user=user).first()
    acolyte = parish.acolytes.filter(user=user).first()
    if not membership and not acolyte and not request.user.is_system_admin:
        return HttpResponseNotFound()
    if request.method == "POST":
        _handle_member_post(request, parish, user=user, acolyte=acolyte)
        return redirect("people_user_detail", user_id=user.id)
    context = _people_member_context(parish, user=user, acolyte=acolyte)
    context.update({"user": user, "acolyte": acolyte})
    return render(request, "people/detail.html", context)


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def people_acolyte_detail(request, acolyte_id):
    parish = request.active_parish
    acolyte = get_object_or_404(AcolyteProfile, parish=parish, id=acolyte_id)
    user = acolyte.user
    if request.method == "POST":
        _handle_member_post(request, parish, user=user, acolyte=acolyte)
        return redirect("people_acolyte_detail", acolyte_id=acolyte.id)
    context = _people_member_context(parish, user=user, acolyte=acolyte)
    context.update({"user": user, "acolyte": acolyte})
    return render(request, "people/detail.html", context)

@login_required
@require_active_parish
def my_assignments(request):
    parish = request.active_parish
    feed_token = CalendarFeedToken.objects.filter(parish=parish, user=request.user).first()
    calendar_feed_url = None
    if feed_token:
        calendar_feed_url = request.build_absolute_uri(
            f"{reverse('calendar_feed')}?token={feed_token.token}"
        )
    assignments = Assignment.objects.filter(parish=parish, acolyte__user=request.user, is_active=True).select_related(
        "slot__mass_instance", "slot__position_type", "confirmation"
    )
    mass_ids = list(assignments.values_list("slot__mass_instance_id", flat=True))
    team_map = {}
    team_names_map = {}
    if mass_ids:
        teammates = (
            Assignment.objects.filter(
                parish=parish,
                is_active=True,
                slot__mass_instance_id__in=mass_ids,
            )
            .select_related("acolyte", "slot__mass_instance")
            .order_by("slot__mass_instance_id", "slot__position_type__name")
        )
        for assignment in teammates:
            team_map.setdefault(assignment.slot.mass_instance_id, []).append(assignment)
        for mass_id, items in team_map.items():
            names = [item.acolyte.display_name for item in items]
            team_names_map[mass_id] = ", ".join(names)

    return render(
        request,
        "acolytes/assignments.html",
        {
            "assignments": assignments,
            "team_map": team_map,
            "team_names_map": team_names_map,
            "calendar_feed_url": calendar_feed_url,
        },
    )


@login_required
@require_active_parish
def calendar_feed_token(request):
    parish = request.active_parish
    if request.method != "POST":
        return redirect("my_assignments")
    token = CalendarFeedToken.objects.filter(parish=parish, user=request.user).first()
    if token:
        token.token = uuid4().hex
        token.rotated_at = timezone.now()
        token.save(update_fields=["token", "rotated_at", "updated_at"])
        messages.success(request, "Link do calendario atualizado.")
    else:
        CalendarFeedToken.objects.create(parish=parish, user=request.user, token=uuid4().hex)
        messages.success(request, "Link do calendario criado.")
    return redirect("my_assignments")


def _ics_escape(value):
    return (
        value.replace("\\", "\\\\")
        .replace(";", "\\;")
        .replace(",", "\\,")
        .replace("\n", "\\n")
    )


def calendar_feed(request):
    token = request.GET.get("token")
    if not token:
        return HttpResponseNotFound("Token invalido.")
    feed = CalendarFeedToken.objects.select_related("parish", "user").filter(token=token).first()
    if not feed:
        return HttpResponseNotFound("Token invalido.")
    if not feed.user.is_system_admin:
        membership = ParishMembership.objects.filter(user=feed.user, parish=feed.parish, active=True).exists()
        if not membership:
            return HttpResponseNotFound("Token invalido.")

    assignments = (
        Assignment.objects.filter(
            parish=feed.parish,
            acolyte__user=feed.user,
            is_active=True,
            assignment_state__in=["published", "locked"],
            slot__mass_instance__status="scheduled",
        )
        .select_related("slot__mass_instance__community", "slot__position_type")
        .order_by("slot__mass_instance__starts_at")
    )

    tz_name = feed.parish.timezone or "America/Sao_Paulo"
    now_stamp = timezone.now().astimezone(dt_timezone.utc)
    dtstamp = now_stamp.strftime("%Y%m%dT%H%M%SZ")
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Acoli//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:Acoli - {_ics_escape(feed.parish.name)}",
        f"X-WR-TIMEZONE:{tz_name}",
    ]
    base_url = getattr(settings, "APP_BASE_URL", "").rstrip("/")
    mass_ids = list(assignments.values_list("slot__mass_instance_id", flat=True))
    team_map = defaultdict(list)
    if mass_ids:
        teammates = (
            Assignment.objects.filter(
                parish=feed.parish,
                is_active=True,
                slot__mass_instance_id__in=mass_ids,
            )
            .select_related("acolyte")
            .order_by("slot__mass_instance_id", "slot__position_type__name")
        )
        for item in teammates:
            team_map[item.slot.mass_instance_id].append(item.acolyte.display_name)
    for assignment in assignments:
        mass = assignment.slot.mass_instance
        starts_at = timezone.localtime(mass.starts_at)
        ends_at = starts_at + timedelta(minutes=feed.parish.default_mass_duration_minutes)
        summary = f"{mass.community.code} - {assignment.slot.position_type.name}"
        url_path = reverse("mass_detail", args=[mass.id])
        full_url = f"{base_url}{url_path}" if base_url else url_path
        team_names = ", ".join(team_map.get(mass.id, []))
        description_parts = [full_url]
        if team_names:
            description_parts.append(f"Equipe: {team_names}")
        uid = f"acoli-{feed.parish_id}-{mass.id}-{assignment.slot.position_type_id}-{assignment.slot.slot_index}"
        lines.extend(
            [
                "BEGIN:VEVENT",
                f"UID:{uid}@acoli",
                f"DTSTAMP:{dtstamp}",
                f"DTSTART;TZID={tz_name}:{starts_at.strftime('%Y%m%dT%H%M%S')}",
                f"DTEND;TZID={tz_name}:{ends_at.strftime('%Y%m%dT%H%M%S')}",
                f"SUMMARY:{_ics_escape(summary)}",
                f"DESCRIPTION:{_ics_escape(' | '.join(description_parts))}",
                "END:VEVENT",
            ]
        )
    lines.append("END:VCALENDAR")
    content = "\r\n".join(lines)
    return HttpResponse(content, content_type="text/calendar; charset=utf-8")


@login_required
@require_active_parish
def my_preferences(request):
    parish = request.active_parish
    acolyte = parish.acolytes.filter(user=request.user).first()
    if not acolyte:
        return render(request, "acolytes/preferences.html", {"acolyte": None})

    availability = acolyte.acolyteavailabilityrule_set.select_related("community")
    weekly_rules = availability.filter(start_date__isnull=True, end_date__isnull=True).order_by("day_of_week", "start_time")
    weekly_by_day = {day: [] for day, _label in WEEKDAY_CHOICES}
    any_day_rules = []
    for rule in weekly_rules:
        if rule.day_of_week is None:
            any_day_rules.append(rule)
        else:
            weekly_by_day[rule.day_of_week].append(rule)
    date_absences = availability.filter(Q(start_date__isnull=False) | Q(end_date__isnull=False)).order_by("start_date")
    preferences = acolyte.acolytepreference_set.select_related(
        "target_community", "target_position", "target_function", "target_template", "target_acolyte"
    )

    diagnostics = None
    diagnostics_reasons = []
    window_end = timezone.localdate() + timedelta(days=30)
    upcoming = (
        MassInstance.objects.filter(
            parish=parish,
            status="scheduled",
            starts_at__date__gte=timezone.localdate(),
            starts_at__date__lte=window_end,
        )
        .select_related("event_series", "requirement_profile")
        .prefetch_related("requirement_profile__positions__position_type")
    )
    total_upcoming = upcoming.count()
    if total_upcoming:
        qualifications = set(
            AcolyteQualification.objects.filter(parish=parish, acolyte=acolyte, qualified=True).values_list(
                "position_type_id", flat=True
            )
        )
        rules = list(AcolyteAvailabilityRule.objects.filter(parish=parish, acolyte=acolyte))
        interested_series = set(
            EventInterest.objects.filter(parish=parish, acolyte=acolyte, interested=True).values_list(
                "event_series_id", flat=True
            )
        )

        blocked_interest = 0
        blocked_qualification = 0
        blocked_availability = 0
        eligible_by_qualification = 0
        eligible_opportunities = 0

        for instance in upcoming:
            if instance.event_series and instance.event_series.candidate_pool == "interested_only":
                if instance.event_series_id not in interested_series:
                    blocked_interest += 1
                    continue
            profile = instance.requirement_profile
            if not profile:
                blocked_qualification += 1
                continue
            position_ids = [pos.position_type_id for pos in profile.positions.all()]
            if not qualifications.intersection(position_ids):
                blocked_qualification += 1
                continue
            eligible_by_qualification += 1
            if not is_acolyte_available_with_rules(rules, instance):
                blocked_availability += 1
                continue
            eligible_opportunities += 1

        if not qualifications:
            diagnostics_reasons.append("Sem qualificacoes definidas para este periodo.")
        if blocked_interest == total_upcoming and total_upcoming:
            diagnostics_reasons.append("Eventos marcados como interessados apenas estao fora da sua lista.")
        if eligible_by_qualification == 0 and qualifications:
            diagnostics_reasons.append("Nenhuma missa combina com suas qualificacoes.")
        if eligible_by_qualification and blocked_availability == eligible_by_qualification:
            diagnostics_reasons.append("Sua disponibilidade atual bloqueia todas as missas no periodo.")
        if not diagnostics_reasons and eligible_opportunities == 0:
            diagnostics_reasons.append("Sem oportunidades elegiveis neste periodo.")

        diagnostics = {
            "total": total_upcoming,
            "qualified_positions": len(qualifications),
            "eligible_by_qualification": eligible_by_qualification,
            "eligible_opportunities": eligible_opportunities,
            "blocked_interest": blocked_interest,
            "blocked_availability": blocked_availability,
            "reasons": diagnostics_reasons,
        }

    weekly_form = WeeklyAvailabilityForm(acolyte=acolyte)
    date_absence_form = DateAbsenceForm()
    preference_form = AcolytePreferenceForm(parish=parish)

    if request.method == "POST":
        form_type = request.POST.get("form_type")
        if form_type == "weekly_availability":
            weekly_form = WeeklyAvailabilityForm(request.POST, acolyte=acolyte)
            message_sent = False
            if weekly_form.is_valid():
                cleaned = weekly_form.cleaned_data
                existing = AcolyteAvailabilityRule.objects.filter(
                    parish=parish,
                    acolyte=acolyte,
                    rule_type=cleaned.get("rule_type"),
                    day_of_week=cleaned.get("day_of_week"),
                    start_time=cleaned.get("start_time"),
                    end_time=cleaned.get("end_time"),
                    community=cleaned.get("community"),
                ).exists()
                if existing:
                    weekly_form.add_error(None, "Voce ja possui uma regra igual.")
                    messages.info(request, "Voce ja possui uma regra igual.")
                    message_sent = True
                else:
                    rule = weekly_form.save(commit=False)
                    rule.parish = parish
                    rule.acolyte = acolyte
                    rule.start_date = None
                    rule.end_date = None
                    rule.save()
                    return redirect("my_preferences")
            if weekly_form.non_field_errors() and not message_sent:
                messages.info(request, weekly_form.non_field_errors()[0])
        elif form_type == "date_absence":
            date_absence_form = DateAbsenceForm(request.POST)
            message_sent = False
            if date_absence_form.is_valid():
                cleaned = date_absence_form.cleaned_data
                start_date = cleaned.get("start_date")
                end_date = cleaned.get("end_date") or start_date
                existing = AcolyteAvailabilityRule.objects.filter(
                    parish=parish,
                    acolyte=acolyte,
                    rule_type="unavailable",
                    day_of_week__isnull=True,
                    start_date=start_date,
                    end_date=end_date,
                    start_time__isnull=True,
                    end_time__isnull=True,
                ).exists()
                if existing:
                    date_absence_form.add_error(None, "Voce ja possui uma regra igual.")
                    messages.info(request, "Voce ja possui uma regra igual.")
                    message_sent = True
                else:
                    rule = date_absence_form.save(commit=False)
                    rule.parish = parish
                    rule.acolyte = acolyte
                    rule.rule_type = "unavailable"
                    rule.day_of_week = None
                    rule.start_time = None
                    rule.end_time = None
                    rule.save()
                    return redirect("my_preferences")
            if date_absence_form.non_field_errors() and not message_sent:
                messages.info(request, date_absence_form.non_field_errors()[0])
        elif form_type == "preference":
            preference_form = AcolytePreferenceForm(request.POST, parish=parish)
            if preference_form.is_valid():
                pref = preference_form.save(commit=False)
                pref.parish = parish
                pref.acolyte = acolyte
                pref.save()
                return redirect("my_preferences")
            if preference_form.non_field_errors():
                messages.info(request, preference_form.non_field_errors()[0])

    return render(
        request,
        "acolytes/preferences.html",
        {
            "acolyte": acolyte,
            "weekly_rules": weekly_rules,
            "weekly_by_day": weekly_by_day,
            "any_day_rules": any_day_rules,
            "date_absences": date_absences,
            "preferences": preferences,
            "weekly_form": weekly_form,
            "date_absence_form": date_absence_form,
            "preference_form": preference_form,
            "weekday_choices": WEEKDAY_CHOICES,
            "diagnostics": diagnostics,
        },
    )


@login_required
@require_active_parish
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
@require_active_parish
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
@require_active_parish
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
@require_active_parish
def swap_request_create(request, assignment_id):
    parish = request.active_parish
    assignment = get_object_or_404(Assignment, parish=parish, id=assignment_id)
    if not assignment.is_active:
        return redirect("my_assignments")
    if assignment.acolyte.user_id != request.user.id and not user_has_role(
        request.user, parish, ADMIN_ROLE_CODES
    ):
        return redirect("my_assignments")
    slots_qs = (
        assignment.slot.mass_instance.slots.exclude(id=assignment.slot_id)
        .filter(assignments__is_active=True)
        .distinct()
    )
    acolytes_qs = parish.acolytes.filter(active=True)
    if request.method == "POST":
        form = SwapRequestForm(request.POST, acolytes_qs=acolytes_qs, slots_qs=slots_qs)
        if form.is_valid():
            swap_type = form.cleaned_data["swap_type"]
            target_acolyte = form.cleaned_data.get("target_acolyte")
            to_slot = form.cleaned_data.get("to_slot")
            if swap_type == "role_swap" and not to_slot:
                return redirect("swap_request_create", assignment_id=assignment.id)
            open_to_admin = False
            if swap_type == "role_swap" and to_slot:
                to_assignment = to_slot.get_active_assignment()
                if to_assignment:
                    target_acolyte = to_assignment.acolyte
            if swap_type == "acolyte_swap" and not target_acolyte:
                open_to_admin = True
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
                open_to_admin=open_to_admin,
            )
            log_audit(parish, request.user, "SwapRequest", swap.id, "create", {"swap_type": swap.swap_type})
            if target_acolyte and target_acolyte.user:
                enqueue_notification(
                    parish,
                    target_acolyte.user,
                    "SWAP_REQUESTED",
                    {"swap_id": swap.id},
                    idempotency_key=f"swap:{swap.id}:request",
                )
            if open_to_admin:
                for user in users_with_roles(parish, ADMIN_ROLE_CODES):
                    enqueue_notification(
                        parish,
                        user,
                        "SWAP_REQUESTED",
                        {"swap_id": swap.id},
                        idempotency_key=f"swap:{swap.id}:admin:{user.id}",
                    )
            return redirect("swap_requests")
    else:
        form = SwapRequestForm(acolytes_qs=acolytes_qs, slots_qs=slots_qs)
    return render(request, "acolytes/swap_form.html", {"form": form, "assignment": assignment})


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def swap_request_assign(request, swap_id):
    parish = request.active_parish
    swap = get_object_or_404(SwapRequest, parish=parish, id=swap_id)
    if not swap.open_to_admin:
        return redirect("swap_requests")
    acolytes_qs = parish.acolytes.filter(active=True)
    if request.method == "POST":
        form = SwapAssignForm(request.POST, acolytes_qs=acolytes_qs)
        if form.is_valid():
            swap.target_acolyte = form.cleaned_data["target_acolyte"]
            swap.open_to_admin = False
            try:
                applied = apply_swap_request(swap, actor=request.user)
            except ConcurrentUpdateError:
                messages.error(request, "Esta vaga foi atualizada por outra acao. Recarregue e tente novamente.")
                return redirect("swap_requests")
            if applied:
                swap.status = "accepted"
                swap.save(update_fields=["target_acolyte", "open_to_admin", "status", "updated_at"])
                if swap.requestor_acolyte.user:
                    enqueue_notification(
                        parish,
                        swap.requestor_acolyte.user,
                        "SWAP_ACCEPTED",
                        {"swap_id": swap.id},
                        idempotency_key=f"swap:{swap.id}:accepted",
                    )
                if swap.target_acolyte.user:
                    enqueue_notification(
                        parish,
                        swap.target_acolyte.user,
                        "SWAP_ACCEPTED",
                        {"swap_id": swap.id},
                        idempotency_key=f"swap:{swap.id}:assigned",
                    )
                return redirect("swap_requests")
            swap.target_acolyte = None
            swap.open_to_admin = True
            swap.save(update_fields=["target_acolyte", "open_to_admin", "updated_at"])
            messages.error(request, "Nao foi possivel aplicar a troca com este acolito.")
    else:
        form = SwapAssignForm(acolytes_qs=acolytes_qs)
    return render(request, "acolytes/swap_assign.html", {"swap": swap, "form": form})


@login_required
@require_active_parish
def swap_request_accept(request, swap_id):
    if request.method != "POST":
        return redirect("swap_requests")
    parish = request.active_parish
    swap = get_object_or_404(SwapRequest, parish=parish, id=swap_id)
    if swap.open_to_admin and not user_has_role(request.user, parish, ADMIN_ROLE_CODES):
        messages.info(request, "Solicitacao em aberto. A coordenacao vai tratar esta troca.")
        return redirect("swap_requests")
    if swap.open_to_admin:
        return redirect("swap_request_assign", swap_id=swap.id)
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
                {"swap_id": swap.id},
                idempotency_key=f"swap:{swap.id}:approval:{user.id}",
            )
        if swap.requestor_acolyte.user:
            enqueue_notification(
                parish,
                swap.requestor_acolyte.user,
                "SWAP_REQUESTED",
                {"swap_id": swap.id},
                idempotency_key=f"swap:{swap.id}:awaiting",
            )
        return redirect("swap_requests")
    try:
        applied = apply_swap_request(swap, actor=request.user)
    except ConcurrentUpdateError:
        messages.error(request, "Esta vaga foi atualizada por outra acao. Recarregue e tente novamente.")
        return redirect("swap_requests")
    if applied:
        swap.status = "accepted"
        swap.save(update_fields=["status", "updated_at"])
        if swap.requestor_acolyte.user:
            enqueue_notification(
                parish,
                swap.requestor_acolyte.user,
                "SWAP_ACCEPTED",
                {"swap_id": swap.id},
                idempotency_key=f"swap:{swap.id}:accepted",
            )
    else:
        messages.error(request, "Nao foi possivel aplicar a troca.")
    return redirect("swap_requests")


@login_required
@require_active_parish
def swap_request_reject(request, swap_id):
    if request.method != "POST":
        return redirect("swap_requests")
    parish = request.active_parish
    swap = get_object_or_404(SwapRequest, parish=parish, id=swap_id)
    if swap.open_to_admin and not user_has_role(request.user, parish, ADMIN_ROLE_CODES):
        messages.info(request, "Solicitacao em aberto. A coordenacao vai tratar esta troca.")
        return redirect("swap_requests")
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
            {"swap_id": swap.id},
            idempotency_key=f"swap:{swap.id}:rejected",
        )
    return redirect("swap_requests")


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def swap_request_approve(request, swap_id):
    if request.method != "POST":
        return redirect("swap_requests")
    parish = request.active_parish
    swap = get_object_or_404(SwapRequest, parish=parish, id=swap_id)
    if swap.status != "awaiting_approval":
        return redirect("swap_requests")
    try:
        applied = apply_swap_request(swap, actor=request.user)
    except ConcurrentUpdateError:
        messages.error(request, "Esta vaga foi atualizada por outra acao. Recarregue e tente novamente.")
        return redirect("swap_requests")
    if applied:
        swap.status = "accepted"
        swap.save(update_fields=["status", "updated_at"])
        if swap.requestor_acolyte.user:
            enqueue_notification(
                parish,
                swap.requestor_acolyte.user,
                "SWAP_ACCEPTED",
                {"swap_id": swap.id},
                idempotency_key=f"swap:{swap.id}:accepted",
            )
    else:
        messages.error(request, "Nao foi possivel aplicar a troca.")
    return redirect("swap_requests")


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def parish_settings(request):
    parish = request.active_parish
    if request.method == "POST":
        if request.POST.get("action") == "reset":
            before = {"schedule_weights": parish.schedule_weights}
            parish.schedule_weights = ParishSettingsForm.DEFAULT_SCHEDULE_WEIGHTS.copy()
            parish.save(update_fields=["schedule_weights", "updated_at"])
            after = {"schedule_weights": parish.schedule_weights}
            log_audit(parish, request.user, "Parish", parish.id, "update", {"from": before, "to": after})
            messages.success(request, "Pesos restaurados para os padroes.")
            return redirect("parish_settings")
        form = ParishSettingsForm(request.POST, parish=parish)
        if form.is_valid():
            before = {
                "consolidation_days": parish.consolidation_days,
                "horizon_days": parish.horizon_days,
                "default_mass_duration_minutes": parish.default_mass_duration_minutes,
                "min_rest_minutes_between_masses": parish.min_rest_minutes_between_masses,
                "swap_requires_approval": parish.swap_requires_approval,
                "notify_on_cancellation": parish.notify_on_cancellation,
                "auto_assign_on_decline": parish.auto_assign_on_decline,
                "schedule_weights": parish.schedule_weights,
            }
            form.save(parish, actor=request.user)
            after = {
                "consolidation_days": parish.consolidation_days,
                "horizon_days": parish.horizon_days,
                "default_mass_duration_minutes": parish.default_mass_duration_minutes,
                "min_rest_minutes_between_masses": parish.min_rest_minutes_between_masses,
                "swap_requires_approval": parish.swap_requires_approval,
                "notify_on_cancellation": parish.notify_on_cancellation,
                "auto_assign_on_decline": parish.auto_assign_on_decline,
                "schedule_weights": parish.schedule_weights,
            }
            log_audit(parish, request.user, "Parish", parish.id, "update", {"from": before, "to": after})
            messages.success(request, "Configuracoes salvas com sucesso.")
            return redirect("parish_settings")
    else:
        form = ParishSettingsForm(parish=parish)
    return render(request, "settings/parish.html", {"parish": parish, "form": form})


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
@require_active_parish
def confirm_assignment(request, assignment_id):
    if request.method != "POST":
        return redirect("my_assignments")
    parish = request.active_parish
    assignment = get_object_or_404(Assignment, parish=parish, id=assignment_id)
    if not assignment.is_active:
        messages.info(request, "Esta escala nao esta mais ativa.")
        return redirect("my_assignments")
    if assignment.slot.mass_instance.status == "canceled":
        messages.info(request, "Esta missa foi cancelada.")
        return redirect("my_assignments")
    if not _can_manage_assignment(request.user, assignment):
        return redirect("my_assignments")
    confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
    confirmation.status = "confirmed"
    confirmation.updated_by = request.user
    confirmation.save(update_fields=["status", "updated_by", "timestamp"])
    log_audit(parish, request.user, "Confirmation", confirmation.id, "update", {"status": "confirmed"})
    return redirect("my_assignments")


@login_required
@require_active_parish
def decline_assignment(request, assignment_id):
    if request.method != "POST":
        return redirect("my_assignments")
    parish = request.active_parish
    assignment = get_object_or_404(Assignment, parish=parish, id=assignment_id)
    if not assignment.is_active:
        messages.info(request, "Esta escala nao esta mais ativa.")
        return redirect("my_assignments")
    if assignment.slot.mass_instance.status == "canceled":
        messages.info(request, "Esta missa foi cancelada.")
        return redirect("my_assignments")
    if not _can_manage_assignment(request.user, assignment):
        return redirect("my_assignments")
    confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
    confirmation.status = "declined"
    confirmation.updated_by = request.user
    confirmation.save(update_fields=["status", "updated_by", "timestamp"])
    slot = assignment.slot
    deactivate_assignment(assignment, "declined", actor=request.user)
    slot.status = "open"
    slot.save(update_fields=["status", "updated_at"])
    log_audit(parish, request.user, "Confirmation", confirmation.id, "update", {"status": "declined"})
    replacement = create_replacement_request(
        parish,
        slot,
        actor=request.user,
        notes=f"Criada por recusa de {assignment.acolyte.display_name}",
    )
    if parish.notify_on_cancellation:
        for user in users_with_roles(parish, ADMIN_ROLE_CODES):
            enqueue_notification(
                parish,
                user,
                "ASSIGNMENT_CANCELED_ALERT_ADMIN",
                {"slot_id": slot.id},
                idempotency_key=f"cancel:{assignment.id}:{user.id}",
            )
    if parish.auto_assign_on_decline:
        candidates = quick_fill_slot(slot, parish, max_candidates=1)
        if candidates:
            try:
                new_assignment = assign_replacement_request(parish, replacement.id, candidates[0], actor=request.user)
            except (ConcurrentUpdateError, ValueError):
                messages.error(request, "Esta vaga foi atualizada por outra acao. Recarregue e tente novamente.")
                return redirect("my_assignments")
            if new_assignment.acolyte.user:
                enqueue_notification(
                    parish,
                    new_assignment.acolyte.user,
                    "REPLACEMENT_ASSIGNED",
                    {"assignment_id": new_assignment.id},
                    idempotency_key=f"replacement:{new_assignment.id}",
                )
    return redirect("my_assignments")


@login_required
@require_active_parish
def cancel_assignment(request, assignment_id):
    if request.method != "POST":
        return redirect("my_assignments")
    parish = request.active_parish
    assignment = get_object_or_404(Assignment, parish=parish, id=assignment_id)
    if not assignment.is_active:
        return redirect("my_assignments")
    if assignment.slot.mass_instance.status == "canceled":
        messages.info(request, "A missa foi cancelada. Nenhuma acao e necessaria.")
        return redirect("my_assignments")
    if not _can_manage_assignment(request.user, assignment):
        return redirect("my_assignments")
    confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
    confirmation.status = "canceled_by_acolyte"
    confirmation.updated_by = request.user
    confirmation.save(update_fields=["status", "updated_by", "timestamp"])
    slot = assignment.slot
    deactivate_assignment(assignment, "canceled", actor=request.user)
    slot.status = "open"
    slot.save(update_fields=["status", "updated_at"])
    log_audit(parish, request.user, "Confirmation", confirmation.id, "update", {"status": "canceled_by_acolyte"})
    create_replacement_request(
        parish,
        slot,
        actor=request.user,
        notes=f"Criada por cancelamento de {assignment.acolyte.display_name}",
    )
    if parish.notify_on_cancellation:
        for user in users_with_roles(parish, ADMIN_ROLE_CODES):
            enqueue_notification(
                parish,
                user,
                "ASSIGNMENT_CANCELED_ALERT_ADMIN",
                {"slot_id": slot.id},
                idempotency_key=f"cancel:{assignment.id}:{user.id}",
            )
    return redirect("my_assignments")
