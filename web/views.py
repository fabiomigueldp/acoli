import calendar as cal
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone as dt_timezone
from uuid import uuid4
from urllib.parse import urlencode
import re

from django.contrib import messages
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core.mail import send_mail
from django.core.paginator import Paginator
from django.db.models import Count, F, Min, Max, Prefetch, Q
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
    AcolyteCreditLedger,
    AcolyteIntent,
    AcolytePreference,
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
    FamilyGroup,
    FunctionType,
    MassInterest,
    MassInstance,
    MassTemplate,
    MassOverride,
    MembershipRole,
    ParishMembership,
    PositionType,
    PositionTypeFunction,
    PositionClaimRequest,
    RequirementProfile,
    RequirementProfilePosition,
    ReplacementRequest,
    SwapRequest,
)
from notifications.models import Notification, NotificationPreference
from core.services.audit import log_audit
from core.services.calendar_generation import generate_instances_for_parish
from core.services.event_series import apply_event_occurrences
from core.services.assignments import ConcurrentUpdateError, assign_manual, deactivate_assignment
from core.services.claims import (
    choose_claim,
    create_position_claim,
    expire_claims_for_assignment,
    reject_claim,
    cancel_claim,
    approve_claim,
)
from core.services.publishing import publish_assignments
from core.services.slots import sync_slots_for_instance
from core.services.permissions import (
    ADMIN_ROLE_CODES,
    require_active_parish,
    require_parish_roles,
    request_has_role,
    user_has_role,
    users_with_roles,
)
from core.services.replacements import (
    assign_replacement_request,
    cancel_mass_and_resolve_dependents,
    create_replacement_request,
    reconcile_pending_replacements,
    should_create_replacement,
)
from core.services.swaps import apply_swap_request
from core.services.availability import is_acolyte_available, is_acolyte_available_with_rules
from core.services.acolytes import deactivate_future_assignments_for_acolyte
from core.services.time_windows import filter_past, filter_upcoming
from scheduler.models import ScheduleJobRequest
from notifications.services import enqueue_notification
from scheduler.services.quick_fill import build_quick_fill_cache, quick_fill_slot
from core.services.recommendations import build_recommendation_cache, get_mass_context, rank_candidates
from web.forms import (
    AcolyteAvailabilityRuleForm,
    AcolyteIntentForm,
    AcolyteLinkForm,
    AcolyteCreateLoginForm,
    AcolytePreferenceForm,
    AssignToSlotForm,
    CommunityForm,
    CreditAdjustmentForm,
    DateAbsenceForm,
    EventOccurrenceForm,
    EventSeriesBasicsForm,
    MassInstanceCancelForm,
    MassInstanceMoveForm,
    MassInstanceUpdateForm,
    MassTemplateForm,
    NotificationPreferenceForm,
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

    is_admin = request_has_role(request, ADMIN_ROLE_CODES)
    acolyte = parish.acolytes.filter(user=request.user).first()
    dashboard_view = "admin" if is_admin else "acolyte"
    requested_view = request.GET.get("view")
    if is_admin:
        if requested_view == "acolyte" and acolyte:
            dashboard_view = "acolyte"
        elif requested_view == "admin":
            dashboard_view = "admin"

    context = {
        "parish": parish,
        "is_admin": is_admin,
        "acolyte": acolyte,
        "dashboard_view": dashboard_view,
    }

    now = timezone.now()
    cutoff = now + timedelta(days=14)
    unfilled = AssignmentSlot.objects.filter(
        parish=parish,
        status="open",
        required=True,
        mass_instance__starts_at__gte=now,
        mass_instance__starts_at__lte=cutoff,
    ).count()
    context["unfilled"] = unfilled

    if dashboard_view == "admin":
        # Management by Exception
        soon_cutoff = now + timedelta(days=2)
        consolidation_end = now + timedelta(days=parish.consolidation_days)
        open_slots_count = unfilled
        pending_swaps = SwapRequest.objects.filter(
            parish=parish,
            status__in=["pending", "awaiting_approval"]
        ).count()
        pending_swap_approvals = SwapRequest.objects.filter(
            parish=parish,
            status="awaiting_approval"
        ).select_related(
            "mass_instance__community",
            "requestor_acolyte",
            "target_acolyte",
            "from_slot__position_type",
            "to_slot__position_type"
        ).order_by("mass_instance__starts_at")[:5]  # Limit to 5 most recent
        pending_replacements = len(_actionable_replacements(parish, now, consolidation_end))
        upcoming_masses = (
            MassInstance.objects.filter(parish=parish, starts_at__gte=now)
            .select_related("community")
            .annotate(
                open_slots_count=Count(
                    "slots",
                    filter=Q(slots__status="open", slots__required=True),
                    distinct=True,
                ),
                active_assignments_count=Count(
                    "slots__assignments",
                    filter=Q(slots__assignments__is_active=True),
                    distinct=True,
                ),
                confirmed_assignments_count=Count(
                    "slots__assignments__confirmation",
                    filter=Q(
                        slots__assignments__is_active=True,
                        slots__assignments__confirmation__status="confirmed",
                    ),
                    distinct=True,
                ),
                pending_confirmations_count=Count(
                    "slots__assignments",
                    filter=Q(slots__assignments__is_active=True)
                    & (
                        Q(slots__assignments__confirmation__status="pending")
                        | Q(slots__assignments__confirmation__isnull=True)
                    ),
                    distinct=True,
                ),
            )
            .order_by("starts_at")[:5]
        )
        upcoming_masses = list(upcoming_masses)
        for mass in upcoming_masses:
            mass.is_soon = mass.starts_at <= soon_cutoff
        context.update({
            "open_slots_count": open_slots_count,
            "pending_swaps": pending_swaps,
            "pending_swap_approvals": pending_swap_approvals,
            "pending_replacements": pending_replacements,
            "upcoming_masses": upcoming_masses,
        })
    else:
        # Acolyte context
        if acolyte:
            # Hero: Next future assignment
            hero_assignment = Assignment.objects.filter(
                parish=parish,
                acolyte=acolyte,
                is_active=True,
                assignment_state__in=["proposed", "published", "locked"],
                slot__mass_instance__starts_at__gte=timezone.now(),
                slot__mass_instance__status="scheduled",
            ).select_related(
                "slot__mass_instance__community",
                "slot__position_type",
                "confirmation",
            ).order_by("slot__mass_instance__starts_at").first()

            # Check if hero has swap options (other active slots in same mass)
            hero_has_swap_options = False
            if hero_assignment:
                hero_has_swap_options = AssignmentSlot.objects.filter(
                    parish=parish,
                    mass_instance=hero_assignment.slot.mass_instance,
                ).exclude(id=hero_assignment.slot_id).filter(
                    assignments__is_active=True
                ).exists()

            # Inbox: Pending confirmations and incoming swaps (excluding next assignment)
            pending_assignments = Assignment.objects.filter(
                parish=parish,
                acolyte=acolyte,
                is_active=True,
                assignment_state__in=["proposed", "published", "locked"],
                slot__mass_instance__status="scheduled",
                slot__mass_instance__starts_at__gte=timezone.now(),
            ).filter(
                Q(confirmation__status="pending") | Q(confirmation__isnull=True)
            ).select_related(
                "slot__mass_instance",
                "slot__position_type",
                "confirmation",
            ).order_by("slot__mass_instance__starts_at")

            incoming_swaps = SwapRequest.objects.filter(
                parish=parish,
                target_acolyte=acolyte,
                mass_instance__starts_at__gte=timezone.now(),
                status="pending",
            ).select_related("mass_instance")

            # Pending position claims
            claims_pending = PositionClaimRequest.objects.filter(
                parish=parish,
                requestor_acolyte=acolyte,
                status__in=["pending_target", "pending_coordination"],
                slot__mass_instance__starts_at__gte=timezone.now(),
            ).select_related(
                "slot__mass_instance__community",
                "slot__position_type",
                "target_assignment__acolyte",
            )

            # Exclude next assignment from pending confirmations if it exists
            if hero_assignment:
                pending_assignments = pending_assignments.exclude(id=hero_assignment.id)

            claim_slot_ids = set()
            if hero_assignment:
                claim_slot_ids.add(hero_assignment.slot_id)
            claim_slot_ids.update(pending_assignments.values_list("slot_id", flat=True))
            claims_by_slot = _claim_map_for_slots(parish, claim_slot_ids)

            # Horizon: Next 4 confirmed assignments after hero
            horizon_assignments = Assignment.objects.filter(
                parish=parish,
                acolyte=acolyte,
                is_active=True,
                assignment_state__in=["proposed", "published", "locked"],
                slot__mass_instance__starts_at__gte=timezone.now(),
            ).select_related(
                "slot__mass_instance__community",
                "slot__position_type",
                "confirmation",
            ).order_by("slot__mass_instance__starts_at")

            if hero_assignment:
                horizon_assignments = horizon_assignments.exclude(id=hero_assignment.id)[:4]
            else:
                horizon_assignments = horizon_assignments[:4]

            context.update({
                "acolyte": acolyte,
                "hero_assignment": hero_assignment,
                "hero_has_swap_options": hero_has_swap_options,
                "pending_assignments": pending_assignments,
                "incoming_swaps": incoming_swaps,
                "claims_pending": claims_pending,
                "horizon_assignments": horizon_assignments,
                "claims_by_slot": claims_by_slot,
            })
        else:
            context["acolyte"] = None

    return render(request, "dashboard.html", context)


def _htmx_or_redirect(request, template_name, context, redirect_url, success_message=None):
    """
    Helper function to handle HTMX partial updates or regular redirects.
    Returns a partial template for HTMX requests, or redirects for normal requests.
    """
    if request.headers.get("HX-Request"):
        response = render(request, template_name, context)
        if success_message:
            response["HX-Success-Message"] = success_message
        return response
    if success_message:
        messages.success(request, success_message)
    return redirect(redirect_url)


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

    my_confirmed_ids = set()
    my_pending_ids = set()
    pending_confirmation_ids = set()
    acolyte = parish.acolytes.filter(user=request.user).first()
    instance_ids = [instance.id for instance in instances]
    if instance_ids:
        assignments = Assignment.objects.filter(
            parish=parish,
            slot__mass_instance_id__in=instance_ids,
            is_active=True,
        ).select_related("confirmation")
        for assignment in assignments:
            confirmation = getattr(assignment, "confirmation", None)
            if not confirmation or confirmation.status == "pending":
                pending_confirmation_ids.add(assignment.slot.mass_instance_id)
            if acolyte and assignment.acolyte_id == acolyte.id:
                if confirmation and confirmation.status == "confirmed":
                    my_confirmed_ids.add(assignment.slot.mass_instance_id)
                else:
                    my_pending_ids.add(assignment.slot.mass_instance_id)

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
            "my_confirmed_ids": my_confirmed_ids,
            "my_pending_ids": my_pending_ids,
            "pending_confirmation_ids": pending_confirmation_ids,
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


def _parse_time_value(value):
    if not value:
        return None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(value, fmt).time()
        except ValueError:
            continue
    return None


def _parse_fk_id(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _replacement_interest_closed(parish, instance, now, weights):
    if not instance.event_series_id or not instance.event_series:
        return True
    if instance.event_series.candidate_pool != "interested_only":
        return True
    context = get_mass_context(instance, weights, interest_map=None, now=now)
    return bool(context.get("interest_closed"))


def _actionable_replacements(parish, now, consolidation_end):
    replacements_qs = (
        ReplacementRequest.objects.filter(
            parish=parish,
            status="pending",
            slot__required=True,
            slot__externally_covered=False,
            slot__status="open",
            slot__mass_instance__status="scheduled",
            slot__mass_instance__starts_at__gte=now,
            slot__mass_instance__starts_at__lte=consolidation_end,
        )
        .select_related("slot__mass_instance__community", "slot__mass_instance__event_series", "slot__position_type")
        .prefetch_related(
            Prefetch(
                "slot__assignments",
                queryset=Assignment.objects.filter(is_active=True).select_related("acolyte"),
                to_attr="active_assignments",
            )
        )
        .order_by("slot__mass_instance__starts_at")
    )
    weights = parish.schedule_weights or {}
    actionable = []
    for replacement in replacements_qs:
        if replacement.slot.get_active_assignment():
            continue
        if not _replacement_interest_closed(parish, replacement.slot.mass_instance, now, weights):
            continue
        actionable.append(replacement)
    return actionable


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
        label = label.replace("_", " + ")
        columns.append({"key": key, "label": label, "title": position.name})

    days = []
    current_day = None
    day_bucket = None
    for instance in instances:
        local_dt = timezone.localtime(instance.starts_at)
        instance_day = local_dt.date()
        if instance_day != current_day:
            day_bucket = {"date": instance_day, "items": []}
            days.append(day_bucket)
            current_day = instance_day
        slot_map = {(slot.position_type_id, slot.slot_index): slot for slot in instance.slots.all()}
        day_bucket["items"].append(
            {"instance": instance, "slot_map": slot_map, "local_dt": local_dt}
        )

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


ROSTER_WEEKDAY_SHORT = [
    "Segunda-feira",
    "Terça-feira",
    "Quarta-feira",
    "Quinta-feira",
    "Sexta-feira",
    "Sábado",
    "Domingo",
]
ROSTER_MONTH_NAMES = [
    "Janeiro",
    "Fevereiro",
    "Março",
    "Abril",
    "Maio",
    "Junho",
    "Julho",
    "Agosto",
    "Setembro",
    "Outubro",
    "Novembro",
    "Dezembro",
]
ROSTER_POSITION_ORDER = ["CER", "CRU_MIC", "LIB", "NAV", "TUR"]


def _format_roster_day_header(value):
    return f"{ROSTER_WEEKDAY_SHORT[value.weekday()]} • {value.strftime('%d/%m')}"


def _format_roster_position_label(position, slot_index, max_index):
    label = position.code or position.name or "Funcao"
    label = label.replace("_", " + ")
    code = (position.code or "").strip().upper()
    if max_index > 1 and code != "CER":
        label = f"{label} {slot_index}"
    return label


def _build_roster_role_entries(instance, slot_map):
    if instance.status == "canceled":
        return []
    slots = list(slot_map.values())
    max_indices = {}
    for slot in slots:
        max_indices[slot.position_type_id] = max(
            max_indices.get(slot.position_type_id, 0), slot.slot_index
        )

    def _position_sort_key(position):
        code = (position.code or position.name or "").upper()
        code = code.replace(" ", "").replace("+", "_")
        if code in ROSTER_POSITION_ORDER:
            return (ROSTER_POSITION_ORDER.index(code), "")
        return (len(ROSTER_POSITION_ORDER), code)

    def _slot_sort(slot):
        return (_position_sort_key(slot.position_type), slot.slot_index)

    entries = []
    for slot in sorted(slots, key=_slot_sort):
        active = slot.get_active_assignment()
        if not (slot.required or slot.externally_covered or active):
            continue
        label = _format_roster_position_label(
            slot.position_type, slot.slot_index, max_indices.get(slot.position_type_id, 1)
        )
        if slot.externally_covered:
            value = "EXTERNO"
        elif active:
            value = active.acolyte.display_name
        else:
            value = "ABERTO"
        entries.append((label, value))
    return entries


def _format_roster_roles_text(entries):
    if not entries:
        return ""
    return " | ".join([f"{label}: {value}" for label, value in entries])


def _build_roster_export_data(days):
    data = []
    for day in days:
        day_items = []
        for item in day["items"]:
            instance = item["instance"]
            local_dt = item.get("local_dt") or timezone.localtime(instance.starts_at)
            day_items.append(
                {
                    "time": local_dt.strftime("%H:%M"),
                    "community": instance.community.code,
                    "label": (instance.liturgy_label or "").strip(),
                    "status": "CANCELADA" if instance.status == "canceled" else "",
                    "roles": _build_roster_role_entries(instance, item["slot_map"]),
                }
            )
        data.append({"date": day["date"], "items": day_items})
    return data


def _build_roster_lines(days):
    lines = []
    for day in _build_roster_export_data(days):
        items = [item for item in day["items"] if not item.get("status")]
        if not items:
            continue
        lines.append(_format_roster_day_header(day["date"]))
        for item in items:
            base = f"{item['time']} {item['community']}"
            if item["label"]:
                base = f"{base} — {item['label']}"
            # single-role masses render inline
            roles = item.get("roles", [])
            if len(roles) == 1:
                label, value = roles[0]
                lines.append(f"{base} — {label}: {value}")
            else:
                lines.append(base)
                for label, value in roles:
                    lines.append(f"  {label}: {value}")
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
    text = _build_roster_lines(context["days"])
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
    data = _build_roster_export_data(context["days"])

    from io import BytesIO
    import os
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    def _wrap_pdf_text(pdf, text, font_name, font_size, max_width):
        words = text.split()
        if not words:
            return [""]
        lines = []
        current = words[0]
        for word in words[1:]:
            candidate = f"{current} {word}"
            if pdf.stringWidth(candidate, font_name, font_size) <= max_width:
                current = candidate
            else:
                lines.append(current)
                current = word
        lines.append(current)
        return lines

    def _wrap_tokens(pdf, tokens, font_name, font_size, max_width, separator="; "):
        lines = []
        current = ""
        for token in tokens:
            if not token:
                continue
            candidate = token if not current else f"{current}{separator}{token}"
            if pdf.stringWidth(candidate, font_name, font_size) <= max_width:
                current = candidate
                continue
            if current:
                lines.append(current)
                current = ""
            if pdf.stringWidth(token, font_name, font_size) <= max_width:
                current = token
            else:
                wrapped = _wrap_pdf_text(pdf, token, font_name, font_size, max_width)
                if wrapped:
                    lines.extend(wrapped[:-1])
                    current = wrapped[-1]
                else:
                    current = token
        if current:
            lines.append(current)
        return lines

    accent_color = colors.HexColor("#7A2E2E")
    muted_color = colors.HexColor("#6B6B6B")
    line_color = colors.HexColor("#D0D0D0")
    text_color = colors.black

    serif_font = "Times-Roman"
    serif_bold_font = "Times-Bold"
    sans_font = "Helvetica"
    sans_bold_font = "Helvetica-Bold"
    sans_italic_font = "Helvetica-Oblique"

    def _find_font_path(paths):
        for path in paths:
            if path and os.path.exists(path):
                return path
        return None

    def _register_font(name, path):
        if not path:
            return None
        try:
            pdfmetrics.registerFont(TTFont(name, path))
            return name
        except Exception:
            return None

    serif_path = _find_font_path(
        [
            "C:\\Windows\\Fonts\\times.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSerif-Regular.ttf",
            "/usr/share/fonts/truetype/noto/NotoSerif-Regular.ttf",
            "/System/Library/Fonts/Supplemental/Times New Roman.ttf",
        ]
    )
    serif_bold_path = _find_font_path(
        [
            "C:\\Windows\\Fonts\\timesbd.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSerif-Bold.ttf",
            "/usr/share/fonts/truetype/noto/NotoSerif-Bold.ttf",
            "/System/Library/Fonts/Supplemental/Times New Roman Bold.ttf",
        ]
    )
    sans_path = _find_font_path(
        [
            "C:\\Windows\\Fonts\\arial.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf",
            "/System/Library/Fonts/Supplemental/Arial.ttf",
        ]
    )
    sans_bold_path = _find_font_path(
        [
            "C:\\Windows\\Fonts\\arialbd.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf",
            "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        ]
    )
    sans_italic_path = _find_font_path(
        [
            "C:\\Windows\\Fonts\\ariali.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Oblique.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Italic.ttf",
            "/usr/share/fonts/truetype/noto/NotoSans-Italic.ttf",
            "/System/Library/Fonts/Supplemental/Arial Italic.ttf",
        ]
    )

    serif_font = _register_font("AcoliSerif", serif_path) or serif_font
    serif_bold_font = _register_font("AcoliSerifBold", serif_bold_path) or serif_bold_font
    sans_font = _register_font("AcoliSans", sans_path) or sans_font
    sans_bold_font = _register_font("AcoliSansBold", sans_bold_path) or sans_bold_font
    sans_italic_font = _register_font("AcoliSansItalic", sans_italic_path) or sans_italic_font

    width, height = A4
    left = 40
    right = 40
    top = 46
    bottom = 50
    max_width = width - left - right

    def _draw_footer(pdf, page_num, page_count):
        footer_y = 30
        pdf.setFont(sans_font, 8)
        pdf.setFillColor(muted_color)
        version = timezone.localtime(timezone.now()).strftime("v%Y%m%d")
        pdf.drawString(left, footer_y, f"{parish.name} - Escala de Acólitos — {version}")
        pdf.drawRightString(width - right, footer_y, f"{page_num}/{page_count}")
        pdf.setFillColor(text_color)

    class _NumberedCanvas(canvas.Canvas):
        def __init__(self, *args, **kwargs):
            self._saved_page_states = []
            super().__init__(*args, **kwargs)

        def showPage(self):
            self._saved_page_states.append(dict(self.__dict__))
            self._startPage()

        def save(self):
            self._saved_page_states.append(dict(self.__dict__))
            page_count = len(self._saved_page_states)
            for state in self._saved_page_states:
                self.__dict__.update(state)
                _draw_footer(self, self._pageNumber, page_count)
                canvas.Canvas.showPage(self)
            canvas.Canvas.save(self)

    def _draw_header(pdf, y, compact=False):
        pdf.setFillColor(text_color)
        if compact:
            pdf.setFont(serif_bold_font, 12)
            pdf.drawString(left, y, parish.name)
            pdf.setFont(sans_font, 9)
            pdf.setFillColor(muted_color)
            pdf.drawRightString(width - right, y, "Escala de Acólitos")
            y -= 12
        else:
            pdf.setFont(serif_bold_font, 18)
            pdf.drawString(left, y, parish.name)
            y -= 20
            pdf.setFont(sans_font, 11)
            pdf.setFillColor(muted_color)
            pdf.drawString(left, y, "Escala de Acólitos - Missas")
            y -= 14
            pdf.setFont(sans_font, 9)
            pdf.drawString(
                left,
                y,
                (
                    f"Período: {start_date.strftime('%d/%m/%Y')} a {end_date.strftime('%d/%m/%Y')}"
                    f" | Gerado em: {timezone.localtime(timezone.now()).strftime('%d/%m/%Y %H:%M')}"
                ),
            )
            y -= 12
        pdf.setStrokeColor(line_color)
        pdf.line(left, y, width - right, y)
        y -= 12
        pdf.setFillColor(text_color)
        return y

    def _start_page(pdf):
        pdf.showPage()
        y = height - top
        return _draw_header(pdf, y, compact=True)

    def _draw_legend(pdf, y, context):
        if not context:
            return y

        role_positions = {}
        for day in context.get("days", []):
            for item in day.get("items", []):
                for slot in item.get("slot_map", {}).values():
                    position = getattr(slot, "position_type", None)
                    if not position:
                        continue
                    role_positions[position.id] = position

        def _position_sort_key(position):
            code = (position.code or position.name or "").upper()
            code = code.replace(" ", "").replace("+", "_")
            if code in ROSTER_POSITION_ORDER:
                return (ROSTER_POSITION_ORDER.index(code), "")
            return (len(ROSTER_POSITION_ORDER), code)

        role_tokens = []
        for position in sorted(role_positions.values(), key=_position_sort_key):
            label = position.code or position.name or "Funcao"
            label = label.replace("_", " + ")
            title = position.name or label
            role_tokens.append(f"{label} = {title}")

        try:
            communities = list(parish.community_set.filter(active=True).order_by("code"))
        except Exception:
            communities = []
        comm_tokens = [f"{c.code} = {c.name}" for c in communities] if communities else []

        if not role_tokens and not comm_tokens:
            return y

        pdf.setFont(sans_font, 8)
        pdf.setFillColor(muted_color)
        line_height = 10
        gap_after_block = 4

        if role_tokens:
            prefix = "Legenda — Funções: "
            prefix_width = pdf.stringWidth(prefix, sans_font, 8)
            lines = _wrap_tokens(pdf, role_tokens, sans_font, 8, max_width - prefix_width)
            if lines:
                pdf.drawString(left, y, prefix)
                pdf.drawString(left + prefix_width, y, lines[0])
                y -= line_height
                for line in lines[1:]:
                    pdf.drawString(left + prefix_width, y, line)
                    y -= line_height
                y -= gap_after_block

        if comm_tokens:
            prefix = "Legenda — Comunidades: "
            prefix_width = pdf.stringWidth(prefix, sans_font, 8)
            lines = _wrap_tokens(pdf, comm_tokens, sans_font, 8, max_width - prefix_width)
            if lines:
                pdf.drawString(left, y, prefix)
                pdf.drawString(left + prefix_width, y, lines[0])
                y -= line_height
                for line in lines[1:]:
                    pdf.drawString(left + prefix_width, y, line)
                    y -= line_height
                y -= gap_after_block

        pdf.setFillColor(text_color)
        return y

    def _draw_month_header(pdf, y, day_date):
        month_label = f"{ROSTER_MONTH_NAMES[day_date.month - 1]} {day_date.year}"
        pdf.setFont(sans_bold_font, 10)
        pdf.setFillColor(muted_color)
        pdf.drawString(left, y, month_label)
        y -= 10
        pdf.setStrokeColor(line_color)
        pdf.line(left, y, width - right, y)
        # breathing after month header
        y -= GAP_AFTER_MONTH_HEADER
        pdf.setFillColor(text_color)
        return y

    def _draw_day_header(pdf, y, day_date):
        pdf.setFont(serif_bold_font, 12)
        pdf.setFillColor(accent_color)
        pdf.drawString(left, y, _format_roster_day_header(day_date))
        # primary day header height
        y -= 14
        # small gap after day header before first mass
        y -= GAP_AFTER_DAY_HEADER
        pdf.setFillColor(text_color)
        return y

    buffer = BytesIO()
    pdf = _NumberedCanvas(buffer, pagesize=A4)
    y = _draw_header(pdf, height - top, compact=False)
    # draw legend on first page after header
    y = _draw_legend(pdf, y, context)

    current_month = None
    time_font = serif_bold_font
    time_size = 10
    community_font = sans_bold_font
    community_size = 9
    label_font = sans_font
    label_size = 9
    role_label_font = sans_bold_font
    role_value_font = sans_font
    role_value_emphasis = sans_bold_font
    status_font = sans_italic_font
    header_leading = 12
    label_leading = 11
    role_leading = 11
    roles_indent = 12
    roles_width = max_width - roles_indent
    column_gap = 18
    # fixed column stops for the mass line
    time_col_width = pdf.stringWidth("00:00", time_font, time_size) + 6
    community_col_width = pdf.stringWidth("WWW", community_font, community_size) + 10
    x_time = left
    x_comm = x_time + time_col_width
    x_sep = x_comm + community_col_width
    sep_text = "—"
    sep_width = pdf.stringWidth("— ", label_font, label_size)
    # vertical spacing constants (typographic polish)
    GAP_AFTER_MONTH_HEADER = 10
    GAP_BEFORE_DAY_HEADER = 8
    GAP_AFTER_DAY_HEADER = 4
    GAP_BETWEEN_MASSES = 2
    GAP_AFTER_DAY_BLOCK = 6
    month_header_height = 18
    day_header_height = 14

    def _role_column(label):
        key = label.upper().replace(" ", "").replace("+", "_")
        if key.startswith("LIB") or key.startswith("NAV") or key.startswith("TUR"):
            return "right"
        return "left"

    role_labels = []
    for day in data:
        for item in day["items"]:
            for label, _ in item.get("roles", []):
                role_labels.append(label)

    left_label_widths = []
    right_label_widths = []
    for label in role_labels:
        width = pdf.stringWidth(f"{label}: ", role_label_font, 9)
        if _role_column(label) == "right":
            right_label_widths.append(width)
        else:
            left_label_widths.append(width)

    default_left = max(
        pdf.stringWidth("CER: ", role_label_font, 9),
        pdf.stringWidth("CRU + MIC: ", role_label_font, 9),
    )
    default_right = max(
        pdf.stringWidth("LIB: ", role_label_font, 9),
        pdf.stringWidth("NAV: ", role_label_font, 9),
        pdf.stringWidth("TUR: ", role_label_font, 9),
    )
    label_width_left = max(left_label_widths) if left_label_widths else default_left
    label_width_right = max(right_label_widths) if right_label_widths else default_right
    label_width_single = max(label_width_left, label_width_right)

    def _build_column_layout(pdf, entries, column_width, label_width=None):
        if not entries:
            return {"entries": [], "height": 0, "label_width": label_width or 0}
        if label_width is None:
            label_texts = [f"{label}: " for label, _ in entries]
            label_width = max(pdf.stringWidth(text, role_label_font, 9) for text in label_texts)
        value_width = max(10, column_width - label_width)
        layout = []
        total_lines = 0
        for label, value in entries:
            label_text = f"{label}: "
            value_font = role_value_emphasis if value in ("ABERTO", "EXTERNO") else role_value_font
            value_lines = _wrap_pdf_text(pdf, value, value_font, 9, value_width)
            layout.append((label_text, label_width, value_lines, value_font))
            total_lines += len(value_lines)
        return {"entries": layout, "height": total_lines * role_leading, "label_width": label_width}

    def _build_item_layout(pdf, item):
        time_text = item["time"]
        community_text = item["community"]
        label_text = item["label"]
        status_text = item["status"]

        roles = item["roles"] if not status_text else []
        single_role = len(roles) == 1 and not status_text
        role_inline_text = None
        if single_role:
            role_label, role_value = roles[0]
            role_inline_text = f"{role_label}: {role_value}"

        details_text = ""
        if label_text and role_inline_text:
            details_text = f"{label_text} — {role_inline_text}"
        elif label_text:
            details_text = label_text
        elif role_inline_text:
            details_text = role_inline_text

        inline_available = max_width - (x_sep - left) - sep_width
        details_lines = []
        details_inline = False
        if details_text:
            if inline_available >= 80:
                details_lines = _wrap_pdf_text(
                    pdf, details_text, label_font, label_size, inline_available
                )
                details_inline = True
            else:
                details_lines = _wrap_pdf_text(
                    pdf, details_text, label_font, label_size, inline_available
                )

        render_roles = bool(roles) and not role_inline_text and not status_text
        use_two_columns = False
        left_layout = None
        right_layout = None
        roles_block_height = 0
        if render_roles:
            if len(roles) >= 4:
                left_entries = []
                right_entries = []
                for label, value in roles:
                    if _role_column(label) == "right":
                        right_entries.append((label, value))
                    else:
                        left_entries.append((label, value))
                if right_entries:
                    use_two_columns = True
                    column_width = (roles_width - column_gap) / 2
                    left_layout = _build_column_layout(
                        pdf, left_entries, column_width, label_width_left
                    )
                    right_layout = _build_column_layout(
                        pdf, right_entries, column_width, label_width_right
                    )
                    roles_block_height = max(left_layout["height"], right_layout["height"])
                else:
                    left_layout = _build_column_layout(
                        pdf, roles, roles_width, label_width_single
                    )
                    roles_block_height = left_layout["height"]
            else:
                left_layout = _build_column_layout(
                    pdf, roles, roles_width, label_width_single
                )
                roles_block_height = left_layout["height"]

        block_height = header_leading
        if details_lines:
            if details_inline:
                block_height += label_leading * max(0, len(details_lines) - 1)
            else:
                block_height += label_leading * len(details_lines)
        if status_text:
            block_height += label_leading + GAP_BETWEEN_MASSES
        elif render_roles and roles_block_height:
            block_height += GAP_BETWEEN_MASSES + roles_block_height + GAP_BETWEEN_MASSES
        else:
            block_height += GAP_BETWEEN_MASSES

        return {
            "time_text": time_text,
            "community_text": community_text,
            "label_text": label_text,
            "status_text": status_text,
            "details_lines": details_lines,
            "details_inline": details_inline,
            "render_roles": render_roles,
            "use_two_columns": use_two_columns,
            "left_layout": left_layout,
            "right_layout": right_layout,
            "roles_block_height": roles_block_height,
            "block_height": block_height,
            "roles": roles,
        }

    for day in data:
        first_layout = _build_item_layout(pdf, day["items"][0]) if day["items"] else None
        month_changed = day["date"].month != current_month

        required_for_day = day_header_height + GAP_AFTER_DAY_HEADER
        if first_layout:
            required_for_day += first_layout["block_height"]

        required = required_for_day
        if month_changed:
            required += month_header_height + GAP_AFTER_MONTH_HEADER
        else:
            required += GAP_BEFORE_DAY_HEADER

        if y - required < bottom:
            y = _start_page(pdf)

        if month_changed:
            y = _draw_month_header(pdf, y, day["date"])
            current_month = day["date"].month
        else:
            y -= GAP_BEFORE_DAY_HEADER

        y = _draw_day_header(pdf, y, day["date"])

        for item_index, item in enumerate(day["items"]):
            layout = first_layout if item_index == 0 and first_layout else _build_item_layout(pdf, item)
            if y - layout["block_height"] < bottom:
                y = _start_page(pdf)
                y = _draw_day_header(pdf, y, day["date"])

            pdf.setFont(time_font, time_size)
            pdf.setFillColor(text_color)
            pdf.drawString(x_time, y, layout["time_text"])

            pdf.setFont(community_font, community_size)
            pdf.setFillColor(muted_color)
            pdf.drawString(x_comm, y, layout["community_text"])
            pdf.setFillColor(text_color)

            if layout["details_lines"] and layout["details_inline"]:
                pdf.setFont(label_font, label_size)
                pdf.drawString(x_sep, y, sep_text)
                pdf.drawString(x_sep + sep_width, y, layout["details_lines"][0])

            y -= header_leading

            if layout["details_lines"]:
                pdf.setFont(label_font, label_size)
                details_indent = x_sep + sep_width
                if layout["details_inline"]:
                    for line in layout["details_lines"][1:]:
                        pdf.drawString(details_indent, y, line)
                        y -= label_leading
                else:
                    pdf.drawString(x_sep, y, sep_text)
                    pdf.drawString(details_indent, y, layout["details_lines"][0])
                    y -= label_leading
                    for line in layout["details_lines"][1:]:
                        pdf.drawString(details_indent, y, line)
                        y -= label_leading

            if layout["status_text"]:
                pdf.setFont(status_font, 9)
                pdf.setFillColor(muted_color)
                pdf.drawString(x_sep + sep_width, y, layout["status_text"])
                pdf.setFillColor(text_color)
                y -= label_leading
                y -= GAP_BETWEEN_MASSES
                continue

            if layout["render_roles"]:
                y -= GAP_BETWEEN_MASSES
                roles_x = left + roles_indent
                if layout["use_two_columns"] and layout["right_layout"]:
                    column_width = (roles_width - column_gap) / 2
                    left_x = roles_x
                    right_x = roles_x + column_width + column_gap
                    y_left = y
                    y_right = y
                    for label_text, label_width, value_lines, value_font in layout["left_layout"]["entries"]:
                        pdf.setFont(role_label_font, 9)
                        pdf.drawString(left_x, y_left, label_text)
                        pdf.setFont(value_font, 9)
                        pdf.drawString(left_x + label_width, y_left, value_lines[0])
                        y_left -= role_leading
                        for line in value_lines[1:]:
                            pdf.drawString(left_x + label_width, y_left, line)
                            y_left -= role_leading
                    for label_text, label_width, value_lines, value_font in layout["right_layout"]["entries"]:
                        pdf.setFont(role_label_font, 9)
                        pdf.drawString(right_x, y_right, label_text)
                        pdf.setFont(value_font, 9)
                        pdf.drawString(right_x + label_width, y_right, value_lines[0])
                        y_right -= role_leading
                        for line in value_lines[1:]:
                            pdf.drawString(right_x + label_width, y_right, line)
                            y_right -= role_leading
                    y -= max(layout["left_layout"]["height"], layout["right_layout"]["height"])
                else:
                    for label_text, label_width, value_lines, value_font in layout["left_layout"]["entries"]:
                        pdf.setFont(role_label_font, 9)
                        pdf.drawString(roles_x, y, label_text)
                        pdf.setFont(value_font, 9)
                        pdf.drawString(roles_x + label_width, y, value_lines[0])
                        y -= role_leading
                        for line in value_lines[1:]:
                            pdf.drawString(roles_x + label_width, y, line)
                            y -= role_leading

            y -= GAP_BETWEEN_MASSES
        y -= GAP_AFTER_DAY_BLOCK
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
    slots_context = _build_slots_context(parish, instance, request.user)
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
    cache = build_recommendation_cache(parish, slots=[slot])
    assigned_acolyte_ids = set(
        Assignment.objects.filter(
            parish=parish,
            slot__mass_instance=slot.mass_instance,
            is_active=True,
        ).values_list("acolyte_id", flat=True)
    )
    return rank_candidates(
        slot,
        parish,
        query=query,
        cache=cache,
        exclude_acolyte_ids=assigned_acolyte_ids,
        enforce_dynamic=True,
        include_meta=True,
    )


def _build_slots_context(parish, instance, user=None):
    """Build the context needed for rendering the slots section."""
    slots = list(
        instance.slots.select_related("position_type").prefetch_related(
            Prefetch(
                "assignments",
                queryset=Assignment.objects.filter(is_active=True).select_related("acolyte", "confirmation"),
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
    quick_fill_cache = build_quick_fill_cache(parish, position_type_ids=position_ids, slots=slots)
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
    claims_by_slot = defaultdict(list)
    user_claims_by_slot = {}
    user_acolyte = None
    user_has_assignment_in_instance = False
    if user:
        user_acolyte = parish.acolytes.filter(user=user).first()
        if user_acolyte:
            user_has_assignment_in_instance = user_acolyte.id in assigned_acolyte_ids
    claim_statuses = ["pending_target", "scheduled_auto_approve", "pending_coordination"]
    actionable_statuses = {"pending_target", "scheduled_auto_approve"}
    claims = PositionClaimRequest.objects.filter(
        parish=parish,
        slot__mass_instance=instance,
        status__in=claim_statuses,
    ).select_related("requestor_acolyte", "target_assignment").order_by("created_at")
    for claim in claims:
        if claim.status in actionable_statuses:
            claims_by_slot[claim.slot_id].append(claim)
        if user_acolyte and claim.requestor_acolyte_id == user_acolyte.id:
            user_claims_by_slot[claim.slot_id] = claim
    return {
        "instance": instance,
        "slots": slots,
        "slot_suggestions": slot_suggestions,
        "claims_by_slot": claims_by_slot,
        "user_claims_by_slot": user_claims_by_slot,
        "user_acolyte": user_acolyte,
        "user_has_assignment_in_instance": user_has_assignment_in_instance,
        "claim_auto_approve_enabled": parish.claim_auto_approve_enabled,
        "claim_auto_approve_hours": parish.claim_auto_approve_hours,
    }


def _claim_map_for_slots(parish, slot_ids, statuses=None):
    if not slot_ids:
        return {}
    status_filter = statuses or ["pending_target", "scheduled_auto_approve"]
    claims = PositionClaimRequest.objects.filter(
        parish=parish,
        slot_id__in=slot_ids,
        status__in=status_filter,
    ).select_related("requestor_acolyte").order_by("created_at")
    claims_by_slot = defaultdict(list)
    for claim in claims:
        claims_by_slot[claim.slot_id].append(claim)
    return claims_by_slot


def _assignment_card_context(parish, assignment):
    mass_id = assignment.slot.mass_instance_id
    team_members = Assignment.objects.filter(
        parish=parish,
        is_active=True,
        slot__mass_instance_id=mass_id,
    ).select_related("acolyte").order_by("acolyte__display_name")
    team_names_map = {}
    mass_has_multiple_slots = {}
    if team_members:
        names = [member.acolyte.display_name for member in team_members]
        team_names_map[mass_id] = ", ".join(names)
        mass_has_multiple_slots[mass_id] = len(names) > 1
    pending_swaps = SwapRequest.objects.filter(
        parish=parish,
        requestor_acolyte=assignment.acolyte,
        status="pending",
        from_slot__isnull=False,
    ).values_list("from_slot_id", flat=True)
    pending_swaps_by_slot = {slot_id: True for slot_id in pending_swaps}
    claims_by_slot = _claim_map_for_slots(parish, [assignment.slot_id])
    return {
        "assignment": assignment,
        "team_names_map": team_names_map,
        "mass_has_multiple_slots": mass_has_multiple_slots,
        "pending_swaps_by_slot": pending_swaps_by_slot,
        "claims_by_slot": claims_by_slot,
    }


def _claim_card_context(request, claim):
    return {
        "claim": claim,
        "can_manage_parish": user_has_role(request.user, claim.parish, ADMIN_ROLE_CODES),
        "claim_status_labels": dict(PositionClaimRequest.STATUS_CHOICES),
        "claim_reason_labels": dict(PositionClaimRequest.RESOLUTION_CHOICES),
    }


def _claim_response(request, claim, return_target, success_message=None):
    parish = claim.parish
    if return_target == "slots_section":
        return _htmx_or_redirect(
            request,
            "calendar/_slots_section.html",
            _build_slots_context(parish, claim.slot.mass_instance, request.user),
            reverse("mass_detail", args=[claim.slot.mass_instance_id]),
            success_message,
        )

    if return_target == "dashboard_hero":
        assignment = Assignment.objects.filter(id=claim.target_assignment_id, is_active=True).select_related(
            "slot__mass_instance__community",
            "slot__position_type",
        ).first()
        if assignment:
            return _htmx_or_redirect(
                request,
                "acolytes/_partials/dashboard_hero_assignment.html",
                {
                    "hero_assignment": assignment,
                    "claims_by_slot": _claim_map_for_slots(parish, [assignment.slot_id]),
                },
                "dashboard",
                success_message,
            )
        if request.headers.get("HX-Request"):
            response = HttpResponse("")
            if success_message:
                response["HX-Success-Message"] = success_message
            return response
        return redirect("dashboard")

    if return_target == "dashboard_pending":
        assignment = Assignment.objects.filter(id=claim.target_assignment_id, is_active=True).select_related(
            "slot__mass_instance",
            "slot__position_type",
            "confirmation",
        ).first()
        if assignment:
            return _htmx_or_redirect(
                request,
                "acolytes/_partials/dashboard_pending_row.html",
                {
                    "assignment": assignment,
                    "claims_by_slot": _claim_map_for_slots(parish, [assignment.slot_id]),
                },
                "dashboard",
                success_message,
            )
        if request.headers.get("HX-Request"):
            response = HttpResponse("")
            if success_message:
                response["HX-Success-Message"] = success_message
            return response
        return redirect("dashboard")

    if return_target == "assignment_card":
        assignment = Assignment.objects.filter(id=claim.target_assignment_id, is_active=True).select_related(
            "slot__mass_instance__community",
            "slot__position_type",
            "confirmation",
        ).first()
        if assignment:
            return _htmx_or_redirect(
                request,
                "acolytes/_partials/assignment_card.html",
                _assignment_card_context(parish, assignment),
                "my_assignments",
                success_message,
            )
        if request.headers.get("HX-Request"):
            response = HttpResponse("")
            if success_message:
                response["HX-Success-Message"] = success_message
            return response
        return redirect("my_assignments")

    if return_target == "claim_card":
        return _htmx_or_redirect(
            request,
            "acolytes/_partials/claim_card.html",
            _claim_card_context(request, claim),
            "swap_requests",
            success_message,
        )

    if request.headers.get("HX-Request"):
        response = HttpResponse("")
        if success_message:
            response["HX-Success-Message"] = success_message
        return response
    if success_message:
        messages.success(request, success_message)
    return redirect("dashboard")


def _handle_slot_assign(request, instance_id, slot_id, action_label):
    parish = request.active_parish
    instance = get_object_or_404(MassInstance, parish=parish, id=instance_id)
    slot = get_object_or_404(AssignmentSlot, parish=parish, id=slot_id, mass_instance=instance)

    def _htmx_or_redirect():
        """Return partial for HTMX or redirect for normal requests."""
        if request.htmx:
            slots_context = _build_slots_context(parish, instance, request.user)
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
def slot_confirm_assignment(request, instance_id, slot_id):
    parish = request.active_parish
    instance = get_object_or_404(MassInstance, parish=parish, id=instance_id)
    slot = get_object_or_404(AssignmentSlot, parish=parish, id=slot_id, mass_instance=instance)
    if request.method != "POST":
        return redirect("mass_detail", instance_id=instance.id)

    assignment = slot.get_active_assignment()
    if not assignment:
        messages.info(request, "Nao ha acolito atribuido para confirmar.")
        return _htmx_or_redirect(
            request,
            "calendar/_slots_section.html",
            _build_slots_context(parish, instance, request.user),
            reverse("mass_detail", args=[instance.id]),
        )

    confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
    confirmation.status = "confirmed"
    confirmation.updated_by = request.user
    confirmation.save(update_fields=["status", "updated_by", "timestamp"])
    log_audit(parish, request.user, "Confirmation", confirmation.id, "update", {"status": "confirmed"})
    expire_claims_for_assignment(assignment, "holder_confirmed", actor=request.user)

    return _htmx_or_redirect(
        request,
        "calendar/_slots_section.html",
        _build_slots_context(parish, instance, request.user),
        reverse("mass_detail", args=[instance.id]),
        "Presenca confirmada.",
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def slot_remove_assignment(request, instance_id, slot_id):
    parish = request.active_parish
    instance = get_object_or_404(MassInstance, parish=parish, id=instance_id)
    slot = get_object_or_404(AssignmentSlot, parish=parish, id=slot_id, mass_instance=instance)
    if request.method != "POST":
        return redirect("mass_detail", instance_id=instance.id)

    assignment = slot.get_active_assignment()
    if not assignment:
        messages.info(request, "Nao ha acolito atribuido para remover.")
        return _htmx_or_redirect(
            request,
            "calendar/_slots_section.html",
            _build_slots_context(parish, instance, request.user),
            reverse("mass_detail", args=[instance.id]),
        )

    try:
        deactivate_assignment(assignment, reason="manual_unassign", actor=request.user)
        slot = assignment.slot
        if slot.required and not slot.externally_covered:
            slot.status = "open"
            slot.save(update_fields=["status", "updated_at"])
    except Exception as exc:
        messages.error(request, f"Erro ao remover escala: {exc}")
        return _htmx_or_redirect(
            request,
            "calendar/_slots_section.html",
            _build_slots_context(parish, instance, request.user),
            reverse("mass_detail", args=[instance.id]),
        )

    return _htmx_or_redirect(
        request,
        "calendar/_slots_section.html",
        _build_slots_context(parish, instance, request.user),
        reverse("mass_detail", args=[instance.id]),
        "Escala removida.",
    )


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
                "Nao e possivel excluir esta funcao. Existem escalas historicas. Desative a funcao se nao for mais usada.",
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
    weights = parish.schedule_weights or {}
    deadline_hours = int(weights.get("interest_deadline_hours", 48) or 0)
    now = timezone.now()
    mass_qs = (
        MassInstance.objects.filter(
            parish=parish,
            status="scheduled",
            starts_at__date__gte=timezone.localdate(),
            event_series__isnull=False,
        )
        .select_related("community", "event_series")
        .order_by("starts_at")
    )
    series = (
        EventSeries.objects.filter(parish=parish, is_active=True, end_date__gte=timezone.localdate())
        .order_by("start_date")
        .prefetch_related(Prefetch("massinstance_set", queryset=mass_qs, to_attr="upcoming_masses"))
    )
    series_list = list(series)
    interests = {}
    if acolyte:
        interests = {
            interest.mass_instance_id: interest
            for interest in MassInterest.objects.filter(parish=parish, acolyte=acolyte)
        }
    closed_ids = set()
    deadline_by_mass = {}
    for item in series_list:
        for mass in getattr(item, "upcoming_masses", []):
            if item.candidate_pool != "interested_only":
                continue
            deadline_at = item.interest_deadline_at
            if deadline_at and timezone.is_naive(deadline_at):
                deadline_at = timezone.make_aware(deadline_at, timezone.get_current_timezone())
            if not deadline_at and deadline_hours:
                deadline_at = mass.starts_at - timedelta(hours=deadline_hours)
            if deadline_at:
                deadline_by_mass[mass.id] = deadline_at
                if now >= deadline_at:
                    closed_ids.add(mass.id)

    if request.method == "POST" and acolyte:
        selected = set(request.POST.getlist("masses"))
        masses = [mass for item in series_list for mass in item.upcoming_masses]
        for mass in masses:
            if mass.id in closed_ids:
                continue
            interested = str(mass.id) in selected
            obj, _ = MassInterest.objects.get_or_create(
                parish=parish, mass_instance=mass, acolyte=acolyte, defaults={"interested": interested}
            )
            if obj.interested != interested:
                obj.interested = interested
                obj.save(update_fields=["interested", "updated_at"])
        return redirect("event_interest")
    return render(
        request,
        "events/interest.html",
        {
            "series": series_list,
            "interests": interests,
            "acolyte": acolyte,
            "closed_ids": closed_ids,
            "deadline_by_mass": deadline_by_mass,
        },
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
                "interest_deadline_at": cleaned["interest_deadline_at"].isoformat() if cleaned.get("interest_deadline_at") else None,
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
    interest_deadline_at = None
    if draft.get("interest_deadline_at"):
        interest_deadline_at = datetime.fromisoformat(draft["interest_deadline_at"])
        if timezone.is_naive(interest_deadline_at):
            interest_deadline_at = timezone.make_aware(interest_deadline_at, timezone.get_current_timezone())

    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)

    OccurrenceFormSet = formset_factory(EventOccurrenceForm, extra=0)
    default_label = draft["title"] if start_date == end_date else ""
    series = None
    if request.method == "POST":
        formset = OccurrenceFormSet(request.POST, form_kwargs={"parish": parish})
        extra_occurrences_by_index = [[] for _ in range(len(formset))]
        base_occurrence_ids = [None for _ in range(len(formset))]
        if formset.is_valid():
            series = EventSeries.objects.create(
                parish=parish,
                series_type=draft["series_type"],
                title=draft["title"],
                start_date=start_date,
                end_date=end_date,
                default_community_id=default_community,
                candidate_pool=draft.get("candidate_pool", "all"),
                interest_deadline_at=interest_deadline_at,
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
            for i, form in enumerate(formset):
                day_index = i
                slot_indices = {0}
                for key in request.POST.keys():
                    match = re.search(rf'^form-{day_index}-time-(\d+)$', key)
                    if match:
                        slot_indices.add(int(match.group(1)))

                for slot_index in sorted(slot_indices):
                    if slot_index == 0:
                        data = form.cleaned_data
                        time_value = data.get("time")
                        community_value = data.get("community")
                        profile_value = data.get("requirement_profile")
                        conflict_value = data.get("conflict_action") or "keep"
                        label_value = data.get("label")
                        move_date_value = data.get("move_to_date")
                        move_time_value = data.get("move_to_time")
                        move_community_value = data.get("move_to_community")
                        community_id = community_value.id if community_value else default_community
                        profile_id = profile_value.id if profile_value else default_profile
                        move_community_id = move_community_value.id if move_community_value else None
                    else:
                        time_key = f'form-{day_index}-time-{slot_index}'
                        community_key = f'form-{day_index}-community-{slot_index}'
                        profile_key = f'form-{day_index}-requirement_profile-{slot_index}'
                        conflict_key = f'form-{day_index}-conflict_action-{slot_index}'
                        label_key = f'form-{day_index}-label-{slot_index}'
                        move_date_key = f'form-{day_index}-move_to_date-{slot_index}'
                        move_time_key = f'form-{day_index}-move_to_time-{slot_index}'
                        move_community_key = f'form-{day_index}-move_to_community-{slot_index}'

                        time_value = _parse_time_value(request.POST.get(time_key))
                        community_value = _parse_fk_id(request.POST.get(community_key))
                        profile_value = _parse_fk_id(request.POST.get(profile_key))
                        conflict_value = request.POST.get(conflict_key, "keep")
                        label_value = request.POST.get(label_key)
                        move_date_value = _parse_date(request.POST.get(move_date_key), None)
                        move_time_value = _parse_time_value(request.POST.get(move_time_key))
                        move_community_value = _parse_fk_id(request.POST.get(move_community_key))
                        community_id = community_value or default_community
                        profile_id = profile_value or default_profile
                        move_community_id = move_community_value or None

                    if time_value:
                        occurrence = EventOccurrence.objects.create(
                            parish=parish,
                            event_series=series,
                            date=form.cleaned_data["date"],
                            time=time_value,
                            community_id=community_id,
                            requirement_profile_id=profile_id,
                            label=label_value or draft["title"],
                            conflict_action=conflict_value,
                            move_to_date=move_date_value,
                            move_to_time=move_time_value,
                            move_to_community_id=move_community_id,
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
                    ).exclude(event_series_id=series.id).first()
                    conflicts.append(conflict)
                return render(
                    request,
                    "events/days.html",
                    {
                        "formset": formset,
                        "conflicts": conflicts,
                        "draft": draft,
                        "extra_occurrences_by_index": extra_occurrences_by_index,
                        "base_occurrence_ids": base_occurrence_ids,
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
        extra_occurrences_by_index = [[] for _ in range(len(formset))]
        base_occurrence_ids = [None for _ in range(len(formset))]

    conflicts = []
    for form in formset:
        data = form.initial
        conflict_query = MassInstance.objects.filter(
            parish=parish,
            community_id=data.get("community"),
            starts_at__date=data.get("date"),
            starts_at__time=data.get("time"),
            status="scheduled",
        )
        if series:
            conflict_query = conflict_query.exclude(event_series_id=series.id)
        conflict = conflict_query.first()
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
            "extra_occurrences_by_index": extra_occurrences_by_index,
            "base_occurrence_ids": base_occurrence_ids,
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def event_series_detail(request, series_id):
    parish = request.active_parish
    series = get_object_or_404(EventSeries, parish=parish, id=series_id)
    weights = parish.schedule_weights or {}
    interest_deadline_hours = int(weights.get("interest_deadline_hours", 48) or 0)
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
            "interest_deadline_hours": interest_deadline_hours,
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
        "interest_deadline_at": series.interest_deadline_at,
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
            series.interest_deadline_at = cleaned.get("interest_deadline_at")
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

    occurrences_by_date = defaultdict(list)
    for occ in series.occurrences.all().order_by("time"):
        occurrences_by_date[occ.date].append(occ)
    base_occurrence_ids = []
    extra_occurrences_by_index = []
    for date_value in dates:
        existing_list = occurrences_by_date.get(date_value, [])
        base_occurrence_ids.append(existing_list[0].id if existing_list else None)
        extra_occurrences_by_index.append(existing_list[1:] if len(existing_list) > 1 else [])
    OccurrenceFormSet = formset_factory(EventOccurrenceForm, extra=0)
    def _normalize_conflict_action(value, date_value, time_value, community_id):
        if value not in {"cancel_existing", "move_existing"}:
            return value
        if not (date_value and time_value and community_id):
            return value
        has_conflict = MassInstance.objects.filter(
            parish=parish,
            community_id=community_id,
            starts_at__date=date_value,
            starts_at__time=time_value,
            status="scheduled",
        ).exclude(event_series_id=series.id).exists()
        return value if has_conflict else "keep"

    def _cancel_mass_instance(instance):
        instance.status = "canceled"
        instance.save(update_fields=["status", "updated_at"])
        MassOverride.objects.create(
            parish=parish,
            instance=instance,
            override_type="cancel_instance",
            payload={"reason": "event_series", "event_series_id": series.id},
            created_by=request.user,
        )
        log_audit(parish, request.user, "MassInstance", instance.id, "cancel", {"event_series_id": series.id})

    def _reconcile_series_masses():
        desired_slots = {
            (occ.date, occ.time, occ.community_id)
            for occ in EventOccurrence.objects.filter(event_series=series).exclude(conflict_action="skip")
        }
        scheduled = MassInstance.objects.filter(event_series=series, status="scheduled")
        grouped = defaultdict(list)
        for instance in scheduled:
            local_dt = timezone.localtime(instance.starts_at)
            key = (local_dt.date(), local_dt.time().replace(tzinfo=None), instance.community_id)
            grouped[key].append(instance)

        for key, instances in grouped.items():
            if key not in desired_slots:
                for instance in instances:
                    _cancel_mass_instance(instance)
                continue
            if len(instances) > 1:
                for instance in instances[1:]:
                    _cancel_mass_instance(instance)

    if request.method == "POST":
        formset = OccurrenceFormSet(request.POST, form_kwargs={"parish": parish})
        if formset.is_valid():
            occurrences = []
            submitted_occurrence_ids = set()
            for i, form in enumerate(formset):
                data = form.cleaned_data
                label = data.get("label") or series.title
                existing_list = occurrences_by_date.get(data["date"], [])
                occurrence_id = _parse_fk_id(request.POST.get(f"form-{i}-occurrence_id"))
                existing = None
                if occurrence_id:
                    existing = EventOccurrence.objects.filter(event_series=series, id=occurrence_id).first()
                if not existing:
                    existing = existing_list[0] if existing_list else None
                if existing:
                    community_id = data["community"].id if data.get("community") else series.default_community_id
                    conflict_value = _normalize_conflict_action(
                        data.get("conflict_action"),
                        data.get("date"),
                        data.get("time"),
                        community_id,
                    )
                    existing.time = data["time"]
                    existing.community = data["community"]
                    existing.requirement_profile = data["requirement_profile"]
                    existing.label = label
                    existing.conflict_action = conflict_value
                    existing.move_to_date = data.get("move_to_date")
                    existing.move_to_time = data.get("move_to_time")
                    existing.move_to_community = data.get("move_to_community")
                    existing.save()
                    occurrences.append(existing)
                    submitted_occurrence_ids.add(existing.id)
                else:
                    community_id = data["community"].id if data.get("community") else series.default_community_id
                    conflict_value = _normalize_conflict_action(
                        data.get("conflict_action"),
                        data.get("date"),
                        data.get("time"),
                        community_id,
                    )
                    created = EventOccurrence.objects.create(
                        parish=parish,
                        event_series=series,
                        date=data["date"],
                        time=data["time"],
                        community=data["community"],
                        requirement_profile=data["requirement_profile"],
                        label=label,
                        conflict_action=conflict_value,
                        move_to_date=data.get("move_to_date"),
                        move_to_time=data.get("move_to_time"),
                        move_to_community=data.get("move_to_community"),
                    )
                    occurrences.append(created)
                    submitted_occurrence_ids.add(created.id)

                base_community = data.get("community")
                base_community_id = base_community.id if base_community else series.default_community_id
                base_profile = data.get("requirement_profile")
                base_profile_id = base_profile.id if base_profile else default_profile_id

                slot_indices = []
                for key in request.POST.keys():
                    match = re.search(rf'^form-{i}-time-(\d+)$', key)
                    if match:
                        slot_indices.append(int(match.group(1)))
                slot_indices = sorted(set(slot_indices))

                for slot_index in slot_indices:
                    if slot_index == 0:
                        continue
                    occurrence_id = _parse_fk_id(request.POST.get(f"form-{i}-occurrence_id-{slot_index}"))
                    time_key = f'form-{i}-time-{slot_index}'
                    community_key = f'form-{i}-community-{slot_index}'
                    profile_key = f'form-{i}-requirement_profile-{slot_index}'
                    conflict_key = f'form-{i}-conflict_action-{slot_index}'
                    label_key = f'form-{i}-label-{slot_index}'
                    move_date_key = f'form-{i}-move_to_date-{slot_index}'
                    move_time_key = f'form-{i}-move_to_time-{slot_index}'
                    move_community_key = f'form-{i}-move_to_community-{slot_index}'

                    time_value = _parse_time_value(request.POST.get(time_key))
                    if not time_value:
                        continue
                    community_id = _parse_fk_id(request.POST.get(community_key)) or base_community_id
                    profile_id = _parse_fk_id(request.POST.get(profile_key)) or base_profile_id
                    conflict_value = _normalize_conflict_action(
                        request.POST.get(conflict_key, "keep"),
                        data.get("date"),
                        time_value,
                        community_id,
                    )
                    label_value = request.POST.get(label_key) or series.title
                    move_date_value = _parse_date(request.POST.get(move_date_key), None)
                    move_time_value = _parse_time_value(request.POST.get(move_time_key))
                    move_community_id = _parse_fk_id(request.POST.get(move_community_key)) or None

                    if time_value == data.get("time") and community_id == base_community_id:
                        continue

                    existing_extra = None
                    if occurrence_id:
                        existing_extra = EventOccurrence.objects.filter(event_series=series, id=occurrence_id).first()
                    if not existing_extra:
                        existing_extra = EventOccurrence.objects.filter(
                            event_series=series,
                            date=data["date"],
                            time=time_value,
                            community_id=community_id,
                        ).first()
                    if existing_extra:
                        existing_extra.requirement_profile_id = profile_id
                        existing_extra.label = label_value
                        existing_extra.conflict_action = conflict_value
                        existing_extra.move_to_date = move_date_value
                        existing_extra.move_to_time = move_time_value
                        existing_extra.move_to_community_id = move_community_id
                        existing_extra.save()
                        occurrences.append(existing_extra)
                        submitted_occurrence_ids.add(existing_extra.id)
                    else:
                        created = EventOccurrence.objects.create(
                            parish=parish,
                            event_series=series,
                            date=data["date"],
                            time=time_value,
                            community_id=community_id,
                            requirement_profile_id=profile_id,
                            label=label_value,
                            conflict_action=conflict_value,
                            move_to_date=move_date_value,
                            move_to_time=move_time_value,
                            move_to_community_id=move_community_id,
                        )
                        occurrences.append(created)
                        submitted_occurrence_ids.add(created.id)
            try:
                apply_event_occurrences(series, occurrences, actor=request.user)
                if submitted_occurrence_ids:
                    EventOccurrence.objects.filter(event_series=series).exclude(id__in=submitted_occurrence_ids).delete()
                _reconcile_series_masses()
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
                    ).exclude(event_series_id=series.id).first()
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
                        "extra_occurrences_by_index": extra_occurrences_by_index,
                        "base_occurrence_ids": base_occurrence_ids,
                    },
                )
            log_audit(parish, request.user, "EventSeries", series.id, "update", {"occurrences": len(occurrences)})
            messages.success(request, "Ocorrencias atualizadas.")
            return redirect("event_series_detail", series_id=series.id)
    else:
        initial = []
        for date_value in dates:
            existing_list = occurrences_by_date.get(date_value, [])
            existing = existing_list[0] if existing_list else None
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
        ).exclude(event_series_id=series.id).first()
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
            "extra_occurrences_by_index": extra_occurrences_by_index,
            "base_occurrence_ids": base_occurrence_ids,
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
    if request.headers.get("HX-Request") and request.GET.get("partial") == "jobs":
        return render(request, "scheduling/_jobs_list.html", {"jobs": jobs})
    if request.method == "POST":
        if request.POST.get("action") == "run":
            ScheduleJobRequest.objects.create(
                parish=parish,
                requested_by=request.user,
                horizon_days=parish.horizon_days,
                job_type="schedule",
            )
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
    now = timezone.now()

    # Calculate min and max dates from MassInstance
    mass_dates = MassInstance.objects.filter(community__parish=parish).aggregate(min_date=Min('starts_at__date'), max_date=Max('starts_at__date'))
    min_date = mass_dates.get('min_date')
    max_date = mass_dates.get('max_date')

    if min_date and max_date:
        default_start = min_date - timedelta(days=1)
        default_end = max_date + timedelta(days=1)
    else:
        default_start = today - timedelta(days=29)
        default_end = today

    start_date = _parse_date(request.GET.get("start"), default_start)
    end_date = _parse_date(request.GET.get("end"), default_end)

    assignment_states = ["proposed", "published", "locked"]
    assignments_period = Assignment.objects.filter(
        parish=parish,
        assignment_state__in=assignment_states,
        slot__mass_instance__starts_at__date__range=(start_date, end_date),
    )
    received_counts = {
        row["acolyte_id"]: row["total"]
        for row in assignments_period.values("acolyte_id").annotate(total=Count("id"))
    }
    assignments_active = (
        assignments_period.filter(created_at__lte=F("slot__mass_instance__starts_at"))
        .filter(Q(ended_at__isnull=True) | Q(ended_at__gte=F("slot__mass_instance__starts_at")))
    )
    active_counts = {
        row["acolyte_id"]: row["total"]
        for row in assignments_active.values("acolyte_id").annotate(total=Count("id"))
    }

    future_assignments = Assignment.objects.filter(
        parish=parish,
        assignment_state__in=assignment_states,
        slot__mass_instance__starts_at__gte=now,
    ).filter(created_at__lte=F("slot__mass_instance__starts_at"))
    future_assignments = future_assignments.filter(
        Q(ended_at__isnull=True) | Q(ended_at__gte=F("slot__mass_instance__starts_at"))
    )
    future_counts = {
        row["acolyte_id"]: row["total"]
        for row in future_assignments.values("acolyte_id").annotate(total=Count("id"))
    }

    all_time_assignments = Assignment.objects.filter(
        parish=parish,
        assignment_state__in=assignment_states,
    ).filter(created_at__lte=F("slot__mass_instance__starts_at"))
    all_time_assignments = all_time_assignments.filter(
        Q(ended_at__isnull=True) | Q(ended_at__gte=F("slot__mass_instance__starts_at"))
    )
    all_time_counts = {
        row["acolyte_id"]: row["total"]
        for row in all_time_assignments.values("acolyte_id").annotate(total=Count("id"))
    }

    confirmation_period = Confirmation.objects.filter(
        parish=parish,
        assignment__assignment_state__in=assignment_states,
        assignment__slot__mass_instance__starts_at__date__range=(start_date, end_date),
    )
    confirmation_counts = {
        row["assignment__acolyte_id"]: row
        for row in confirmation_period.values("assignment__acolyte_id").annotate(
            declined=Count("id", filter=Q(status="declined")),
            canceled=Count("id", filter=Q(status="canceled_by_acolyte")),
            no_show=Count("id", filter=Q(status="no_show")),
        )
    }

    ceded_counts = {
        row["acolyte_id"]: row["total"]
        for row in Assignment.objects.filter(
            parish=parish,
            assignment_state__in=assignment_states,
            end_reason__in=["swap", "claim_transfer"],
            slot__mass_instance__starts_at__date__range=(start_date, end_date),
        )
        .values("acolyte_id")
        .annotate(total=Count("id"))
    }

    stats_map = {stat.acolyte_id: stat for stat in AcolyteStats.objects.filter(parish=parish)}
    intent_map = {intent.acolyte_id: intent for intent in AcolyteIntent.objects.filter(parish=parish)}

    acolytes = list(parish.acolytes.filter(active=True).order_by("display_name"))
    acolyte_count = len(acolytes)
    avg_total = (
        sum(active_counts.get(acolyte.id, 0) for acolyte in acolytes) / acolyte_count
        if acolyte_count
        else 0
    )
    avg_received = (
        sum(received_counts.get(acolyte.id, 0) for acolyte in acolytes) / acolyte_count
        if acolyte_count
        else 0
    )

    period_days = max((end_date - start_date).days + 1, 1)
    period_factor = period_days / 30

    def _allocation_text(ratio, target, is_reserve=False):
        if is_reserve:
            return "Reserva"
        if not target or target <= 0:
            return "Sem alvo"
        if ratio is None:
            return "Sem alvo"
        if ratio < 0.8:
            label = "Sub"
        elif ratio <= 1.2:
            label = "Na media"
        else:
            label = "Super"
        return f"{label} {ratio:.1f}x"

    rows = []
    for acolyte in acolytes:
        stat = stats_map.get(acolyte.id)
        intent = intent_map.get(acolyte.id)
        is_reserve = acolyte.scheduling_mode == "reserve"
        total = active_counts.get(acolyte.id, 0)
        received = received_counts.get(acolyte.id, 0)
        future = future_counts.get(acolyte.id, 0)
        total_all_time = all_time_counts.get(acolyte.id, 0)
        confirmation = confirmation_counts.get(acolyte.id, {})
        declined = confirmation.get("declined", 0)
        canceled = confirmation.get("canceled", 0)
        no_show = confirmation.get("no_show", 0)
        ceded = ceded_counts.get(acolyte.id, 0)
        reliability = stat.reliability_score if stat else 0
        credit = stat.credit_balance if stat else 0

        if is_reserve:
            target = 0
        elif intent and intent.desired_frequency_per_month:
            target = intent.desired_frequency_per_month * period_factor
        else:
            level = intent.willingness_level if intent else "normal"
            factor = {"low": 0.8, "normal": 1.0, "high": 1.2}.get(level, 1.0)
            target = avg_received * factor

        raw_ratio = (received / target) if target else None
        effective = max(0.0, received - declined - canceled - no_show - ceded)
        reliability_factor = (stat.reliability_score / 100.0) if stat else 1.0
        adjusted_ratio = ((effective * reliability_factor) / target) if target else None

        rows.append(
            {
                "acolyte": acolyte,
                "total": total,
                "received": received,
                "future": future,
                "total_all_time": total_all_time,
                "services_30": stat.services_last_30_days if stat else 0,
                "services_90": stat.services_last_90_days if stat else 0,
                "reliability": reliability,
                "credit": credit,
                "declined": declined,
                "canceled": canceled,
                "no_show": no_show,
                "ceded": ceded,
                "allocation_raw": _allocation_text(raw_ratio, target, is_reserve),
                "allocation_adjusted": _allocation_text(adjusted_ratio, target, is_reserve),
                "has_user": bool(acolyte.user_id),
            }
        )

    max_total = max([row["total"] for row in rows], default=0) or 1

    return render(
        request,
        "reports/frequency.html",
        {
            "rows": rows,
            "start_date": start_date,
            "end_date": end_date,
            "max_total": max_total,
            "avg_total": avg_total,
            "avg_received": avg_received,
            "period_days": period_days,
        },
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def replacement_center(request):
    parish = request.active_parish
    now = timezone.now()
    consolidation_end = now + timedelta(days=parish.consolidation_days)
    reconcile_pending_replacements(parish, actor=request.user, now=now)
    replacements = _actionable_replacements(parish, now, consolidation_end)
    pending_in_window = len(replacements)
    open_slots = AssignmentSlot.objects.filter(
        parish=parish,
        status="open",
        required=True,
        externally_covered=False,
        mass_instance__status="scheduled",
        mass_instance__starts_at__gte=now,
        mass_instance__starts_at__lte=consolidation_end,
    ).count()
    pending_confirmations = Confirmation.objects.filter(
        parish=parish,
        status="pending",
        assignment__is_active=True,
        assignment__slot__mass_instance__status="scheduled",
        assignment__slot__mass_instance__starts_at__gte=now,
        assignment__slot__mass_instance__starts_at__lte=consolidation_end,
    ).count()
    if replacements:
        position_ids = {item.slot.position_type_id for item in replacements}
        quick_fill_cache = build_quick_fill_cache(
            parish,
            position_type_ids=position_ids,
            slots=[item.slot for item in replacements],
        )
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
    if community_id in [None, 'None', '']:
        community_id = None
    else:
        try:
            community_id = int(community_id)
        except ValueError:
            community_id = None
    experience = filters.get("experience")
    status = filters.get("status")
    role = filters.get("role")

    # Qualification code mapping for compact display
    QUALIFICATION_CODE_MAP = {
        "Ceroferario": "CER",
        "Cruciferario": "CRU",
        "Cruciferario + Microfonario": "CRU+MIC",
        "Librifero": "LIB",
        "Microfonario": "MIC",
        "Naveteiro": "NAV",
        "Turiferario": "TUR",
    }
    ALL_QUALIFICATION_COUNT = 7

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
    if community_id is not None:
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
        qualification_codes = []
        is_all_qualified = False
        stats = None
        if acolyte:
            quals = list(acolyte.acolytequalification_set.all())
            qualifications = [qual.position_type.name for qual in quals]
            qualification_codes = [QUALIFICATION_CODE_MAP.get(name, name[:3].upper()) for name in qualifications]
            is_all_qualified = len(qualification_codes) >= ALL_QUALIFICATION_COUNT
            stats = stats_map.get(acolyte.id)
        member_rows.append(
            {
                "user": user,
                "acolyte": acolyte,
                "name": acolyte.display_name if acolyte else user.full_name,
                "roles": list(membership.roles.all()),
                "experience": acolyte.experience_level if acolyte else None,
                "qualifications": qualifications,
                "qualification_codes": qualification_codes,
                "is_all_qualified": is_all_qualified,
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
        qualifications = [qual.position_type.name for qual in quals]
        qualification_codes = [QUALIFICATION_CODE_MAP.get(name, name[:3].upper()) for name in qualifications]
        is_all_qualified = len(qualification_codes) >= ALL_QUALIFICATION_COUNT
        member_rows.append(
            {
                "user": acolyte.user,
                "acolyte": acolyte,
                "name": acolyte.display_name,
                "roles": [],
                "experience": acolyte.experience_level,
                "qualifications": qualifications,
                "qualification_codes": qualification_codes,
                "is_all_qualified": is_all_qualified,
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
    context = {
        "members": members,
        "communities": communities,
        "roles": roles,
        "experience_choices": AcolyteProfile.EXPERIENCE_CHOICES,
        "filters": filters,
    }
    if request.headers.get("HX-Request"):
        return render(request, "people/_people_list.html", context)
    return render(request, "people/directory.html", context)


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
    login_form = AcolyteCreateLoginForm() if acolyte and not user else None
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
        "login_form": login_form,
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
            updated_user = form.save(commit=False)
            if request.user.id == user.id and not updated_user.is_active:
                # Prevent self-lockout via the profile form.
                updated_user.is_active = True
                messages.info(request, "Sua conta nao pode ser desativada por este formulario.")
            updated_user.save()

            # Handle membership roles if provided
            roles = request.POST.getlist("roles")
            if roles is not None:
                membership, _ = ParishMembership.objects.get_or_create(parish=parish, user=user, defaults={"active": True})
                membership.roles.clear()
                if roles:
                    membership.roles.add(*roles)
                # Also update active status if provided
                active = request.POST.get("active")
                if active is not None:
                    membership.active = active == "on"
                    membership.save(update_fields=["active", "updated_at"])

            messages.success(request, "Identidade atualizada.")
        else:
            messages.error(request, "Revise os dados de identidade.")
    elif form_type == "login" and acolyte and not user:
        form = AcolyteCreateLoginForm(request.POST)
        if form.is_valid():
            email = form.cleaned_data["email"]
            password = form.cleaned_data["password"]
            send_invite = form.cleaned_data.get("send_invite", False)
            User = get_user_model()
            user = User.objects.create_user(email=email, full_name=acolyte.display_name, password=password)
            acolyte.user = user
            acolyte.save(update_fields=["user", "updated_at"])
            membership, _ = ParishMembership.objects.get_or_create(parish=parish, user=user, defaults={"active": True})
            acolyte_role = MembershipRole.objects.filter(code="ACOLYTE").first()
            if acolyte_role:
                membership.roles.add(acolyte_role)
            messages.success(request, "Credenciais de login criadas com sucesso.")
            if send_invite:
                send_mail(
                    "Acesso ao Acoli",
                    f"Seu acesso foi criado.\nEmail: {user.email}\nSenha: {password}\n",
                    settings.DEFAULT_FROM_EMAIL,
                    [user.email],
                )
        else:
            messages.error(request, "Revise os dados de login.")
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
    """
    Main admin hub for managing an acolyte. Supports tabbed interface with HTMX.
    """
    parish = request.active_parish
    acolyte = get_object_or_404(
        AcolyteProfile.objects.select_related("user", "community_of_origin", "family_group"),
        parish=parish,
        id=acolyte_id
    )
    user = acolyte.user
    
    # Handle POST requests
    if request.method == "POST":
        _handle_member_post(request, parish, user=user, acolyte=acolyte)
        # If it's an HTMX request, return to the same tab
        if request.headers.get("HX-Request"):
            tab = request.POST.get("current_tab", "overview")
            return redirect(f"{reverse('people_acolyte_detail', args=[acolyte.id])}?tab={tab}")
        return redirect("people_acolyte_detail", acolyte_id=acolyte.id)
    
    # Determine which tab to show
    tab = request.GET.get("tab", "overview")
    valid_tabs = ["overview", "schedule", "availability", "preferences", "qualifications", "credits", "swaps", "notifications", "audit"]
    if tab not in valid_tabs:
        tab = "overview"
    
    # Build header context (always needed)
    stats = AcolyteStats.objects.filter(parish=parish, acolyte=acolyte).first()
    intent = AcolyteIntent.objects.filter(parish=parish, acolyte=acolyte).first()
    
    # KPIs for header
    next_assignment = Assignment.objects.filter(
        parish=parish,
        acolyte=acolyte,
        is_active=True,
        slot__mass_instance__starts_at__gte=timezone.now()
    ).select_related("slot__mass_instance__community", "slot__position_type").order_by("slot__mass_instance__starts_at").first()
    
    pending_confirmations = Confirmation.objects.filter(
        parish=parish,
        assignment__acolyte=acolyte,
        assignment__is_active=True,
        status="pending"
    ).count()
    
    open_swaps = SwapRequest.objects.filter(
        Q(requestor_acolyte=acolyte) | Q(target_acolyte=acolyte),
        parish=parish,
        status__in=["pending", "awaiting_approval"]
    ).count()
    
    open_replacements = ReplacementRequest.objects.filter(
        parish=parish,
        slot__assignments__acolyte=acolyte,
        slot__assignments__is_active=True,
        status="pending"
    ).count()
    
    context = {
        "acolyte": acolyte,
        "user": user,
        "stats": stats,
        "intent": intent,
        "current_tab": tab,
        "next_assignment": next_assignment,
        "pending_confirmations": pending_confirmations,
        "open_swaps": open_swaps,
        "open_replacements": open_replacements,
        "tabs": [
            {"id": "overview", "label": "Visao Geral", "icon": "home"},
            {"id": "schedule", "label": "Escala", "icon": "calendar"},
            {"id": "availability", "label": "Disponibilidade", "icon": "clock"},
            {"id": "preferences", "label": "Preferencias", "icon": "heart"},
            {"id": "qualifications", "label": "Qualificacoes", "icon": "award"},
            {"id": "credits", "label": "Creditos", "icon": "star"},
            {"id": "swaps", "label": "Trocas", "icon": "repeat"},
            {"id": "notifications", "label": "Notificacoes", "icon": "bell"},
            {"id": "audit", "label": "Auditoria", "icon": "list"},
        ],
    }
    
    # For HTMX partial requests, only render the tab content
    if request.headers.get("HX-Request"):
        tab_context = _get_acolyte_tab_context(request, parish, acolyte, user, tab, stats, intent)
        context.update(tab_context)
        return render(request, f"people/acolyte_tabs/{tab}.html", context)
    
    # For full page loads, render everything
    tab_context = _get_acolyte_tab_context(request, parish, acolyte, user, tab, stats, intent)
    context.update(tab_context)
    return render(request, "people/acolyte_detail.html", context)


def _get_acolyte_tab_context(request, parish, acolyte, user, tab, stats, intent):
    """Build context for a specific tab."""
    context = {}
    
    if tab == "overview":
        context.update(_acolyte_overview_context(parish, acolyte, user, stats, intent))
    elif tab == "schedule":
        context.update(_acolyte_schedule_context(request, parish, acolyte))
    elif tab == "availability":
        context.update(_acolyte_availability_context(parish, acolyte))
    elif tab == "preferences":
        context.update(_acolyte_preferences_context(parish, acolyte))
    elif tab == "qualifications":
        context.update(_acolyte_qualifications_context(parish, acolyte))
    elif tab == "credits":
        context.update(_acolyte_credits_context(request, parish, acolyte, stats))
    elif tab == "swaps":
        context.update(_acolyte_swaps_context(request, parish, acolyte))
    elif tab == "notifications":
        context.update(_acolyte_notifications_context(request, parish, acolyte, user))
    elif tab == "audit":
        context.update(_acolyte_audit_context(request, parish, acolyte))
    
    return context


def _acolyte_overview_context(parish, acolyte, user, stats, intent):
    """Context for the Overview tab."""
    # Status alerts
    alerts = []
    if not acolyte.active:
        alerts.append({"type": "warning", "message": "Acolito inativo - nao sera escalado automaticamente."})
    if acolyte.scheduling_mode == "reserve":
        alerts.append({"type": "info", "message": "Modo reserva tecnica - sera escalado apenas quando necessario."})
    
    # Check qualification gaps
    all_positions = PositionType.objects.filter(parish=parish, active=True)
    qualified_positions = set(AcolyteQualification.objects.filter(
        parish=parish, acolyte=acolyte, qualified=True
    ).values_list("position_type_id", flat=True))
    missing_qualifications = all_positions.exclude(id__in=qualified_positions)
    if missing_qualifications.exists():
        alerts.append({
            "type": "info",
            "message": f"Falta qualificacao em: {', '.join(p.name for p in missing_qualifications[:3])}"
            + ("..." if missing_qualifications.count() > 3 else "")
        })
    
    # Check availability issues
    availability_rules = AcolyteAvailabilityRule.objects.filter(parish=parish, acolyte=acolyte)
    if not availability_rules.exists():
        alerts.append({"type": "info", "message": "Nenhuma regra de disponibilidade definida."})
    
    # Upcoming assignments (future, active only)
    now = timezone.now()
    upcoming_assignments = filter_upcoming(
        Assignment.objects.filter(
            parish=parish,
            acolyte=acolyte,
            is_active=True,
        ),
        now=now,
    ).select_related(
        "slot__mass_instance__community", "slot__position_type", "confirmation"
    ).order_by("slot__mass_instance__starts_at")[:3]
    
    # Past assignments (served - confirmed or attended)
    past_assignments = filter_past(
        Assignment.objects.filter(
            parish=parish,
            acolyte=acolyte,
            is_active=True,
        ),
        now=now,
    ).select_related(
        "slot__mass_instance__community", "slot__position_type", "confirmation"
    ).order_by("-slot__mass_instance__starts_at")[:3]
    
    recent_audit = AuditEvent.objects.filter(
        parish=parish,
        entity_type="AcolyteProfile",
        entity_id=str(acolyte.id)
    ).order_by("-timestamp")[:5]
    
    # Intent form
    intent_form = AcolyteIntentForm(instance=intent, parish=parish, acolyte=acolyte)
    
    # Forms for the legacy section (profile editing)
    membership = None
    if user:
        membership = ParishMembership.objects.filter(parish=parish, user=user).prefetch_related("roles").first()
    
    user_form = PeopleUserForm(instance=user) if user else None
    membership_form = PeopleMembershipForm(
        parish=parish,
        initial={
            "active": membership.active if membership else True,
            "roles": membership.roles.all() if membership else [],
        },
    )
    acolyte_form = PeopleAcolyteForm(instance=acolyte, parish=parish)
    
    return {
        "alerts": alerts,
        "upcoming_assignments": upcoming_assignments,
        "past_assignments": past_assignments,
        "recent_audit": recent_audit,
        "intent_form": intent_form,
        "membership": membership,
        "user_form": user_form,
        "membership_form": membership_form,
        "acolyte_form": acolyte_form,
    }


def _acolyte_schedule_context(request, parish, acolyte):
    """Context for the Schedule tab."""
    # Filters
    status_filter = request.GET.get("status", "all")
    time_filter = request.GET.get("time", "all")
    
    assignments_qs = Assignment.objects.filter(
        parish=parish, acolyte=acolyte
    ).select_related(
        "slot__mass_instance__community",
        "slot__position_type",
        "confirmation",
        "assigned_by"
    )
    
    # Apply filters
    now = timezone.now()
    if time_filter == "upcoming":
        assignments_qs = filter_upcoming(assignments_qs, now=now)
    elif time_filter == "past":
        assignments_qs = filter_past(assignments_qs, now=now)
    
    if status_filter == "active":
        assignments_qs = assignments_qs.filter(is_active=True)
    elif status_filter == "ended":
        assignments_qs = assignments_qs.filter(is_active=False)
    
    assignments_qs = assignments_qs.order_by("-slot__mass_instance__starts_at")
    
    # Pagination
    paginator = Paginator(assignments_qs, 20)
    page = request.GET.get("page", 1)
    assignments = paginator.get_page(page)
    
    return {
        "assignments": assignments,
        "status_filter": status_filter,
        "time_filter": time_filter,
    }


def _acolyte_availability_context(parish, acolyte):
    """Context for the Availability tab."""
    availability = acolyte.acolyteavailabilityrule_set.select_related("community")
    
    weekly_rules = availability.filter(
        start_date__isnull=True, end_date__isnull=True
    ).order_by("day_of_week", "start_time")
    
    date_absences = availability.filter(
        Q(start_date__isnull=False) | Q(end_date__isnull=False)
    ).order_by("start_date")
    
    # Build weekly grid for visualization
    weekdays = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sab", "Dom"]
    weekly_grid = {i: [] for i in range(7)}
    for rule in weekly_rules:
        if rule.day_of_week is not None:
            weekly_grid[rule.day_of_week].append(rule)
    
    weekly_form = WeeklyAvailabilityForm(acolyte=acolyte)
    date_absence_form = DateAbsenceForm()
    
    return {
        "weekly_rules": weekly_rules,
        "date_absences": date_absences,
        "weekly_grid": weekly_grid,
        "weekdays": weekdays,
        "weekly_form": weekly_form,
        "date_absence_form": date_absence_form,
    }


def _acolyte_preferences_context(parish, acolyte):
    """Context for the Preferences tab."""
    preferences = acolyte.acolytepreference_set.select_related(
        "target_community", "target_position", "target_function", "target_template", "target_acolyte"
    )
    
    # Group preferences by type
    preference_groups = defaultdict(list)
    for pref in preferences:
        preference_groups[pref.preference_type].append(pref)
    
    preference_form = AcolytePreferenceForm(parish=parish)
    
    # Preference type labels
    pref_type_labels = dict(AcolytePreference.PREFERENCE_CHOICES)
    
    return {
        "preferences": preferences,
        "preference_groups": dict(preference_groups),
        "preference_form": preference_form,
        "pref_type_labels": pref_type_labels,
    }


def _acolyte_qualifications_context(parish, acolyte):
    """Context for the Qualifications tab."""
    all_positions = PositionType.objects.filter(parish=parish, active=True)
    qualifications = {
        q.position_type_id: q
        for q in AcolyteQualification.objects.filter(parish=parish, acolyte=acolyte)
    }
    
    # Build checklist with qualification status
    checklist = []
    for position in all_positions:
        qual = qualifications.get(position.id)
        checklist.append({
            "position": position,
            "qualified": qual.qualified if qual else False,
            "qualification": qual,
        })
    
    qual_form = PeopleQualificationsForm(
        parish=parish,
        initial={
            "qualifications": [q.position_type_id for q in qualifications.values() if q.qualified]
        },
    )
    
    return {
        "checklist": checklist,
        "qual_form": qual_form,
    }


def _acolyte_credits_context(request, parish, acolyte, stats):
    """Context for the Credits tab."""
    # Credit ledger with pagination
    ledger_qs = AcolyteCreditLedger.objects.filter(
        parish=parish, acolyte=acolyte
    ).select_related("related_assignment__slot__mass_instance", "created_by").order_by("-created_at")
    
    paginator = Paginator(ledger_qs, 20)
    page = request.GET.get("page", 1)
    ledger = paginator.get_page(page)
    
    # Credit balance
    balance = stats.credit_balance if stats else 0
    
    # Adjustment form
    adjustment_form = CreditAdjustmentForm(parish=parish, acolyte=acolyte)
    
    # Reason code labels
    reason_labels = dict(AcolyteCreditLedger.REASON_CHOICES)
    
    return {
        "ledger": ledger,
        "balance": balance,
        "adjustment_form": adjustment_form,
        "reason_labels": reason_labels,
    }


def _acolyte_swaps_context(request, parish, acolyte):
    """Context for the Swaps & Replacements tab."""
    # Swap requests
    swaps_qs = SwapRequest.objects.filter(
        Q(requestor_acolyte=acolyte) | Q(target_acolyte=acolyte),
        parish=parish
    ).select_related(
        "requestor_acolyte", "target_acolyte",
        "requestor_assignment__slot__mass_instance__community",
        "target_assignment__slot__mass_instance__community"
    ).order_by("-created_at")
    
    # Replacement requests - find where acolyte is proposed or where acolyte's slot needs replacement
    replacements_qs = ReplacementRequest.objects.filter(
        Q(proposed_acolyte=acolyte) | Q(slot__assignments__acolyte=acolyte, slot__assignments__is_active=True),
        parish=parish
    ).select_related(
        "slot__mass_instance__community",
        "slot__position_type",
        "proposed_acolyte"
    ).order_by("-created_at").distinct()
    
    # Pagination for swaps
    swap_paginator = Paginator(swaps_qs, 10)
    swap_page = request.GET.get("swap_page", 1)
    swaps = swap_paginator.get_page(swap_page)
    
    # Pagination for replacements
    repl_paginator = Paginator(replacements_qs, 10)
    repl_page = request.GET.get("repl_page", 1)
    replacements = repl_paginator.get_page(repl_page)
    
    return {
        "swaps": swaps,
        "replacements": replacements,
    }


def _acolyte_notifications_context(request, parish, acolyte, user):
    """Context for the Notifications tab."""
    # Notification preferences
    prefs = None
    if user:
        prefs, _ = NotificationPreference.objects.get_or_create(
            parish=parish, user=user,
            defaults={"email_enabled": True}
        )
    
    notification_form = NotificationPreferenceForm(instance=prefs, parish=parish, user=user) if prefs else None
    
    # Notification delivery log
    notifications_qs = Notification.objects.none()
    if user:
        notifications_qs = Notification.objects.filter(
            parish=parish, user=user
        ).order_by("-created_at")
    
    paginator = Paginator(notifications_qs, 20)
    page = request.GET.get("page", 1)
    notifications = paginator.get_page(page)
    
    # Status labels
    status_labels = dict(Notification.STATUS_CHOICES)
    channel_labels = dict(Notification.CHANNEL_CHOICES)
    
    return {
        "notification_prefs": prefs,
        "notification_form": notification_form,
        "notifications": notifications,
        "status_labels": status_labels,
        "channel_labels": channel_labels,
    }


def _acolyte_audit_context(request, parish, acolyte):
    """Context for the Audit tab."""
    # All audit events related to this acolyte
    audit_qs = AuditEvent.objects.filter(
        parish=parish
    ).filter(
        Q(entity_type="AcolyteProfile", entity_id=str(acolyte.id)) |
        Q(entity_type="Assignment", diff_json__acolyte_id=acolyte.id) |
        Q(entity_type="AcolyteQualification", diff_json__acolyte_id=acolyte.id) |
        Q(entity_type="AcolyteAvailabilityRule", diff_json__acolyte_id=acolyte.id) |
        Q(entity_type="AcolytePreference", diff_json__acolyte_id=acolyte.id) |
        Q(entity_type="AcolyteCreditLedger", diff_json__acolyte_id=acolyte.id) |
        Q(entity_type="Confirmation", diff_json__acolyte_id=acolyte.id)
    ).select_related("actor_user").order_by("-timestamp")
    
    paginator = Paginator(audit_qs, 30)
    page = request.GET.get("page", 1)
    audit_events = paginator.get_page(page)
    
    return {
        "audit_events": audit_events,
    }


# ============================================================================
# ACOLYTE HUB ACTION ENDPOINTS
# ============================================================================

@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def acolyte_update_intent(request, acolyte_id):
    """Update an acolyte's intent (frequency/willingness)."""
    parish = request.active_parish
    acolyte = get_object_or_404(AcolyteProfile, parish=parish, id=acolyte_id)
    
    if request.method != "POST":
        return redirect("people_acolyte_detail", acolyte_id=acolyte.id)
    
    intent, created = AcolyteIntent.objects.get_or_create(
        parish=parish, acolyte=acolyte,
        defaults={"desired_frequency_per_month": None, "willingness_level": "normal"}
    )
    
    form = AcolyteIntentForm(request.POST, instance=intent, parish=parish, acolyte=acolyte)
    if form.is_valid():
        form.save()
        log_audit(
            parish=parish,
            actor=request.user,
            entity_type="AcolyteIntent",
            entity_id=intent.id,
            action_type="update",
            diff={"acolyte_id": acolyte.id, "changes": form.changed_data}
        )
        messages.success(request, "Intencoes atualizadas.")
    else:
        messages.error(request, "Erro ao atualizar intencoes.")
    
    if request.headers.get("HX-Request"):
        return redirect(f"{reverse('people_acolyte_detail', args=[acolyte.id])}?tab=overview")
    return redirect("people_acolyte_detail", acolyte_id=acolyte.id)


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def acolyte_adjust_credits(request, acolyte_id):
    """Manually adjust an acolyte's credit balance."""
    parish = request.active_parish
    acolyte = get_object_or_404(AcolyteProfile, parish=parish, id=acolyte_id)
    
    if request.method != "POST":
        return redirect("people_acolyte_detail", acolyte_id=acolyte.id)
    
    form = CreditAdjustmentForm(request.POST, parish=parish, acolyte=acolyte)
    if form.is_valid():
        delta = form.cleaned_data["delta"]
        notes = form.cleaned_data["notes"]
        
        # Create ledger entry
        ledger = AcolyteCreditLedger.objects.create(
            parish=parish,
            acolyte=acolyte,
            delta=delta,
            reason_code="manual_adjustment",
            notes=notes,
            created_by=request.user,
        )
        
        # Update stats
        stats, _ = AcolyteStats.objects.get_or_create(parish=parish, acolyte=acolyte)
        stats.credit_balance = (stats.credit_balance or 0) + delta
        stats.save(update_fields=["credit_balance", "updated_at"])
        
        log_audit(
            parish=parish,
            actor=request.user,
            entity_type="AcolyteCreditLedger",
            entity_id=ledger.id,
            action_type="create",
            diff={"acolyte_id": acolyte.id, "delta": delta, "notes": notes}
        )
        messages.success(request, f"Ajuste de {delta:+d} creditos aplicado.")
    else:
        messages.error(request, "Erro ao ajustar creditos.")
    
    if request.headers.get("HX-Request"):
        return redirect(f"{reverse('people_acolyte_detail', args=[acolyte.id])}?tab=credits")
    return redirect("people_acolyte_detail", acolyte_id=acolyte.id)


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def acolyte_update_notifications(request, acolyte_id):
    """Update notification preferences for an acolyte's user."""
    parish = request.active_parish
    acolyte = get_object_or_404(AcolyteProfile, parish=parish, id=acolyte_id)
    user = acolyte.user
    
    if request.method != "POST" or not user:
        return redirect("people_acolyte_detail", acolyte_id=acolyte.id)
    
    prefs, _ = NotificationPreference.objects.get_or_create(
        parish=parish, user=user,
        defaults={"email_enabled": True}
    )
    
    form = NotificationPreferenceForm(request.POST, instance=prefs, parish=parish, user=user)
    if form.is_valid():
        form.save()
        log_audit(
            parish=parish,
            actor=request.user,
            entity_type="NotificationPreference",
            entity_id=prefs.id,
            action_type="update",
            diff={"user_id": user.id, "changes": form.changed_data}
        )
        messages.success(request, "Preferencias de notificacao atualizadas.")
    else:
        messages.error(request, "Erro ao atualizar preferencias.")
    
    if request.headers.get("HX-Request"):
        return redirect(f"{reverse('people_acolyte_detail', args=[acolyte.id])}?tab=notifications")
    return redirect("people_acolyte_detail", acolyte_id=acolyte.id)


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def acolyte_resend_notification(request, acolyte_id, notification_id):
    """Resend a failed notification."""
    parish = request.active_parish
    acolyte = get_object_or_404(AcolyteProfile, parish=parish, id=acolyte_id)
    user = acolyte.user
    
    if request.method != "POST" or not user:
        return redirect("people_acolyte_detail", acolyte_id=acolyte.id)
    
    notification = get_object_or_404(Notification, parish=parish, user=user, id=notification_id)
    
    if notification.status in ["failed", "skipped"]:
        notification.status = "pending"
        notification.error_message = ""
        notification.save(update_fields=["status", "error_message"])
        
        log_audit(
            parish=parish,
            actor=request.user,
            entity_type="Notification",
            entity_id=notification.id,
            action_type="resend",
            diff={"previous_status": notification.status}
        )
        messages.success(request, "Notificacao reenfileirada para envio.")
    else:
        messages.info(request, "Esta notificacao nao pode ser reenviada.")
    
    if request.headers.get("HX-Request"):
        return redirect(f"{reverse('people_acolyte_detail', args=[acolyte.id])}?tab=notifications")
    return redirect("people_acolyte_detail", acolyte_id=acolyte.id)


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def acolyte_assign_to_slot(request, acolyte_id):
    """Quick action to assign acolyte to an open slot."""
    parish = request.active_parish
    acolyte = get_object_or_404(AcolyteProfile, parish=parish, id=acolyte_id)
    
    if request.method != "POST":
        return redirect("people_acolyte_detail", acolyte_id=acolyte.id)
    
    form = AssignToSlotForm(request.POST, parish=parish, acolyte=acolyte)
    if form.is_valid():
        slot = form.cleaned_data["slot"]
        try:
            assignment = assign_manual(
                slot,
                acolyte,
                actor=request.user,
            )
            messages.success(request, f"Acolito escalado para {slot.mass_instance} - {slot.position_type.name}.")
        except ConcurrentUpdateError:
            messages.error(request, "Este slot ja foi preenchido por outro usuario.")
        except Exception as e:
            messages.error(request, f"Erro ao escalar: {str(e)}")
    else:
        messages.error(request, "Slot invalido.")
    
    if request.headers.get("HX-Request"):
        return redirect(f"{reverse('people_acolyte_detail', args=[acolyte.id])}?tab=schedule")
    return redirect("people_acolyte_detail", acolyte_id=acolyte.id)


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def acolyte_remove_assignment(request, acolyte_id, assignment_id):
    """Remove an assignment from an acolyte."""
    parish = request.active_parish
    acolyte = get_object_or_404(AcolyteProfile, parish=parish, id=acolyte_id)
    assignment = get_object_or_404(Assignment, parish=parish, acolyte=acolyte, id=assignment_id, is_active=True)
    
    if request.method != "POST":
        return redirect("people_acolyte_detail", acolyte_id=acolyte.id)
    
    try:
        deactivate_assignment(assignment, reason="manual_unassign", actor=request.user)
        if slot.required and not slot.externally_covered:
            slot.status = "open"
            slot.save(update_fields=["status", "updated_at"])
        
        # For HTMX requests, return empty response to remove the row
        if request.headers.get("HX-Request"):
            response = HttpResponse("")
            response["HX-Success-Message"] = "Escala removida."
            return response
        
        messages.success(request, "Escala removida.")
    except Exception as e:
        messages.error(request, f"Erro ao remover escala: {str(e)}")
    
    return redirect("people_acolyte_detail", acolyte_id=acolyte.id)


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def acolyte_confirm_assignment(request, acolyte_id, assignment_id):
    """Admin action to confirm an assignment on behalf of the acolyte."""
    parish = request.active_parish
    acolyte = get_object_or_404(AcolyteProfile, parish=parish, id=acolyte_id)
    assignment = get_object_or_404(Assignment, parish=parish, acolyte=acolyte, id=assignment_id, is_active=True)
    
    if request.method != "POST":
        return redirect("people_acolyte_detail", acolyte_id=acolyte.id)
    
    confirmation, created = Confirmation.objects.get_or_create(
        parish=parish, assignment=assignment,
        defaults={"status": "pending"}
    )
    
    confirmation.status = "confirmed"
    confirmation.updated_by = request.user
    confirmation.notes = f"Confirmado pelo admin {request.user.full_name}"
    confirmation.save()
    expire_claims_for_assignment(assignment, "holder_confirmed", actor=request.user)
    
    log_audit(
        parish=parish,
        actor=request.user,
        entity_type="Confirmation",
        entity_id=confirmation.id,
        action_type="admin_confirm",
        diff={"acolyte_id": acolyte.id, "assignment_id": assignment.id}
    )
    
    # Refetch assignment with all needed relations for the template
    assignment = Assignment.objects.select_related(
        'slot__mass_instance__community',
        'slot__position_type',
        'confirmation',
        'assigned_by'
    ).get(id=assignment.id)
    
    return _htmx_or_redirect(
        request,
        "people/acolyte_tabs/_schedule_row.html",
        {"assignment": assignment, "acolyte": acolyte},
        reverse("people_acolyte_detail", args=[acolyte.id]),
        "Presenca confirmada."
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def acolyte_open_slots(request, acolyte_id):
    """HTMX endpoint to get available slots for assigning an acolyte."""
    parish = request.active_parish
    acolyte = get_object_or_404(AcolyteProfile, parish=parish, id=acolyte_id)
    
    # Get qualified positions
    qualified_positions = AcolyteQualification.objects.filter(
        parish=parish, acolyte=acolyte, qualified=True
    ).values_list("position_type_id", flat=True)
    
    # Get open slots
    slots = AssignmentSlot.objects.filter(
        parish=parish,
        status="open",
        position_type_id__in=qualified_positions,
        mass_instance__starts_at__gte=timezone.now(),
        mass_instance__status="scheduled",
    ).select_related(
        "mass_instance", "position_type", "mass_instance__community"
    ).order_by("mass_instance__starts_at")[:50]
    
    form = AssignToSlotForm(parish=parish, acolyte=acolyte)
    
    return render(request, "people/acolyte_tabs/_assign_slot_modal.html", {
        "acolyte": acolyte,
        "slots": slots,
        "form": form,
    })


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
    
    # Filtrar escalas do dia atual em diante (início do dia atual)
    today_start = timezone.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    assignments = Assignment.objects.filter(
        parish=parish, 
        acolyte__user=request.user, 
        is_active=True,
        slot__mass_instance__starts_at__gte=today_start
    ).select_related(
        "slot__mass_instance__community", "slot__position_type", "confirmation"
    ).order_by("slot__mass_instance__starts_at")
    mass_ids = list(assignments.values_list("slot__mass_instance_id", flat=True))
    team_map = {}
    team_names_map = {}
    mass_has_multiple_slots = {}
    
    if mass_ids:
        teammates = (
            Assignment.objects.filter(
                parish=parish,
                is_active=True,
                slot__mass_instance_id__in=mass_ids,
            )
            .select_related("acolyte", "slot__mass_instance", "slot__position_type")
            .order_by("slot__mass_instance_id", "slot__position_type__name")
        )
        for assignment in teammates:
            team_map.setdefault(assignment.slot.mass_instance_id, []).append(assignment)
        for mass_id, items in team_map.items():
            names = [item.acolyte.display_name for item in items]
            team_names_map[mass_id] = ", ".join(names)
            # Missa tem múltiplos slots se tem mais de 1 acólito ativo
            mass_has_multiple_slots[mass_id] = len(items) > 1
    
    # Check for pending swap requests for user's assignments
    acolyte = AcolyteProfile.objects.filter(parish=parish, user=request.user).first()
    pending_swaps_by_slot = {}
    if acolyte:
        pending_swaps = SwapRequest.objects.filter(
            parish=parish,
            requestor_acolyte=acolyte,
            status="pending",
            from_slot__isnull=False
        ).values_list("from_slot_id", flat=True)
        pending_swaps_by_slot = {slot_id: True for slot_id in pending_swaps}

    claims_by_slot = _claim_map_for_slots(parish, assignments.values_list("slot_id", flat=True))

    return render(
        request,
        "acolytes/assignments.html",
        {
            "assignments": assignments,
            "team_map": team_map,
            "team_names_map": team_names_map,
            "mass_has_multiple_slots": mass_has_multiple_slots,
            "pending_swaps_by_slot": pending_swaps_by_slot,
            "claims_by_slot": claims_by_slot,
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
    success_message = None
    
    if token:
        token.token = uuid4().hex
        token.rotated_at = timezone.now()
        token.save(update_fields=["token", "rotated_at", "updated_at"])
        success_message = "Link do calendario atualizado."
    else:
        token = CalendarFeedToken.objects.create(parish=parish, user=request.user, token=uuid4().hex)
        success_message = "Link do calendario criado."
    
    # Build calendar feed URL
    calendar_feed_url = request.build_absolute_uri(
        f"{reverse('calendar_feed')}?token={token.token}"
    )
    
    return _htmx_or_redirect(
        request,
        "acolytes/_partials/calendar_feed_section.html",
        {"calendar_feed_url": calendar_feed_url},
        "my_assignments",
        success_message
    )


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
        interested_masses = set(
            MassInterest.objects.filter(parish=parish, acolyte=acolyte, interested=True).values_list(
                "mass_instance_id", flat=True
            )
        )

        blocked_interest = 0
        blocked_qualification = 0
        blocked_availability = 0
        eligible_by_qualification = 0
        eligible_opportunities = 0

        for instance in upcoming:
            if instance.event_series and instance.event_series.candidate_pool == "interested_only":
                if instance.id not in interested_masses:
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
            diagnostics_reasons.append("Missas com pool de interessados estao fora da sua lista.")
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
def position_claim_create(request, instance_id, slot_id):
    parish = request.active_parish
    if request.method != "POST":
        return redirect("mass_detail", instance_id=instance_id)
    slot = get_object_or_404(
        AssignmentSlot,
        parish=parish,
        id=slot_id,
        mass_instance_id=instance_id,
    )
    acolyte = parish.acolytes.filter(user=request.user, active=True).first()
    if not acolyte:
        messages.error(request, "Seu usuario nao esta vinculado a um acolito.")
        return redirect("mass_detail", instance_id=instance_id)
    claim, error = create_position_claim(parish, slot, acolyte, actor=request.user)
    if error:
        if request.headers.get("HX-Request"):
            response = HttpResponse(status=204)
            response["HX-Success-Message"] = error
            return response
        messages.error(request, error)
        return redirect("mass_detail", instance_id=instance_id)
    return _claim_response(
        request,
        claim,
        request.POST.get("return") or "slots_section",
        "Solicitacao enviada.",
    )


@login_required
@require_active_parish
def position_claim_choose(request, claim_id):
    parish = request.active_parish
    if request.method != "POST":
        return redirect("dashboard")
    claim = get_object_or_404(PositionClaimRequest, parish=parish, id=claim_id)
    assignment = claim.slot.get_active_assignment()
    if not assignment or assignment.acolyte.user_id != request.user.id:
        return redirect("dashboard")
    success = choose_claim(claim, actor=request.user, require_coordination=parish.claim_require_coordination)
    return _claim_response(
        request,
        claim,
        request.POST.get("return") or "assignment_card",
        "Solicitacao atualizada." if success else "Solicitacao indisponivel.",
    )


@login_required
@require_active_parish
def position_claim_reject(request, claim_id):
    parish = request.active_parish
    if request.method != "POST":
        return redirect("dashboard")
    claim = get_object_or_404(PositionClaimRequest, parish=parish, id=claim_id)
    assignment = claim.slot.get_active_assignment()
    if assignment and assignment.acolyte.user_id == request.user.id:
        success = reject_claim(claim, actor=request.user, reason="holder_rejected", approval_mode="target")
    elif user_has_role(request.user, parish, ADMIN_ROLE_CODES) and claim.status == "pending_coordination":
        success = reject_claim(claim, actor=request.user, reason="coordination_rejected", approval_mode="coordination")
    else:
        return redirect("dashboard")
    return _claim_response(
        request,
        claim,
        request.POST.get("return") or "claim_card",
        "Solicitacao recusada." if success else "Solicitacao indisponivel.",
    )


@login_required
@require_active_parish
def position_claim_cancel(request, claim_id):
    parish = request.active_parish
    if request.method != "POST":
        return redirect("swap_requests")
    claim = get_object_or_404(PositionClaimRequest, parish=parish, id=claim_id)
    if claim.requestor_acolyte.user_id != request.user.id:
        return redirect("swap_requests")
    success = cancel_claim(claim, actor=request.user)
    return _claim_response(
        request,
        claim,
        request.POST.get("return") or "claim_card",
        "Solicitacao cancelada." if success else "Solicitacao indisponivel.",
    )


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def position_claim_approve(request, claim_id):
    parish = request.active_parish
    if request.method != "POST":
        return redirect("swap_requests")
    claim = get_object_or_404(PositionClaimRequest, parish=parish, id=claim_id)
    if claim.status != "pending_coordination":
        return redirect("swap_requests")
    success = approve_claim(claim, actor=request.user, approval_mode="coordination", resolution_reason="coordination_approved")
    return _claim_response(
        request,
        claim,
        request.POST.get("return") or "claim_card",
        "Solicitacao aprovada." if success else "Solicitacao indisponivel.",
    )


@login_required
@require_active_parish
def swap_requests(request):
    parish = request.active_parish
    swaps = SwapRequest.objects.filter(parish=parish).exclude(status="canceled").select_related(
        "mass_instance__community",
        "requestor_acolyte",
        "target_acolyte",
        "from_slot__position_type",
        "to_slot__position_type"
    ).order_by("-created_at")

    claims = PositionClaimRequest.objects.filter(parish=parish).select_related(
        "slot__mass_instance__community",
        "slot__position_type",
        "requestor_acolyte",
        "target_assignment__acolyte",
    ).order_by("-created_at")

    can_manage_parish = user_has_role(request.user, parish, ADMIN_ROLE_CODES)
    show_all = request.GET.get('all') == 'true' and can_manage_parish

    if not show_all:
        # Show user's own swaps OR awaiting_approval swaps (for coordinators to approve)
        swaps = (
            swaps.filter(requestor_acolyte__user=request.user)
            | swaps.filter(target_acolyte__user=request.user)
            | swaps.filter(status="awaiting_approval")  # Coordinators can see and approve these
        ).distinct()

        claim_filter = Q(requestor_acolyte__user=request.user) | Q(target_assignment__acolyte__user=request.user)
        if can_manage_parish:
            claim_filter |= Q(status__in=["pending_coordination", "scheduled_auto_approve"])
        claims = claims.filter(claim_filter).distinct()

    return render(request, "acolytes/swaps.html", {
        "swaps": swaps,
        "claims": claims,
        "can_manage_parish": can_manage_parish,
        "show_all": show_all,
        "claim_status_labels": dict(PositionClaimRequest.STATUS_CHOICES),
        "claim_reason_labels": dict(PositionClaimRequest.RESOLUTION_CHOICES),
    })


@login_required
@require_active_parish
def swap_request_create(request, assignment_id):
    """Simplified swap request - only for role swaps within the same mass."""
    parish = request.active_parish
    assignment = get_object_or_404(
        Assignment.objects.select_related(
            "slot__mass_instance__community", 
            "slot__position_type",
            "acolyte"
        ),
        parish=parish, 
        id=assignment_id
    )
    
    if not assignment.is_active:
        messages.info(request, "Esta escala não está mais ativa.")
        return redirect("my_assignments")
    
    if assignment.acolyte.user_id != request.user.id and not user_has_role(
        request.user, parish, ADMIN_ROLE_CODES
    ):
        return redirect("my_assignments")
    
    mass_instance = assignment.slot.mass_instance
    
    # Get other slots in the same mass with active assignments
    other_slots = (
        mass_instance.slots
        .exclude(id=assignment.slot_id)
        .filter(assignments__is_active=True)
        .select_related("position_type")
        .prefetch_related(
            Prefetch(
                "assignments",
                queryset=Assignment.objects.filter(is_active=True).select_related("acolyte"),
                to_attr="active_assignments"
            )
        )
    )
    
    # Build list of swap options
    swap_options = []
    for slot in other_slots:
        active_assignment = slot.active_assignments[0] if slot.active_assignments else None
        if active_assignment:
            swap_options.append({
                "slot": slot,
                "position_name": slot.position_type.name,
                "acolyte": active_assignment.acolyte,
            })
    
    if request.method == "POST":
        target_slot_ids = request.POST.getlist("target_slots")
        notes = request.POST.get("notes", "").strip()
        
        if not target_slot_ids:
            messages.error(request, "Selecione pelo menos uma função para trocar.")
            return redirect("swap_request_create", assignment_id=assignment.id)
        
        # Generate a group_id if multiple targets selected
        group_id = uuid4() if len(target_slot_ids) > 1 else None
        
        created_swaps = []
        for target_slot_id in target_slot_ids:
            to_slot = AssignmentSlot.objects.filter(parish=parish, id=target_slot_id).first()
            if not to_slot:
                continue
            
            to_assignment = to_slot.get_active_assignment()
            if not to_assignment:
                continue
            
            swap = SwapRequest.objects.create(
                parish=parish,
                swap_type="role_swap",
                requestor_acolyte=assignment.acolyte,
                target_acolyte=to_assignment.acolyte,
                mass_instance=mass_instance,
                from_slot=assignment.slot,
                to_slot=to_slot,
                status="pending",
                notes=notes,
                open_to_admin=False,
                group_id=group_id,
            )
            
            log_audit(parish, request.user, "SwapRequest", swap.id, "create", {"swap_type": "role_swap", "group_id": str(group_id) if group_id else None})
            
            if to_assignment.acolyte.user:
                enqueue_notification(
                    parish,
                    to_assignment.acolyte.user,
                    "SWAP_REQUESTED",
                    {"swap_id": swap.id},
                    idempotency_key=f"swap:{swap.id}:request",
                )
            
            created_swaps.append(swap)
        
        if not created_swaps:
            messages.error(request, "Não foi possível criar a solicitação de troca.")
            return redirect("swap_request_create", assignment_id=assignment.id)
        
        if len(created_swaps) == 1:
            messages.success(request, "Solicitação de troca enviada.")
        else:
            messages.success(request, f"Solicitações de troca enviadas para {len(created_swaps)} acólitos.")
        return redirect("my_assignments")
    
    return render(request, "acolytes/swap_form.html", {
        "assignment": assignment,
        "mass_instance": mass_instance,
        "swap_options": swap_options,
    })


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
        if request.headers.get("HX-Request"):
            response = HttpResponse(status=204)
            response["HX-Success-Message"] = "Solicitacao em aberto. A coordenacao vai tratar esta troca."
            return response
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
        
        # Refetch swap to get updated status
        swap = SwapRequest.objects.select_related('mass_instance', 'requestor_acolyte', 'target_acolyte').get(id=swap_id)
        return _htmx_or_redirect(
            request,
            "acolytes/_partials/swap_card.html",
            {"swap": swap, "can_manage_parish": user_has_role(request.user, parish, ADMIN_ROLE_CODES)},
            "swap_requests",
            "Troca enviada para aprovacao da coordenacao."
        )
    try:
        applied = apply_swap_request(swap, actor=request.user)
    except ConcurrentUpdateError:
        if request.headers.get("HX-Request"):
            response = HttpResponse(status=204)
            response["HX-Success-Message"] = "Esta vaga foi atualizada por outra acao. Recarregue e tente novamente."
            return response
        messages.error(request, "Esta vaga foi atualizada por outra acao. Recarregue e tente novamente.")
        return redirect("swap_requests")
    if applied:
        swap.status = "accepted"
        swap.save(update_fields=["status", "updated_at"])
        
        # Cancel other pending swaps in the same group
        if swap.group_id:
            other_swaps = SwapRequest.objects.filter(
                group_id=swap.group_id,
                status="pending"
            ).exclude(id=swap.id)
            other_swaps.update(status="canceled")
        
        if swap.requestor_acolyte.user:
            enqueue_notification(
                parish,
                swap.requestor_acolyte.user,
                "SWAP_ACCEPTED",
                {"swap_id": swap.id},
                idempotency_key=f"swap:{swap.id}:accepted",
            )
        
        # Refetch swap to get updated status
        swap = SwapRequest.objects.select_related('mass_instance', 'requestor_acolyte', 'target_acolyte').get(id=swap_id)
        return _htmx_or_redirect(
            request,
            "acolytes/_partials/swap_card.html",
            {"swap": swap, "can_manage_parish": user_has_role(request.user, parish, ADMIN_ROLE_CODES)},
            "swap_requests",
            "Troca aceita com sucesso."
        )
    else:
        if request.headers.get("HX-Request"):
            response = HttpResponse(status=204)
            response["HX-Success-Message"] = "Nao foi possivel aplicar a troca."
            return response
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
        if request.headers.get("HX-Request"):
            response = HttpResponse(status=204)
            response["HX-Success-Message"] = "Solicitacao em aberto. A coordenacao vai tratar esta troca."
            return response
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
    
    # Refetch swap to get updated status
    swap = SwapRequest.objects.select_related('mass_instance', 'requestor_acolyte', 'target_acolyte').get(id=swap_id)
    return _htmx_or_redirect(
        request,
        "acolytes/_partials/swap_card.html",
        {"swap": swap, "can_manage_parish": user_has_role(request.user, parish, ADMIN_ROLE_CODES)},
        "swap_requests",
        "Troca recusada."
    )


@login_required
@require_active_parish
def swap_request_cancel(request, swap_id):
    """Cancel a swap request (by the requestor)."""
    if request.method != "POST":
        return redirect("swap_requests")
    parish = request.active_parish
    swap = get_object_or_404(SwapRequest, parish=parish, id=swap_id)
    
    # Only the requestor or admin can cancel
    is_requestor = swap.requestor_acolyte.user_id == request.user.id
    is_admin = user_has_role(request.user, parish, ADMIN_ROLE_CODES)
    
    if not is_requestor and not is_admin:
        return redirect("swap_requests")
    
    # Can only cancel pending or awaiting_approval swaps
    if swap.status not in ("pending", "awaiting_approval"):
        if request.headers.get("HX-Request"):
            response = HttpResponse(status=204)
            response["HX-Success-Message"] = "Esta solicitação não pode ser cancelada."
            return response
        messages.info(request, "Esta solicitação não pode ser cancelada.")
        return redirect("swap_requests")
    
    swap.status = "canceled"
    swap.save(update_fields=["status", "updated_at"])
    
    log_audit(parish, request.user, "SwapRequest", swap.id, "cancel", {"status": "canceled"})
    
    # For HTMX, return empty response to remove the card
    if request.headers.get("HX-Request"):
        response = HttpResponse("")
        response["HX-Success-Message"] = "Solicitação cancelada."
        return response
    
    messages.success(request, "Solicitação cancelada.")
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
        if request.headers.get("HX-Request"):
            response = HttpResponse(status=204)
            response["HX-Success-Message"] = "Esta troca nao esta aguardando aprovacao."
            return response
        return redirect("swap_requests")
    try:
        applied = apply_swap_request(swap, actor=request.user)
    except ConcurrentUpdateError:
        if request.headers.get("HX-Request"):
            response = HttpResponse(status=204)
            response["HX-Success-Message"] = "Esta vaga foi atualizada por outra acao. Recarregue e tente novamente."
            return response
        messages.error(request, "Esta vaga foi atualizada por outra acao. Recarregue e tente novamente.")
        return redirect("swap_requests")
    if applied:
        swap.status = "accepted"
        swap.save(update_fields=["status", "updated_at"])

        # Cancel other pending swaps in the same group
        if swap.group_id:
            other_swaps = SwapRequest.objects.filter(
                group_id=swap.group_id,
                status="pending"
            ).exclude(id=swap.id)
            other_swaps.update(status="canceled")

        if swap.requestor_acolyte.user:
            enqueue_notification(
                parish,
                swap.requestor_acolyte.user,
                "SWAP_ACCEPTED",
                {"swap_id": swap.id},
                idempotency_key=f"swap:{swap.id}:accepted",
            )
        
        # Refetch swap to get updated status
        swap = SwapRequest.objects.select_related('mass_instance', 'requestor_acolyte', 'target_acolyte').get(id=swap_id)
        return _htmx_or_redirect(
            request,
            "acolytes/_partials/swap_card.html",
            {"swap": swap, "can_manage_parish": True},
            "swap_requests",
            "Troca aprovada com sucesso."
        )
    else:
        if request.headers.get("HX-Request"):
            response = HttpResponse(status=204)
            response["HX-Success-Message"] = "Nao foi possivel aplicar a troca."
            return response
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
                "claim_auto_approve_enabled": parish.claim_auto_approve_enabled,
                "claim_auto_approve_hours": parish.claim_auto_approve_hours,
                "claim_require_coordination": parish.claim_require_coordination,
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
                "claim_auto_approve_enabled": parish.claim_auto_approve_enabled,
                "claim_auto_approve_hours": parish.claim_auto_approve_hours,
                "claim_require_coordination": parish.claim_require_coordination,
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
        if request.headers.get("HX-Request"):
            response = HttpResponse(status=204)
            response["HX-Success-Message"] = "Esta escala nao esta mais ativa."
            return response
        messages.info(request, "Esta escala nao esta mais ativa.")
        return redirect("my_assignments")
    if assignment.slot.mass_instance.status == "canceled":
        if request.headers.get("HX-Request"):
            response = HttpResponse(status=204)
            response["HX-Success-Message"] = "Esta missa foi cancelada."
            return response
        messages.info(request, "Esta missa foi cancelada.")
        return redirect("my_assignments")
    if not _can_manage_assignment(request.user, assignment):
        return redirect("my_assignments")
    confirmation, _ = Confirmation.objects.get_or_create(parish=parish, assignment=assignment)
    confirmation.status = "confirmed"
    confirmation.updated_by = request.user
    confirmation.save(update_fields=["status", "updated_by", "timestamp"])
    log_audit(parish, request.user, "Confirmation", confirmation.id, "update", {"status": "confirmed"})
    expire_claims_for_assignment(assignment, "holder_confirmed", actor=request.user)
    
    # Refetch assignment to get updated confirmation
    assignment = Assignment.objects.select_related(
        "slot__mass_instance__community", 
        "slot__position_type",
        "confirmation",
        "acolyte"
    ).get(id=assignment_id)
    
    return_target = request.POST.get("return")
    if return_target == "dashboard_hero":
        return _htmx_or_redirect(
            request,
            "acolytes/_partials/dashboard_hero_assignment.html",
            {
                "hero_assignment": assignment,
                "claims_by_slot": _claim_map_for_slots(parish, [assignment.slot_id]),
            },
            "dashboard",
        )
    if return_target == "dashboard_pending":
        return _htmx_or_redirect(
            request,
            "acolytes/_partials/dashboard_pending_row.html",
            {
                "assignment": assignment,
                "claims_by_slot": _claim_map_for_slots(parish, [assignment.slot_id]),
            },
            "dashboard",
        )
    return _htmx_or_redirect(
        request,
        "acolytes/_partials/assignment_card.html",
        _assignment_card_context(parish, assignment),
        "my_assignments",
    )


@login_required
@require_active_parish
def decline_assignment(request, assignment_id):
    if request.method != "POST":
        return redirect("my_assignments")
    parish = request.active_parish
    assignment = get_object_or_404(Assignment, parish=parish, id=assignment_id)
    if not assignment.is_active:
        if request.headers.get("HX-Request"):
            response = HttpResponse(status=204)
            response["HX-Success-Message"] = "Esta escala nao esta mais ativa."
            return response
        messages.info(request, "Esta escala nao esta mais ativa.")
        return redirect("my_assignments")
    if assignment.slot.mass_instance.status == "canceled":
        if request.headers.get("HX-Request"):
            response = HttpResponse(status=204)
            response["HX-Success-Message"] = "Esta missa foi cancelada."
            return response
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
    replacement = None
    if should_create_replacement(parish, slot, now=timezone.now()):
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
        if parish.auto_assign_on_decline and replacement:
            ScheduleJobRequest.objects.create(
                parish=parish,
                requested_by=request.user,
                job_type="replacement",
                horizon_days=0,
                payload_json={
                    "slot_id": slot.id,
                    "replacement_request_id": replacement.id,
                },
            )
    
    # For HTMX, return updated hero assignment
    if request.headers.get("HX-Request"):
        # Get new hero assignment
        hero_assignment = Assignment.objects.filter(
            parish=parish,
            acolyte=assignment.acolyte,
            is_active=True,
            assignment_state__in=["proposed", "published", "locked"],
            slot__mass_instance__starts_at__gte=timezone.now(),
            slot__mass_instance__status="scheduled",
        ).select_related(
            "slot__mass_instance__community",
            "slot__position_type",
        ).order_by("slot__mass_instance__starts_at").first()

        claims_by_slot = {}
        if hero_assignment:
            claims_by_slot = _claim_map_for_slots(parish, [hero_assignment.slot_id])

        return render(request, "acolytes/_partials/dashboard_hero_assignment.html", {
            "hero_assignment": hero_assignment,
            "claims_by_slot": claims_by_slot,
        })
    return redirect("my_assignments")


@login_required
@require_active_parish
def cancel_assignment(request, assignment_id):
    if request.method != "POST":
        return redirect("my_assignments")
    parish = request.active_parish
    assignment = get_object_or_404(Assignment, parish=parish, id=assignment_id)
    if not assignment.is_active:
        if request.headers.get("HX-Request"):
            response = HttpResponse("")
            response["HX-Success-Message"] = "Esta escala nao esta mais ativa."
            return response
        return redirect("my_assignments")
    if assignment.slot.mass_instance.status == "canceled":
        if request.headers.get("HX-Request"):
            response = HttpResponse("")
            response["HX-Success-Message"] = "A missa foi cancelada. Nenhuma acao e necessaria."
            return response
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
    if should_create_replacement(parish, slot, now=timezone.now()):
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
    
    # For HTMX, return empty response with success message (will remove the card)
    if request.headers.get("HX-Request"):
        response = HttpResponse("")
        response["HX-Success-Message"] = "Escala cancelada."
        return response
    return redirect("my_assignments")


# ============================================================================
# OPEN SLOTS DASHBOARD
# ============================================================================

@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def roster_open_slots(request):
    """Dashboard showing all open slots with filtering and quick assignment."""
    parish = request.active_parish
    today = timezone.localdate()
    
    # Get filter parameters
    start_date = _parse_date(request.GET.get("start"), today)
    end_date = _parse_date(request.GET.get("end"), today + timedelta(days=14))
    community_id = request.GET.get("community")
    if community_id in [None, 'None', '']:
        community_id = None
    else:
        try:
            community_id = int(community_id)
        except ValueError:
            community_id = None
    position_id = request.GET.get("position")
    if position_id in [None, 'None', '']:
        position_id = None
    else:
        try:
            position_id = int(position_id)
        except ValueError:
            position_id = None
    urgent_only = request.GET.get("urgent") == "1"
    
    # Build query for open slots
    slots = AssignmentSlot.objects.filter(
        parish=parish,
        status="open",
        required=True,
        externally_covered=False,
        mass_instance__status="scheduled",
        mass_instance__starts_at__gte=timezone.now(),
        mass_instance__starts_at__date__range=(start_date, end_date)
    ).select_related(
        "mass_instance__community",
        "position_type",
        "mass_instance__requirement_profile"
    ).prefetch_related(
        "mass_instance__slots__assignments"
    )
    
    if community_id:
        slots = slots.filter(mass_instance__community_id=community_id)
    if position_id:
        slots = slots.filter(position_type_id=position_id)
    if urgent_only:
        consolidation_threshold = timezone.now() + timedelta(days=7)
        slots = slots.filter(mass_instance__starts_at__lte=consolidation_threshold)
    
    slots = slots.order_by("mass_instance__starts_at")
    
    # Calculate KPIs
    total_open_slots = slots.count()
    urgent_slots = slots.filter(
        mass_instance__starts_at__lte=timezone.now() + timedelta(days=7)
    ).count()
    
    # Calculate fill rate across all upcoming masses
    all_upcoming_slots = AssignmentSlot.objects.filter(
        parish=parish,
        required=True,
        mass_instance__status="scheduled",
        mass_instance__starts_at__gte=timezone.now(),
        mass_instance__starts_at__date__range=(start_date, end_date)
    ).exclude(externally_covered=True)
    total_required_slots = all_upcoming_slots.count()
    filled_slots = all_upcoming_slots.filter(status="assigned").count()
    fill_rate = int((filled_slots / total_required_slots * 100)) if total_required_slots > 0 else 100
    
    # Pagination
    paginator = Paginator(slots, 20)
    page = request.GET.get("page", 1)
    slots_page = paginator.get_page(page)
    
    # Get communities and positions for filters
    communities = Community.objects.filter(parish=parish, active=True).order_by("code")
    positions = PositionType.objects.filter(parish=parish).order_by("name")
    
    # For HTMX requests, return only the table partial
    if request.headers.get("HX-Request"):
        return render(request, "roster/_open_slots_table.html", {
            "slots": slots_page,
            "page_obj": slots_page,
        })
    
    return render(request, "roster/open_slots.html", {
        "parish": parish,
        "slots": slots_page,
        "page_obj": slots_page,
        "start_date": start_date,
        "end_date": end_date,
        "community_id": community_id or "",
        "position_id": position_id or "",
        "urgent_only": urgent_only,
        "communities": communities,
        "positions": positions,
        "total_open_slots": total_open_slots,
        "urgent_slots": urgent_slots,
        "fill_rate": fill_rate,
    })


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def roster_open_slot_assign(request, slot_id):
    """HTMX endpoint to assign an acolyte to a specific slot."""
    if request.method != "POST":
        return HttpResponse("Method not allowed", status=405)
    
    parish = request.active_parish
    slot = get_object_or_404(AssignmentSlot, parish=parish, id=slot_id)
    acolyte_id = request.POST.get("acolyte_id") or request.POST.get("acolyte")
    notes = request.POST.get("notes", "")
    
    if not acolyte_id:
        return HttpResponse("Acolyte required", status=400)
    
    acolyte = get_object_or_404(AcolyteProfile, parish=parish, id=acolyte_id)
    
    try:
        assign_manual(slot, acolyte, actor=request.user)
        response = HttpResponse("")
        response["HX-Success-Message"] = f"{acolyte.display_name} atribuido com sucesso."
        return response
    except Exception as e:
        return HttpResponse(str(e), status=400)


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def roster_open_slot_mark_external(request, slot_id):
    """HTMX endpoint to mark a slot as externally covered."""
    if request.method != "POST":
        return HttpResponse("Method not allowed", status=405)
    
    parish = request.active_parish
    slot = get_object_or_404(AssignmentSlot, parish=parish, id=slot_id)
    
    slot.externally_covered = True
    slot.status = "finalized"
    slot.save(update_fields=["externally_covered", "status", "updated_at"])
    
    log_audit(
        parish=parish,
        actor=request.user,
        entity_type="AssignmentSlot",
        entity_id=slot.id,
        action_type="update",
        diff={"externally_covered": True}
    )
    
    response = HttpResponse("")
    response["HX-Success-Message"] = "Slot marcado como externo."
    return response


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def roster_open_slot_get_candidates(request, slot_id):
    """HTMX endpoint to get candidate acolytes for a slot."""
    parish = request.active_parish
    slot = get_object_or_404(AssignmentSlot, parish=parish, id=slot_id)
    
    cache = build_recommendation_cache(parish, slots=[slot])
    assigned_acolyte_ids = set(
        Assignment.objects.filter(
            parish=parish,
            slot__mass_instance=slot.mass_instance,
            is_active=True,
        ).values_list("acolyte_id", flat=True)
    )
    candidates_meta = rank_candidates(
        slot,
        parish,
        max_candidates=10,
        exclude_acolyte_ids=assigned_acolyte_ids,
        cache=cache,
        include_meta=True,
        enforce_dynamic=True,
    )

    candidates = []
    for entry in candidates_meta:
        acolyte = entry["acolyte"]
        stats = AcolyteStats.objects.filter(parish=parish, acolyte=acolyte).first()
        candidates.append({
            "acolyte": acolyte,
            "last_served": stats.last_served_at if stats else None,
            "score": entry.get("score"),
            "reason": entry.get("reason"),
            "conflicts": False,
        })
    
    return render(request, "roster/_slot_candidates.html", {
        "slot": slot,
        "candidates": candidates,
    })


# ============================================================================
# ACOLYTE MULTI-SLOT ASSIGNMENT
# ============================================================================

@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def acolyte_assign_to_multiple_slots(request, acolyte_id):
    """Interface to assign an acolyte to multiple open slots at once."""
    parish = request.active_parish
    acolyte = get_object_or_404(AcolyteProfile, parish=parish, id=acolyte_id)
    
    # Get filter parameters
    today = timezone.localdate()
    start_date = _parse_date(request.GET.get("start"), today)
    end_date = _parse_date(request.GET.get("end"), today + timedelta(days=30))
    community_id = request.GET.get("community")
    if community_id in [None, 'None', '']:
        community_id = None
    else:
        try:
            community_id = int(community_id)
        except ValueError:
            community_id = None
    
    # Get acolyte's qualifications
    qualified_positions = AcolyteQualification.objects.filter(
        parish=parish, acolyte=acolyte, qualified=True
    ).values_list("position_type_id", flat=True)
    
    # Get open slots that match qualifications
    slots = AssignmentSlot.objects.filter(
        parish=parish,
        status="open",
        required=True,
        externally_covered=False,
        mass_instance__status="scheduled",
        mass_instance__starts_at__gte=timezone.now(),
        mass_instance__starts_at__date__range=(start_date, end_date),
        position_type_id__in=qualified_positions
    ).select_related(
        "mass_instance__community",
        "position_type"
    ).order_by("mass_instance__starts_at")
    
    if community_id:
        slots = slots.filter(mass_instance__community_id=community_id)
    
    # Get acolyte stats
    stats = AcolyteStats.objects.filter(parish=parish, acolyte=acolyte).first()
    
    # Get communities for filter
    communities = Community.objects.filter(parish=parish, active=True).order_by("code")
    
    return render(request, "people/acolyte_assign_multiple_slots.html", {
        "parish": parish,
        "acolyte": acolyte,
        "slots": slots,
        "stats": stats,
        "start_date": start_date,
        "end_date": end_date,
        "community_id": community_id or "",
        "communities": communities,
    })


@login_required
@require_active_parish
@require_parish_roles(ADMIN_ROLE_CODES)
def acolyte_assign_to_multiple_slots_submit(request, acolyte_id):
    """Handle submission of multiple slot assignments."""
    if request.method != "POST":
        return redirect("people_acolyte_detail", acolyte_id=acolyte_id)
    
    parish = request.active_parish
    acolyte = get_object_or_404(AcolyteProfile, parish=parish, id=acolyte_id)
    
    slot_ids = request.POST.getlist("slots")
    notes = request.POST.get("notes", "")
    
    if not slot_ids:
        messages.error(request, "Nenhum slot selecionado.")
        return redirect("acolyte_assign_to_multiple_slots", acolyte_id=acolyte_id)
    
    # Validate and assign each slot
    success_count = 0
    errors = []
    
    for slot_id in slot_ids:
        try:
            slot = AssignmentSlot.objects.get(parish=parish, id=slot_id)
            assign_manual(slot, acolyte, actor=request.user)
            success_count += 1
        except AssignmentSlot.DoesNotExist:
            errors.append(f"Slot {slot_id} não encontrado")
        except Exception as e:
            errors.append(f"Erro no slot {slot_id}: {str(e)}")
    
    if success_count > 0:
        messages.success(request, f"{success_count} atribuições criadas com sucesso.")
    if errors:
        for error in errors:
            messages.error(request, error)
    
    return redirect("people_acolyte_detail", acolyte_id=acolyte_id)
