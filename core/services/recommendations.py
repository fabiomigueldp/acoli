from collections import defaultdict
from datetime import timedelta

from django.utils import timezone
from django.db.models import F, Q

from core.models import (
    AcolyteAvailabilityRule,
    AcolytePreference,
    AcolyteQualification,
    AcolyteStats,
    Assignment,
    MassInterest,
)
from core.services.availability import group_rules_by_acolyte, is_acolyte_available_with_rules
from core.services.preferences import preference_score_breakdown


DEFAULT_WEIGHTS = {
    "home_community_bonus": 40,
    "community_recent_penalty": 6,
    "community_recent_window_days": 30,
    "scarcity_bonus": 15,
    "event_series_community_factor": 0.4,
    "single_mass_community_policy": "recurring",
    "interested_pool_fallback": "relax_to_all",
    "interest_deadline_hours": 48,
    "rotation_days": 60,
    "rotation_penalty": 3,
}


def _get_weight(weights, key, default=None):
    if weights and key in weights:
        return weights.get(key)
    if default is not None:
        return default
    return DEFAULT_WEIGHTS.get(key)


def rotation_key_for_mass(instance):
    if instance.event_series_id:
        return ("series", instance.event_series_id)
    if instance.template_id:
        return ("template", instance.template_id)
    return ("community", instance.community_id)


def get_mass_context(mass_instance, weights, interest_map=None, now=None):
    now = now or timezone.now()
    candidate_pool = "all"
    if mass_instance.event_series_id and mass_instance.event_series:
        candidate_pool = getattr(mass_instance.event_series, "candidate_pool", "all")

    pool_ids = set()
    if interest_map is not None:
        pool_ids = set(interest_map.get(mass_instance.id, set()))

    interest_deadline_at = None
    if mass_instance.event_series_id and getattr(mass_instance.event_series, "interest_deadline_at", None):
        interest_deadline_at = mass_instance.event_series.interest_deadline_at
        if timezone.is_naive(interest_deadline_at):
            interest_deadline_at = timezone.make_aware(interest_deadline_at, timezone.get_current_timezone())
    if not interest_deadline_at:
        interest_deadline_hours = int(_get_weight(weights, "interest_deadline_hours", 48) or 0)
        interest_deadline_at = mass_instance.starts_at - timedelta(hours=interest_deadline_hours)
    interest_closed = now >= interest_deadline_at
    fallback = _get_weight(weights, "interested_pool_fallback", "relax_to_all")

    pool_mode = "all"
    if candidate_pool == "interested_only":
        if pool_ids:
            pool_mode = "interested_only"
        else:
            if not interest_closed:
                pool_mode = "empty"
            elif fallback == "relax_to_preferred":
                pool_mode = "preferred_only"
            elif fallback == "strict":
                pool_mode = "empty"
            else:
                pool_mode = "all"

    community_factor = 1.0
    single_policy = _get_weight(weights, "single_mass_community_policy", "recurring")
    if candidate_pool == "interested_only":
        community_factor = 0.0
    elif mass_instance.event_series_id:
        community_factor = float(_get_weight(weights, "event_series_community_factor", 0.4) or 0)
    elif not mass_instance.template_id and single_policy == "special":
        community_factor = float(_get_weight(weights, "event_series_community_factor", 0.4) or 0)

    return {
        "candidate_pool": candidate_pool,
        "pool_ids": pool_ids,
        "pool_mode": pool_mode,
        "interest_closed": interest_closed,
        "interest_deadline_at": interest_deadline_at,
        "community_factor": community_factor,
        "rotation_key": rotation_key_for_mass(mass_instance),
        "is_recurring": bool(mass_instance.template_id),
        "is_special": bool(mass_instance.event_series_id),
        "is_single": not mass_instance.template_id and not mass_instance.event_series_id,
    }


def build_recommendation_cache(parish, slots=None, acolytes=None, now=None):
    now = now or timezone.now()
    weights = parish.schedule_weights or {}
    rotation_days = int(_get_weight(weights, "rotation_days", 60) or 0)
    community_recent_window_days = int(_get_weight(weights, "community_recent_window_days", 30) or 0)

    if acolytes is None:
        acolytes = list(
            parish.acolytes.filter(active=True).select_related("community_of_origin", "family_group")
        )
    else:
        acolytes = list(acolytes)
    acolyte_ids = [acolyte.id for acolyte in acolytes]

    position_type_ids = set()
    mass_instance_ids = set()
    if slots:
        for slot in slots:
            position_type_ids.add(slot.position_type_id)
            mass_instance_ids.add(slot.mass_instance_id)

    qualifications = AcolyteQualification.objects.filter(parish=parish, qualified=True)
    if position_type_ids:
        qualifications = qualifications.filter(position_type_id__in=position_type_ids)

    qualified_by_position = defaultdict(set)
    qualified_pairs = {}
    for qual in qualifications:
        qualified_by_position[qual.position_type_id].add(qual.acolyte_id)
        qualified_pairs[(qual.acolyte_id, qual.position_type_id)] = True

    preferences = AcolytePreference.objects.filter(parish=parish, acolyte_id__in=acolyte_ids)
    pref_by_acolyte = defaultdict(list)
    avoid_communities = defaultdict(set)
    preferred_communities = defaultdict(set)
    for pref in preferences:
        pref_by_acolyte[pref.acolyte_id].append(pref)
        if pref.preference_type == "avoid_community" and pref.target_community_id:
            avoid_communities[pref.acolyte_id].add(pref.target_community_id)
        if pref.preference_type == "preferred_community" and pref.target_community_id:
            preferred_communities[pref.acolyte_id].add(pref.target_community_id)

    stats_map = {stat.acolyte_id: stat for stat in AcolyteStats.objects.filter(parish=parish)}
    availability_rules = AcolyteAvailabilityRule.objects.filter(parish=parish, acolyte_id__in=acolyte_ids)
    rules_by_acolyte = group_rules_by_acolyte(availability_rules)

    interest_map = defaultdict(set)
    if mass_instance_ids:
        for interest in MassInterest.objects.filter(parish=parish, interested=True, mass_instance_id__in=mass_instance_ids):
            interest_map[interest.mass_instance_id].add(interest.acolyte_id)

    starts = [slot.mass_instance.starts_at for slot in slots] if slots else []
    if starts:
        min_start = min(starts)
        max_start = max(starts)
    else:
        min_start = now
        max_start = now + timedelta(days=getattr(parish, "horizon_days", 60))

    buffer_days = max(rotation_days, community_recent_window_days, 60)
    window_start = min_start - timedelta(days=buffer_days)
    window_end = max_start + timedelta(days=buffer_days)

    assignments = (
        Assignment.objects.filter(
            parish=parish,
            is_active=True,
            assignment_state__in=["proposed", "published", "locked"],
            slot__mass_instance__starts_at__gte=window_start,
            slot__mass_instance__starts_at__lte=window_end,
            slot__mass_instance__status="scheduled",
        )
        .select_related("slot__mass_instance")
    )

    assignments_by_acolyte = defaultdict(list)
    weekly_counts_by_acolyte = defaultdict(lambda: defaultdict(int))
    weekend_dates_by_acolyte = defaultdict(set)
    rotation_assignment_times = defaultdict(list)
    community_assignment_times = defaultdict(list)

    for assignment in assignments:
        start = assignment.slot.mass_instance.starts_at
        acolyte_id = assignment.acolyte_id
        assignments_by_acolyte[acolyte_id].append(start)

        week_key = start.isocalendar()[:2]
        weekly_counts_by_acolyte[acolyte_id][week_key] += 1

        weekday = start.weekday()
        if weekday in (5, 6):
            weekend_date = start.date()
            if weekday == 6:
                weekend_date = weekend_date - timedelta(days=1)
            weekend_dates_by_acolyte[acolyte_id].add(weekend_date)

        rotation_key = rotation_key_for_mass(assignment.slot.mass_instance)
        rotation_assignment_times[(acolyte_id, rotation_key)].append(start)
        community_assignment_times[(acolyte_id, assignment.slot.mass_instance.community_id)].append(start)

    for key, times in rotation_assignment_times.items():
        times.sort()
    for key, times in community_assignment_times.items():
        times.sort()
    for acolyte_id, times in assignments_by_acolyte.items():
        times.sort()

    return {
        "acolytes": acolytes,
        "qualified_by_position": qualified_by_position,
        "qualified_pairs": qualified_pairs,
        "pref_by_acolyte": pref_by_acolyte,
        "avoid_communities": avoid_communities,
        "preferred_communities": preferred_communities,
        "stats_map": stats_map,
        "rules_by_acolyte": rules_by_acolyte,
        "interest_map": interest_map,
        "assignments_by_acolyte": assignments_by_acolyte,
        "weekly_counts_by_acolyte": weekly_counts_by_acolyte,
        "weekend_dates_by_acolyte": weekend_dates_by_acolyte,
        "rotation_assignment_times": rotation_assignment_times,
        "community_assignment_times": community_assignment_times,
        "now": now,
        "weights": weights,
    }


def _count_within_window(times, window_start, window_end):
    if not times:
        return 0
    count = 0
    for ts in times:
        if ts < window_start:
            continue
        if ts > window_end:
            break
        count += 1
    return count


def _has_recent_rotation(times, window_start, window_end):
    if not times:
        return False
    for ts in times:
        if ts < window_start:
            continue
        if ts > window_end:
            break
        return True
    return False


def _would_exceed_consecutive_weekends(existing_weekends, weekend_date, max_consecutive):
    if not weekend_date or not max_consecutive:
        return False
    if weekend_date in existing_weekends:
        return False
    count = 1
    step = 1
    while step <= max_consecutive:
        prev_date = weekend_date - timedelta(days=7 * step)
        if prev_date in existing_weekends:
            count += 1
            step += 1
        else:
            break
    step = 1
    while step <= max_consecutive:
        next_date = weekend_date + timedelta(days=7 * step)
        if next_date in existing_weekends:
            count += 1
            step += 1
        else:
            break
    return count > max_consecutive


def is_candidate_eligible_static(acolyte, slot, context, cache, exclude_acolyte_ids=None):
    if exclude_acolyte_ids and acolyte.id in exclude_acolyte_ids:
        return False
    if not cache["qualified_pairs"].get((acolyte.id, slot.position_type_id)):
        return False
    if not is_acolyte_available_with_rules(cache["rules_by_acolyte"].get(acolyte.id, []), slot.mass_instance):
        return False

    pool_mode = context.get("pool_mode")
    if pool_mode == "interested_only":
        if acolyte.id not in context.get("pool_ids", set()):
            return False
    elif pool_mode == "preferred_only":
        preferred = cache["preferred_communities"].get(acolyte.id, set())
        if slot.mass_instance.community_id not in preferred:
            return False
    elif pool_mode == "empty":
        return False
    return True


def is_candidate_eligible_dynamic(acolyte, slot, cache):
    weights = cache["weights"] or {}
    starts_at = slot.mass_instance.starts_at
    max_services_per_week = weights.get("max_services_per_week")
    max_consecutive_weekends = weights.get("max_consecutive_weekends")
    min_rest_minutes = getattr(slot.mass_instance.parish, "min_rest_minutes_between_masses", 0)
    mass_duration = getattr(slot.mass_instance.parish, "default_mass_duration_minutes", 60)
    min_gap = (int(min_rest_minutes) + int(mass_duration)) * 60

    assigned_times = cache["assignments_by_acolyte"].get(acolyte.id, [])
    if min_gap and assigned_times:
        for ts in assigned_times:
            if abs((ts - starts_at).total_seconds()) < min_gap:
                return False

    if max_services_per_week:
        week_key = starts_at.isocalendar()[:2]
        current_count = cache["weekly_counts_by_acolyte"].get(acolyte.id, {}).get(week_key, 0)
        if current_count + 1 > int(max_services_per_week):
            return False

    if max_consecutive_weekends:
        weekday = starts_at.weekday()
        weekend_date = None
        if weekday in (5, 6):
            weekend_date = starts_at.date()
            if weekday == 6:
                weekend_date = weekend_date - timedelta(days=1)
        if weekend_date:
            existing_weekends = cache["weekend_dates_by_acolyte"].get(acolyte.id, set())
            if _would_exceed_consecutive_weekends(existing_weekends, weekend_date, int(max_consecutive_weekends)):
                return False

    return True


def score_candidate(acolyte, slot, context, cache, local_eligible_count=0):
    weights = cache["weights"] or {}
    stats = cache["stats_map"].get(acolyte.id)
    preferences = cache["pref_by_acolyte"].get(acolyte.id, [])
    reliability_score = int(stats.reliability_score) if stats else 100
    services_last_30 = int(stats.services_last_30_days) if stats else 0
    credit_balance = int(stats.credit_balance or 0) if stats else 0

    breakdown = preference_score_breakdown(acolyte, slot.mass_instance, slot, preferences)
    community_factor = context.get("community_factor", 1.0)
    community_score = breakdown["community"] * community_factor
    base_score = breakdown["other"] + community_score

    home_bonus = int(_get_weight(weights, "home_community_bonus", 40) or 0)
    if acolyte.community_of_origin_id == slot.mass_instance.community_id:
        if slot.mass_instance.community_id not in cache["avoid_communities"].get(acolyte.id, set()):
            base_score += int(home_bonus * community_factor)

    scarcity_bonus = int(_get_weight(weights, "scarcity_bonus", 15) or 0)
    if local_eligible_count and local_eligible_count <= 2:
        if acolyte.community_of_origin_id == slot.mass_instance.community_id:
            if local_eligible_count == 1:
                base_score += scarcity_bonus
            else:
                base_score += int(round(scarcity_bonus / 2))

    community_recent_penalty = int(_get_weight(weights, "community_recent_penalty", 6) or 0)
    recent_window = int(_get_weight(weights, "community_recent_window_days", 30) or 0)
    if community_recent_penalty and recent_window > 0:
        window_start = slot.mass_instance.starts_at - timedelta(days=recent_window)
        window_end = slot.mass_instance.starts_at
        times = cache["community_assignment_times"].get((acolyte.id, slot.mass_instance.community_id), [])
        recent_count = _count_within_window(times, window_start, window_end)
        base_score -= community_recent_penalty * recent_count

    rotation_days = int(_get_weight(weights, "rotation_days", 60) or 0)
    rotation_penalty = int(_get_weight(weights, "rotation_penalty", 3) or 0)
    if rotation_days > 0 and rotation_penalty:
        window_start = slot.mass_instance.starts_at - timedelta(days=rotation_days)
        window_end = slot.mass_instance.starts_at
        rotation_times = cache["rotation_assignment_times"].get((acolyte.id, context.get("rotation_key")), [])
        if _has_recent_rotation(rotation_times, window_start, window_end):
            base_score -= rotation_penalty

    reserve_penalty = int(_get_weight(weights, "reserve_penalty", 1000) or 0)
    if acolyte.scheduling_mode == "reserve":
        base_score -= reserve_penalty

    credit_weight = int(_get_weight(weights, "credit_weight", 1) or 0)
    credit_cap = int(_get_weight(weights, "credit_cap", 10) or 10)
    if credit_weight:
        credit_bonus = min(max(credit_balance, 0), credit_cap)
        base_score += credit_weight * credit_bonus

    reliability_penalty = int(_get_weight(weights, "reliability_penalty", 0) or 0)
    if reliability_penalty:
        penalty = int(reliability_penalty * (100 - reliability_score) / 100)
        base_score -= penalty
    else:
        base_score += int(reliability_score / 25)

    base_score += max(0, 10 - int(services_last_30 / 2))

    return base_score


def rank_candidates(
    slot,
    parish,
    max_candidates=None,
    exclude_acolyte_ids=None,
    query=None,
    cache=None,
    include_meta=False,
    enforce_dynamic=True,
):
    exclude_acolyte_ids = exclude_acolyte_ids or set()
    cache = cache or build_recommendation_cache(parish, slots=[slot])
    context = get_mass_context(slot.mass_instance, cache["weights"], cache.get("interest_map"), cache["now"])

    eligible_acolytes = []
    for acolyte in cache["acolytes"]:
        if query and query.lower() not in acolyte.display_name.lower():
            continue
        if not is_candidate_eligible_static(acolyte, slot, context, cache, exclude_acolyte_ids=exclude_acolyte_ids):
            continue
        if enforce_dynamic and not is_candidate_eligible_dynamic(acolyte, slot, cache):
            continue
        eligible_acolytes.append(acolyte)

    local_eligible = [
        acolyte
        for acolyte in eligible_acolytes
        if acolyte.community_of_origin_id == slot.mass_instance.community_id
        and slot.mass_instance.community_id not in cache["avoid_communities"].get(acolyte.id, set())
    ]
    local_eligible_count = len(local_eligible)

    scored = []
    for acolyte in eligible_acolytes:
        score = score_candidate(acolyte, slot, context, cache, local_eligible_count=local_eligible_count)
        scored.append((score, acolyte))

    scored.sort(key=lambda item: item[0], reverse=True)
    if max_candidates:
        scored = scored[: int(max_candidates)]

    if not include_meta:
        return [acolyte for _score, acolyte in scored]

    results = []
    for score, acolyte in scored:
        reason = None
        if acolyte.community_of_origin_id == slot.mass_instance.community_id and context.get("community_factor", 1.0) > 0:
            reason = "Comunidade de origem"
        elif slot.mass_instance.community_id in cache["preferred_communities"].get(acolyte.id, set()):
            reason = "Preferencia por comunidade"
        elif acolyte.scheduling_mode == "reserve":
            reason = "Reserva tecnica"
        results.append({
            "acolyte": acolyte,
            "score": score,
            "reason": reason,
        })
    return results


def build_candidate_map(slots, parish, cache=None):
    cache = cache or build_recommendation_cache(parish, slots=slots)
    candidates = {}
    deferred = []
    for slot in slots:
        context = get_mass_context(slot.mass_instance, cache["weights"], cache.get("interest_map"), cache["now"])
        if context.get("pool_mode") == "empty" and context.get("candidate_pool") == "interested_only":
            deferred.append(slot)
            continue
        slot_candidates = []
        for acolyte in cache["acolytes"]:
            if not is_candidate_eligible_static(acolyte, slot, context, cache):
                continue
            slot_candidates.append(acolyte)
        candidates[slot.id] = slot_candidates
    return candidates, deferred, cache
