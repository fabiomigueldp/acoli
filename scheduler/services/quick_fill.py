from core.models import AcolyteAvailabilityRule, AcolytePreference, AcolyteQualification, AcolyteStats
from core.services.availability import group_rules_by_acolyte, is_acolyte_available_with_rules
from core.services.preferences import preference_score


def build_quick_fill_cache(parish, position_type_ids=None):
    acolytes = list(parish.acolytes.filter(active=True))
    qualifications = AcolyteQualification.objects.filter(parish=parish, qualified=True)
    if position_type_ids:
        qualifications = qualifications.filter(position_type_id__in=position_type_ids)
    qualified_by_position = {}
    for qual in qualifications:
        if qual.qualified:
            qualified_by_position.setdefault(qual.position_type_id, set()).add(qual.acolyte_id)

    qualified_ids = set()
    for ids in qualified_by_position.values():
        qualified_ids.update(ids)

    preferences = AcolytePreference.objects.filter(parish=parish, acolyte_id__in=qualified_ids)
    pref_by_acolyte = {}
    for pref in preferences:
        pref_by_acolyte.setdefault(pref.acolyte_id, []).append(pref)

    stats_map = {stat.acolyte_id: stat for stat in AcolyteStats.objects.filter(parish=parish)}
    availability_rules = AcolyteAvailabilityRule.objects.filter(parish=parish, acolyte_id__in=qualified_ids)
    rules_by_acolyte = group_rules_by_acolyte(availability_rules)
    return {
        "acolytes": acolytes,
        "qualified_by_position": qualified_by_position,
        "pref_by_acolyte": pref_by_acolyte,
        "stats_map": stats_map,
        "rules_by_acolyte": rules_by_acolyte,
    }


def quick_fill_slot(slot, parish, max_candidates=3, cache=None, exclude_acolyte_ids=None):
    """
    Return a list of candidate acolytes for a slot, ranked by preference score.

    Args:
        slot: The AssignmentSlot to fill
        parish: The parish context
        max_candidates: Maximum number of candidates to return (default 3)
        cache: Optional pre-built cache from build_quick_fill_cache()
        exclude_acolyte_ids: Set of acolyte IDs to exclude (e.g., already assigned in this mass)
    """
    if exclude_acolyte_ids is None:
        exclude_acolyte_ids = set()

    if cache:
        acolytes = cache.get("acolytes", [])
        qualified_ids = cache.get("qualified_by_position", {}).get(slot.position_type_id, set())
        pref_by_acolyte = cache.get("pref_by_acolyte", {})
        stats_map = cache.get("stats_map", {})
        rules_by_acolyte = cache.get("rules_by_acolyte", {})
    else:
        acolytes = list(parish.acolytes.filter(active=True))
        qualifications = AcolyteQualification.objects.filter(
            parish=parish, qualified=True, position_type=slot.position_type
        )
        qualified_ids = set(qualifications.values_list("acolyte_id", flat=True))
        preferences = AcolytePreference.objects.filter(parish=parish, acolyte_id__in=qualified_ids)
        pref_by_acolyte = {}
        for pref in preferences:
            pref_by_acolyte.setdefault(pref.acolyte_id, []).append(pref)
        stats_map = {stat.acolyte_id: stat for stat in AcolyteStats.objects.filter(parish=parish)}
        availability_rules = AcolyteAvailabilityRule.objects.filter(parish=parish, acolyte_id__in=qualified_ids)
        rules_by_acolyte = group_rules_by_acolyte(availability_rules)

    reserve_penalty = int((parish.schedule_weights or {}).get("reserve_penalty", 1000))
    scores = []
    for acolyte in acolytes:
        if acolyte.id not in qualified_ids:
            continue
        if acolyte.id in exclude_acolyte_ids:
            continue
        if not is_acolyte_available_with_rules(rules_by_acolyte.get(acolyte.id, []), slot.mass_instance):
            continue
        score = preference_score(acolyte, slot.mass_instance, slot, pref_by_acolyte.get(acolyte.id, []))
        if acolyte.scheduling_mode == "reserve":
            score -= reserve_penalty
        stats = stats_map.get(acolyte.id)
        if stats:
            score += max(0, 10 - int(stats.services_last_30_days / 2))
            score += int(stats.reliability_score / 25)
        scores.append((score, acolyte))

    scores.sort(key=lambda item: item[0], reverse=True)
    return [acolyte for _, acolyte in scores[:max_candidates]]

