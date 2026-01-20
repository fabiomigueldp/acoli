from core.models import AcolytePreference, AcolyteQualification, AcolyteStats
from core.services.availability import is_acolyte_available
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
    return {
        "acolytes": acolytes,
        "qualified_by_position": qualified_by_position,
        "pref_by_acolyte": pref_by_acolyte,
        "stats_map": stats_map,
    }


def quick_fill_slot(slot, parish, max_candidates=3, cache=None):
    if cache:
        acolytes = cache.get("acolytes", [])
        qualified_ids = cache.get("qualified_by_position", {}).get(slot.position_type_id, set())
        pref_by_acolyte = cache.get("pref_by_acolyte", {})
        stats_map = cache.get("stats_map", {})
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

    scores = []
    for acolyte in acolytes:
        if acolyte.id not in qualified_ids:
            continue
        if not is_acolyte_available(acolyte, slot.mass_instance):
            continue
        score = preference_score(acolyte, slot.mass_instance, slot, pref_by_acolyte.get(acolyte.id, []))
        stats = stats_map.get(acolyte.id)
        if stats:
            score += max(0, 10 - int(stats.services_last_30_days / 2))
            score += int(stats.reliability_score / 25)
        scores.append((score, acolyte))

    scores.sort(key=lambda item: item[0], reverse=True)
    return [acolyte for _, acolyte in scores[:max_candidates]]

