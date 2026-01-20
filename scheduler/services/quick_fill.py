from core.models import AcolytePreference, AcolyteQualification
from core.services.availability import is_acolyte_available
from core.services.preferences import preference_score


def quick_fill_slot(slot, parish, max_candidates=3):
    acolytes = parish.acolytes.filter(active=True)
    qualifications = AcolyteQualification.objects.filter(parish=parish, qualified=True, position_type=slot.position_type)
    qualified_ids = set(qualifications.values_list("acolyte_id", flat=True))
    preferences = AcolytePreference.objects.filter(parish=parish, acolyte_id__in=qualified_ids)
    pref_by_acolyte = {}
    for pref in preferences:
        pref_by_acolyte.setdefault(pref.acolyte_id, []).append(pref)

    scores = []
    for acolyte in acolytes:
        if acolyte.id not in qualified_ids:
            continue
        if not is_acolyte_available(acolyte, slot.mass_instance):
            continue
        score = preference_score(acolyte, slot.mass_instance, slot, pref_by_acolyte.get(acolyte.id, []))
        scores.append((score, acolyte))

    scores.sort(key=lambda item: item[0], reverse=True)
    return [acolyte for _, acolyte in scores[:max_candidates]]

