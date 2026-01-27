from core.models import AcolyteAvailabilityRule, AcolytePreference, AcolyteQualification, AcolyteStats
from core.services.recommendations import build_recommendation_cache, rank_candidates


def build_quick_fill_cache(parish, position_type_ids=None, slots=None):
    cache = build_recommendation_cache(parish, slots=slots)
    if position_type_ids:
        filtered = {}
        for position_id, acolyte_ids in cache["qualified_by_position"].items():
            if position_id in position_type_ids:
                filtered[position_id] = acolyte_ids
        cache["qualified_by_position"] = filtered
        qualified_pairs = {}
        for position_id, acolyte_ids in filtered.items():
            for acolyte_id in acolyte_ids:
                qualified_pairs[(acolyte_id, position_id)] = True
        cache["qualified_pairs"] = qualified_pairs
    return cache


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
    exclude_acolyte_ids = exclude_acolyte_ids or set()
    return rank_candidates(
        slot,
        parish,
        max_candidates=max_candidates,
        exclude_acolyte_ids=exclude_acolyte_ids,
        cache=cache,
        enforce_dynamic=True,
    )

