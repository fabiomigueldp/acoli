def preference_score_breakdown(acolyte, mass_instance, slot, preferences):
    total = 0
    community_score = 0
    other_score = 0
    for pref in preferences:
        weight = pref.weight or 0
        if pref.preference_type == "preferred_community" and pref.target_community_id == mass_instance.community_id:
            total += weight
            community_score += weight
        elif pref.preference_type == "avoid_community" and pref.target_community_id == mass_instance.community_id:
            total -= weight
            community_score -= weight
        elif pref.preference_type == "preferred_position" and pref.target_position_id == slot.position_type_id:
            total += weight
            other_score += weight
        elif pref.preference_type == "avoid_position" and pref.target_position_id == slot.position_type_id:
            total -= weight
            other_score -= weight
        elif pref.target_function_id:
            if not hasattr(slot.position_type, "_function_id_cache"):
                slot.position_type._function_id_cache = {fn.id for fn in slot.position_type.functions.all()}
            has_function = pref.target_function_id in slot.position_type._function_id_cache
            if pref.preference_type == "preferred_function" and has_function:
                total += weight
                other_score += weight
            if pref.preference_type == "avoid_function" and has_function:
                total -= weight
                other_score -= weight
        elif pref.preference_type == "preferred_mass_template" and pref.target_template_id == mass_instance.template_id:
            total += weight
            other_score += weight
        elif pref.preference_type == "preferred_timeslot":
            if pref.weekday is not None and pref.weekday != mass_instance.starts_at.weekday():
                continue
            if pref.start_time and mass_instance.starts_at.time() < pref.start_time:
                continue
            if pref.end_time and mass_instance.starts_at.time() >= pref.end_time:
                continue
            total += weight
            other_score += weight
    return {
        "total": total,
        "community": community_score,
        "other": other_score,
    }


def preference_score(acolyte, mass_instance, slot, preferences):
    breakdown = preference_score_breakdown(acolyte, mass_instance, slot, preferences)
    return breakdown["total"]

