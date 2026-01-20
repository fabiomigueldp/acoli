from core.models import AcolyteAvailabilityRule


def is_acolyte_available(acolyte, mass_instance):
    rules = AcolyteAvailabilityRule.objects.filter(acolyte=acolyte, parish=acolyte.parish)
    start = mass_instance.starts_at
    for rule in rules:
        if rule.start_date and start.date() < rule.start_date:
            continue
        if rule.end_date and start.date() > rule.end_date:
            continue
        if rule.day_of_week is not None and start.weekday() != rule.day_of_week:
            continue
        if rule.community and rule.community_id != mass_instance.community_id:
            continue
        if rule.start_time and start.time() < rule.start_time:
            continue
        if rule.end_time and start.time() > rule.end_time:
            continue
        if rule.rule_type == "unavailable":
            return False
    available_only_rules = rules.filter(rule_type="available_only")
    if available_only_rules.exists():
        for rule in available_only_rules:
            if rule.day_of_week is not None and start.weekday() != rule.day_of_week:
                continue
            if rule.community and rule.community_id != mass_instance.community_id:
                continue
            if rule.start_time and start.time() < rule.start_time:
                continue
            if rule.end_time and start.time() > rule.end_time:
                continue
            return True
        return False
    return True

