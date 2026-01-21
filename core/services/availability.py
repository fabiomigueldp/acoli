from collections import defaultdict

from core.models import AcolyteAvailabilityRule


def _is_valid_interval(rule):
    if rule.start_time and rule.end_time:
        return rule.start_time < rule.end_time
    return True


def _time_matches(rule, mass_time):
    if not rule.start_time and not rule.end_time:
        return True
    if rule.start_time and rule.end_time:
        if rule.start_time >= rule.end_time:
            return False
        return rule.start_time <= mass_time < rule.end_time
    if rule.start_time:
        return mass_time >= rule.start_time
    return mass_time < rule.end_time


def _rule_applies(rule, mass_instance):
    start = mass_instance.starts_at
    if rule.start_date and start.date() < rule.start_date:
        return False
    if rule.end_date and start.date() > rule.end_date:
        return False
    if rule.day_of_week is not None and start.weekday() != rule.day_of_week:
        return False
    if rule.community and rule.community_id != mass_instance.community_id:
        return False
    if not _time_matches(rule, start.time()):
        return False
    return True


def _split_rules(rules):
    unavailable_rules = []
    available_only_rules = []
    for rule in rules:
        if not _is_valid_interval(rule):
            continue
        if rule.rule_type == "unavailable":
            unavailable_rules.append(rule)
        elif rule.rule_type == "available_only":
            available_only_rules.append(rule)
    return unavailable_rules, available_only_rules


def group_rules_by_acolyte(rules):
    rules_by_acolyte = defaultdict(list)
    for rule in rules:
        rules_by_acolyte[rule.acolyte_id].append(rule)
    return rules_by_acolyte


def is_acolyte_available_with_rules(rules, mass_instance):
    unavailable_rules, available_only_rules = _split_rules(rules)

    if any(_rule_applies(rule, mass_instance) for rule in unavailable_rules):
        return False

    if available_only_rules:
        return any(_rule_applies(rule, mass_instance) for rule in available_only_rules)

    return True


def is_acolyte_available(acolyte, mass_instance):
    rules = list(AcolyteAvailabilityRule.objects.filter(acolyte=acolyte, parish=acolyte.parish))
    return is_acolyte_available_with_rules(rules, mass_instance)

