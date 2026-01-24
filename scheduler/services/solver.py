from collections import defaultdict
from datetime import timedelta

from django.utils import timezone
from ortools.sat.python import cp_model

from django.db.models import F, Prefetch, Q

from core.models import (
    AcolyteAvailabilityRule,
    AcolyteIntent,
    AcolytePreference,
    AcolyteQualification,
    AcolyteStats,
    Assignment,
    AssignmentSlot,
    MassInterest,
)
from core.services.audit import log_audit
from django.db import transaction

from core.services.assignments import _assign_acolyte_to_slot_locked, _lock_slot, ConcurrentUpdateError, deactivate_assignment
from core.services.availability import group_rules_by_acolyte, is_acolyte_available_with_rules
from core.services.preferences import preference_score


class ScheduleSolveResult:
    def __init__(
        self,
        coverage,
        preference_score,
        fairness_std,
        changes,
        required_slots_count=0,
        unfilled_slots_count=0,
        unfilled_details=None,
        feasible=True,
    ):
        self.coverage = coverage
        self.preference_score = preference_score
        self.fairness_std = fairness_std
        self.changes = changes
        self.required_slots_count = required_slots_count
        self.unfilled_slots_count = unfilled_slots_count
        self.unfilled_details = unfilled_details or []
        self.feasible = feasible


def _ensure_slots(instances):
    for instance in instances:
        if not instance.requirement_profile:
            continue
        for position in instance.requirement_profile.positions.select_related("position_type").all():
            for idx in range(1, position.quantity + 1):
                AssignmentSlot.objects.get_or_create(
                    parish=instance.parish,
                    mass_instance=instance,
                    position_type=position.position_type,
                    slot_index=idx,
                    defaults={"required": True, "status": "open"},
                )


def _build_candidate_map(slots, acolytes, qualifications, interest_map=None):
    qualified_map = defaultdict(set)
    for qual in qualifications:
        if qual.qualified:
            qualified_map[(qual.acolyte_id, qual.position_type_id)] = True
    candidates = {}
    for slot in slots:
        slot_candidates = []
        event_series = slot.mass_instance.event_series
        pool_ids = None
        if event_series and getattr(event_series, "candidate_pool", "all") == "interested_only":
            pool_ids = interest_map.get(slot.mass_instance_id, set()) if interest_map else set()
        for acolyte in acolytes:
            if not qualified_map.get((acolyte.id, slot.position_type_id)):
                continue
            if pool_ids is not None and acolyte.id not in pool_ids:
                continue
            slot_candidates.append(acolyte)
        candidates[slot.id] = slot_candidates
    return candidates


def solve_schedule(parish, instances, consolidation_days, weights, allow_changes=False):
    instances = [instance for instance in instances if instance.status == "scheduled"]
    if not instances:
        return ScheduleSolveResult(coverage=0, preference_score=0, fairness_std=0, changes=0, feasible=False)

    _ensure_slots(instances)
    slots = list(
        AssignmentSlot.objects.filter(mass_instance__in=instances)
        .select_related("mass_instance", "mass_instance__event_series", "position_type")
        .prefetch_related(
            "position_type__functions",
            Prefetch("assignments", queryset=Assignment.objects.filter(is_active=True), to_attr="active_assignments"),
        )
    )
    if not slots:
        return ScheduleSolveResult(coverage=0, preference_score=0, fairness_std=0, changes=0, feasible=False)

    decision_slots = [slot for slot in slots if slot.required]
    if not decision_slots:
        return ScheduleSolveResult(
            coverage=0,
            preference_score=0,
            fairness_std=0,
            changes=0,
            required_slots_count=0,
            unfilled_slots_count=0,
            unfilled_details=[],
            feasible=True,
        )

    acolytes = list(parish.acolytes.filter(active=True))
    if not acolytes:
        return ScheduleSolveResult(coverage=0, preference_score=0, fairness_std=0, changes=0, feasible=False)
    reserve_ids = {acolyte.id for acolyte in acolytes if acolyte.scheduling_mode == "reserve"}

    qualifications = AcolyteQualification.objects.filter(parish=parish, qualified=True)
    preferences = AcolytePreference.objects.filter(parish=parish)
    intents = {intent.acolyte_id: intent for intent in AcolyteIntent.objects.filter(parish=parish)}
    stats = {stat.acolyte_id: stat for stat in AcolyteStats.objects.filter(parish=parish)}
    availability_rules = AcolyteAvailabilityRule.objects.filter(
        parish=parish, acolyte_id__in=[acolyte.id for acolyte in acolytes]
    )
    rules_by_acolyte = group_rules_by_acolyte(availability_rules)

    pref_by_acolyte = defaultdict(list)
    for pref in preferences:
        pref_by_acolyte[pref.acolyte_id].append(pref)

    interest_map = defaultdict(set)
    for interest in MassInterest.objects.filter(parish=parish, interested=True):
        interest_map[interest.mass_instance_id].add(interest.acolyte_id)
    candidates = _build_candidate_map(decision_slots, acolytes, qualifications, interest_map=interest_map)
    max_candidates = weights.get("max_candidates_per_slot")
    reserve_penalty = int(weights.get("reserve_penalty", 1000))
    try:
        max_candidates = int(max_candidates) if max_candidates else None
    except (TypeError, ValueError):
        max_candidates = None
    for slot in decision_slots:
        slot_candidates = []
        for acolyte in candidates.get(slot.id, []):
            rules = rules_by_acolyte.get(acolyte.id, [])
            if is_acolyte_available_with_rules(rules, slot.mass_instance):
                slot_candidates.append(acolyte)
        if max_candidates:
            scored = []
            for acolyte in slot_candidates:
                score = preference_score(
                    acolyte,
                    slot.mass_instance,
                    slot,
                    pref_by_acolyte.get(acolyte.id, []),
                )
                if acolyte.id in reserve_ids:
                    score -= reserve_penalty
                scored.append((score, acolyte))
            scored.sort(key=lambda item: item[0], reverse=True)
            slot_candidates = [acolyte for _score, acolyte in scored[: int(max_candidates)]]
        candidates[slot.id] = slot_candidates
    unfilled_details = []
    for slot in decision_slots:
        if not candidates.get(slot.id):
            unfilled_details.append(
                {
                    "mass_instance_id": slot.mass_instance_id,
                    "starts_at": slot.mass_instance.starts_at.isoformat(),
                    "community_id": slot.mass_instance.community_id,
                    "position_type_id": slot.position_type_id,
                    "slot_index": slot.slot_index,
                }
            )
    if unfilled_details:
        return ScheduleSolveResult(
            coverage=0,
            preference_score=0,
            fairness_std=0,
            changes=0,
            required_slots_count=len(decision_slots),
            unfilled_slots_count=len(unfilled_details),
            unfilled_details=unfilled_details,
            feasible=False,
        )

    model = cp_model.CpModel()
    x = {}

    for slot in decision_slots:
        for acolyte in candidates.get(slot.id, []):
            x[(slot.id, acolyte.id)] = model.NewBoolVar(f"x_{slot.id}_{acolyte.id}")

    for slot in decision_slots:
        vars_for_slot = [x[(slot.id, a.id)] for a in candidates.get(slot.id, [])]
        if not vars_for_slot:
            continue
        model.Add(sum(vars_for_slot) == 1)

    slots_by_mass = defaultdict(list)
    for slot in decision_slots:
        slots_by_mass[slot.mass_instance_id].append(slot)
    for mass_slots in slots_by_mass.values():
        for acolyte in acolytes:
            vars_for_acolyte = [x[(slot.id, acolyte.id)] for slot in mass_slots if (slot.id, acolyte.id) in x]
            if vars_for_acolyte:
                model.Add(sum(vars_for_acolyte) <= 1)

    senior_ids = {acolyte.id for acolyte in acolytes if acolyte.experience_level == "senior"}
    if senior_ids:
        for mass_id, mass_slots in slots_by_mass.items():
            min_senior = 0
            mass_instance = mass_slots[0].mass_instance if mass_slots else None
            if mass_instance and mass_instance.requirement_profile:
                min_senior = getattr(mass_instance.requirement_profile, "min_senior_per_mass", 0) or 0
            if min_senior:
                vars_for_senior = [
                    x[(slot.id, acolyte_id)]
                    for slot in mass_slots
                    for acolyte_id in senior_ids
                    if (slot.id, acolyte_id) in x
                ]
                model.Add(sum(vars_for_senior) >= int(min_senior))

    mass_duration = int(getattr(parish, "default_mass_duration_minutes", 60))
    rest_minutes = int(getattr(parish, "min_rest_minutes_between_masses", 0))
    min_gap = (mass_duration + rest_minutes) * 60
    sorted_slots = sorted(decision_slots, key=lambda slot: slot.mass_instance.starts_at)
    conflict_pairs = []
    for idx, slot_a in enumerate(sorted_slots):
        a_start = slot_a.mass_instance.starts_at
        for slot_b in sorted_slots[idx + 1 :]:
            if slot_a.mass_instance_id == slot_b.mass_instance_id:
                continue
            b_start = slot_b.mass_instance.starts_at
            if (b_start - a_start).total_seconds() >= min_gap:
                break
            conflict_pairs.append((slot_a, slot_b))
    for acolyte in acolytes:
        for slot_a, slot_b in conflict_pairs:
            if (slot_a.id, acolyte.id) in x and (slot_b.id, acolyte.id) in x:
                model.Add(x[(slot_a.id, acolyte.id)] + x[(slot_b.id, acolyte.id)] <= 1)

    max_services_per_week = weights.get("max_services_per_week")
    if max_services_per_week:
        slots_by_week = defaultdict(list)
        for slot in decision_slots:
            key = slot.mass_instance.starts_at.isocalendar()[:2]
            slots_by_week[key].append(slot)
        for week_slots in slots_by_week.values():
            for acolyte in acolytes:
                vars_for_week = [x[(slot.id, acolyte.id)] for slot in week_slots if (slot.id, acolyte.id) in x]
                if vars_for_week:
                    model.Add(sum(vars_for_week) <= int(max_services_per_week))

    max_consecutive_weekends = weights.get("max_consecutive_weekends")
    if max_consecutive_weekends:
        weekend_map = defaultdict(list)
        for slot in decision_slots:
            weekday = slot.mass_instance.starts_at.weekday()
            if weekday in (5, 6):
                date = slot.mass_instance.starts_at.date()
                if weekday == 6:
                    date = date - timedelta(days=1)
                weekend_map[date].append(slot)
        weekend_keys = sorted(weekend_map.keys())
        if weekend_keys:
            weekend_vars = {acolyte.id: {} for acolyte in acolytes}
            for acolyte in acolytes:
                for weekend_date, weekend_slots in weekend_map.items():
                    vars_for_weekend = [x[(slot.id, acolyte.id)] for slot in weekend_slots if (slot.id, acolyte.id) in x]
                    if not vars_for_weekend:
                        continue
                    weekend_var = model.NewBoolVar(f"weekend_{acolyte.id}_{weekend_date}")
                    for var in vars_for_weekend:
                        model.Add(weekend_var >= var)
                    model.Add(weekend_var <= sum(vars_for_weekend))
                    weekend_vars[acolyte.id][weekend_date] = weekend_var
            window_size = int(max_consecutive_weekends) + 1
            for acolyte in acolytes:
                for idx in range(0, max(0, len(weekend_keys) - window_size + 1)):
                    window = weekend_keys[idx : idx + window_size]
                    vars_in_window = [weekend_vars[acolyte.id].get(key) for key in window if key in weekend_vars[acolyte.id]]
                    if vars_in_window:
                        model.Add(sum(vars_in_window) <= int(max_consecutive_weekends))

    consolidation_limit = timezone.now() + timedelta(days=consolidation_days)
    locked_slots = []
    for slot in decision_slots:
        if slot.mass_instance.starts_at <= consolidation_limit and slot.is_locked:
            locked_slots.append(slot)
    for slot in locked_slots:
        existing_assignment = slot.get_active_assignment()
        if existing_assignment:
            for acolyte in candidates.get(slot.id, []):
                value = 1 if acolyte.id == existing_assignment.acolyte_id else 0
                if (slot.id, acolyte.id) in x:
                    model.Add(x[(slot.id, acolyte.id)] == value)

    preference_terms = []
    fairness_terms = []
    stability_terms = []
    rotation_terms = []
    partner_terms = []
    family_terms = []

    for slot in decision_slots:
        for acolyte in candidates.get(slot.id, []):
            base_score = preference_score(acolyte, slot.mass_instance, slot, pref_by_acolyte.get(acolyte.id, []))
            if acolyte.id in reserve_ids:
                base_score -= reserve_penalty
            credit_balance = stats.get(acolyte.id).credit_balance if stats.get(acolyte.id) else 0
            credit_cap = int(weights.get("credit_cap", 10))
            credit_bonus = min(max(credit_balance, 0), credit_cap)
            base_score += int(weights.get("credit_weight", 1)) * credit_bonus

            reliability_weight = int(weights.get("reliability_penalty", 0))
            if reliability_weight and stats.get(acolyte.id):
                reliability_score = stats[acolyte.id].reliability_score
                penalty = int(reliability_weight * (100 - reliability_score) / 100)
                base_score -= penalty

            preference_terms.append(base_score * x[(slot.id, acolyte.id)])

            existing_assignment = slot.get_active_assignment()
            if existing_assignment and existing_assignment.assignment_state in ["published", "locked"]:
                if not allow_changes:
                    penalty = weights.get("stability_penalty", 10)
                    if acolyte.id != existing_assignment.acolyte_id:
                        stability_terms.append(penalty * x[(slot.id, acolyte.id)])

    partner_prefs = [pref for pref in preferences if pref.preference_type in ["preferred_partner", "avoid_partner"] and pref.target_acolyte_id]
    if partner_prefs:
        partner_ids = set()
        for pref in partner_prefs:
            partner_ids.add(pref.acolyte_id)
            partner_ids.add(pref.target_acolyte_id)
        mass_acolyte_vars = defaultdict(dict)
        for mass_id, mass_slots in slots_by_mass.items():
            for acolyte_id in partner_ids:
                vars_for_acolyte = [x[(slot.id, acolyte_id)] for slot in mass_slots if (slot.id, acolyte_id) in x]
                if not vars_for_acolyte:
                    continue
                assigned_var = model.NewBoolVar(f"mass_{mass_id}_a_{acolyte_id}")
                for var in vars_for_acolyte:
                    model.Add(assigned_var >= var)
                model.Add(assigned_var <= sum(vars_for_acolyte))
                mass_acolyte_vars[mass_id][acolyte_id] = assigned_var
        for pref in partner_prefs:
            for mass_id in slots_by_mass.keys():
                a_var = mass_acolyte_vars.get(mass_id, {}).get(pref.acolyte_id)
                b_var = mass_acolyte_vars.get(mass_id, {}).get(pref.target_acolyte_id)
                if a_var is None or b_var is None:
                    continue
                pair_var = model.NewBoolVar(f"pair_{pref.acolyte_id}_{pref.target_acolyte_id}_{mass_id}")
                model.Add(pair_var <= a_var)
                model.Add(pair_var <= b_var)
                model.Add(pair_var >= a_var + b_var - 1)
                weight = pref.weight or 0
                if pref.preference_type == "avoid_partner":
                    weight = -weight
                partner_terms.append(weight * pair_var)

    family_bonus = int(weights.get("family_group_bonus", 2))
    if family_bonus:
        families = defaultdict(list)
        for acolyte in acolytes:
            if acolyte.family_group_id:
                families[acolyte.family_group_id].append(acolyte.id)
        for family_id, members in families.items():
            if len(members) < 2:
                continue
            for mass_id, mass_slots in slots_by_mass.items():
                member_vars = {}
                for acolyte_id in members:
                    vars_for_acolyte = [
                        x[(slot.id, acolyte_id)]
                        for slot in mass_slots
                        if (slot.id, acolyte_id) in x
                    ]
                    if not vars_for_acolyte:
                        continue
                    assigned_var = model.NewBoolVar(f"family_{family_id}_{mass_id}_{acolyte_id}")
                    for var in vars_for_acolyte:
                        model.Add(assigned_var >= var)
                    model.Add(assigned_var <= sum(vars_for_acolyte))
                    member_vars[acolyte_id] = assigned_var
                member_ids = list(member_vars.keys())
                for idx, acolyte_id in enumerate(member_ids):
                    for other_id in member_ids[idx + 1 :]:
                        pair_var = model.NewBoolVar(f"family_pair_{family_id}_{mass_id}_{acolyte_id}_{other_id}")
                        model.Add(pair_var <= member_vars[acolyte_id])
                        model.Add(pair_var <= member_vars[other_id])
                        model.Add(pair_var >= member_vars[acolyte_id] + member_vars[other_id] - 1)
                        family_terms.append(family_bonus * pair_var)

    rotation_days = int(weights.get("rotation_days", 60))
    if rotation_days > 0:
        cutoff = timezone.now() - timedelta(days=rotation_days)
        recent_pairs = set(
            Assignment.objects.filter(
                parish=parish,
                assignment_state__in=["published", "locked"],
                slot__mass_instance__starts_at__gte=cutoff,
            )
            .filter(created_at__lte=F("slot__mass_instance__starts_at"))
            .filter(Q(ended_at__isnull=True) | Q(ended_at__gte=F("slot__mass_instance__starts_at")))
            .values_list("acolyte_id", "slot__position_type_id")
        )
        for slot in decision_slots:
            for acolyte in candidates.get(slot.id, []):
                if (acolyte.id, slot.position_type_id) in recent_pairs:
                    rotation_terms.append(int(weights.get("rotation_penalty", 3)) * x[(slot.id, acolyte.id)])

    horizon_days = 30
    if decision_slots:
        starts = [slot.mass_instance.starts_at for slot in decision_slots]
        horizon_days = max((max(starts) - min(starts)).days, 1)

    if acolytes:
        base_target = len(decision_slots) / len(acolytes)
        raw_targets = {}
        for acolyte in acolytes:
            intent = intents.get(acolyte.id)
            if acolyte.id in reserve_ids:
                raw_target = 0
            elif intent and intent.desired_frequency_per_month:
                raw_target = intent.desired_frequency_per_month * (horizon_days / 30)
            else:
                level = getattr(intent, "willingness_level", "normal") if intent else "normal"
                factor = {"low": 0.8, "normal": 1.0, "high": 1.2}.get(level, 1.0)
                raw_target = base_target * factor
            raw_targets[acolyte.id] = raw_target
        total_raw = sum(raw_targets.values()) or 1
        scale = len(decision_slots) / total_raw
        target_loads = {acolyte_id: int(round(raw * scale)) for acolyte_id, raw in raw_targets.items()}

        for acolyte in acolytes:
            vars_for_acolyte = [x[(slot.id, acolyte.id)] for slot in decision_slots if (slot.id, acolyte.id) in x]
            if vars_for_acolyte:
                target = target_loads.get(acolyte.id, int(round(base_target)))
                diff = model.NewIntVar(-len(decision_slots), len(decision_slots), f"diff_{acolyte.id}")
                model.Add(diff == sum(vars_for_acolyte) - target)
                abs_diff = model.NewIntVar(0, len(decision_slots), f"abs_diff_{acolyte.id}")
                model.AddAbsEquality(abs_diff, diff)
                fairness_terms.append(abs_diff)

    objective_terms = []
    if preference_terms:
        objective_terms.append(sum(preference_terms))
    if fairness_terms:
        objective_terms.append(-weights.get("fairness_penalty", 1) * sum(fairness_terms))
    if stability_terms:
        objective_terms.append(-sum(stability_terms))
    if rotation_terms:
        objective_terms.append(-sum(rotation_terms))
    if partner_terms:
        objective_terms.append(sum(partner_terms))
    if family_terms:
        objective_terms.append(sum(family_terms))

    model.Maximize(sum(objective_terms) if objective_terms else 0)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = float(weights.get("max_solve_seconds", 15))
    status = solver.Solve(model)

    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return ScheduleSolveResult(
            coverage=0,
            preference_score=0,
            fairness_std=0,
            changes=0,
            required_slots_count=len(decision_slots),
            unfilled_slots_count=len(unfilled_details),
            unfilled_details=unfilled_details,
            feasible=False,
        )

    changes = 0
    coverage = 0
    preference_total = 0
    assignment_counts = []

    for slot in decision_slots:
        assigned_acolyte = None
        for acolyte in candidates.get(slot.id, []):
            if solver.Value(x[(slot.id, acolyte.id)]) == 1:
                assigned_acolyte = acolyte
                score = preference_score(acolyte, slot.mass_instance, slot, pref_by_acolyte.get(acolyte.id, []))
                preference_total += score
                break
        if assigned_acolyte:
            coverage += 1
            try:
                with transaction.atomic():
                    locked_slot = _lock_slot(slot.id)
                    existing = locked_slot.get_active_assignment()
                    desired_state = existing.assignment_state if existing else "proposed"
                    if not existing:
                        _assign_acolyte_to_slot_locked(
                            locked_slot,
                            assigned_acolyte,
                            assignment_state=desired_state,
                            end_reason="replaced_by_solver",
                        )
                        locked_slot.status = "finalized" if locked_slot.is_locked else "assigned"
                        locked_slot.save(update_fields=["status", "updated_at"])
                        changes += 1
                    elif existing.acolyte_id != assigned_acolyte.id:
                        deactivate_assignment(existing, "replaced_by_solver", actor=None)
                        _assign_acolyte_to_slot_locked(
                            locked_slot,
                            assigned_acolyte,
                            assignment_state=desired_state,
                            end_reason="replaced_by_solver",
                        )
                        locked_slot.status = "finalized" if locked_slot.is_locked else "assigned"
                        locked_slot.save(update_fields=["status", "updated_at"])
                        changes += 1
            except (ConcurrentUpdateError, ValueError):
                continue

    for acolyte in acolytes:
        count = 0
        for slot in decision_slots:
            key = (slot.id, acolyte.id)
            if key in x and solver.Value(x[key]) == 1:
                count += 1
        assignment_counts.append(count)
    if assignment_counts:
        mean = sum(assignment_counts) / len(assignment_counts)
        variance = sum((value - mean) ** 2 for value in assignment_counts) / len(assignment_counts)
        fairness_std = variance ** 0.5
    else:
        fairness_std = 0
    return ScheduleSolveResult(
        coverage=coverage,
        preference_score=preference_total,
        fairness_std=fairness_std,
        changes=changes,
        required_slots_count=len(decision_slots),
        unfilled_slots_count=0,
        unfilled_details=[],
        feasible=True,
    )
