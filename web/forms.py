from datetime import time as dt_time
import re

from django import forms
from django.forms import inlineformset_factory
from django.forms.models import BaseInlineFormSet
from django.db.models import Q
from django.contrib.auth import get_user_model

from core.models import (
    AcolyteAvailabilityRule,
    AcolyteCreditLedger,
    AcolyteIntent,
    AcolytePreference,
    AcolyteProfile,
    AcolyteQualification,
    AssignmentSlot,
    Community,
    EventOccurrence,
    EventSeries,
    FamilyGroup,
    FunctionType,
    MassTemplate,
    MassInstance,
    MembershipRole,
    ParishMembership,
    RequirementProfile,
    RequirementProfilePosition,
    PositionType,
)
from notifications.models import NotificationPreference

import json

WEEKDAY_CHOICES = [
    (0, "Seg"),
    (1, "Ter"),
    (2, "Qua"),
    (3, "Qui"),
    (4, "Sex"),
    (5, "Sab"),
    (6, "Dom"),
]


def normalize_code(value):
    if not value:
        return ""
    value = value.strip().upper().replace(" ", "_")
    value = re.sub(r"[^A-Z0-9_]", "", value)
    return value


def generate_unique_code(parish, name, model, max_len, exclude_id=None):
    base = normalize_code(name) or "ROLE"
    base = base[:max_len]
    code = base
    suffix = 1
    qs = model.objects.filter(parish=parish)
    if exclude_id:
        qs = qs.exclude(id=exclude_id)
    while qs.filter(code=code).exists():
        tail = str(suffix)
        code = f"{base[: max_len - len(tail)]}{tail}"
        suffix += 1
    return code


class MassTemplateForm(forms.ModelForm):
    class Meta:
        model = MassTemplate
        fields = ["title", "community", "weekday", "time", "rrule_text", "default_requirement_profile", "notes", "active"]

    def __init__(self, *args, **kwargs):
        self.parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)
        if self.parish:
            self.fields["community"].queryset = Community.objects.filter(parish=self.parish, active=True)
            self.fields["default_requirement_profile"].queryset = RequirementProfile.objects.filter(
                parish=self.parish, active=True
            )

    def clean(self):
        cleaned = super().clean()
        if not self.parish:
            return cleaned
        community = cleaned.get("community")
        profile = cleaned.get("default_requirement_profile")
        if community and community.parish_id != self.parish.id:
            self.add_error("community", "Selecione uma comunidade valida para esta paroquia.")
        if profile and profile.parish_id != self.parish.id:
            self.add_error("default_requirement_profile", "Selecione um perfil valido para esta paroquia.")
        return cleaned


class EventSeriesBasicsForm(forms.Form):
    SERIES_TYPE_CHOICES = [
        ("Solenidade", "Solenidade"),
        ("Festa", "Festa"),
        ("Memoria", "Memoria"),
        ("Triduo", "Triduo"),
        ("Novena", "Novena"),
        ("Oitava", "Oitava"),
        ("Missa especial", "Missa especial"),
        ("Outro", "Outro"),
    ]

    series_type = forms.ChoiceField(choices=SERIES_TYPE_CHOICES)
    series_type_other = forms.CharField(max_length=40, required=False)
    title = forms.CharField(max_length=200)
    start_date = forms.DateField(
        widget=forms.DateInput(attrs={"type": "date"}, format="%Y-%m-%d"),
        input_formats=["%Y-%m-%d"],
    )
    end_date = forms.DateField(
        widget=forms.DateInput(attrs={"type": "date"}, format="%Y-%m-%d"),
        input_formats=["%Y-%m-%d"],
    )
    default_time = forms.TimeField(
        widget=forms.TimeInput(attrs={"type": "time"}, format="%H:%M"),
        input_formats=["%H:%M"],
    )
    candidate_pool = forms.ChoiceField(
        choices=[("all", "Todos os acolitos"), ("interested_only", "Somente interessados (opt-in)")]
    )
    interest_deadline_at = forms.DateTimeField(
        required=False,
        widget=forms.DateTimeInput(attrs={"type": "datetime-local"}, format="%Y-%m-%dT%H:%M"),
        input_formats=["%Y-%m-%dT%H:%M"],
    )
    default_community = forms.ModelChoiceField(queryset=Community.objects.none(), required=False)
    default_requirement_profile = forms.ModelChoiceField(queryset=RequirementProfile.objects.none(), required=False)

    def __init__(self, *args, **kwargs):
        parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)
        self.parish = parish
        if parish:
            self.fields["default_community"].queryset = Community.objects.filter(parish=parish, active=True)
            self.fields["default_requirement_profile"].queryset = RequirementProfile.objects.filter(parish=parish, active=True)

    def clean(self):
        cleaned = super().clean()
        if self.parish:
            community = cleaned.get("default_community")
            profile = cleaned.get("default_requirement_profile")
            if community and community.parish_id != self.parish.id:
                self.add_error("default_community", "Selecione uma comunidade valida para esta paroquia.")
            if profile and profile.parish_id != self.parish.id:
                self.add_error("default_requirement_profile", "Selecione um perfil valido para esta paroquia.")
        start_date = cleaned.get("start_date")
        end_date = cleaned.get("end_date")
        if start_date and end_date and end_date < start_date:
            self.add_error("end_date", "Data final deve ser maior ou igual a data inicial.")
        if cleaned.get("series_type") == "Outro":
            other = (cleaned.get("series_type_other") or "").strip()
            if not other:
                self.add_error("series_type_other", "Informe o tipo da celebracao.")
            else:
                cleaned["series_type"] = other
        if cleaned.get("candidate_pool") != "interested_only":
            cleaned["interest_deadline_at"] = None
        return cleaned


class EventOccurrenceForm(forms.Form):
    date = forms.DateField(widget=forms.HiddenInput)
    time = forms.TimeField(widget=forms.TimeInput(attrs={"type": "time"}))
    community = forms.ModelChoiceField(queryset=Community.objects.none())
    requirement_profile = forms.ModelChoiceField(queryset=RequirementProfile.objects.none(), required=False)
    label = forms.CharField(max_length=200, required=False)
    conflict_action = forms.ChoiceField(choices=EventOccurrence.CONFLICT_CHOICES)
    move_to_date = forms.DateField(required=False, widget=forms.DateInput(attrs={"type": "date"}))
    move_to_time = forms.TimeField(required=False, widget=forms.TimeInput(attrs={"type": "time"}))
    move_to_community = forms.ModelChoiceField(queryset=Community.objects.none(), required=False)

    def __init__(self, *args, **kwargs):
        parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)
        self.parish = parish
        if parish:
            community_qs = Community.objects.filter(parish=parish, active=True)
            self.fields["community"].queryset = community_qs
            self.fields["move_to_community"].queryset = community_qs
            self.fields["requirement_profile"].queryset = RequirementProfile.objects.filter(parish=parish, active=True)

    def clean(self):
        cleaned = super().clean()
        if not self.parish:
            return cleaned
        community = cleaned.get("community")
        move_to_community = cleaned.get("move_to_community")
        profile = cleaned.get("requirement_profile")
        if community and community.parish_id != self.parish.id:
            self.add_error("community", "Selecione uma comunidade valida para esta paroquia.")
        if move_to_community and move_to_community.parish_id != self.parish.id:
            self.add_error("move_to_community", "Selecione uma comunidade valida para esta paroquia.")
        if profile and profile.parish_id != self.parish.id:
            self.add_error("requirement_profile", "Selecione um perfil valido para esta paroquia.")
        conflict_action = cleaned.get("conflict_action")
        move_to_date = cleaned.get("move_to_date")
        move_to_time = cleaned.get("move_to_time")
        if conflict_action == "move_existing":
            if not move_to_date:
                self.add_error("move_to_date", "Informe a data para mover a missa.")
            if not move_to_time:
                self.add_error("move_to_time", "Informe o horario para mover a missa.")
            if not move_to_community:
                self.add_error("move_to_community", "Informe a comunidade para mover a missa.")
        else:
            cleaned["move_to_date"] = None
            cleaned["move_to_time"] = None
            cleaned["move_to_community"] = None
        return cleaned


class MassInstanceUpdateForm(forms.ModelForm):
    class Meta:
        model = MassInstance
        fields = ["liturgy_label", "requirement_profile"]

    def __init__(self, *args, **kwargs):
        parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)
        if parish:
            self.fields["requirement_profile"].queryset = RequirementProfile.objects.filter(parish=parish, active=True)


class MassInstanceMoveForm(forms.Form):
    starts_at = forms.DateTimeField(
        input_formats=["%Y-%m-%dT%H:%M"],
        widget=forms.DateTimeInput(attrs={"type": "datetime-local"}),
    )
    community = forms.ModelChoiceField(queryset=Community.objects.none())

    def __init__(self, *args, **kwargs):
        parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)
        if parish:
            self.fields["community"].queryset = Community.objects.filter(parish=parish, active=True)


class MassInstanceCancelForm(forms.Form):
    reason = forms.CharField(max_length=200, required=False)


class AcolyteAvailabilityRuleForm(forms.ModelForm):
    class Meta:
        model = AcolyteAvailabilityRule
        fields = [
            "rule_type",
            "day_of_week",
            "start_time",
            "end_time",
            "community",
            "start_date",
            "end_date",
            "notes",
        ]
        widgets = {
            "start_date": forms.DateInput(attrs={"type": "date"}),
            "end_date": forms.DateInput(attrs={"type": "date"}),
            "start_time": forms.TimeInput(attrs={"type": "time"}),
            "end_time": forms.TimeInput(attrs={"type": "time"}),
        }

    def __init__(self, *args, **kwargs):
        self.parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)
        self.fields["day_of_week"].choices = [("", "Qualquer dia")] + WEEKDAY_CHOICES
        if self.parish:
            self.fields["community"].queryset = Community.objects.filter(parish=self.parish, active=True)

    def clean_day_of_week(self):
        value = self.cleaned_data.get("day_of_week")
        if value in ("", None):
            return None
        return value

    def clean(self):
        cleaned = super().clean()
        if self.parish:
            community = cleaned.get("community")
            if community and community.parish_id != self.parish.id:
                self.add_error("community", "Selecione uma comunidade valida para esta paroquia.")
        return cleaned


class WeeklyAvailabilityForm(forms.ModelForm):
    class Meta:
        model = AcolyteAvailabilityRule
        fields = ["rule_type", "day_of_week", "start_time", "end_time", "community", "notes"]
        widgets = {
            "start_time": forms.TimeInput(attrs={"type": "time"}),
            "end_time": forms.TimeInput(attrs={"type": "time"}),
        }

    def __init__(self, *args, **kwargs):
        self.acolyte = kwargs.pop("acolyte", None)
        super().__init__(*args, **kwargs)
        self.fields["day_of_week"].choices = [("", "Qualquer dia")] + WEEKDAY_CHOICES
        self.fields["day_of_week"].required = False
        self.fields["rule_type"].choices = [
            ("unavailable", "Indisponivel"),
            ("available_only", "Disponivel apenas"),
        ]
        if self.acolyte:
            self.fields["community"].queryset = Community.objects.filter(parish=self.acolyte.parish, active=True)

    def clean(self):
        cleaned = super().clean()
        day_of_week = cleaned.get("day_of_week")
        start_time = cleaned.get("start_time")
        end_time = cleaned.get("end_time")
        if day_of_week in ("", None):
            cleaned["day_of_week"] = None
        if start_time and end_time and start_time >= end_time:
            self.add_error("end_time", "Horario final deve ser depois do inicio.")
        if self.acolyte and not self.errors:
            community = cleaned.get("community")
            if community and community.parish_id != self.acolyte.parish_id:
                self.add_error("community", "Selecione uma comunidade valida para esta paroquia.")
                return cleaned
            base_filters = {
                "parish": self.acolyte.parish,
                "acolyte": self.acolyte,
                "rule_type": cleaned.get("rule_type"),
                "community": cleaned.get("community"),
            }
            day_value = cleaned.get("day_of_week")
            if day_value is None:
                day_filter = Q()
                duplicate_filter = Q(day_of_week__isnull=True)
            else:
                day_filter = Q(day_of_week=day_value) | Q(day_of_week__isnull=True)
                duplicate_filter = Q(day_of_week=day_value)

            duplicates = AcolyteAvailabilityRule.objects.filter(
                parish=self.acolyte.parish,
                acolyte=self.acolyte,
                rule_type=cleaned.get("rule_type"),
                community=cleaned.get("community"),
                start_time=cleaned.get("start_time"),
                end_time=cleaned.get("end_time"),
            ).filter(duplicate_filter)
            if self.instance and self.instance.pk:
                duplicates = duplicates.exclude(pk=self.instance.pk)
            if duplicates.exists():
                self.add_error(None, "Voce ja possui uma regra igual.")
                return cleaned

            qs = AcolyteAvailabilityRule.objects.filter(**base_filters).filter(day_filter)
            if self.instance and self.instance.pk:
                qs = qs.exclude(pk=self.instance.pk)
            new_start = cleaned.get("start_time") or dt_time.min
            new_end = cleaned.get("end_time") or dt_time.max
            for rule in qs:
                existing_start = rule.start_time or dt_time.min
                existing_end = rule.end_time or dt_time.max
                if rule.start_time and rule.end_time and rule.start_time >= rule.end_time:
                    continue
                if existing_start < new_end and new_start < existing_end:
                    self.add_error(None, "Ja existe uma regra semelhante nesse dia/horario.")
                    break
        return cleaned


class DateAbsenceForm(forms.ModelForm):
    class Meta:
        model = AcolyteAvailabilityRule
        fields = ["start_date", "end_date", "notes"]
        widgets = {
            "start_date": forms.DateInput(attrs={"type": "date"}),
            "end_date": forms.DateInput(attrs={"type": "date"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["start_date"].required = True

    def clean(self):
        cleaned = super().clean()
        start_date = cleaned.get("start_date")
        end_date = cleaned.get("end_date")
        if not start_date:
            self.add_error("start_date", "Informe a data de inicio.")
            return cleaned
        if not end_date:
            cleaned["end_date"] = start_date
            end_date = start_date
        if end_date and start_date and end_date < start_date:
            self.add_error("end_date", "Data final deve ser maior ou igual a data inicial.")
        return cleaned


class AcolytePreferenceForm(forms.ModelForm):
    class Meta:
        model = AcolytePreference
        fields = [
            "preference_type",
            "target_community",
            "target_position",
            "target_function",
            "target_template",
            "target_acolyte",
            "weekday",
            "start_time",
            "end_time",
            "weight",
        ]
        widgets = {
            "start_time": forms.TimeInput(attrs={"type": "time"}),
            "end_time": forms.TimeInput(attrs={"type": "time"}),
        }

    def __init__(self, *args, **kwargs):
        self.parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)
        self.fields["weekday"].choices = [("", "Qualquer dia")] + WEEKDAY_CHOICES
        self.fields["preference_type"].choices = [
            ("preferred_community", "Preferir comunidade"),
            ("avoid_community", "Evitar comunidade"),
            ("preferred_timeslot", "Preferir horario"),
            ("preferred_mass_template", "Preferir modelo"),
            ("preferred_position", "Preferir posicao"),
            ("avoid_position", "Evitar posicao"),
            ("preferred_function", "Preferir funcao"),
            ("avoid_function", "Evitar funcao"),
            ("preferred_partner", "Preferir parceiro"),
            ("avoid_partner", "Evitar parceiro"),
        ]
        if self.parish:
            self.fields["target_community"].queryset = Community.objects.filter(parish=self.parish, active=True)
            self.fields["target_position"].queryset = self.parish.positiontype_set.filter(active=True)
            self.fields["target_function"].queryset = self.parish.functiontype_set.filter(active=True)
            self.fields["target_template"].queryset = self.parish.masstemplate_set.filter(active=True)
            self.fields["target_acolyte"].queryset = self.parish.acolytes.filter(active=True)

    def clean_weekday(self):
        value = self.cleaned_data.get("weekday")
        if value in ("", None):
            return None
        return value

    def clean(self):
        cleaned = super().clean()
        pref_type = cleaned.get("preference_type")
        if not pref_type:
            return cleaned

        weight = cleaned.get("weight")
        if weight in (None, ""):
            cleaned["weight"] = 50

        target_fields = {
            "target_community",
            "target_position",
            "target_function",
            "target_template",
            "target_acolyte",
        }
        required_target = {
            "preferred_community": "target_community",
            "avoid_community": "target_community",
            "preferred_position": "target_position",
            "avoid_position": "target_position",
            "preferred_function": "target_function",
            "avoid_function": "target_function",
            "preferred_mass_template": "target_template",
            "preferred_partner": "target_acolyte",
            "avoid_partner": "target_acolyte",
        }
        allowed_targets = set()
        if pref_type in required_target:
            allowed_targets.add(required_target[pref_type])

        for field in target_fields - allowed_targets:
            cleaned[field] = None

        if pref_type == "preferred_timeslot":
            weekday = cleaned.get("weekday")
            start_time = cleaned.get("start_time")
            end_time = cleaned.get("end_time")
            if not weekday and not start_time and not end_time:
                self.add_error(None, "Informe dia ou horario.")
            if start_time and end_time and start_time >= end_time:
                self.add_error("end_time", "Horario final deve ser depois do inicio.")
        else:
            cleaned["weekday"] = None
            cleaned["start_time"] = None
            cleaned["end_time"] = None

        required_field = required_target.get(pref_type)
        if required_field and not cleaned.get(required_field):
            self.add_error(required_field, "Campo obrigatorio.")

        if self.parish:
            target_community = cleaned.get("target_community")
            target_position = cleaned.get("target_position")
            target_function = cleaned.get("target_function")
            target_template = cleaned.get("target_template")
            target_acolyte = cleaned.get("target_acolyte")
            if target_community and target_community.parish_id != self.parish.id:
                self.add_error("target_community", "Selecione uma comunidade valida para esta paroquia.")
            if target_position and target_position.parish_id != self.parish.id:
                self.add_error("target_position", "Selecione uma posicao valida para esta paroquia.")
            if target_function and target_function.parish_id != self.parish.id:
                self.add_error("target_function", "Selecione uma funcao valida para esta paroquia.")
            if target_template and target_template.parish_id != self.parish.id:
                self.add_error("target_template", "Selecione um modelo valido para esta paroquia.")
            if target_acolyte and target_acolyte.parish_id != self.parish.id:
                self.add_error("target_acolyte", "Selecione um acolito valido para esta paroquia.")

        return cleaned


class SwapRequestForm(forms.Form):
    swap_type = forms.ChoiceField(choices=[("acolyte_swap", "Trocar acolito"), ("role_swap", "Trocar funcao")])
    target_acolyte = forms.ModelChoiceField(queryset=AcolyteProfile.objects.none(), required=False)
    to_slot = forms.ModelChoiceField(queryset=AssignmentSlot.objects.none(), required=False)
    notes = forms.CharField(required=False, max_length=200)

    def __init__(self, *args, **kwargs):
        acolytes_qs = kwargs.pop("acolytes_qs", None)
        slots_qs = kwargs.pop("slots_qs", None)
        super().__init__(*args, **kwargs)
        if acolytes_qs is not None:
            self.fields["target_acolyte"].queryset = acolytes_qs
        if slots_qs is not None:
            self.fields["to_slot"].queryset = slots_qs


class SwapAssignForm(forms.Form):
    target_acolyte = forms.ModelChoiceField(queryset=AcolyteProfile.objects.none())

    def __init__(self, *args, **kwargs):
        acolytes_qs = kwargs.pop("acolytes_qs", None)
        super().__init__(*args, **kwargs)
        if acolytes_qs is not None:
            self.fields["target_acolyte"].queryset = acolytes_qs


class ParishSettingsForm(forms.Form):
    DEFAULT_SCHEDULE_WEIGHTS = {
        "stability_penalty": 10,
        "fairness_penalty": 1,
        "credit_weight": 1,
        "max_solve_seconds": 15,
        "rotation_days": 60,
        "rotation_penalty": 3,
        "reserve_penalty": 1000,
        "home_community_bonus": 40,
        "community_recent_penalty": 6,
        "community_recent_window_days": 30,
        "scarcity_bonus": 15,
        "event_series_community_factor": 0.4,
        "single_mass_community_policy": "recurring",
        "interest_deadline_hours": 48,
        "interested_pool_fallback": "relax_to_all",
    }
    consolidation_days = forms.IntegerField(min_value=7, max_value=90)
    horizon_days = forms.IntegerField(min_value=30, max_value=90)
    default_mass_duration_minutes = forms.IntegerField(min_value=45, max_value=120)
    min_rest_minutes_between_masses = forms.IntegerField(min_value=0, max_value=60)
    swap_requires_approval = forms.BooleanField(required=False)
    notify_on_cancellation = forms.BooleanField(required=False)
    auto_assign_on_decline = forms.BooleanField(required=False)
    stability_penalty = forms.IntegerField(min_value=0, max_value=100, required=False)
    fairness_penalty = forms.IntegerField(min_value=0, max_value=100, required=False)
    credit_weight = forms.IntegerField(min_value=0, max_value=50, required=False)
    max_solve_seconds = forms.IntegerField(min_value=5, max_value=60, required=False)
    max_services_per_week = forms.IntegerField(min_value=0, max_value=10, required=False)
    max_consecutive_weekends = forms.IntegerField(min_value=0, max_value=10, required=False)
    rotation_penalty = forms.IntegerField(min_value=0, max_value=20, required=False)
    rotation_days = forms.IntegerField(min_value=0, max_value=120, required=False)
    reserve_penalty = forms.IntegerField(min_value=0, max_value=10000, required=False)
    home_community_bonus = forms.IntegerField(min_value=0, max_value=200, required=False)
    community_recent_penalty = forms.IntegerField(min_value=0, max_value=50, required=False)
    community_recent_window_days = forms.IntegerField(min_value=0, max_value=180, required=False)
    scarcity_bonus = forms.IntegerField(min_value=0, max_value=100, required=False)
    event_series_community_factor = forms.FloatField(min_value=0, max_value=1, required=False)
    single_mass_community_policy = forms.ChoiceField(
        choices=[
            ("recurring", "Tratar como recorrente"),
            ("special", "Tratar como evento"),
        ],
        required=False,
    )
    interest_deadline_hours = forms.IntegerField(min_value=0, max_value=168, required=False)
    interested_pool_fallback = forms.ChoiceField(
        choices=[
            ("relax_to_all", "Abrir para todos"),
            ("strict", "Manter fechado"),
            ("relax_to_preferred", "Somente preferidos"),
        ],
        required=False,
    )
    claim_auto_approve_enabled = forms.BooleanField(required=False)
    claim_auto_approve_hours = forms.IntegerField(min_value=0, max_value=168, required=False)
    claim_require_coordination = forms.BooleanField(required=False)
    schedule_weights_json = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 4}))

    def __init__(self, *args, **kwargs):
        parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)
        if parish:
            weights = parish.schedule_weights or {}
            self.fields["consolidation_days"].initial = parish.consolidation_days
            self.fields["horizon_days"].initial = parish.horizon_days
            self.fields["default_mass_duration_minutes"].initial = parish.default_mass_duration_minutes
            self.fields["min_rest_minutes_between_masses"].initial = parish.min_rest_minutes_between_masses
            self.fields["swap_requires_approval"].initial = parish.swap_requires_approval
            self.fields["notify_on_cancellation"].initial = parish.notify_on_cancellation
            self.fields["auto_assign_on_decline"].initial = parish.auto_assign_on_decline
            self.fields["claim_auto_approve_enabled"].initial = parish.claim_auto_approve_enabled
            self.fields["claim_auto_approve_hours"].initial = parish.claim_auto_approve_hours
            self.fields["claim_require_coordination"].initial = parish.claim_require_coordination
            self.fields["stability_penalty"].initial = weights.get("stability_penalty", 10)
            self.fields["fairness_penalty"].initial = weights.get("fairness_penalty", 1)
            self.fields["credit_weight"].initial = weights.get("credit_weight", 1)
            self.fields["max_solve_seconds"].initial = weights.get("max_solve_seconds", 15)
            self.fields["max_services_per_week"].initial = weights.get("max_services_per_week")
            self.fields["max_consecutive_weekends"].initial = weights.get("max_consecutive_weekends")
            self.fields["rotation_penalty"].initial = weights.get("rotation_penalty", 3)
            self.fields["rotation_days"].initial = weights.get("rotation_days", 60)
            self.fields["reserve_penalty"].initial = weights.get("reserve_penalty", 1000)
            self.fields["home_community_bonus"].initial = weights.get("home_community_bonus", 40)
            self.fields["community_recent_penalty"].initial = weights.get("community_recent_penalty", 6)
            self.fields["community_recent_window_days"].initial = weights.get("community_recent_window_days", 30)
            self.fields["scarcity_bonus"].initial = weights.get("scarcity_bonus", 15)
            self.fields["event_series_community_factor"].initial = weights.get("event_series_community_factor", 0.4)
            self.fields["single_mass_community_policy"].initial = weights.get("single_mass_community_policy", "recurring")
            self.fields["interest_deadline_hours"].initial = weights.get("interest_deadline_hours", 48)
            self.fields["interested_pool_fallback"].initial = weights.get("interested_pool_fallback", "relax_to_all")
            self.fields["schedule_weights_json"].initial = json.dumps(weights, ensure_ascii=True, indent=2)

    def clean(self):
        cleaned = super().clean()
        consolidation = cleaned.get("consolidation_days")
        horizon = cleaned.get("horizon_days")
        if consolidation and horizon and horizon < consolidation:
            self.add_error("horizon_days", "O horizonte precisa ser maior ou igual a consolidacao.")
        raw_json = cleaned.get("schedule_weights_json")
        if raw_json:
            try:
                cleaned["schedule_weights"] = json.loads(raw_json)
            except json.JSONDecodeError:
                self.add_error("schedule_weights_json", "JSON invalido.")
        return cleaned

    def save(self, parish, actor=None):
        parish.consolidation_days = self.cleaned_data["consolidation_days"]
        parish.horizon_days = self.cleaned_data["horizon_days"]
        parish.default_mass_duration_minutes = self.cleaned_data["default_mass_duration_minutes"]
        parish.min_rest_minutes_between_masses = self.cleaned_data["min_rest_minutes_between_masses"]
        parish.swap_requires_approval = self.cleaned_data["swap_requires_approval"]
        parish.notify_on_cancellation = self.cleaned_data["notify_on_cancellation"]
        parish.auto_assign_on_decline = self.cleaned_data["auto_assign_on_decline"]
        parish.claim_auto_approve_enabled = self.cleaned_data.get("claim_auto_approve_enabled") or False
        if self.cleaned_data.get("claim_auto_approve_hours") is not None:
            parish.claim_auto_approve_hours = self.cleaned_data.get("claim_auto_approve_hours")
        parish.claim_require_coordination = self.cleaned_data.get("claim_require_coordination") or False

        weights = parish.schedule_weights or {}
        if "schedule_weights" in self.cleaned_data:
            weights = self.cleaned_data["schedule_weights"]
        else:
            for key in [
                "stability_penalty",
                "fairness_penalty",
                "credit_weight",
                "max_solve_seconds",
                "max_services_per_week",
                "max_consecutive_weekends",
                "rotation_penalty",
                "rotation_days",
                "reserve_penalty",
                "home_community_bonus",
                "community_recent_penalty",
                "community_recent_window_days",
                "scarcity_bonus",
                "event_series_community_factor",
                "single_mass_community_policy",
                "interest_deadline_hours",
                "interested_pool_fallback",
            ]:
                if self.cleaned_data.get(key) is not None:
                    value = self.cleaned_data[key]
                    if key == "event_series_community_factor":
                        weights[key] = float(value)
                    elif key in {"single_mass_community_policy", "interested_pool_fallback"}:
                        weights[key] = value
                    else:
                        weights[key] = int(value)
        parish.schedule_weights = weights
        parish.save(
            update_fields=[
                "consolidation_days",
                "horizon_days",
                "default_mass_duration_minutes",
                "min_rest_minutes_between_masses",
                "swap_requires_approval",
                "notify_on_cancellation",
                "auto_assign_on_decline",
                "claim_auto_approve_enabled",
                "claim_auto_approve_hours",
                "claim_require_coordination",
                "schedule_weights",
                "updated_at",
            ]
        )
        return parish


class CommunityForm(forms.ModelForm):
    class Meta:
        model = Community
        fields = ["code", "name", "address", "active"]

    def __init__(self, *args, **kwargs):
        self.parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)

    def clean_code(self):
        value = normalize_code(self.cleaned_data.get("code"))
        if not value:
            raise forms.ValidationError("Informe um codigo.")
        if self.parish:
            qs = Community.objects.filter(parish=self.parish, code=value)
            if self.instance and self.instance.pk:
                qs = qs.exclude(pk=self.instance.pk)
            if qs.exists():
                raise forms.ValidationError("Codigo ja utilizado nesta paroquia.")
        return value


class RoleForm(forms.Form):
    name = forms.CharField(max_length=100)
    code = forms.CharField(max_length=20, required=False)
    active = forms.BooleanField(required=False, initial=True)
    extra_functions = forms.ModelMultipleChoiceField(
        queryset=FunctionType.objects.none(),
        required=False,
        widget=forms.CheckboxSelectMultiple(
            attrs={
                "class": "peer h-4 w-4 shrink-0 rounded border-slate-300 text-emerald-600 focus:ring-emerald-500 p-0 mt-0.5",
            }
        ),
        help_text="Marque as funcoes secundarias necessarias para esta vaga.",
    )

    def __init__(self, *args, **kwargs):
        self.parish = kwargs.pop("parish", None)
        self.position_type = kwargs.pop("position_type", None)
        super().__init__(*args, **kwargs)
        primary = None
        if self.position_type:
            primary = FunctionType.objects.filter(
                parish=self.position_type.parish, code=self.position_type.code
            ).first()
        if self.parish:
            qs = FunctionType.objects.filter(parish=self.parish, active=True)
            if primary:
                qs = qs.exclude(id=primary.id)
            self.fields["extra_functions"].queryset = qs
        if self.position_type:
            self.fields["name"].initial = self.position_type.name
            self.fields["code"].initial = self.position_type.code
            self.fields["active"].initial = self.position_type.active
            extras = self.position_type.functions.exclude(id=primary.id) if primary else self.position_type.functions.all()
            self.fields["extra_functions"].initial = extras

    def clean_code(self):
        value = normalize_code(self.cleaned_data.get("code"))
        if not value:
            return value
        if len(value) > 10:
            raise forms.ValidationError("Use ate 10 caracteres.")
        if self.parish:
            qs = PositionType.objects.filter(parish=self.parish, code=value)
            if self.position_type:
                qs = qs.exclude(pk=self.position_type.pk)
            if qs.exists():
                raise forms.ValidationError("Codigo ja utilizado por outra funcao na escala.")
        return value


class RequirementProfileForm(forms.ModelForm):
    class Meta:
        model = RequirementProfile
        fields = ["name", "notes", "min_senior_per_mass", "active"]


class RequirementProfilePositionForm(forms.ModelForm):
    class Meta:
        model = RequirementProfilePosition
        fields = ["position_type", "quantity"]

    def __init__(self, *args, **kwargs):
        parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)
        self.fields["quantity"].min_value = 1
        if parish:
            self.fields["position_type"].queryset = PositionType.objects.filter(parish=parish, active=True)


class BaseRequirementProfilePositionFormSet(BaseInlineFormSet):
    def __init__(self, *args, **kwargs):
        self.parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)

    def _construct_form(self, i, **kwargs):
        if self.parish:
            kwargs["parish"] = self.parish
        return super()._construct_form(i, **kwargs)

    def clean(self):
        super().clean()
        seen = set()
        for form in self.forms:
            if not hasattr(form, "cleaned_data"):
                continue
            if form.cleaned_data.get("DELETE"):
                continue
            position = form.cleaned_data.get("position_type")
            if not position:
                continue
            if position.id in seen:
                raise forms.ValidationError("Nao repita a mesma funcao no perfil.")
            seen.add(position.id)


RequirementProfilePositionFormSet = inlineformset_factory(
    RequirementProfile,
    RequirementProfilePosition,
    form=RequirementProfilePositionForm,
    formset=BaseRequirementProfilePositionFormSet,
    extra=1,
    can_delete=True,
)


class AcolyteLinkForm(forms.Form):
    ALLOWED_ROLE_CODES = ["PARISH_ADMIN", "ACOLYTE_COORDINATOR", "PASTOR", "SECRETARY", "ACOLYTE"]
    email = forms.EmailField()
    full_name = forms.CharField(required=False)
    password = forms.CharField(required=False, widget=forms.PasswordInput)
    acolyte = forms.ModelChoiceField(queryset=AcolyteProfile.objects.none())
    roles = forms.ModelMultipleChoiceField(queryset=MembershipRole.objects.none(), required=False)

    def __init__(self, *args, **kwargs):
        parish = kwargs.pop("parish", None)
        actor = kwargs.pop("actor", None)
        super().__init__(*args, **kwargs)
        if parish:
            self.fields["acolyte"].queryset = AcolyteProfile.objects.filter(parish=parish, active=True)
            roles_qs = MembershipRole.objects.filter(code__in=self.ALLOWED_ROLE_CODES).order_by("code")
            if actor and getattr(actor, "is_system_admin", False):
                self.fields["roles"].queryset = roles_qs
            else:
                self.fields["roles"].queryset = roles_qs

    def clean(self):
        cleaned = super().clean()
        email = cleaned.get("email")
        full_name = cleaned.get("full_name")
        password = cleaned.get("password")
        if not email:
            return cleaned
        User = get_user_model()
        user_exists = User.objects.filter(email=email).exists()
        if not user_exists and (not full_name or not password):
            raise forms.ValidationError("Informe nome e senha para criar o usuario.")
        return cleaned


class AcolyteCreateLoginForm(forms.Form):
    email = forms.EmailField()
    password = forms.CharField(widget=forms.PasswordInput)
    send_invite = forms.BooleanField(required=False)

    def clean_email(self):
        email = self.cleaned_data['email']
        User = get_user_model()
        if User.objects.filter(email=email).exists():
            raise forms.ValidationError("Este email ja esta em uso.")
        return email


class ReplacementResolveForm(forms.Form):
    resolution_type = forms.ChoiceField(
        choices=[
            ("mass_canceled", "Missa cancelada"),
            ("slot_not_required", "Funcao nao necessaria"),
            ("covered_externally", "Coberto externamente"),
            ("other", "Outro motivo"),
        ]
    )
    notes = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}))
    confirm_cancel_mass = forms.BooleanField(required=False)


class PeopleUserForm(forms.ModelForm):
    class Meta:
        model = get_user_model()
        fields = ["full_name", "email", "phone", "is_active"]

    def clean_email(self):
        email = self.cleaned_data.get("email")
        if not email:
            return email
        qs = get_user_model().objects.filter(email=email)
        if self.instance:
            qs = qs.exclude(id=self.instance.id)
        if qs.exists():
            raise forms.ValidationError("Email ja utilizado por outro usuario.")
        return email


class PeopleMembershipForm(forms.Form):
    ALLOWED_ROLE_CODES = AcolyteLinkForm.ALLOWED_ROLE_CODES
    active = forms.BooleanField(required=False)
    roles = forms.ModelMultipleChoiceField(
        queryset=MembershipRole.objects.none(),
        required=False,
        widget=forms.CheckboxSelectMultiple,
    )

    def __init__(self, *args, **kwargs):
        parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)
        roles_qs = MembershipRole.objects.filter(code__in=self.ALLOWED_ROLE_CODES).order_by("code")
        self.fields["roles"].queryset = roles_qs
        if parish is None:
            return


class PeopleAcolyteForm(forms.ModelForm):
    class Meta:
        model = AcolyteProfile
        fields = [
            "display_name",
            "community_of_origin",
            "experience_level",
            "scheduling_mode",
            "family_group",
            "notes",
            "active",
        ]

    def __init__(self, *args, **kwargs):
        parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)
        if parish:
            self.fields["community_of_origin"].queryset = Community.objects.filter(parish=parish, active=True)
            self.fields["family_group"].queryset = FamilyGroup.objects.filter(parish=parish)


class PeopleQualificationsForm(forms.Form):
    qualifications = forms.ModelMultipleChoiceField(
        queryset=PositionType.objects.none(),
        required=False,
        widget=forms.CheckboxSelectMultiple,
    )

    def __init__(self, *args, **kwargs):
        parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)
        if parish:
            self.fields["qualifications"].queryset = PositionType.objects.filter(parish=parish, active=True)


class PeopleCreateForm(forms.Form):
    ALLOWED_ROLE_CODES = AcolyteLinkForm.ALLOWED_ROLE_CODES

    full_name = forms.CharField()
    phone = forms.CharField(required=False)
    email = forms.EmailField(required=False)
    has_login = forms.BooleanField(required=False)
    password = forms.CharField(required=False, widget=forms.PasswordInput)
    send_invite = forms.BooleanField(required=False)

    is_acolyte = forms.BooleanField(required=False)
    community_of_origin = forms.ModelChoiceField(queryset=Community.objects.none(), required=False)
    experience_level = forms.ChoiceField(choices=AcolyteProfile.EXPERIENCE_CHOICES, required=False)
    scheduling_mode = forms.ChoiceField(choices=AcolyteProfile.SCHEDULING_MODE_CHOICES, required=False)
    family_group = forms.ModelChoiceField(queryset=FamilyGroup.objects.none(), required=False)
    notes = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}))
    acolyte_active = forms.BooleanField(required=False, initial=True)

    has_admin_access = forms.BooleanField(required=False)
    roles = forms.ModelMultipleChoiceField(
        queryset=MembershipRole.objects.none(),
        required=False,
        widget=forms.CheckboxSelectMultiple,
    )
    qualifications = forms.ModelMultipleChoiceField(
        queryset=PositionType.objects.none(),
        required=False,
        widget=forms.CheckboxSelectMultiple,
    )

    def __init__(self, *args, **kwargs):
        parish = kwargs.pop("parish", None)
        super().__init__(*args, **kwargs)
        if parish:
            self.fields["community_of_origin"].queryset = Community.objects.filter(parish=parish, active=True)
            self.fields["family_group"].queryset = FamilyGroup.objects.filter(parish=parish)
            self.fields["roles"].queryset = MembershipRole.objects.filter(
                code__in=self.ALLOWED_ROLE_CODES
            ).order_by("code")
            self.fields["qualifications"].queryset = PositionType.objects.filter(parish=parish, active=True)

    def clean(self):
        cleaned = super().clean()
        has_login = cleaned.get("has_login")
        email = cleaned.get("email")
        password = cleaned.get("password")
        send_invite = cleaned.get("send_invite")
        is_acolyte = cleaned.get("is_acolyte")
        community = cleaned.get("community_of_origin")
        experience = cleaned.get("experience_level")
        has_admin_access = cleaned.get("has_admin_access")
        roles = cleaned.get("roles")

        if has_login and not email:
            self.add_error("email", "Informe o email para criar acesso.")
        if has_login and not password and not send_invite:
            self.add_error("password", "Defina uma senha ou marque envio de convite.")
        if is_acolyte:
            if not community:
                self.add_error("community_of_origin", "Selecione a comunidade de origem.")
            if not experience:
                self.add_error("experience_level", "Defina o nivel de experiencia.")
            if not cleaned.get("scheduling_mode"):
                cleaned["scheduling_mode"] = "normal"
        if has_admin_access and not roles:
            self.add_error("roles", "Selecione pelo menos um papel.")
        if not has_login:
            cleaned["email"] = ""
            cleaned["password"] = ""
        if not has_admin_access:
            cleaned["roles"] = []
        if not is_acolyte:
            cleaned["qualifications"] = []
        return cleaned


class AcolyteIntentForm(forms.ModelForm):
    """Form to edit an acolyte's intent (desired frequency and willingness level)."""
    
    class Meta:
        model = AcolyteIntent
        fields = ["desired_frequency_per_month", "willingness_level"]
        widgets = {
            "desired_frequency_per_month": forms.NumberInput(attrs={"min": 0, "max": 12}),
            "willingness_level": forms.Select(),
        }
        labels = {
            "desired_frequency_per_month": "Frequencia desejada (por mes)",
            "willingness_level": "Nivel de disponibilidade",
        }

    def __init__(self, *args, **kwargs):
        self.parish = kwargs.pop("parish", None)
        self.acolyte = kwargs.pop("acolyte", None)
        super().__init__(*args, **kwargs)


class CreditAdjustmentForm(forms.Form):
    """Form to manually adjust an acolyte's credit balance."""
    
    delta = forms.IntegerField(
        label="Valor do ajuste",
        help_text="Use valores positivos para adicionar creditos, negativos para remover.",
        widget=forms.NumberInput(attrs={"class": "input", "placeholder": "Ex: 10 ou -5"}),
    )
    notes = forms.CharField(
        label="Justificativa",
        required=True,
        widget=forms.Textarea(attrs={"rows": 3, "class": "input", "placeholder": "Motivo do ajuste..."}),
    )

    def __init__(self, *args, **kwargs):
        self.parish = kwargs.pop("parish", None)
        self.acolyte = kwargs.pop("acolyte", None)
        super().__init__(*args, **kwargs)

    def clean_delta(self):
        delta = self.cleaned_data.get("delta")
        if delta == 0:
            raise forms.ValidationError("O valor do ajuste deve ser diferente de zero.")
        return delta


class NotificationPreferenceForm(forms.ModelForm):
    """Form to edit notification preferences for a user."""
    
    class Meta:
        model = NotificationPreference
        fields = ["email_enabled", "email_digest", "whatsapp_enabled"]
        labels = {
            "email_enabled": "Receber notificacoes por email",
            "email_digest": "Receber resumo diario (em vez de notificacoes individuais)",
            "whatsapp_enabled": "Receber notificacoes por WhatsApp",
        }

    def __init__(self, *args, **kwargs):
        self.parish = kwargs.pop("parish", None)
        self.user = kwargs.pop("user", None)
        super().__init__(*args, **kwargs)


class AssignToSlotForm(forms.Form):
    """Form to assign an acolyte to a specific slot."""
    
    slot = forms.ModelChoiceField(
        queryset=AssignmentSlot.objects.none(),
        label="Slot disponivel",
        widget=forms.Select(attrs={"class": "custom-select"}),
    )
    notes = forms.CharField(
        label="Observacoes",
        required=False,
        widget=forms.Textarea(attrs={"rows": 2, "class": "input"}),
    )

    def __init__(self, *args, **kwargs):
        self.parish = kwargs.pop("parish", None)
        self.acolyte = kwargs.pop("acolyte", None)
        super().__init__(*args, **kwargs)
        if self.parish and self.acolyte:
            # Get open slots for positions the acolyte is qualified for
            qualified_positions = AcolyteQualification.objects.filter(
                parish=self.parish, acolyte=self.acolyte, qualified=True
            ).values_list("position_type_id", flat=True)
            self.fields["slot"].queryset = AssignmentSlot.objects.filter(
                parish=self.parish,
                status="open",
                position_type_id__in=qualified_positions,
            ).select_related(
                "mass_instance", "position_type", "mass_instance__community"
            ).order_by("mass_instance__starts_at")

