from datetime import time as dt_time

from django import forms
from django.db.models import Q
from django.contrib.auth import get_user_model

from core.models import (
    AcolyteAvailabilityRule,
    AcolytePreference,
    AcolyteProfile,
    AssignmentSlot,
    Community,
    EventOccurrence,
    EventSeries,
    MassTemplate,
    MassInstance,
    MembershipRole,
    RequirementProfile,
)

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
    series_type = forms.CharField(max_length=40)
    title = forms.CharField(max_length=200)
    start_date = forms.DateField(widget=forms.DateInput(attrs={"type": "date"}))
    end_date = forms.DateField(widget=forms.DateInput(attrs={"type": "date"}))
    default_time = forms.TimeField(widget=forms.TimeInput(attrs={"type": "time"}))
    candidate_pool = forms.ChoiceField(
        choices=[("all", "Todos os acolitos"), ("interested_only", "Somente interessados")]
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
    }
    consolidation_days = forms.IntegerField(min_value=7, max_value=21)
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
            self.fields["stability_penalty"].initial = weights.get("stability_penalty", 10)
            self.fields["fairness_penalty"].initial = weights.get("fairness_penalty", 1)
            self.fields["credit_weight"].initial = weights.get("credit_weight", 1)
            self.fields["max_solve_seconds"].initial = weights.get("max_solve_seconds", 15)
            self.fields["max_services_per_week"].initial = weights.get("max_services_per_week")
            self.fields["max_consecutive_weekends"].initial = weights.get("max_consecutive_weekends")
            self.fields["rotation_penalty"].initial = weights.get("rotation_penalty", 3)
            self.fields["rotation_days"].initial = weights.get("rotation_days", 60)
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
            ]:
                if self.cleaned_data.get(key) is not None:
                    weights[key] = int(self.cleaned_data[key])
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
                "schedule_weights",
                "updated_at",
            ]
        )
        return parish


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

