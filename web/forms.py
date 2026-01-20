from django import forms
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
        if parish:
            self.fields["default_community"].queryset = Community.objects.filter(parish=parish, active=True)
            self.fields["default_requirement_profile"].queryset = RequirementProfile.objects.filter(parish=parish, active=True)

    def clean(self):
        cleaned = super().clean()
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
        if parish:
            community_qs = Community.objects.filter(parish=parish, active=True)
            self.fields["community"].queryset = community_qs
            self.fields["move_to_community"].queryset = community_qs
            self.fields["requirement_profile"].queryset = RequirementProfile.objects.filter(parish=parish, active=True)


class MassInstanceUpdateForm(forms.ModelForm):
    class Meta:
        model = MassInstance
        fields = ["liturgy_label", "requirement_profile"]


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
        super().__init__(*args, **kwargs)
        self.fields["day_of_week"].choices = [("", "Qualquer dia")] + WEEKDAY_CHOICES


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
        super().__init__(*args, **kwargs)
        self.fields["weekday"].choices = [("", "Qualquer dia")] + WEEKDAY_CHOICES


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

