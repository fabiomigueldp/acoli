from django import forms

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
    RequirementProfile,
)


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

