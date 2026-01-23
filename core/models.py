from django.conf import settings
from django.db import models
from django.db.models import Q


class TimeStampedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class Parish(TimeStampedModel):
    name = models.CharField(max_length=200)
    city = models.CharField(max_length=100, blank=True)
    state = models.CharField(max_length=50, blank=True)
    timezone = models.CharField(max_length=64, default="America/Sao_Paulo")
    contact_email = models.EmailField(blank=True)
    contact_phone = models.CharField(max_length=30, blank=True)
    consolidation_days = models.PositiveIntegerField(default=14)
    horizon_days = models.PositiveIntegerField(default=60)
    default_mass_duration_minutes = models.PositiveIntegerField(default=60)
    min_rest_minutes_between_masses = models.PositiveIntegerField(default=0)
    schedule_weights = models.JSONField(default=dict, blank=True)
    swap_requires_approval = models.BooleanField(default=True)
    notify_on_cancellation = models.BooleanField(default=True)
    auto_assign_on_decline = models.BooleanField(default=False)

    def __str__(self):
        return self.name


class Community(TimeStampedModel):
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    code = models.CharField(max_length=10)
    name = models.CharField(max_length=200)
    address = models.TextField(blank=True)
    active = models.BooleanField(default=True)

    class Meta:
        unique_together = ("parish", "code")

    def __str__(self):
        return f"{self.code} - {self.name}"


class MembershipRole(models.Model):
    code = models.CharField(max_length=40, unique=True)
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name


class ParishMembership(TimeStampedModel):
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    roles = models.ManyToManyField(MembershipRole, blank=True)
    active = models.BooleanField(default=True)

    class Meta:
        unique_together = ("parish", "user")

    def __str__(self):
        return f"{self.user} @ {self.parish}"

    def has_role(self, role_code):
        return self.roles.filter(code=role_code).exists()


class FamilyGroup(TimeStampedModel):
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    name = models.CharField(max_length=120)
    notes = models.TextField(blank=True)

    def __str__(self):
        return self.name


class AcolyteProfile(TimeStampedModel):
    EXPERIENCE_CHOICES = [
        ("beginner", "Iniciante"),
        ("intermediate", "Intermediario"),
        ("senior", "Senior"),
    ]
    SCHEDULING_MODE_CHOICES = [
        ("normal", "Normal"),
        ("reserve", "Reserva tecnica"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE, related_name="acolytes")
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    display_name = models.CharField(max_length=200)
    community_of_origin = models.ForeignKey(Community, on_delete=models.SET_NULL, null=True, blank=True)
    family_group = models.ForeignKey(FamilyGroup, on_delete=models.SET_NULL, null=True, blank=True)
    notes = models.TextField(blank=True)
    active = models.BooleanField(default=True)
    experience_level = models.CharField(max_length=20, choices=EXPERIENCE_CHOICES, default="intermediate")
    scheduling_mode = models.CharField(max_length=20, choices=SCHEDULING_MODE_CHOICES, default="normal")

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["parish", "user"],
                condition=Q(user__isnull=False),
                name="unique_acolyte_user_per_parish",
            )
        ]

    def __str__(self):
        return self.display_name


class AcolyteIntent(TimeStampedModel):
    WILLINGNESS_CHOICES = [
        ("low", "Baixo"),
        ("normal", "Normal"),
        ("high", "Alto"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    acolyte = models.OneToOneField(AcolyteProfile, on_delete=models.CASCADE)
    desired_frequency_per_month = models.PositiveIntegerField(null=True, blank=True)
    willingness_level = models.CharField(max_length=10, choices=WILLINGNESS_CHOICES, default="normal")


class AcolyteAvailabilityRule(TimeStampedModel):
    RULE_CHOICES = [
        ("unavailable", "Indisponivel"),
        ("available_only", "Disponivel apenas"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    acolyte = models.ForeignKey(AcolyteProfile, on_delete=models.CASCADE)
    rule_type = models.CharField(max_length=20, choices=RULE_CHOICES)
    day_of_week = models.PositiveSmallIntegerField(null=True, blank=True)
    start_time = models.TimeField(null=True, blank=True)
    end_time = models.TimeField(null=True, blank=True)
    community = models.ForeignKey(Community, on_delete=models.SET_NULL, null=True, blank=True)
    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)
    notes = models.TextField(blank=True)


class FunctionType(TimeStampedModel):
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    code = models.CharField(max_length=10)
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True)
    active = models.BooleanField(default=True)

    class Meta:
        unique_together = ("parish", "code")

    def __str__(self):
        return self.name


class PositionType(TimeStampedModel):
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    code = models.CharField(max_length=20)
    name = models.CharField(max_length=100)
    active = models.BooleanField(default=True)
    functions = models.ManyToManyField(FunctionType, through="PositionTypeFunction", blank=True)

    class Meta:
        unique_together = ("parish", "code")

    def __str__(self):
        return self.name


class PositionTypeFunction(models.Model):
    position_type = models.ForeignKey(PositionType, on_delete=models.CASCADE)
    function_type = models.ForeignKey(FunctionType, on_delete=models.CASCADE)

    class Meta:
        unique_together = ("position_type", "function_type")


class AcolyteQualification(TimeStampedModel):
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    acolyte = models.ForeignKey(AcolyteProfile, on_delete=models.CASCADE)
    position_type = models.ForeignKey(PositionType, on_delete=models.CASCADE)
    qualified = models.BooleanField(default=True)

    class Meta:
        unique_together = ("parish", "acolyte", "position_type")


class AcolytePreference(TimeStampedModel):
    PREFERENCE_CHOICES = [
        ("preferred_community", "Preferir comunidade"),
        ("avoid_community", "Evitar comunidade"),
        ("preferred_timeslot", "Preferir horario"),
        ("preferred_mass_template", "Preferir modelo"),
        ("preferred_position", "Preferir funcao"),
        ("avoid_position", "Evitar funcao"),
        ("preferred_function", "Preferir funcao base"),
        ("avoid_function", "Evitar funcao base"),
        ("preferred_partner", "Preferir parceiro"),
        ("avoid_partner", "Evitar parceiro"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    acolyte = models.ForeignKey(AcolyteProfile, on_delete=models.CASCADE)
    preference_type = models.CharField(max_length=40, choices=PREFERENCE_CHOICES)
    target_community = models.ForeignKey(Community, on_delete=models.SET_NULL, null=True, blank=True)
    target_position = models.ForeignKey(PositionType, on_delete=models.SET_NULL, null=True, blank=True)
    target_function = models.ForeignKey(FunctionType, on_delete=models.SET_NULL, null=True, blank=True)
    target_template = models.ForeignKey("MassTemplate", on_delete=models.SET_NULL, null=True, blank=True)
    target_acolyte = models.ForeignKey(AcolyteProfile, on_delete=models.SET_NULL, null=True, blank=True, related_name="preferred_by")
    weekday = models.PositiveSmallIntegerField(null=True, blank=True)
    start_time = models.TimeField(null=True, blank=True)
    end_time = models.TimeField(null=True, blank=True)
    weight = models.PositiveIntegerField(default=50)


class RequirementProfile(TimeStampedModel):
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    name = models.CharField(max_length=150)
    notes = models.TextField(blank=True)
    active = models.BooleanField(default=True)
    min_senior_per_mass = models.PositiveSmallIntegerField(default=0)

    def __str__(self):
        return self.name


class RequirementProfilePosition(models.Model):
    profile = models.ForeignKey(RequirementProfile, on_delete=models.CASCADE, related_name="positions")
    position_type = models.ForeignKey(PositionType, on_delete=models.CASCADE)
    quantity = models.PositiveSmallIntegerField(default=1)

    class Meta:
        unique_together = ("profile", "position_type")


class MassTemplate(TimeStampedModel):
    WEEKDAY_CHOICES = [
        (0, "Mon"),
        (1, "Tue"),
        (2, "Wed"),
        (3, "Thu"),
        (4, "Fri"),
        (5, "Sat"),
        (6, "Sun"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    title = models.CharField(max_length=200)
    community = models.ForeignKey(Community, on_delete=models.CASCADE)
    weekday = models.PositiveSmallIntegerField(choices=WEEKDAY_CHOICES)
    time = models.TimeField()
    rrule_text = models.TextField(blank=True)
    active = models.BooleanField(default=True)
    default_requirement_profile = models.ForeignKey(RequirementProfile, on_delete=models.SET_NULL, null=True, blank=True)
    notes = models.TextField(blank=True)

    def __str__(self):
        return self.title


class EventSeries(TimeStampedModel):
    CANDIDATE_POOL_CHOICES = [
        ("all", "Todos"),
        ("interested_only", "Somente interessados"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    series_type = models.CharField(max_length=40)
    title = models.CharField(max_length=200)
    start_date = models.DateField()
    end_date = models.DateField()
    default_community = models.ForeignKey(Community, on_delete=models.SET_NULL, null=True, blank=True)
    ruleset_json = models.JSONField(default=dict, blank=True)
    candidate_pool = models.CharField(max_length=20, choices=CANDIDATE_POOL_CHOICES, default="all")
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True, related_name="eventseries_created")
    updated_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True, related_name="eventseries_updated")
    is_active = models.BooleanField(default=True)


class EventOccurrence(TimeStampedModel):
    CONFLICT_CHOICES = [
        ("keep", "Manter existente"),
        ("cancel_existing", "Cancelar existente"),
        ("move_existing", "Mover existente"),
        ("skip", "Ignorar"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    event_series = models.ForeignKey(EventSeries, on_delete=models.CASCADE, related_name="occurrences")
    date = models.DateField()
    time = models.TimeField()
    community = models.ForeignKey(Community, on_delete=models.CASCADE)
    requirement_profile = models.ForeignKey(RequirementProfile, on_delete=models.SET_NULL, null=True, blank=True)
    label = models.CharField(max_length=200, blank=True)
    conflict_action = models.CharField(max_length=20, choices=CONFLICT_CHOICES, default="keep")
    move_to_date = models.DateField(null=True, blank=True)
    move_to_time = models.TimeField(null=True, blank=True)
    move_to_community = models.ForeignKey(Community, on_delete=models.SET_NULL, null=True, blank=True, related_name="moved_occurrences")


class EventInterest(TimeStampedModel):
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    event_series = models.ForeignKey(EventSeries, on_delete=models.CASCADE)
    acolyte = models.ForeignKey(AcolyteProfile, on_delete=models.CASCADE)
    interested = models.BooleanField(default=True)
    notes = models.TextField(blank=True)

    class Meta:
        unique_together = ("event_series", "acolyte")


class MassInstance(TimeStampedModel):
    STATUS_CHOICES = [
        ("scheduled", "Agendada"),
        ("canceled", "Cancelada"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    template = models.ForeignKey(MassTemplate, on_delete=models.SET_NULL, null=True, blank=True)
    event_series = models.ForeignKey(EventSeries, on_delete=models.SET_NULL, null=True, blank=True)
    community = models.ForeignKey(Community, on_delete=models.CASCADE)
    starts_at = models.DateTimeField()
    liturgy_label = models.CharField(max_length=200, blank=True)
    requirement_profile = models.ForeignKey(RequirementProfile, on_delete=models.SET_NULL, null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="scheduled")
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True, related_name="mass_created")
    updated_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True, related_name="mass_updated")

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["parish", "starts_at", "community"],
                condition=Q(status="scheduled"),
                name="unique_scheduled_mass_per_slot",
            )
        ]

    def __str__(self):
        return f"{self.community.code} - {self.starts_at}"


class MassOverride(TimeStampedModel):
    OVERRIDE_CHOICES = [
        ("cancel_instance", "Cancelar missa"),
        ("move_instance", "Mover missa"),
        ("change_requirements", "Alterar requisitos"),
        ("change_display_fields", "Alterar exibicao"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    instance = models.ForeignKey(MassInstance, on_delete=models.CASCADE)
    override_type = models.CharField(max_length=40, choices=OVERRIDE_CHOICES)
    payload = models.JSONField(default=dict, blank=True)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)


class AssignmentSlot(TimeStampedModel):
    STATUS_CHOICES = [
        ("open", "Aberta"),
        ("assigned", "Atribuida"),
        ("finalized", "Finalizada"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    mass_instance = models.ForeignKey(MassInstance, on_delete=models.CASCADE, related_name="slots")
    position_type = models.ForeignKey(PositionType, on_delete=models.CASCADE)
    slot_index = models.PositiveSmallIntegerField(default=1)
    required = models.BooleanField(default=True)
    externally_covered = models.BooleanField(default=False)
    external_coverage_notes = models.TextField(blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="open")
    is_locked = models.BooleanField(default=False)
    locked_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = ("mass_instance", "position_type", "slot_index")

    def get_active_assignment(self):
        if hasattr(self, "active_assignments"):
            return self.active_assignments[0] if self.active_assignments else None
        return self.assignments.filter(is_active=True).first()

    @property
    def active_assignment(self):
        return self.get_active_assignment()


class Assignment(TimeStampedModel):
    END_REASON_CHOICES = [
        ("declined", "Recusado"),
        ("canceled", "Cancelado"),
        ("replaced", "Substituido"),
        ("replaced_by_solver", "Substituido pelo sistema"),
        ("manual_unassign", "Removido manualmente"),
        ("moved_to_another_slot", "Movido para outra posicao"),
        ("swap", "Trocado"),
    ]
    STATE_CHOICES = [
        ("proposed", "Proposta"),
        ("published", "Publicada"),
        ("locked", "Consolidada"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    slot = models.ForeignKey(AssignmentSlot, on_delete=models.CASCADE, related_name="assignments")
    acolyte = models.ForeignKey(AcolyteProfile, on_delete=models.PROTECT)
    assigned_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    assigned_at = models.DateTimeField(auto_now_add=True)
    assignment_state = models.CharField(max_length=20, choices=STATE_CHOICES, default="proposed")
    published_at = models.DateTimeField(null=True, blank=True)
    is_active = models.BooleanField(default=True)
    ended_at = models.DateTimeField(null=True, blank=True)
    end_reason = models.CharField(max_length=30, choices=END_REASON_CHOICES, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["slot"],
                condition=Q(is_active=True),
                name="unique_active_assignment_per_slot",
            )
        ]


class Confirmation(TimeStampedModel):
    STATUS_CHOICES = [
        ("pending", "Pendente"),
        ("confirmed", "Confirmada"),
        ("declined", "Recusada"),
        ("canceled_by_acolyte", "Cancelada"),
        ("replaced", "Substituida"),
        ("no_show", "Nao compareceu"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    assignment = models.OneToOneField(Assignment, on_delete=models.CASCADE, related_name="confirmation")
    status = models.CharField(max_length=30, choices=STATUS_CHOICES, default="pending")
    notes = models.TextField(blank=True)
    updated_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    timestamp = models.DateTimeField(auto_now=True)


class SwapRequest(TimeStampedModel):
    STATUS_CHOICES = [
        ("pending", "Pendente"),
        ("awaiting_approval", "Aguardando aprovacao"),
        ("accepted", "Aceita"),
        ("rejected", "Recusada"),
        ("canceled", "Cancelada"),
    ]
    TYPE_CHOICES = [
        ("acolyte_swap", "Troca de acolito"),
        ("role_swap", "Troca de funcao"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    swap_type = models.CharField(max_length=20, choices=TYPE_CHOICES)
    requestor_acolyte = models.ForeignKey(AcolyteProfile, on_delete=models.CASCADE, related_name="swap_requests")
    target_acolyte = models.ForeignKey(AcolyteProfile, on_delete=models.SET_NULL, null=True, blank=True, related_name="swap_targets")
    mass_instance = models.ForeignKey(MassInstance, on_delete=models.CASCADE)
    from_slot = models.ForeignKey(AssignmentSlot, on_delete=models.SET_NULL, null=True, blank=True, related_name="swap_from")
    to_slot = models.ForeignKey(AssignmentSlot, on_delete=models.SET_NULL, null=True, blank=True, related_name="swap_to")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending")
    notes = models.TextField(blank=True)
    open_to_admin = models.BooleanField(default=False)


class ReplacementRequest(TimeStampedModel):
    STATUS_CHOICES = [
        ("pending", "Pendente"),
        ("assigned", "Atribuida"),
        ("canceled", "Cancelada"),
        ("resolved", "Resolvida"),
    ]
    RESOLUTION_CHOICES = [
        ("mass_canceled", "Missa cancelada"),
        ("slot_not_required", "Funcao nao necessaria"),
        ("covered_externally", "Coberto externamente"),
        ("other", "Outro"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    slot = models.ForeignKey(AssignmentSlot, on_delete=models.CASCADE)
    requested_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    proposed_acolyte = models.ForeignKey(AcolyteProfile, on_delete=models.SET_NULL, null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="pending")
    notes = models.TextField(blank=True)
    resolved_reason = models.CharField(max_length=30, choices=RESOLUTION_CHOICES, blank=True)
    resolved_notes = models.TextField(blank=True)
    resolved_at = models.DateTimeField(null=True, blank=True)


class AcolyteCreditLedger(TimeStampedModel):
    REASON_CHOICES = [
        ("served_unpopular_slot", "Serviu em horario pouco procurado"),
        ("accepted_last_minute_substitution", "Aceitou substituicao de ultima hora"),
        ("high_attendance_streak", "Boa sequencia de presencas"),
        ("received_high_demand_assignment", "Recebeu funcao disputada"),
    ]
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    acolyte = models.ForeignKey(AcolyteProfile, on_delete=models.CASCADE)
    delta = models.IntegerField()
    reason_code = models.CharField(max_length=40, choices=REASON_CHOICES)
    related_assignment = models.ForeignKey(Assignment, on_delete=models.SET_NULL, null=True, blank=True)


class AcolyteStats(TimeStampedModel):
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    acolyte = models.OneToOneField(AcolyteProfile, on_delete=models.CASCADE)
    services_last_30_days = models.PositiveIntegerField(default=0)
    services_last_90_days = models.PositiveIntegerField(default=0)
    confirmation_rate = models.FloatField(default=0.0)
    cancellations_rate = models.FloatField(default=0.0)
    no_show_count = models.PositiveIntegerField(default=0)
    last_served_at = models.DateTimeField(null=True, blank=True)
    reliability_score = models.FloatField(default=0.0)
    credit_balance = models.IntegerField(default=0)


class CalendarFeedToken(TimeStampedModel):
    parish = models.ForeignKey(Parish, on_delete=models.CASCADE)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    token = models.CharField(max_length=64, unique=True)
    rotated_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = ("parish", "user")


class AuditEvent(models.Model):
    parish = models.ForeignKey(Parish, on_delete=models.SET_NULL, null=True, blank=True)
    actor_user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True)
    entity_type = models.CharField(max_length=100)
    entity_id = models.CharField(max_length=100)
    action_type = models.CharField(max_length=50)
    diff_json = models.JSONField(default=dict, blank=True)
    timestamp = models.DateTimeField(auto_now_add=True)

    class Meta:
        indexes = [
            models.Index(fields=["entity_type", "entity_id"]),
            models.Index(fields=["parish", "timestamp"]),
        ]

