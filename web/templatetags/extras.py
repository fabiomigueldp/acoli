from django import template
import json

register = template.Library()


@register.filter
def index(sequence, idx):
    try:
        return sequence[idx]
    except Exception:
        return None


@register.filter
def get_item(mapping, key):
    if not mapping:
        return None
    return mapping.get(key)


@register.filter
def get_list_item(mapping, key):
    if not mapping:
        return []
    return mapping.get(key, [])


@register.filter
def weekday_label(value):
    labels = {
        0: "Seg",
        1: "Ter",
        2: "Qua",
        3: "Qui",
        4: "Sex",
        5: "Sab",
        6: "Dom",
    }
    if value is None or value == "":
        return "Qualquer dia"
    return labels.get(value, str(value))


def _format_time(value):
    if not value:
        return None
    return value.strftime("%H:%M")


@register.filter
def preference_priority_label(weight):
    if weight is None:
        return "Media"
    if weight >= 75:
        return "Alta"
    if weight <= 35:
        return "Baixa"
    return "Media"


@register.filter
def preference_summary(pref):
    if not pref:
        return ""
    mapping = {
        "preferred_community": ("Preferir comunidade", pref.target_community),
        "avoid_community": ("Evitar comunidade", pref.target_community),
        "preferred_position": ("Preferir posicao", pref.target_position),
        "avoid_position": ("Evitar posicao", pref.target_position),
        "preferred_function": ("Preferir funcao", pref.target_function),
        "avoid_function": ("Evitar funcao", pref.target_function),
        "preferred_mass_template": ("Preferir modelo", pref.target_template),
        "preferred_partner": ("Preferir parceiro", pref.target_acolyte),
        "avoid_partner": ("Evitar parceiro", pref.target_acolyte),
        "preferred_timeslot": ("Preferir horario", None),
    }
    label, target = mapping.get(pref.preference_type, (pref.get_preference_type_display(), None))
    if target:
        name = getattr(target, "display_name", None) or getattr(target, "name", None) or str(target)
        return f"{label}: {name}"
    return label


@register.filter
def preference_detail(pref):
    if not pref:
        return ""
    if pref.preference_type == "preferred_timeslot":
        parts = []
        if pref.weekday is not None:
            parts.append(weekday_label(pref.weekday))
        start = _format_time(pref.start_time)
        end = _format_time(pref.end_time)
        if start or end:
            parts.append(f"{start or '--:--'}-{end or '--:--'}")
        if not start and not end:
            parts.append("Qualquer horario")
        return " | ".join(parts)
    return ""


@register.filter
def initials(value):
    if not value:
        return "--"
    parts = [part for part in str(value).strip().split() if part]
    if not parts:
        return "--"
    if len(parts) == 1:
        return parts[0][:2].upper()
    return f"{parts[0][0]}{parts[-1][0]}".upper()


def _normalize_choice(value):
    if value is None:
        return ""
    return str(value).strip()


def _map_choice_label(mapping, value):
    key = _normalize_choice(value)
    if not key:
        return ""
    key_lower = key.lower()
    return mapping.get(key_lower, mapping.get(key, key))


@register.filter
def mass_status_label(value):
    mapping = {
        "scheduled": "Agendada",
        "canceled": "Cancelada",
    }
    return _map_choice_label(mapping, value)


@register.filter
def slot_status_label(value):
    mapping = {
        "open": "Aberta",
        "assigned": "Atribuida",
        "finalized": "Finalizada",
    }
    return _map_choice_label(mapping, value)


@register.filter
def assignment_state_label(value):
    mapping = {
        "proposed": "Proposta",
        "published": "Publicada",
        "locked": "Consolidada",
    }
    return _map_choice_label(mapping, value)


@register.filter
def confirmation_status_label(value):
    mapping = {
        "pending": "Pendente",
        "confirmed": "Confirmada",
        "declined": "Recusada",
        "canceled_by_acolyte": "Cancelada pelo acolito",
        "replaced": "Substituida",
        "no_show": "Nao compareceu",
    }
    return _map_choice_label(mapping, value)


@register.filter
def assignment_end_reason_label(value):
    mapping = {
        "declined": "Recusado",
        "canceled": "Cancelado",
        "replaced": "Substituido",
        "replaced_by_solver": "Substituido pelo sistema",
        "manual_unassign": "Removido manualmente",
        "moved_to_another_slot": "Movido para outra posicao",
        "swap": "Trocado",
    }
    return _map_choice_label(mapping, value)


@register.filter
def swap_status_label(value):
    mapping = {
        "pending": "Pendente",
        "awaiting_approval": "Aguardando aprovacao",
        "accepted": "Aceita",
        "rejected": "Recusada",
        "canceled": "Cancelada",
    }
    return _map_choice_label(mapping, value)


@register.filter
def swap_type_label(value):
    mapping = {
        "acolyte_swap": "Troca de acolito",
        "role_swap": "Troca de funcao",
    }
    return _map_choice_label(mapping, value)


@register.filter
def job_status_label(value):
    mapping = {
        "pending": "Pendente",
        "running": "Executando",
        "success": "Concluido",
        "failed": "Falhou",
    }
    return _map_choice_label(mapping, value)


@register.filter
def pprint(value):
    """Pretty print JSON data for audit diffs."""
    if not value:
        return ""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    try:
        return json.dumps(value, indent=2, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(value)

