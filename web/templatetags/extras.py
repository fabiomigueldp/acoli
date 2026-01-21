from django import template

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

