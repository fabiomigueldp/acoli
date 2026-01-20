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

