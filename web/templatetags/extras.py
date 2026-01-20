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

