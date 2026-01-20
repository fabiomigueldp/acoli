from core.models import AssignmentSlot


def sync_slots_for_instance(instance):
    if not instance.requirement_profile:
        return []

    desired = set()
    created = []
    for position in instance.requirement_profile.positions.select_related("position_type").all():
        for idx in range(1, position.quantity + 1):
            desired.add((position.position_type_id, idx))
            slot, was_created = AssignmentSlot.objects.get_or_create(
                parish=instance.parish,
                mass_instance=instance,
                position_type=position.position_type,
                slot_index=idx,
                defaults={"required": True, "status": "open"},
            )
            if was_created:
                created.append(slot)
            elif not slot.required:
                slot.required = True
                slot.save(update_fields=["required", "updated_at"])

    for slot in AssignmentSlot.objects.filter(mass_instance=instance):
        if (slot.position_type_id, slot.slot_index) not in desired and slot.required:
            slot.required = False
            if slot.status == "open":
                slot.status = "open"
            slot.save(update_fields=["required", "status", "updated_at"])
    return created


def sync_slots_for_parish(parish, start_date, end_date):
    from core.models import MassInstance

    instances = MassInstance.objects.filter(
        parish=parish,
        starts_at__date__gte=start_date,
        starts_at__date__lte=end_date,
    ).select_related("requirement_profile")
    for instance in instances:
        sync_slots_for_instance(instance)
    return instances.count()

