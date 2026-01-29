import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "acoli.settings")
import django
django.setup()

from core.models import AssignmentSlot

# Fix slot statuses
slots = AssignmentSlot.objects.filter(required=True, status='assigned')
for slot in slots:
    if not slot.active_assignment:
        slot.status = 'open'
        slot.save()
        print(f"Fixed slot {slot.id} status to 'open'")

print("Slot statuses fixed.")