import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "acoli.settings")
import django
django.setup()

from core.models import MassInstance, Community
c = Community.objects.get(code='NSL')
mi = MassInstance.objects.filter(parish__isnull=False, community=c, starts_at__date='2026-02-08', starts_at__hour=8, starts_at__minute=15)
print('Found:', mi.count(), 'instances')
if mi.exists():
    m = mi.first()
    print('ID:', m.id, 'Status:', m.status)
    m.delete()
    print('Deleted')
else:
    print('No instance found')