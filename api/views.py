from rest_framework import permissions, viewsets

from core.models import AcolyteProfile, Assignment, MassInstance
from api.serializers import AcolyteProfileSerializer, AssignmentSerializer, MassInstanceSerializer


class ParishScopedMixin:
    def get_queryset(self):
        parish = getattr(self.request, "active_parish", None)
        return super().get_queryset().filter(parish=parish)


class AcolyteProfileViewSet(ParishScopedMixin, viewsets.ReadOnlyModelViewSet):
    queryset = AcolyteProfile.objects.all()
    serializer_class = AcolyteProfileSerializer
    permission_classes = [permissions.IsAuthenticated]


class MassInstanceViewSet(ParishScopedMixin, viewsets.ReadOnlyModelViewSet):
    queryset = MassInstance.objects.all()
    serializer_class = MassInstanceSerializer
    permission_classes = [permissions.IsAuthenticated]


class AssignmentViewSet(ParishScopedMixin, viewsets.ReadOnlyModelViewSet):
    queryset = Assignment.objects.all()
    serializer_class = AssignmentSerializer
    permission_classes = [permissions.IsAuthenticated]

