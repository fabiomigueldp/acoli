from rest_framework import serializers

from core.models import AcolyteProfile, Assignment, MassInstance


class AcolyteProfileSerializer(serializers.ModelSerializer):
    class Meta:
        model = AcolyteProfile
        fields = ["id", "display_name", "community_of_origin", "active"]


class MassInstanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = MassInstance
        fields = ["id", "community", "starts_at", "liturgy_label", "status"]


class AssignmentSerializer(serializers.ModelSerializer):
    class Meta:
        model = Assignment
        fields = ["id", "slot", "acolyte", "assignment_state", "published_at"]

