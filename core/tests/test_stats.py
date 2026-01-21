from datetime import timedelta

from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteProfile,
    AcolyteCreditLedger,
    AcolyteStats,
    Assignment,
    AssignmentSlot,
    Community,
    Confirmation,
    MassInstance,
    Parish,
    PositionType,
)
from core.services.stats import recompute_stats


class StatsRecomputeTests(TestCase):
    def test_recompute_stats_scopes_by_parish(self):
        parish_a = Parish.objects.create(name="Parish A")
        parish_b = Parish.objects.create(name="Parish B")
        community_a = Community.objects.create(parish=parish_a, code="MAT", name="Matriz")
        community_b = Community.objects.create(parish=parish_b, code="STM", name="Comunidade")
        position_a = PositionType.objects.create(parish=parish_a, code="LIB", name="Libriferario")
        position_b = PositionType.objects.create(parish=parish_b, code="LIB", name="Libriferario")
        acolyte_a = AcolyteProfile.objects.create(parish=parish_a, display_name="Acolito A")
        acolyte_b = AcolyteProfile.objects.create(parish=parish_b, display_name="Acolito B")

        instance_a = MassInstance.objects.create(
            parish=parish_a,
            community=community_a,
            starts_at=timezone.now() - timedelta(days=1),
            status="scheduled",
        )
        slot_a = AssignmentSlot.objects.create(
            parish=parish_a,
            mass_instance=instance_a,
            position_type=position_a,
            slot_index=1,
            required=True,
            status="assigned",
        )
        assignment_a = Assignment.objects.create(
            parish=parish_a,
            slot=slot_a,
            acolyte=acolyte_a,
            assignment_state="published",
        )
        Confirmation.objects.create(parish=parish_a, assignment=assignment_a, status="confirmed")

        instance_b = MassInstance.objects.create(
            parish=parish_b,
            community=community_b,
            starts_at=timezone.now() - timedelta(days=1),
            status="scheduled",
        )
        slot_b = AssignmentSlot.objects.create(
            parish=parish_b,
            mass_instance=instance_b,
            position_type=position_b,
            slot_index=1,
            required=True,
            status="assigned",
        )
        assignment_b = Assignment.objects.create(
            parish=parish_b,
            slot=slot_b,
            acolyte=acolyte_b,
            assignment_state="published",
        )
        Confirmation.objects.create(parish=parish_b, assignment=assignment_b, status="declined")

        recompute_stats(parish_a)

        stats = AcolyteStats.objects.get(parish=parish_a, acolyte=acolyte_a)
        self.assertEqual(stats.confirmation_rate, 1.0)
        self.assertEqual(stats.cancellations_rate, 0.0)

    def test_credit_balance_scopes_by_parish(self):
        parish_a = Parish.objects.create(name="Parish A")
        parish_b = Parish.objects.create(name="Parish B")
        community_a = Community.objects.create(parish=parish_a, code="MAT", name="Matriz")
        position_a = PositionType.objects.create(parish=parish_a, code="LIB", name="Libriferario")
        acolyte_a = AcolyteProfile.objects.create(parish=parish_a, display_name="Acolito A")
        acolyte_b = AcolyteProfile.objects.create(parish=parish_b, display_name="Acolito B")

        instance_a = MassInstance.objects.create(
            parish=parish_a,
            community=community_a,
            starts_at=timezone.now() - timedelta(days=1),
            status="scheduled",
        )
        slot_a = AssignmentSlot.objects.create(
            parish=parish_a,
            mass_instance=instance_a,
            position_type=position_a,
            slot_index=1,
            required=True,
            status="assigned",
        )
        Assignment.objects.create(
            parish=parish_a,
            slot=slot_a,
            acolyte=acolyte_a,
            assignment_state="published",
        )

        AcolyteCreditLedger.objects.create(
            parish=parish_a,
            acolyte=acolyte_a,
            delta=5,
            reason_code="served_unpopular_slot",
        )
        AcolyteCreditLedger.objects.create(
            parish=parish_b,
            acolyte=acolyte_b,
            delta=100,
            reason_code="served_unpopular_slot",
        )

        recompute_stats(parish_a)

        stats = AcolyteStats.objects.get(parish=parish_a, acolyte=acolyte_a)
        self.assertEqual(stats.credit_balance, 5)
