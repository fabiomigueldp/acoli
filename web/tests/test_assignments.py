from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from core.models import (
    AcolyteProfile,
    AcolyteQualification,
    Assignment,
    AssignmentSlot,
    Confirmation,
    Community,
    MassInstance,
    MembershipRole,
    Parish,
    ParishMembership,
    PositionType,
    ReplacementRequest,
)

User = get_user_model()


class AssignmentLifecycleTests(TestCase):
    def test_decline_deactivates_assignment(self):
        User = get_user_model()
        user = User.objects.create_user(email="user@example.com", full_name="User", password="pass")
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, user=user, display_name="Acolito")
        ParishMembership.objects.create(parish=parish, user=user, active=True)
        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(
            parish=parish,
            mass_instance=instance,
            position_type=position,
            slot_index=1,
            required=True,
            status="assigned",
        )
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte)

        self.client.login(email="user@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        response = self.client.post(f"/assignments/{assignment.id}/decline/")
        self.assertEqual(response.status_code, 302)
        assignment.refresh_from_db()
        slot.refresh_from_db()
        self.assertFalse(assignment.is_active)
        self.assertEqual(slot.status, "open")

    def test_confirm_inactive_assignment_is_blocked(self):
        User = get_user_model()
        user = User.objects.create_user(email="user2@example.com", full_name="User", password="pass")
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, user=user, display_name="Acolito")
        ParishMembership.objects.create(parish=parish, user=user, active=True)
        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(
            parish=parish,
            mass_instance=instance,
            position_type=position,
            slot_index=1,
            required=True,
            status="assigned",
        )
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte, is_active=False)

        self.client.login(email="user2@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        response = self.client.post(f"/assignments/{assignment.id}/confirm/")
        self.assertEqual(response.status_code, 302)
        self.assertFalse(Confirmation.objects.filter(parish=parish, assignment=assignment).exists())

    def test_decline_inactive_assignment_is_blocked(self):
        User = get_user_model()
        user = User.objects.create_user(email="user3@example.com", full_name="User", password="pass")
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, user=user, display_name="Acolito")
        ParishMembership.objects.create(parish=parish, user=user, active=True)
        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=3),
            status="scheduled",
        )
        slot = AssignmentSlot.objects.create(
            parish=parish,
            mass_instance=instance,
            position_type=position,
            slot_index=1,
            required=True,
            status="assigned",
        )
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte, is_active=False)

        self.client.login(email="user3@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        response = self.client.post(f"/assignments/{assignment.id}/decline/")
        self.assertEqual(response.status_code, 302)
        self.assertFalse(Confirmation.objects.filter(parish=parish, assignment=assignment).exists())

    def test_cancel_assignment_blocked_when_mass_canceled(self):
        User = get_user_model()
        user = User.objects.create_user(email="user4@example.com", full_name="User", password="pass")
        parish = Parish.objects.create(name="Parish")
        community = Community.objects.create(parish=parish, code="MAT", name="Matriz")
        position = PositionType.objects.create(parish=parish, code="LIB", name="Libriferario")
        acolyte = AcolyteProfile.objects.create(parish=parish, user=user, display_name="Acolito")
        ParishMembership.objects.create(parish=parish, user=user, active=True)
        instance = MassInstance.objects.create(
            parish=parish,
            community=community,
            starts_at=timezone.now() + timedelta(days=3),
            status="canceled",
        )
        slot = AssignmentSlot.objects.create(
            parish=parish,
            mass_instance=instance,
            position_type=position,
            slot_index=1,
            required=True,
            status="assigned",
        )
        assignment = Assignment.objects.create(parish=parish, slot=slot, acolyte=acolyte, is_active=True)

        self.client.login(email="user4@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        response = self.client.post(f"/assignments/{assignment.id}/cancel/")
        self.assertEqual(response.status_code, 302)
        assignment.refresh_from_db()
        self.assertTrue(assignment.is_active)
        self.assertFalse(ReplacementRequest.objects.filter(parish=parish, slot=slot).exists())

    def test_assign_manual_conflict_shows_modal(self):
        parish = Parish.objects.create(name="Test Parish", city="Test City")
        user = User.objects.create_user(email="admin@example.com", password="pass")
        role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Admin")
        membership = ParishMembership.objects.create(parish=parish, user=user, active=True)
        membership.roles.add(role)
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Test Acolyte")
        community = Community.objects.create(parish=parish, code="COM", name="Test Community")
        mass = MassInstance.objects.create(
            parish=parish, community=community, starts_at=timezone.now()
        )
        position1 = PositionType.objects.create(parish=parish, code="POS1", name="Position 1")
        position2 = PositionType.objects.create(parish=parish, code="POS2", name="Position 2")
        slot1 = AssignmentSlot.objects.create(
            parish=parish, mass_instance=mass, position_type=position1, slot_index=1, required=True
        )
        slot2 = AssignmentSlot.objects.create(
            parish=parish, mass_instance=mass, position_type=position2, slot_index=1, required=True
        )
        
        # Add qualifications for the acolyte
        AcolyteQualification.objects.create(parish=parish, acolyte=acolyte, position_type=position1, qualified=True)
        AcolyteQualification.objects.create(parish=parish, acolyte=acolyte, position_type=position2, qualified=True)
        
        # Assign to slot1
        Assignment.objects.create(parish=parish, slot=slot1, acolyte=acolyte, is_active=True)

        self.client.login(email="admin@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        # Try to assign to slot2 - should redirect to mass_detail with conflict params
        response = self.client.post(f"/calendar/{mass.id}/slots/{slot2.id}/assign/", {"acolyte_id": acolyte.id}, follow=True)
        self.assertEqual(response.status_code, 200)
        
        # Check modal is shown
        self.assertTrue(response.context.get('show_conflict_modal'))
        self.assertEqual(response.context.get('conflict_acolyte'), acolyte)
        self.assertContains(response, "Position 1")
        self.assertContains(response, acolyte.display_name)

    def test_move_acolyte_success(self):
        parish = Parish.objects.create(name="Test Parish", city="Test City")
        user = User.objects.create_user(email="admin@example.com", password="pass")
        role = MembershipRole.objects.create(code="PARISH_ADMIN", name="Admin")
        membership = ParishMembership.objects.create(parish=parish, user=user, active=True)
        membership.roles.add(role)
        acolyte = AcolyteProfile.objects.create(parish=parish, display_name="Test Acolyte")
        community = Community.objects.create(parish=parish, code="COM", name="Test Community")
        mass = MassInstance.objects.create(
            parish=parish, community=community, starts_at=timezone.now()
        )
        position1 = PositionType.objects.create(parish=parish, code="POS1", name="Position 1")
        position2 = PositionType.objects.create(parish=parish, code="POS2", name="Position 2")
        slot1 = AssignmentSlot.objects.create(
            parish=parish, mass_instance=mass, position_type=position1, slot_index=1, required=True
        )
        slot2 = AssignmentSlot.objects.create(
            parish=parish, mass_instance=mass, position_type=position2, slot_index=1, required=True
        )
        
        # Add qualifications for the acolyte
        AcolyteQualification.objects.create(parish=parish, acolyte=acolyte, position_type=position1, qualified=True)
        AcolyteQualification.objects.create(parish=parish, acolyte=acolyte, position_type=position2, qualified=True)
        
        # Assign to slot1
        assignment1 = Assignment.objects.create(parish=parish, slot=slot1, acolyte=acolyte, is_active=True)

        self.client.login(email="admin@example.com", password="pass")
        session = self.client.session
        session["active_parish_id"] = parish.id
        session.save()

        # Move to slot2 - normal form submission returns redirect
        response = self.client.post(
            f"/calendar/{mass.id}/slots/{slot2.id}/move/",
            {"current_slot_id": slot1.id, "acolyte_id": acolyte.id}
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, f"/calendar/{mass.id}/")
        
        # Verify the move was successful
        assignment1.refresh_from_db()
        self.assertFalse(assignment1.is_active)
        slot2.refresh_from_db()
        self.assertEqual(slot2.status, "assigned")
        active_assignment = slot2.get_active_assignment()
        self.assertEqual(active_assignment.acolyte_id, acolyte.id)
