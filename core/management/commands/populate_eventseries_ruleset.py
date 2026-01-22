"""
Django management command to populate empty ruleset_json in EventSeries.

This fixes EventSeries created before the ruleset_json field was properly populated.
It infers default_time and default_requirement_profile_id from existing EventOccurrences.
"""
from django.core.management.base import BaseCommand
from core.models import EventSeries


class Command(BaseCommand):
    help = "Populate empty ruleset_json in EventSeries from EventOccurrence data"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be updated without making changes",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]

        series_list = EventSeries.objects.filter(ruleset_json={})

        if not series_list.exists():
            self.stdout.write(self.style.SUCCESS("No EventSeries with empty ruleset_json found"))
            return

        self.stdout.write(f"Found {series_list.count()} EventSeries with empty ruleset_json\n")

        updated_count = 0
        for series in series_list:
            first_occurrence = series.occurrences.first()

            # Infer default_time from first occurrence
            if first_occurrence and first_occurrence.time:
                default_time = first_occurrence.time.strftime("%H:%M")
            else:
                default_time = "19:00"  # Reasonable default

            # Infer default_requirement_profile_id from first occurrence
            profile_id = None
            if first_occurrence and first_occurrence.requirement_profile:
                profile_id = first_occurrence.requirement_profile.id

            ruleset = {
                "default_time": default_time,
                "default_requirement_profile_id": profile_id,
            }

            self.stdout.write(f"  {series.title} (ID: {series.id})")
            self.stdout.write(f"    - default_time: {default_time}")
            if profile_id:
                self.stdout.write(f"    - default_requirement_profile_id: {profile_id}")

            if not dry_run:
                series.ruleset_json = ruleset
                series.save()
                updated_count += 1

        if dry_run:
            self.stdout.write(self.style.WARNING(f"\nDry run: {series_list.count()} would be updated"))
        else:
            self.stdout.write(self.style.SUCCESS(f"\nSuccessfully updated {updated_count} EventSeries"))
