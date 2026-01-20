from django.test import TestCase

from web.forms import AcolyteAvailabilityRuleForm, AcolytePreferenceForm


class PreferenceFormTests(TestCase):
    def test_availability_form_accepts_blank_weekday(self):
        form = AcolyteAvailabilityRuleForm(data={"rule_type": "unavailable", "day_of_week": ""})
        self.assertTrue(form.is_valid())
        self.assertIsNone(form.cleaned_data["day_of_week"])

    def test_preference_form_accepts_blank_weekday(self):
        form = AcolytePreferenceForm(
            data={"preference_type": "preferred_community", "weekday": "", "weight": 50}
        )
        self.assertTrue(form.is_valid())
        self.assertIsNone(form.cleaned_data["weekday"])
