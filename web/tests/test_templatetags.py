from django.test import SimpleTestCase

from web.templatetags.extras import (
    assignment_end_reason_label,
    confirmation_status_label,
    get_item,
    get_list_item,
    slot_status_label,
)


class TemplateTagTests(SimpleTestCase):
    def test_get_item_returns_none_when_missing(self):
        self.assertIsNone(get_item(None, "key"))
        self.assertIsNone(get_item({}, "key"))
        self.assertEqual(get_item({"key": "value"}, "key"), "value")

    def test_get_list_item_returns_empty_list_when_missing(self):
        self.assertEqual(get_list_item(None, "key"), [])
        self.assertEqual(get_list_item({}, "key"), [])
        self.assertEqual(get_list_item({"key": [1, 2]}, "key"), [1, 2])

    def test_choice_labels_normalize_case(self):
        self.assertEqual(slot_status_label("Assigned"), "Atribuida")
        self.assertEqual(slot_status_label("ASSIGNED"), "Atribuida")
        self.assertEqual(assignment_end_reason_label("replaced_by_solver"), "Substituido pelo sistema")
        self.assertEqual(assignment_end_reason_label("moved_to_another_slot"), "Movido para outra posicao")
        self.assertEqual(confirmation_status_label("PENDING"), "Pendente")
