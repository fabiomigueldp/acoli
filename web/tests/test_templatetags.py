from django.test import SimpleTestCase

from web.templatetags.extras import get_item, get_list_item


class TemplateTagTests(SimpleTestCase):
    def test_get_item_returns_none_when_missing(self):
        self.assertIsNone(get_item(None, "key"))
        self.assertIsNone(get_item({}, "key"))
        self.assertEqual(get_item({"key": "value"}, "key"), "value")

    def test_get_list_item_returns_empty_list_when_missing(self):
        self.assertEqual(get_list_item(None, "key"), [])
        self.assertEqual(get_list_item({}, "key"), [])
        self.assertEqual(get_list_item({"key": [1, 2]}, "key"), [1, 2])
