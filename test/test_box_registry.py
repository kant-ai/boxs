import unittest.mock

from boxs.box_registry import get_box, register_box, unregister_box
from boxs.errors import BoxAlreadyDefined, BoxNotDefined


class FakeBox:

    def __init__(self, box_id):
        self.box_id = box_id


class TestBoxRegistry(unittest.TestCase):

    def test_unknown_box_raises_error(self):
        with self.assertRaises(BoxNotDefined):
            get_box('unknown-box-id')

    def test_get_box_uses_default_from_config(self):
        try:
            register_box(FakeBox('default-box-id'))
            with unittest.mock.patch('boxs.box_registry.get_config') as get_config_mock:
                get_config_mock.return_value.default_box = 'default-box-id'
                box = get_box()
                self.assertEqual('default-box-id', box.box_id)
        finally:
            unregister_box('default-box-id')

    def test_boxes_must_have_unique_ids(self):
        try:
            register_box(FakeBox('box-id'))
            with self.assertRaisesRegex(BoxAlreadyDefined, 'box-id already defined'):
                register_box(FakeBox('box-id'))
        finally:
            unregister_box('box-id')

    def test_unregister_unknown_box_raises_error(self):
        with self.assertRaises(BoxNotDefined):
            unregister_box('unknown-box-id')


if __name__ == '__main__':
    unittest.main()
