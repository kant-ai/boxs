import unittest.mock

from boxs.api import info, load, store
from boxs.data import DataRef
from boxs.errors import DataNotFound, BoxNotDefined
from boxs.box import Box
from boxs.box_registry import unregister_box


class TestStore(unittest.TestCase):

    def setUp(self):
        self.box = Box('box-id', None)

    def tearDown(self):
        unregister_box(self.box.box_id)

    def test_store_resolves_origin_from_where_it_is_called(self):
        self.box.store = unittest.mock.MagicMock()
        store('my-input', box=self.box)
        self.assertEqual(
            self.box.store.call_args[1]['origin'],
            'test_store_resolves_origin_from_where_it_is_called',
        )

    def test_box_is_required(self):
        with self.assertRaisesRegex(ValueError, "'box' must be set."):
            store(None, box=None)

    def test_box_is_resolved_from_id(self):
        self.box.store = unittest.mock.MagicMock()
        store(None, box='box-id')
        self.box.store.assert_called()

    def test_box_can_be_given_as_object(self):
        self.box.store = unittest.mock.MagicMock()
        store(None, box=self.box)
        self.box.store.assert_called()

    def test_box_store_gets_all_arguments(self):
        self.box.store = unittest.mock.MagicMock()
        store(
            'my-input', 'parent1', 'parent2',
            box=self.box,
            name='my-name', origin='my-origin', tags={'my': 'tag'},
            run_id='run-id', meta={'my': 'meta'},
        )
        self.box.store.assert_called_with(
            'my-input', 'parent1', 'parent2',
            name='my-name', origin='my-origin', tags={'my': 'tag'},
            run_id='run-id', meta={'my': 'meta'},
        )

    def test_box_store_return_value_is_returned(self):
        self.box.store = unittest.mock.MagicMock(return_value='My value')
        result = store(None, box=self.box)
        self.assertEqual('My value', result)


class TestLoad(unittest.TestCase):

    def setUp(self):
        self.storage = unittest.mock.MagicMock()
        self.box = Box('box-id', self.storage)
        self.data_ref = DataRef('data-id', 'box-id', 'run-id')

    def tearDown(self):
        unregister_box(self.box.box_id)

    def test_box_is_resolved_from_data(self):
        self.box.load = unittest.mock.MagicMock()
        load(None, self.data_ref)
        self.box.load.assert_called()

    def test_load_raises_if_box_does_not_exist(self):
        self.storage.exists = unittest.mock.MagicMock(return_value=False)
        with self.assertRaisesRegex(BoxNotDefined, "box .* not defined"):
            load(None, DataRef('data-id', 'unknown-box-id', 'run-id'))

    def test_load_raises_if_data_does_not_exist(self):
        self.storage.exists = unittest.mock.MagicMock(return_value=False)
        with self.assertRaisesRegex(DataNotFound, "Data .* does not exist"):
            load(None, self.data_ref)

    def test_box_load_gets_all_arguments(self):
        self.box.load = unittest.mock.MagicMock()
        load('my-output', self.data_ref)
        self.box.load.assert_called_with('my-output', self.data_ref)

    def test_box_load_return_value_is_returned(self):
        self.box.load = unittest.mock.MagicMock(return_value='My value')
        result = load(None, self.data_ref)
        self.assertEqual('My value', result)


class TestInfo(unittest.TestCase):

    def setUp(self):
        self.storage = unittest.mock.MagicMock()
        self.box = Box('box-id', self.storage)
        self.data_ref = DataRef('data-id', 'box-id', 'run-id')

    def tearDown(self):
        unregister_box(self.box.box_id)

    def test_box_is_resolved_from_data(self):
        self.box.info = unittest.mock.MagicMock()
        info(self.data_ref)
        self.box.info.assert_called()

    def test_info_raises_if_box_does_not_exist(self):
        self.storage.exists = unittest.mock.MagicMock(return_value=False)
        with self.assertRaisesRegex(BoxNotDefined, "box .* not defined"):
            info(DataRef('data-id', 'unknown-box-id', 'run-id'))

    def test_info_raises_if_data_does_not_exist(self):
        self.storage.exists = unittest.mock.MagicMock(return_value=False)
        with self.assertRaisesRegex(DataNotFound, "Data .* does not exist"):
            info(self.data_ref)

    def test_box_info_gets_reference(self):
        self.box.info = unittest.mock.MagicMock()
        info(self.data_ref)
        self.box.info.assert_called_with(self.data_ref)

    def test_box_info_return_value_is_returned(self):
        self.box.info = unittest.mock.MagicMock(return_value='My value')
        result = info(self.data_ref)
        self.assertEqual('My value', result)


if __name__ == '__main__':
    unittest.main()
