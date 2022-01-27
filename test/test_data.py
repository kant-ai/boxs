import json
import unittest.mock

from boxs.data import DataInfo, DataRef
from boxs.storage import Item


class TestDataRef(unittest.TestCase):

    def test_uri_contains_run_id_if_set(self):
        data_ref = DataRef('my-storage', 'data-id', 'my-revision')
        uri = data_ref.uri
        self.assertEqual('boxs://my-storage/data-id/my-revision', uri)

    def test_from_uri_sets_ids(self):
        uri = 'boxs://my-storage/data-id/run-id'
        data_ref = DataRef.from_uri(uri)
        self.assertEqual('my-storage', data_ref.box_id)
        self.assertEqual('data-id', data_ref.data_id)
        self.assertEqual('run-id', data_ref.run_id)

    def test_str_returns_uri(self):
        data_ref = DataRef('my-storage', 'data-id', 'my-revision')
        uri = data_ref.uri
        self.assertEqual(uri, str(data_ref))

    def test_from_value_info(self):
        value_info = {
            'data_id': 'data-id',
            'run_id': 'run-id',
            'box_id': 'my-box',
        }
        data_ref = DataRef.from_value_info(value_info)
        self.assertEqual('my-box', data_ref.box_id)
        self.assertEqual('data-id', data_ref.data_id)
        self.assertEqual('run-id', data_ref.run_id)

    def test_from_item(self):
        item = Item('my-box', 'data-id', 'run-id', None, 'some-time')
        data_ref = DataRef.from_item(item)
        self.assertEqual('my-box', data_ref.box_id)
        self.assertEqual('data-id', data_ref.data_id)
        self.assertEqual('run-id', data_ref.run_id)

    def test_from_uri_raises_if_wrong_scheme(self):
        uri = 'not://my-storage/data-id'
        with self.assertRaisesRegex(ValueError, "Invalid scheme"):
            DataRef.from_uri(uri)

    @unittest.mock.patch('boxs.data.info')
    def test_info_is_loaded_at_first_access_and_cached(self, info_mock):
        data_ref = DataRef('my-box', 'data-id', 'my-revision')

        self.assertIsNone(data_ref._info)

        data_info = DataInfo(data_ref, 'origin')
        info_mock.return_value = data_info

        info1 = data_ref.info
        self.assertIs(info1, data_info)

        info2 = data_ref.info

        info_mock.assert_called_once_with(data_ref)
        self.assertIs(info1, info2)

    @unittest.mock.patch('boxs.data.load')
    @unittest.mock.patch('boxs.data.info')
    def test_load_calls_api_with_itself(self, info_mock, load_mock):
        data_ref = DataRef('my-box', 'data-id', 'my-revision')

        data_info = DataInfo(data_ref, 'origin')
        info_mock.return_value = data_info
        load_mock.return_value = 'result'

        result = data_ref.load()
        self.assertEqual('result', result)
        load_mock.assert_called_once_with(data_info, value_type=None)

    def test_data_refs_are_equal_if_ids_match(self):
        data_ref1 = DataRef('my-box', 'data-id', 'my-revision')
        data_ref2 = DataRef('my-box', 'data-id', 'my-revision')
        self.assertEqual(data_ref1, data_ref2)

    def test_data_is_different_to_other_types(self):
        data_ref = DataRef('my-box', 'data-id', 'my-revision')
        other = ('data-id', 'my-box', 'my-revision')
        self.assertNotEqual(data_ref, other)

    def test_data_ref_hashs_are_equal_if_refs_are_equal(self):
        data_ref1 = DataRef('my-box', 'data-id', 'my-revision')
        data_ref2 = DataRef('my-box', 'data-id', 'my-revision')
        self.assertEqual(hash(data_ref1), hash(data_ref2))


class TestDataInfo(unittest.TestCase):

    def test_data_id_is_taken_from_ref(self):
        data = DataInfo(DataRef('my-storage', 'data-id', 'revision-id'), 'origin')
        self.assertEqual('data-id', data.data_id)

    def test_box_id_is_taken_from_ref(self):
        data = DataInfo(DataRef('my-storage', 'data-id', 'revision-id'), 'origin')
        self.assertEqual('my-storage', data.box_id)

    def test_run_id_is_taken_from_ref(self):
        data = DataInfo(DataRef('my-storage', 'data-id', 'revision-id'), 'origin')
        self.assertEqual('revision-id', data.run_id)

    def test_info_returns_self(self):
        data = DataInfo(DataRef('my-storage', 'data-id', 'revision-id'), 'origin')
        info = data.info
        self.assertIs(info, data)

    @unittest.mock.patch('boxs.data.load')
    def test_load_calls_api_with_itself(self, load_mock):
        data_ref = DataInfo(DataRef('my-box', 'data-id', 'my-revision'), 'origin')

        load_mock.return_value = 'result'

        result = data_ref.load()
        self.assertIs('result', result)
        load_mock.assert_called_once_with(data_ref, value_type=None)

    def test_value_info_contains_data_id(self):
        data = DataInfo(DataRef('my-storage', 'data-id', 'revision-id'), 'origin')
        info = data.value_info()
        self.assertEqual('data-id', info['ref']['data_id'])

    def test_value_info_contains_run_id(self):
        data = DataInfo(DataRef('my-storage', 'data-id', 'revision-id'), 'origin')
        info = data.value_info()
        self.assertEqual('revision-id', info['ref']['run_id'])

    def test_value_info_contains_box_id(self):
        data = DataInfo(DataRef('my-storage', 'data-id', 'revision-id'), 'origin')
        info = data.value_info()
        self.assertEqual('my-storage', info['ref']['box_id'])

    def test_value_info_contains_name(self):
        data = DataInfo(DataRef('my-storage', 'data-id', 'revision-id'), 'origin', name='my-name')
        info = data.value_info()
        self.assertEqual('my-name', info['name'])

    def test_value_info_contains_tags(self):
        data = DataInfo(DataRef('my-storage', 'data-id', 'revision-id'), 'origin', tags={'my': 'tag'})
        info = data.value_info()
        self.assertEqual({'my': 'tag'}, info['tags'])

    def test_value_info_contains_meta(self):
        data = DataInfo(DataRef('my-storage', 'data-id', 'revision-id'), 'origin', meta={'my': 'meta'})
        info = data.value_info()
        self.assertEqual({'my': 'meta'}, info['meta'])

    def test_value_info_contains_parent_info(self):
        parent = DataInfo(DataRef('my-storage', 'parent-id', 'revision-id'), 'origin')
        data = DataInfo(DataRef('my-storage', 'data-id', 'revision-id'), 'origin', parents=[parent])
        parent_info = parent.value_info()
        info = data.value_info()
        self.assertIn(parent_info, info['parents'])

    def test_info_size(self):
        def create_parents(level, number_parents):
            parents = []
            if level == 0:
                return []
            for index in range(number_parents):
                parents.append(
                    DataInfo(
                        DataRef('box-id', f'{level}-{index}', 'revision-id'),
                        'origin',
                        parents=create_parents(level-1, number_parents),
                    )
                )
            return parents
        data = DataInfo(DataRef('box-id', 'data-id', 'revision-id'), 'origin', parents=create_parents(10, 2))
        info = data.value_info()
        json_info = json.dumps(info)
        self.assertEqual(298867, len(json_info))

    def test_from_value_info_recreates_same_info(self):
        parent = DataInfo(DataRef('my-storage', 'parent-id', 'revision-id'), 'origin')
        data_info = DataInfo(
            DataRef('my-storage', 'data-id', 'revision-id'),
            'origin',
            parents=[parent],
            name='My name',
            tags={'my': 'tag'},
            meta={'my': 'meta'},
        )
        value_info = data_info.value_info()
        recreated_info = DataInfo.from_value_info(value_info)
        self.assertEqual(data_info.ref, recreated_info.ref)
        self.assertEqual(data_info.origin, recreated_info.origin)
        self.assertEqual(data_info.name, recreated_info.name)
        self.assertEqual(data_info.tags, recreated_info.tags)
        self.assertEqual(data_info.meta, recreated_info.meta)
        self.assertEqual(parent.ref, recreated_info.parents[0].ref)

    def test_from_value_info_supports_parents_as_data_ref(self):
        parent = DataRef('my-storage', 'parent-id', 'revision-id')
        data_info = DataInfo(
            DataRef('my-storage', 'data-id', 'revision-id'),
            'origin',
            parents=[parent],
            name='My name',
            tags={'my': 'tag'},
            meta={'my': 'meta'},
        )
        value_info = data_info.value_info()
        recreated_info = DataInfo.from_value_info(value_info)
        self.assertEqual(data_info.ref, recreated_info.ref)
        self.assertEqual(data_info.origin, recreated_info.origin)
        self.assertEqual(data_info.name, recreated_info.name)
        self.assertEqual(data_info.tags, recreated_info.tags)
        self.assertEqual(data_info.meta, recreated_info.meta)
        self.assertEqual(parent, recreated_info.parents[0])

    def test_str_returns_uri(self):
        data = DataInfo(DataRef('my-storage', 'data-id', 'revision-id'), 'origin')
        uri = data.uri
        self.assertEqual(uri, str(data))


if __name__ == '__main__':
    unittest.main()
