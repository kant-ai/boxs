import json
import unittest

from datastock.data import DataItem, DataRef


class TestDataRef(unittest.TestCase):

    def test_uri_contains_run_id_if_set(self):
        data = DataRef('data-id', 'my-storage', 'my-revision')
        uri = data.uri
        self.assertEqual('stock://my-storage/data-id/my-revision', uri)

    def test_from_uri_sets_ids(self):
        uri = 'stock://my-storage/data-id/run-id'
        data_ref = DataRef.from_uri(uri)
        self.assertEqual('my-storage', data_ref.stock_id)
        self.assertEqual('data-id', data_ref.data_id)
        self.assertEqual('run-id', data_ref.run_id)

    def test_from_uri_raises_if_wrong_scheme(self):
        uri = 'not://my-storage/data-id'
        with self.assertRaisesRegex(ValueError, "Invalid scheme"):
            DataRef.from_uri(uri)


class TestData(unittest.TestCase):

    def test_data_id_is_taken_from_ref(self):
        data = DataItem(DataRef('data-id', 'my-storage', 'revision-id'), 'origin')
        self.assertEqual('data-id', data.data_id)

    def test_stock_id_is_taken_from_ref(self):
        data = DataItem(DataRef('data-id', 'my-storage', 'revision-id'), 'origin')
        self.assertEqual('my-storage', data.stock_id)

    def test_run_id_is_taken_from_ref(self):
        data = DataItem(DataRef('data-id', 'my-storage', 'revision-id'), 'origin')
        self.assertEqual('revision-id', data.run_id)

    def test_info_contains_data_id(self):
        data = DataItem(DataRef('data-id', 'my-storage', 'revision-id'), 'origin')
        info = data.info()
        self.assertEqual('data-id', info['ref']['data_id'])

    def test_info_contains_run_id(self):
        data = DataItem(DataRef('data-id', 'my-storage', 'revision-id'), 'origin')
        info = data.info()
        self.assertEqual('revision-id', info['ref']['run_id'])

    def test_info_contains_stock_id(self):
        data = DataItem(DataRef('data-id', 'my-storage', 'revision-id'), 'origin')
        info = data.info()
        self.assertEqual('my-storage', info['ref']['stock_id'])

    def test_info_contains_name(self):
        data = DataItem(DataRef('data-id', 'my-storage', 'revision-id'), 'origin', name='my-name')
        info = data.info()
        self.assertEqual('my-name', info['name'])

    def test_info_contains_tags(self):
        data = DataItem(DataRef('data-id', 'my-storage', 'revision-id'), 'origin', tags={'my': 'tag'})
        info = data.info()
        self.assertEqual({'my': 'tag'}, info['tags'])

    def test_info_contains_meta(self):
        data = DataItem(DataRef('data-id', 'my-storage', 'revision-id'), 'origin', meta={'my': 'meta'})
        info = data.info()
        self.assertEqual({'my': 'meta'}, info['meta'])

    def test_info_contains_parent_info(self):
        parent = DataItem(DataRef('parent-id', 'my-storage', 'revision-id'), 'origin')
        data = DataItem(DataRef('data-id', 'my-storage', 'revision-id'), 'origin', parents=[parent])
        parent_info = parent.info()
        info = data.info()
        self.assertIn(parent_info, info['parents'])

    def test_info_size(self):
        def create_parents(level, number_parents):
            parents = []
            if level == 0:
                return []
            for index in range(number_parents):
                parents.append(
                    DataItem(
                        DataRef(
                            f'{level}-{index}',
                            'stock-id',
                            'revision-id',
                        ),
                        'origin',
                        parents=create_parents(level-1, number_parents),
                    )
                )
            return parents
        data = DataItem(DataRef('data-id', 'stock-id', 'revision-id'), 'origin', parents=create_parents(10, 2))
        info = data.info()
        json_info = json.dumps(info)
        self.assertEqual(307055, len(json_info))


if __name__ == '__main__':
    unittest.main()
