import io

import unittest.mock

from boxs.data import DataRef
from boxs.errors import DataCollision, DataNotFound
from boxs.box import calculate_data_id, Box
from boxs.storage import Writer


class TestCalculateDataId(unittest.TestCase):

    def test_data_id_depends_on_origin(self):
        data_id1 = calculate_data_id(origin='my.data')
        data_id2 = calculate_data_id(origin='my.other.data')
        self.assertNotEqual(data_id1, data_id2)

    def test_data_id_depends_on_parents(self):
        data_id1 = calculate_data_id(parent_ids=['parent_id1'], origin='my.data')
        data_id2 = calculate_data_id(parent_ids=['parent_id2'], origin='my.data')
        self.assertNotEqual(data_id1, data_id2)

    def test_data_id_independent_of_parent_orders(self):
        data_id1 = calculate_data_id(parent_ids=['parent_id1', 'parent_id2'], origin='my.data')
        data_id2 = calculate_data_id(parent_ids=['parent_id2', 'parent_id1'], origin='my.data')
        self.assertEqual(data_id1, data_id2)


class DummyStorage:
    def __init__(self):
        self.created = set()
        self.writer = BytesIOWriter()
        self.reader = BytesIOReader(b'My content')

    def exists(self, data_id, run_id):
        return (data_id, run_id) in self.created

    def create_reader(self, data_id, run_id):
        return self.reader

    def create_writer(self, data_id, run_id):
        self.created.add((data_id, run_id))
        return self.writer


class BytesIOReader:

    def read_info(self):
        return self.info

    def __init__(self, content):
        self.info = {}
        self.meta = {}
        self.stream = io.BytesIO(content)

    def as_stream(self):
        return self.stream

    def read_content(self, output):
        return output(self)


class BytesIOWriter(Writer):

    def __init__(self):
        self.info = {}
        self.stream = io.BytesIO()
        self._meta = {}

    @property
    def meta(self):
        return self._meta

    def as_stream(self):
        return self.stream

    def write_info(self, info):
        self.info = info

    def write_content(self, input):
        input(self)


class TestBox(unittest.TestCase):

    def setUp(self):
        self.register_patcher = unittest.mock.patch('boxs.box.register_box')
        self.register_mock = self.register_patcher.start()
        self.storage = DummyStorage()
        self.box = Box('box-id', self.storage)

    def tearDown(self):
        self.register_patcher.stop()

    def test_box_registers_itself_automatically(self):
        self.register_mock.assert_called_with(self.box)

    def test_store_writes_content_and_info(self):

        def from_content(writer):
            writer.as_stream().write(b"My content")
            self.assertIsNotNone(writer)

        data = self.box.store(from_content, run_id='1')
        self.assertEqual('df854a08d6f482a0', data.data_id)
        self.assertEqual(b'My content', self.storage.writer.stream.getvalue())
        self.assertEqual({
            'ref': {
                'data_id': 'df854a08d6f482a0',
                'box_id': 'box-id',
                'run_id': '1',
            },
            'meta': {},
            'name': None,
            'parents': [],
            'origin': 'test_store_writes_content_and_info',
            'tags': {},
        }, self.storage.writer.info)

    def test_store_uses_tags_and_meta(self):

        def from_content(writer):
            writer.as_stream().write(b"My content")
            self.assertIsNotNone(writer)

        self.box.store(from_content, tags={'my': 'tag'}, meta={'my': 'meta'}, run_id='1')
        self.assertEqual({
            'ref': {
                'data_id': '6b9507ecd44bd3f2',
                'box_id': 'box-id',
                'run_id': '1',
            },
            'meta': {'my': 'meta'},
            'name': None,
            'parents': [],
            'origin': 'test_store_uses_tags_and_meta',
            'tags': {'my': 'tag'},
        }, self.storage.writer.info)

    def test_store_without_origin_raises(self):
        def from_content(writer):
            pass
        with self.assertRaisesRegex(ValueError, "No origin given"):
            self.box.store(from_content, origin=None)

    def test_storing_same_data_twice_with_same_revision_fails(self):
        def from_content(writer):
            writer.as_stream().write(b"My content")
            self.assertIsNotNone(writer)

        self.box.store(from_content, run_id='1')
        with self.assertRaisesRegex(DataCollision, "Data .* already exists"):
            self.box.store(from_content, run_id='1')

    def test_store_applies_transformer(self):
        input = unittest.mock.MagicMock()
        transformer = unittest.mock.MagicMock()
        box = Box('box-with-transformer', self.storage, transformer)

        box.store(input, origin='origin')

        transformer.transform_writer.assert_called_with(self.storage.writer)
        transformer.transform_writer.return_value.write_content.assert_called_once_with(input)

    def test_store_applies_multiple_transformers(self):
        input = unittest.mock.MagicMock()
        transformer1 = unittest.mock.MagicMock()
        transformer2 = unittest.mock.MagicMock()
        box = Box('box-with-transformers', self.storage, transformer1, transformer2)

        box.store(input, origin='origin')

        transformer1.transform_writer.assert_called_with(self.storage.writer)
        transformer2.transform_writer.assert_called_once_with(transformer1.transform_writer.return_value)
        transformer2.transform_writer.return_value.write_content.assert_called_once_with(input)

    def test_load_reads_content(self):
        self.storage.exists = unittest.mock.MagicMock(return_value=True)
        result = []

        def dest(writer):
            result.append(writer.as_stream().read())

        data = DataRef('data-id', self.box.box_id, 'rev-id')

        self.box.load(dest, data)
        self.assertEqual(b'My content', result[0])

    def test_load_raises_if_wrong_box_id(self):
        data = DataRef('data-id', 'wrong-box-id', 'rev-id')

        with self.assertRaisesRegex(ValueError, "different box id"):
            self.box.load(None, data)

    def test_load_raises_if_not_exists(self):
        data = DataRef('data-id', 'box-id', 'rev-id')
        self.storage.exists = unittest.mock.MagicMock(return_value=False)
        with self.assertRaisesRegex(DataNotFound, "Data data-id .* does not exist"):
            self.box.load(None, data)

    def test_load_applies_transformer(self):
        dest = unittest.mock.MagicMock()
        transformer = unittest.mock.MagicMock()
        box = Box('box-with-transformer', self.storage, transformer)

        self.storage.exists = unittest.mock.MagicMock(return_value=True)
        data = DataRef('data-id', box.box_id, 'rev-id')

        box.load(dest, data)

        transformer.transform_reader.assert_called_with(self.storage.reader)
        transformer.transform_reader.return_value.read_content.assert_called_once_with(dest)

    def test_load_applies_multiple_transformers_in_reverse(self):
        dest = unittest.mock.MagicMock()
        transformer1 = unittest.mock.MagicMock()
        transformer2 = unittest.mock.MagicMock()
        box = Box('box-with-transformers', self.storage, transformer1, transformer2)

        self.storage.exists = unittest.mock.MagicMock(return_value=True)
        data = DataRef('data-id', box.box_id, 'rev-id')

        box.load(dest, data)

        transformer2.transform_reader.assert_called_with(self.storage.reader)
        transformer1.transform_reader.assert_called_once_with(transformer2.transform_reader.return_value)
        transformer1.transform_reader.return_value.read_content.assert_called_once_with(dest)

    def test_info_reads_content(self):
        self.storage.exists = unittest.mock.MagicMock(return_value=True)
        self.storage.reader.info['my'] = 'info'

        data = DataRef('data-id', self.box.box_id, 'rev-id')

        result = self.box.info(data)
        self.assertEqual({'my': 'info'}, result)

    def test_info_raises_if_wrong_box_id(self):
        data = DataRef('data-id', 'wrong-box-id', 'rev-id')

        with self.assertRaisesRegex(ValueError, "different box id"):
            self.box.info(data)

    def test_info_raises_if_not_exists(self):
        data = DataRef('data-id', 'box-id', 'rev-id')
        self.storage.exists = unittest.mock.MagicMock(return_value=False)
        with self.assertRaisesRegex(DataNotFound, "Data data-id .* does not exist"):
            self.box.info(data)


if __name__ == '__main__':
    unittest.main()
