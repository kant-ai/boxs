import io

import unittest.mock

from boxs.box import calculate_data_id, Box
from boxs.box_registry import get_box, unregister_box
from boxs.data import DataInfo, DataRef
from boxs.errors import DataCollision, DataNotFound, MissingValueType
from boxs.storage import Storage, Writer
from boxs.value_types import StringValueType, ValueType


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


class DummyStorage(Storage):

    def delete_run(self, box_id, run_id):
        pass

    def list_items(self, item_query):
        pass

    def list_runs(self, limit=None):
        pass

    def list_items_in_run(self, run_id):
        pass

    def set_run_name(self, run_id, name):
        pass

    def __init__(self):
        self.created = set()
        self.writer = BytesIOWriter()
        self.reader = BytesIOReader(b'My content')

    def exists(self, data_ref):
        return data_ref in self.created

    def create_reader(self, data_ref):
        return self.reader

    def create_writer(self, data_ref, name=None, tags=None):
        self.created.add(data_ref)
        self.writer.ref = data_ref
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

    def read_value(self, value_type):
        return value_type.read_value_from_reader(self)


class BytesIOWriter(Writer):

    def __init__(self):
        self.ref = None
        self.info_origin = None
        self.info_parents = None
        self.info_meta = None
        self.stream = io.BytesIO()
        def stream_close():
            self.stream_closed = True
        self.stream.close = stream_close
        self.stream_closed = False
        self._meta = {}

    @property
    def meta(self):
        return self._meta

    def as_stream(self):
        return self.stream

    def write_info(self, origin, parents, meta):
        self.info_origin = origin
        self.info_parents = parents
        self.info_meta = meta
        return DataInfo(self.ref, self.info_origin)

    def write_value(self, value, value_type):
        value_type.write_value_to_writer(value, self)


class TestBox(unittest.TestCase):

    def setUp(self):
        self.storage = DummyStorage()
        self.box = Box('box-id', self.storage)
        self.data = DataInfo(
            DataRef(self.box.box_id, 'data-id', 'rev-id'),
            'origin',
            meta={'value_type': 'boxs.value_types:BytesValueType:'},
        )

    def tearDown(self):
        unregister_box(self.box.box_id)

    def test_box_registers_itself_automatically(self):
        box = get_box('box-id')
        self.assertIs(self.box, box)

    def test_store_can_use_custom_value_type(self):
        class MyClass:
            def __str__(self):
                return '<MyClass>'

        self._write_value_called = False

        class MyClassValueType(ValueType):
            def supports(value_type, value):
                return isinstance(value, MyClass)

            def write_value_to_writer(value_type, value, writer):

                self._write_value_called = True

            def read_value_from_reader(value_type, reader):
                pass

        self.box.add_value_type(MyClassValueType())
        self.box.store(MyClass(), run_id='1')
        self.assertTrue(self._write_value_called)

    def test_store_raises_if_no_supported_value_type(self):
        class MyClass:
            def __str__(self):
                return '<MyClass>'
        with self.assertRaisesRegex(MissingValueType, "No value type found for '<MyClass>'"):
            self.box.store(MyClass(), run_id='1')

    def test_store_uses_specified_value_type_if_given(self):
        value_type = unittest.mock.MagicMock()
        self.box.store('My value', origin='origin', value_type=value_type)
        value_type.write_value_to_writer.assert_called_once_with('My value', self.storage.writer)

    def test_store_writes_content_and_info(self):
        data = self.box.store(b'My content', run_id='1')
        self.assertEqual('1fd070fa88f35b3a', data.data_id)
        self.assertEqual(b'My content', self.storage.writer.stream.getvalue())
        self.assertEqual('test_store_writes_content_and_info', self.storage.writer.info_origin)
        self.assertEqual(tuple(), self.storage.writer.info_parents)
        self.assertEqual({'value_type': 'boxs.value_types:BytesValueType:'}, self.storage.writer.info_meta)

    def test_store_uses_tags_and_meta(self):
        self.box.store('My content', tags={'my': 'tag'}, meta={'my': 'meta'}, run_id='1')
        self.assertEqual(
            {'my': 'meta', 'value_type': 'boxs.value_types:StringValueType:utf-8'},
            self.storage.writer.info_meta,
        )

    def test_store_without_origin_raises(self):
        with self.assertRaisesRegex(ValueError, "No origin given"):
            self.box.store('My content', origin=None)

    def test_storing_data_twice_with_same_origin_and_revision_fails(self):
        self.box.store('My content', run_id='1')
        with self.assertRaisesRegex(DataCollision, "Data .* already exists"):
            self.box.store('My new content', run_id='1')

    def test_store_applies_transformer(self):
        transformer = unittest.mock.MagicMock()
        value_type = unittest.mock.MagicMock()
        box = Box('box-with-transformer', self.storage, transformer)

        box.store('My value', origin='origin', value_type=value_type)

        transformer.transform_writer.assert_called_with(self.storage.writer)
        transformer.transform_writer.return_value.write_value.assert_called_once_with('My value', value_type)

    def test_store_applies_multiple_transformers(self):
        transformer1 = unittest.mock.MagicMock()
        transformer2 = unittest.mock.MagicMock()
        value_type = unittest.mock.MagicMock()
        box = Box('box-with-transformers', self.storage, transformer1, transformer2)

        box.store('My value', origin='origin', value_type=value_type)

        transformer1.transform_writer.assert_called_with(self.storage.writer)
        transformer2.transform_writer.assert_called_once_with(transformer1.transform_writer.return_value)
        transformer2.transform_writer.return_value.write_value('My value', value_type)

    def test_load_reads_content(self):
        self.storage.exists = unittest.mock.MagicMock(return_value=True)
        result = self.box.load(self.data)
        self.assertEqual(b'My content', result)

    def test_load_can_override_value_type(self):
        self.storage.exists = unittest.mock.MagicMock(return_value=True)
        result = self.box.load(self.data, value_type=StringValueType())
        self.assertEqual('My content', result)

    def test_load_raises_if_wrong_box_id(self):
        data = DataRef('wrong-box-id', 'data-id', 'rev-id')

        with self.assertRaisesRegex(ValueError, "different box id"):
            self.box.load(data)

    def test_load_raises_if_not_exists(self):
        data = DataRef('box-id', 'data-id', 'rev-id')
        self.storage.exists = unittest.mock.MagicMock(return_value=False)
        with self.assertRaisesRegex(DataNotFound, "Data data-id .* does not exist"):
            self.box.load(data)

    def test_load_applies_transformer(self):
        transformer = unittest.mock.MagicMock()
        value_type = unittest.mock.MagicMock()
        self.box.transformers = [transformer]
        self.storage.exists = unittest.mock.MagicMock(return_value=True)

        self.box.load(self.data, value_type=value_type)

        transformer.transform_reader.assert_called_with(self.storage.reader)
        transformer.transform_reader.return_value.read_value.assert_called_once_with(value_type)

    def test_load_applies_multiple_transformers_in_reverse(self):
        transformer1 = unittest.mock.MagicMock()
        transformer2 = unittest.mock.MagicMock()
        value_type = unittest.mock.MagicMock()
        self.box.transformers = [transformer1, transformer2]
        self.storage.exists = unittest.mock.MagicMock(return_value=True)

        self.box.load(self.data, value_type=value_type)

        transformer2.transform_reader.assert_called_with(self.storage.reader)
        transformer1.transform_reader.assert_called_once_with(transformer2.transform_reader.return_value)
        transformer1.transform_reader.return_value.read_value.assert_called_once_with(value_type)

    def test_info_reads_content(self):
        self.storage.exists = unittest.mock.MagicMock(return_value=True)
        ref = DataRef(self.box.box_id, 'data-id', 'rev-id')
        self.storage.reader.info['ref'] = ref.value_info()
        self.storage.reader.info['origin'] = 'my-origin'
        self.storage.reader.info['name'] = None
        self.storage.reader.info['parents'] = []
        self.storage.reader.info['tags'] = {'my': 'tag'}
        self.storage.reader.info['meta'] = {'my': 'meta'}

        info = self.box.info(ref)
        self.assertEqual(ref, info.ref)
        self.assertEqual('my-origin', info.origin)
        self.assertIsNone(info.name)
        self.assertEqual(tuple(), info.parents)
        self.assertEqual({'my': 'tag'}, info.tags)
        self.assertEqual({'my': 'meta'}, info.meta)

    def test_info_raises_if_wrong_box_id(self):
        data = DataRef('wrong-box-id', 'data-id', 'rev-id')

        with self.assertRaisesRegex(ValueError, "different box id"):
            self.box.info(data)

    def test_info_raises_if_not_exists(self):
        data = DataRef('box-id', 'data-id', 'rev-id')
        self.storage.exists = unittest.mock.MagicMock(return_value=False)
        with self.assertRaisesRegex(DataNotFound, "Data data-id .* does not exist"):
            self.box.info(data)


if __name__ == '__main__':
    unittest.main()
