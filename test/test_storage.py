import unittest.mock

from boxs.data import DataRef
from boxs.storage import DelegatingReader, DelegatingWriter, ItemQuery, Reader, Transformer, Writer


class TestItemQuery(unittest.TestCase):

    def test_single_string_is_only_run(self):
        query = ItemQuery('run-id')
        self.assertIsNotNone(query.run)
        self.assertEqual(query.run, 'run-id')
        self.assertIsNone(query.data)
        self.assertIsNone(query.box)

    def test_single_string_with_trailing_colon_is_only_data(self):
        query = ItemQuery('data-id:')
        self.assertIsNotNone(query.data)
        self.assertEqual(query.data, 'data-id')
        self.assertIsNone(query.run)
        self.assertIsNone(query.box)

    def test_single_string_with_leading_colon_is_only_run(self):
        query = ItemQuery(':run-id')
        self.assertIsNotNone(query.run)
        self.assertEqual(query.run, 'run-id')
        self.assertIsNone(query.data)
        self.assertIsNone(query.box)

    def test_single_string_with_leading_and_trailing_colon_is_only_data(self):
        query = ItemQuery(':data-id:')
        self.assertIsNotNone(query.data)
        self.assertEqual(query.data, 'data-id')
        self.assertIsNone(query.run)
        self.assertIsNone(query.box)

    def test_empty_query_raises_value_error(self):
        with self.assertRaisesRegex(ValueError, "Neither, box, data or run is specified."):
            ItemQuery('::')
        with self.assertRaisesRegex(ValueError, "Neither, box, data or run is specified."):
            ItemQuery(':')
        with self.assertRaisesRegex(ValueError, "Neither, box, data or run is specified."):
            ItemQuery('')
        with self.assertRaisesRegex(ValueError, "Neither, box, data or run is specified."):
            ItemQuery('    ')

    def test_more_than_2_colons_raises_value_error(self):
        with self.assertRaisesRegex(ValueError, "Invalid query"):
            ItemQuery('something:box:data:run')

    def test_str(self):
        query = ItemQuery('box-id:data-id:run-id')
        self.assertEqual('box-id:data-id:run-id', str(query))

        query = ItemQuery(':data-id:run-id')
        self.assertEqual(':data-id:run-id', str(query))
        query = ItemQuery('data-id:run-id')
        self.assertEqual(':data-id:run-id', str(query))

        query = ItemQuery('::run-id')
        self.assertEqual('::run-id', str(query))
        query = ItemQuery(':run-id')
        self.assertEqual('::run-id', str(query))
        query = ItemQuery('run-id')
        self.assertEqual('::run-id', str(query))

        query = ItemQuery(':data-id:')
        self.assertEqual(':data-id:', str(query))
        query = ItemQuery('data-id:')
        self.assertEqual(':data-id:', str(query))

    def test_from_fields(self):
        query = ItemQuery.from_fields(box='box-id', data='data-id', run='run-id')
        self.assertEqual('box-id:data-id:run-id', str(query))

        query = ItemQuery.from_fields(data='data-id', run='run-id')
        self.assertEqual(':data-id:run-id', str(query))

        query = ItemQuery.from_fields(run='run-id')
        self.assertEqual('::run-id', str(query))

        query = ItemQuery.from_fields(data='data-id')
        self.assertEqual(':data-id:', str(query))


class ReaderImplementation(Reader):

    @property
    def info(self):
        pass

    @property
    def meta(self):
        pass

    def as_stream(self):
        pass


class WriterImplementation(Writer):

    def write_info(self, info):
        pass

    @property
    def info(self):
        pass

    @property
    def meta(self):
        pass

    def as_stream(self):
        pass


class TestReader(unittest.TestCase):

    def setUp(self):
        self.data_ref = DataRef('box_id', 'data-id', 'run-id')
        self.reader = ReaderImplementation(self.data_ref)

    def test_data_ref_is_taken_from_constructor(self):
        result = self.reader.item
        self.assertEqual(self.data_ref, result)

    def test_read_value_calls_method_on_value_type(self):
        value_type = unittest.mock.MagicMock()
        self.reader.read_value(value_type)
        value_type.read_value_from_reader.assert_called_once_with(self.reader)


class TestWriter(unittest.TestCase):

    def setUp(self):
        self.data_ref = DataRef('box_id', 'data-id', 'run-id')
        self.writer = WriterImplementation(self.data_ref, None, {})

    def test_data_ref_is_taken_from_constructor(self):
        result = self.writer.item
        self.assertEqual(self.data_ref, result)

    def test_read_value_calls_method_on_value_type(self):
        value_type = unittest.mock.MagicMock()
        self.writer.write_value('my value', value_type)
        value_type.write_value_to_writer.assert_called_once_with('my value', self.writer)


class TestDelegatingReader(unittest.TestCase):

    def setUp(self):
        self.delegate = unittest.mock.MagicMock()
        self.delegate.item = 'data-id'
        self.reader = DelegatingReader(self.delegate)

    def test_item_is_same_as_delegated(self):
        result = self.reader.item
        self.assertEqual('data-id', result)

    def test_info_is_delegated(self):
        self.delegate.info = {'my': 'info'}
        result = self.reader.info
        self.assertEqual({'my': 'info'}, result)

    def test_meta_is_delegated(self):
        self.delegate.meta = {'my': 'meta'}
        result = self.reader.meta
        self.assertEqual({'my': 'meta'}, result)

    def test_as_stream_is_delegated(self):
        self.delegate.as_stream.return_value = 'stream'
        result = self.reader.as_stream()
        self.assertEqual('stream', result)
        self.delegate.as_stream.assert_called_once()

    def test_read_value_is_delegated(self):
        self.reader.read_value('value-type')
        self.delegate.read_value.assert_called_once_with('value-type')


class TestDelegatingWriter(unittest.TestCase):

    def setUp(self):
        self.delegate = unittest.mock.MagicMock()
        self.delegate.item = 'data-id'
        self.writer = DelegatingWriter(self.delegate)

    def test_item_is_delegated(self):
        result = self.writer.item
        self.assertEqual('data-id', result)

    def test_meta_is_delegated(self):
        self.delegate.meta = {'my': 'meta'}
        result = self.writer.meta
        self.assertEqual({'my': 'meta'}, result)

    def test_write_info_is_delegated(self):
        self.delegate.write_info.return_value = 'DataInfo'
        result = self.writer.write_info('origin', [], {'my': 'info'})
        self.delegate.write_info.assert_called_with('origin', [], {'my': 'info'})
        self.assertEqual('DataInfo', result)

    def test_as_stream_is_delegated(self):
        self.delegate.as_stream.return_value = 'stream'
        result = self.writer.as_stream()
        self.assertEqual('stream', result)
        self.delegate.as_stream.assert_called_once()

    def test_write_value_is_delegated(self):
        self.writer.write_value('my value', 'value-type')
        self.delegate.write_value.assert_called_with('my value', 'value-type')


class TestTransformer(unittest.TestCase):

    def setUp(self):
        self.transformer = Transformer()

    def test_transform_writer_doesnt_change_writer_as_default(self):
        writer_mock = {}
        result = self.transformer.transform_writer(writer_mock)
        self.assertIs(writer_mock, result)

    def test_transform_reader_doesnt_change_reader_as_default(self):
        reader_mock = {}
        result = self.transformer.transform_reader(reader_mock)
        self.assertIs(reader_mock, result)


if __name__ == '__main__':
    unittest.main()
