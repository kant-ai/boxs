import unittest.mock

from boxs.data import DataRef
from boxs.storage import Item, ItemQuery, Reader, Run, Writer


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


class TestItem(unittest.TestCase):

    def test_str_representation(self):
        item = Item('box-id', 'data-id', 'run-id')
        self.assertEqual('Item(boxs://box-id/data-id/run-id)', str(item))


class TestRun(unittest.TestCase):

    def test_str_representation(self):
        run = Run('box-id', 'run-id')
        self.assertEqual('Run(box-id/run-id)', str(run))


class ReaderImplementation(Reader):

    @property
    def info(self):
        return {'my': 'info', 'meta': {'my': 'meta'}}

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

    def test_meta_returns_part_of_info(self):
        meta = self.reader.meta
        self.assertEqual({'my': 'meta'}, meta)


class TestWriter(unittest.TestCase):

    def setUp(self):
        self.data_ref = DataRef('box_id', 'data-id', 'run-id')
        self.writer = WriterImplementation(self.data_ref, None, {'my': 'tag'})

    def test_data_ref_is_taken_from_constructor(self):
        result = self.writer.item
        self.assertEqual(self.data_ref, result)

    def test_read_value_calls_method_on_value_type(self):
        value_type = unittest.mock.MagicMock()
        self.writer.write_value('my value', value_type)
        value_type.write_value_to_writer.assert_called_once_with('my value', self.writer)

    def test_tags(self):
        tags = self.writer.tags
        self.assertEqual({'my': 'tag'}, tags)


if __name__ == '__main__':
    unittest.main()
