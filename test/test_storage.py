import unittest.mock

from boxs.storage import DelegatingReader, DelegatingWriter, Reader, Transformer, Writer


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
        self.reader = ReaderImplementation('data-id', 'run-id')

    def test_data_id_is_taken_from_constructor(self):
        result = self.reader.data_id
        self.assertEqual('data-id', result)

    def test_run_id_is_taken_from_constructor(self):
        result = self.reader.run_id
        self.assertEqual('run-id', result)

    def test_read_value_calls_method_on_value_type(self):
        value_type = unittest.mock.MagicMock()
        self.reader.read_value(value_type)
        value_type.read_value_from_reader.assert_called_once_with(self.reader)


class TestWriter(unittest.TestCase):

    def setUp(self):
        self.writer = WriterImplementation('data-id', 'run-id')

    def test_data_id_is_taken_from_constructor(self):
        result = self.writer.data_id
        self.assertEqual('data-id', result)

    def test_run_id_is_taken_from_constructor(self):
        result = self.writer.run_id
        self.assertEqual('run-id', result)

    def test_read_value_calls_method_on_value_type(self):
        value_type = unittest.mock.MagicMock()
        self.writer.write_value('my value', value_type)
        value_type.write_value_to_writer.assert_called_once_with('my value', self.writer)


class TestDelegatingReader(unittest.TestCase):

    def setUp(self):
        self.delegate = unittest.mock.MagicMock()
        self.reader = DelegatingReader(self.delegate)

    def test_data_id_is_delegated(self):
        self.delegate.data_id = 'data-id'
        result = self.reader.data_id
        self.assertEqual('data-id', result)

    def test_run_id_is_delegated(self):
        self.delegate.run_id = 'run-id'
        result = self.reader.run_id
        self.assertEqual('run-id', result)

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
        self.writer = DelegatingWriter(self.delegate)

    def test_data_id_is_delegated(self):
        self.delegate.data_id = 'data-id'
        result = self.writer.data_id
        self.assertEqual('data-id', result)

    def test_run_id_is_delegated(self):
        self.delegate.run_id = 'run-id'
        result = self.writer.run_id
        self.assertEqual('run-id', result)

    def test_meta_is_delegated(self):
        self.delegate.meta = {'my': 'meta'}
        result = self.writer.meta
        self.assertEqual({'my': 'meta'}, result)

    def test_write_info_is_delegated(self):
        self.writer.write_info({'my': 'info'})
        self.delegate.write_info.assert_called_with({'my': 'info'})

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
