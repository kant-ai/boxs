import unittest.mock

from boxs.storage import Reader, Writer
from boxs.transform import DelegatingReader, DelegatingWriter, Transformer


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
        result = self.writer.write_info({'my': 'info'})
        self.delegate.write_info.assert_called_with({'my': 'info'})
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
