import unittest.mock

from boxs.storage import DelegatingReader, DelegatingWriter, Transformer


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

    def test_read_content_is_delegated(self):
        self.delegate.read_content.return_value = 'content'
        result = self.reader.read_content('output')
        self.assertEqual('content', result)
        self.delegate.read_content.assert_called_with('output')

    def test_as_stream_is_delegated(self):
        self.delegate.as_stream.return_value = 'stream'
        result = self.reader.as_stream()
        self.assertEqual('stream', result)
        self.delegate.as_stream.assert_called_once()


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

    def test_write_content_is_delegated(self):
        self.writer.write_content('output')
        self.delegate.write_content.assert_called_with('output')

    def test_write_content_is_delegated(self):
        self.writer.write_content('output')
        self.delegate.write_content.assert_called_with('output')

    def test_write_info_is_delegated(self):
        self.writer.write_info({'my': 'info'})
        self.delegate.write_info.assert_called_with({'my': 'info'})

    def test_as_stream_is_delegated(self):
        self.delegate.as_stream.return_value = 'stream'
        result = self.writer.as_stream()
        self.assertEqual('stream', result)
        self.delegate.as_stream.assert_called_once()


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
