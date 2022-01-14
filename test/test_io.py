import io
import unittest.mock

from boxs.io import DelegatingStream


class TestDelegatingStream(unittest.TestCase):

    def setUp(self):
        self.delegate = unittest.mock.MagicMock()
        self.stream = DelegatingStream(self.delegate)

    def test_close(self):
        self.stream.close()
        self.delegate.close.assert_called_once()

    def test_closed(self):
        self.delegate.closed = 'closed_value'
        result = self.stream.closed
        self.assertEqual('closed_value', result)

    def test_flush(self):
        self.stream.flush()
        self.delegate.flush.assert_called_once()

    def test_seek(self):
        self.stream.seek(100, io.SEEK_END)
        self.delegate.seek.assert_called_with(100, io.SEEK_END)

    def test_seekable(self):
        self.delegate.seekable.return_value = True
        result = self.stream.seekable()
        self.assertTrue(result)

    def test_tell(self):
        self.delegate.tell.return_value = 123
        result = self.stream.tell()
        self.assertEqual(123, result)

    def test_truncate(self):
        self.delegate.truncate.return_value = 123
        result = self.stream.truncate(321)
        self.delegate.truncate.assert_called_with(321)
        self.assertEqual(123, result)

    def test_writable(self):
        self.delegate.writable.return_value = True
        result = self.stream.writable()
        self.assertTrue(result)

    def test_readinto(self):
        self.delegate.readinto.return_value = 123
        result = self.stream.readinto(321)
        self.delegate.readinto.assert_called_with(321)
        self.assertEqual(123, result)

    def test_write(self):
        self.delegate.write.return_value = 123
        result = self.stream.write(321)
        self.delegate.write.assert_called_with(321)
        self.assertEqual(123, result)


if __name__ == '__main__':
    unittest.main()
