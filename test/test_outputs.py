import io
import json
import pathlib
import tempfile
import unittest

from datastock.outputs import as_bytes, as_file, as_json_value, as_stream, as_string


class DummyReader:
    def __init__(self):
        self.stream = io.BytesIO()
        self.closed = False
        self.stream.close = self.close
        self.meta = {}

    def close(self):
        self.closed = True

    def as_stream(self):
        return self.stream

    def set_content(self, content):
        self.stream.write(content)
        self.stream.seek(0)


class TestAsBytes(unittest.TestCase):

    def setUp(self):
        self.reader = DummyReader()

    def test_empty_reader_returns_empty_bytes(self):
        to_result = as_bytes()
        result = to_result(self.reader)
        self.assertEqual(b'', result)

    def test_to_result_read_closes_stream(self):
        to_result = as_bytes()
        result = to_result(self.reader)
        self.assertTrue(self.reader.closed)

    def test_to_result_reads_complete_content_as_string(self):
        self.reader.set_content(b'My bytes content')
        to_result = as_bytes()
        result = to_result(self.reader)
        self.assertEqual(b'My bytes content', result)


class TestAsFile(unittest.TestCase):

    def setUp(self):
        self.reader = DummyReader()
        self.file_path = pathlib.Path(tempfile.mkstemp()[1])

    def tearDown(self):
        self.file_path.unlink()

    def test_empty_reader_returns_empty_bytes(self):
        to_result = as_file(self.file_path)
        file_path = to_result(self.reader)
        result = file_path.read_bytes()
        self.assertEqual(b'', result)

    def test_to_result_read_closes_stream(self):
        to_result = as_file(self.file_path)
        file_path = to_result(self.reader)
        file_path.read_bytes()
        self.assertTrue(self.reader.closed)

    def test_to_result_reads_complete_content_as_string(self):
        self.reader.set_content(b'My bytes content')
        to_result = as_file(self.file_path)
        file_path = to_result(self.reader)
        result = file_path.read_bytes()
        self.assertEqual(b'My bytes content', result)

    def test_to_result_uses_temp_file_if_not_given(self):
        self.reader.set_content(b'My bytes content')
        to_result = as_file()
        file_path = to_result(self.reader)
        result = file_path.read_bytes()
        self.assertEqual(b'My bytes content', result)
        file_path.unlink()


class TestAsJsonValue(unittest.TestCase):

    def setUp(self):
        self.reader = DummyReader()

    def test_empty_reader_raises_decoding_error(self):
        to_result = as_json_value()
        with self.assertRaisesRegex(json.decoder.JSONDecodeError, "Expecting value"):
            to_result(self.reader)

    def test_to_result_read_closes_stream(self):
        self.reader.set_content(b'{}')
        to_result = as_json_value()
        result = to_result(self.reader)
        self.assertTrue(self.reader.closed)

    def test_to_result_parses_complete_content(self):
        self.reader.set_content(b'{"a":"1"}')
        to_result = as_json_value()
        result = to_result(self.reader)
        self.assertEqual({'a': '1'}, result)


class TestAsStream(unittest.TestCase):

    def setUp(self):
        self.reader = DummyReader()

    def test_empty_reader_returns_empty_bytes(self):
        to_result = as_stream()
        with to_result(self.reader) as stream:
            result = stream.read()
        self.assertEqual(b'', result)

    def test_to_result_read_closes_stream(self):
        to_result = as_stream()
        with to_result(self.reader) as stream:
            stream.read()
        self.assertTrue(self.reader.closed)

    def test_to_result_reads_complete_content_as_string(self):
        self.reader.set_content(b'My bytes content')
        to_result = as_stream()
        with to_result(self.reader) as stream:
            result = stream.read()
        self.assertEqual(b'My bytes content', result)


class TestAsString(unittest.TestCase):

    def setUp(self):
        self.reader = DummyReader()

    def test_empty_reader_returns_empty_string(self):
        to_result = as_string()
        result = to_result(self.reader)
        self.assertEqual('', result)

    def test_to_result_read_closes_stream(self):
        to_result = as_string()
        to_result(self.reader)
        self.assertTrue(self.reader.closed)

    def test_to_result_reads_complete_content_as_string(self):
        self.reader.set_content(b'My bytes content')
        to_result = as_string()
        result = to_result(self.reader)
        self.assertEqual('My bytes content', result)

    def test_default_encoding_is_utf8(self):
        self.reader.set_content(
            b'This is non-ascii \xc3\x96\xc3\x84\xc3\x9c\xe1\xba\x9e',
        )
        to_result = as_string()
        result = to_result(self.reader)
        self.assertEqual('This is non-ascii ÖÄÜẞ', result)

    def test_default_encoding_can_be_specified(self):
        self.reader.set_content(
            b'\xff\xfeM\x00U\x00L\x00T\x00I\x00B\x00Y\x00T\x00E\x00',
        )
        to_result = as_string(default_encoding='utf-16')
        result = to_result(self.reader)
        self.assertEqual('MULTIBYTE', result)

    def test_encoding_is_used_from_meta_if_exists(self):
        self.reader.set_content(
            b'\xff\xfeM\x00U\x00L\x00T\x00I\x00B\x00Y\x00T\x00E\x00',
        )
        self.reader.meta['encoding'] = 'utf-16'
        to_result = as_string()
        result = to_result(self.reader)
        self.assertEqual('MULTIBYTE', result)


if __name__ == '__main__':
    unittest.main()
