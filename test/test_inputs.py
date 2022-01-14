import io
import pathlib
import tempfile
import unittest

from boxs.inputs import from_bytes, from_file, from_stream, from_string, from_value_as_json


class DummyWriter:
    def __init__(self):
        self.stream = io.BytesIO()
        self.closed = False
        self.stream.close = self.close
        self.meta = {}

    def close(self):
        self.closed = True

    def as_stream(self):
        return self.stream

    @property
    def content(self):
        return self.stream.getvalue()


class TestFromBytes(unittest.TestCase):

    def setUp(self):
        self.writer = DummyWriter()

    def test_empty_input_writes_nothing(self):
        write = from_bytes(b'')
        write(self.writer)
        self.assertEqual(b'', self.writer.content)

    def test_input_write_closes_stream(self):
        write = from_bytes(b'')
        write(self.writer)
        self.assertTrue(self.writer.closed)

    def test_input_writes_complete_content(self):
        write = from_bytes(b'This is a bytes string' * 1000)
        write(self.writer)
        self.assertEqual(b'This is a bytes string' * 1000, self.writer.content)


class TestFromFile(unittest.TestCase):

    def setUp(self):
        self.writer = DummyWriter()
        self.file_path = pathlib.Path(tempfile.mkstemp()[1])

    def tearDown(self):
        self.file_path.unlink()

    def test_empty_input_writes_nothing(self):
        write = from_file(self.file_path)
        write(self.writer)
        self.assertEqual(b'', self.writer.content)

    def test_input_write_closes_stream(self):
        write = from_file(self.file_path)
        write(self.writer)
        self.assertTrue(self.writer.closed)

    def test_input_writes_complete_content(self):
        self.file_path.write_text('This is a bytes string' * 1000)
        write = from_file(self.file_path)
        write(self.writer)
        self.assertEqual(b'This is a bytes string' * 1000, self.writer.content)


class TestFromStream(unittest.TestCase):

    def setUp(self):
        self.writer = DummyWriter()

    def test_empty_input_writes_nothing(self):
        write = from_stream(io.BytesIO())
        write(self.writer)
        self.assertEqual(b'', self.writer.content)

    def test_input_write_closes_stream(self):
        write = from_stream(io.BytesIO())
        write(self.writer)
        self.assertTrue(self.writer.closed)

    def test_input_writes_complete_content(self):
        write = from_stream(io.BytesIO(b'This is a bytes string' * 1000))
        write(self.writer)
        self.assertEqual(b'This is a bytes string' * 1000, self.writer.content)


class TestFromString(unittest.TestCase):

    def setUp(self):
        self.writer = DummyWriter()

    def test_empty_input_writes_nothing(self):
        write = from_string('')
        write(self.writer)
        self.assertEqual(b'', self.writer.content)

    def test_input_write_closes_stream(self):
        write = from_string('')
        write(self.writer)
        self.assertTrue(self.writer.closed)

    def test_input_writes_complete_content(self):
        write = from_string('This is a bytes string' * 1000)
        write(self.writer)
        self.assertEqual(b'This is a bytes string' * 1000, self.writer.content)

    def test_default_encoding_is_utf8(self):
        write = from_string('This is non-ascii ÖÄÜẞ')
        write(self.writer)
        self.assertEqual(
            b'This is non-ascii \xc3\x96\xc3\x84\xc3\x9c\xe1\xba\x9e',
            self.writer.content
        )
        self.assertEqual('utf-8', self.writer.meta['encoding'])

    def test_encoding_can_be_specified(self):
        write = from_string('MULTIBYTE', encoding='utf-16')
        write(self.writer)
        self.assertEqual(
            b'\xff\xfeM\x00U\x00L\x00T\x00I\x00B\x00Y\x00T\x00E\x00',
            self.writer.content
        )
        self.assertEqual('utf-16', self.writer.meta['encoding'])


class TestFromValueAsJson(unittest.TestCase):

    def setUp(self):
        self.writer = DummyWriter()

    def test_empty_string_writes_json_string(self):
        write = from_value_as_json('')
        write(self.writer)
        self.assertEqual(b'""', self.writer.content)

    def test_input_write_closes_stream(self):
        write = from_value_as_json('')
        write(self.writer)
        self.assertTrue(self.writer.closed)

    def test_input_sets_media_type_in_meta(self):
        write = from_value_as_json('')
        write(self.writer)
        self.assertEqual('application/json', self.writer.meta['media_type'])

    def test_input_writes_complete_content(self):
        write = from_value_as_json('This is a bytes string' * 1000)
        write(self.writer)
        self.assertEqual(b'"' + b'This is a bytes string' * 1000 + b'"', self.writer.content)

    def test_input_writes_dicts_as_object(self):
        write = from_value_as_json({'my': 'object'})
        write(self.writer)
        self.assertEqual(b'{"my":"object"}', self.writer.content)

    def test_input_writes_lists_as_json_lists(self):
        write = from_value_as_json(['my', 'values'])
        write(self.writer)
        self.assertEqual(b'["my","values"]', self.writer.content)


if __name__ == '__main__':
    unittest.main()
