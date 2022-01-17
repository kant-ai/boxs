import io
import json
import pathlib
import tempfile
import unittest

from boxs.value_types import (
    BytesValueType, FileValueType, StreamValueType, StringValueType, JsonValueType,
    ValueType,
)


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


class TestBytesValueType(unittest.TestCase):

    def setUp(self):
        self.reader = DummyReader()
        self.writer = DummyWriter()

    def test_empty_reader_returns_empty_bytes(self):
        value_type = BytesValueType()
        result = value_type.read_value_from_reader(self.reader)
        self.assertEqual(b'', result)

    def test_to_result_read_closes_stream(self):
        value_type = BytesValueType()
        value_type.read_value_from_reader(self.reader)
        self.assertTrue(self.reader.closed)

    def test_to_result_reads_complete_content_as_string(self):
        self.reader.set_content(b'My bytes content')
        value_type = BytesValueType()
        result = value_type.read_value_from_reader(self.reader)
        self.assertEqual(b'My bytes content', result)

    def test_empty_value_writes_nothing(self):
        value_type = BytesValueType()
        value_type.write_value_to_writer(b'', self.writer)
        self.assertEqual(b'', self.writer.content)

    def test_value_write_closes_stream(self):
        value_type = BytesValueType()
        value_type.write_value_to_writer(b'', self.writer)
        self.assertTrue(self.writer.closed)

    def test_value_writes_complete_content(self):
        value_type = BytesValueType()
        value_type.write_value_to_writer(
            b'This is a bytes string' * 1000,
            self.writer,
        )
        self.assertEqual(b'This is a bytes string' * 1000, self.writer.content)

    def test_get_specification_returns_class_and_module_name(self):
        value_type = BytesValueType()
        specification = value_type.get_specification()
        self.assertEqual('boxs.value_types:BytesValueType:', specification)

    def test_create_from_specification_can_create_from_specification(self):
        value_type = BytesValueType()
        specification = value_type.get_specification()
        recreated_value_type = ValueType.from_specification(specification)
        self.assertIsInstance(recreated_value_type, BytesValueType)

    def test_repr_returns_specification(self):
        value_type = BytesValueType()
        result = repr(value_type)
        self.assertEqual('boxs.value_types:BytesValueType:', result)

    def test_str_returns_specification(self):
        value_type = BytesValueType()
        result = str(value_type)
        self.assertEqual('boxs.value_types:BytesValueType:', result)


class TestFileValueType(unittest.TestCase):

    def setUp(self):
        self.reader = DummyReader()
        self.writer = DummyWriter()
        self.file_path = pathlib.Path(tempfile.mkstemp()[1])

    def tearDown(self):
        self.file_path.unlink()

    def test_empty_reader_returns_empty_bytes(self):
        value_type = FileValueType(file_path=self.file_path)
        file_path = value_type.read_value_from_reader(self.reader)
        result = file_path.read_bytes()
        self.assertEqual(b'', result)

    def test_reader_uses_configured_file_path(self):
        value_type = FileValueType(file_path=self.file_path)
        file_path = value_type.read_value_from_reader(self.reader)
        self.assertEqual(file_path, self.file_path)

    def test_to_result_read_closes_stream(self):
        value_type = FileValueType(file_path=self.file_path)
        file_path = value_type.read_value_from_reader(self.reader)
        file_path.read_bytes()
        self.assertTrue(self.reader.closed)

    def test_to_result_reads_complete_content_as_bytes(self):
        self.reader.set_content(b'My bytes content')
        value_type = FileValueType(file_path=self.file_path)
        file_path = value_type.read_value_from_reader(self.reader)
        result = file_path.read_bytes()
        self.assertEqual(b'My bytes content', result)

    def test_to_result_uses_temp_file_if_not_given(self):
        self.reader.set_content(b'My bytes content')
        value_type = FileValueType()
        file_path = value_type.read_value_from_reader(self.reader)
        result = file_path.read_bytes()
        self.assertEqual(b'My bytes content', result)
        file_path.unlink()

    def test_empty_value_writes_nothing(self):
        value_type = FileValueType()
        value_type.write_value_to_writer(self.file_path, self.writer)
        self.assertEqual(b'', self.writer.content)

    def test_value_write_closes_stream(self):
        value_type = FileValueType()
        value_type.write_value_to_writer(self.file_path, self.writer)
        self.assertTrue(self.writer.closed)

    def test_value_writes_complete_content(self):
        self.file_path.write_text('This is a bytes string' * 1000)
        value_type = FileValueType()
        value_type.write_value_to_writer(self.file_path, self.writer)
        self.assertEqual(b'This is a bytes string' * 1000, self.writer.content)

    def test_get_specification_returns_class_and_module_name(self):
        value_type = FileValueType()
        specification = value_type.get_specification()
        self.assertEqual('boxs.value_types:FileValueType:', specification)

    def test_create_from_specification_can_create_from_specification(self):
        value_type = FileValueType()
        specification = value_type.get_specification()
        recreated_value_type = ValueType.from_specification(specification)
        self.assertIsInstance(recreated_value_type, FileValueType)

    def test_repr_returns_specification(self):
        value_type = FileValueType()
        result = repr(value_type)
        self.assertEqual('boxs.value_types:FileValueType:', result)

    def test_str_returns_specification(self):
        value_type = FileValueType()
        result = str(value_type)
        self.assertEqual('boxs.value_types:FileValueType:', result)


class TestStreamValueType(unittest.TestCase):

    def setUp(self):
        self.reader = DummyReader()
        self.writer = DummyWriter()

    def test_empty_reader_returns_empty_bytes(self):
        value_type = StreamValueType()
        stream = value_type.read_value_from_reader(self.reader)
        with stream:
            result = stream.read()
        self.assertEqual(b'', result)

    def test_to_result_read_closes_stream(self):
        value_type = StreamValueType()
        stream = value_type.read_value_from_reader(self.reader)
        with stream:
            stream.read()
        self.assertTrue(self.reader.closed)

    def test_to_result_reads_complete_content_as_string(self):
        self.reader.set_content(b'My bytes content')
        value_type = StreamValueType()
        stream = value_type.read_value_from_reader(self.reader)
        with stream:
            result = stream.read()
        self.assertEqual(b'My bytes content', result)

    def test_empty_value_writes_nothing(self):
        value_type = StreamValueType()
        value_type.write_value_to_writer(io.BytesIO(), self.writer)
        self.assertEqual(b'', self.writer.content)

    def test_value_write_closes_stream(self):
        value_type = StreamValueType()
        value_type.write_value_to_writer(io.BytesIO(), self.writer)
        self.assertTrue(self.writer.closed)

    def test_value_writes_complete_content(self):
        value_type = StreamValueType()
        value_type.write_value_to_writer(io.BytesIO(b'This is a bytes string' * 1000), self.writer)
        self.assertEqual(b'This is a bytes string' * 1000, self.writer.content)

    def test_get_specification_returns_class_and_module_name(self):
        value_type = StreamValueType()
        specification = value_type.get_specification()
        self.assertEqual('boxs.value_types:StreamValueType:', specification)

    def test_create_from_specification_can_create_from_specification(self):
        value_type = StreamValueType()
        specification = value_type.get_specification()
        recreated_value_type = ValueType.from_specification(specification)
        self.assertIsInstance(recreated_value_type, StreamValueType)

    def test_repr_returns_specification(self):
        value_type = StreamValueType()
        result = repr(value_type)
        self.assertEqual('boxs.value_types:StreamValueType:', result)

    def test_str_returns_specification(self):
        value_type = StreamValueType()
        result = str(value_type)
        self.assertEqual('boxs.value_types:StreamValueType:', result)


class TestStringValueType(unittest.TestCase):

    def setUp(self):
        self.reader = DummyReader()
        self.writer = DummyWriter()

    def test_empty_reader_returns_empty_string(self):
        value_type = StringValueType()
        result = value_type.read_value_from_reader(self.reader)
        self.assertEqual('', result)

    def test_to_result_read_closes_stream(self):
        value_type = StringValueType()
        result = value_type.read_value_from_reader(self.reader)
        self.assertTrue(self.reader.closed)

    def test_to_result_reads_complete_content_as_string(self):
        self.reader.set_content(b'My bytes content')
        value_type = StringValueType()
        result = value_type.read_value_from_reader(self.reader)
        self.assertEqual('My bytes content', result)

    def test_reader_default_encoding_is_utf8(self):
        self.reader.set_content(
            b'This is non-ascii \xc3\x96\xc3\x84\xc3\x9c\xe1\xba\x9e',
        )
        value_type = StringValueType()
        result = value_type.read_value_from_reader(self.reader)
        self.assertEqual('This is non-ascii ÖÄÜẞ', result)

    def test_default_encoding_can_be_specified(self):
        self.reader.set_content(
            b'\xff\xfeM\x00U\x00L\x00T\x00I\x00B\x00Y\x00T\x00E\x00',
        )
        value_type = StringValueType(default_encoding='utf-16')
        result = value_type.read_value_from_reader(self.reader)
        self.assertEqual('MULTIBYTE', result)

    def test_encoding_is_used_from_meta_if_exists(self):
        self.reader.set_content(
            b'\xff\xfeM\x00U\x00L\x00T\x00I\x00B\x00Y\x00T\x00E\x00',
        )
        self.reader.meta['encoding'] = 'utf-16'
        value_type = StringValueType()
        result = value_type.read_value_from_reader(self.reader)
        self.assertEqual('MULTIBYTE', result)

    def test_empty_value_writes_nothing(self):
        value_type = StringValueType()
        value_type.write_value_to_writer('', self.writer)
        self.assertEqual(b'', self.writer.content)

    def test_value_write_closes_stream(self):
        value_type = StringValueType()
        value_type.write_value_to_writer('', self.writer)
        self.assertTrue(self.writer.closed)

    def test_value_writes_complete_content(self):
        value_type = StringValueType()
        value_type.write_value_to_writer('This is a bytes string' * 1000, self.writer)
        self.assertEqual(b'This is a bytes string' * 1000, self.writer.content)

    def test_default_encoding_is_utf8(self):
        value_type = StringValueType()
        value_type.write_value_to_writer('This is non-ascii ÖÄÜẞ', self.writer)
        self.assertEqual(
            b'This is non-ascii \xc3\x96\xc3\x84\xc3\x9c\xe1\xba\x9e',
            self.writer.content
        )
        self.assertEqual('utf-8', self.writer.meta['encoding'])

    def test_encoding_can_be_specified(self):
        value_type = StringValueType(default_encoding='utf-16')
        value_type.write_value_to_writer('MULTIBYTE', self.writer)
        self.assertEqual(
            b'\xff\xfeM\x00U\x00L\x00T\x00I\x00B\x00Y\x00T\x00E\x00',
            self.writer.content
        )
        self.assertEqual('utf-16', self.writer.meta['encoding'])

    def test_get_specification_returns_class_and_module_name(self):
        value_type = StringValueType()
        specification = value_type.get_specification()
        self.assertEqual('boxs.value_types:StringValueType:utf-8', specification)

    def test_create_from_specification_can_create_from_specification(self):
        value_type = StringValueType()
        specification = value_type.get_specification()
        recreated_value_type = ValueType.from_specification(specification)
        self.assertIsInstance(recreated_value_type, StringValueType)

    def test_repr_returns_specification(self):
        value_type = StringValueType()
        result = repr(value_type)
        self.assertEqual('boxs.value_types:StringValueType:utf-8', result)

    def test_str_returns_specification(self):
        value_type = StringValueType()
        result = str(value_type)
        self.assertEqual('boxs.value_types:StringValueType:utf-8', result)

    def test_can_be_recreated_with_non_default_encoding(self):
        value_type = StringValueType(default_encoding='utf-16')
        specification = value_type.get_specification()
        recreated_value_type = ValueType.from_specification(specification)
        self.assertEqual(value_type._default_encoding, recreated_value_type._default_encoding)


class TestJsonValueType(unittest.TestCase):

    def setUp(self):
        self.reader = DummyReader()
        self.writer = DummyWriter()

    def test_supports_returns_false_even_though_it_supports_strings(self):
        value_type = JsonValueType()
        support = value_type.supports('My string')
        self.assertFalse(support)

    def test_empty_reader_raises_decoding_error(self):
        value_type = JsonValueType()
        with self.assertRaisesRegex(json.decoder.JSONDecodeError, "Expecting value"):
            value_type.read_value_from_reader(self.reader)

    def test_to_result_read_closes_stream(self):
        self.reader.set_content(b'{}')
        value_type = JsonValueType()
        value_type.read_value_from_reader(self.reader)
        self.assertTrue(self.reader.closed)

    def test_to_result_parses_complete_content(self):
        self.reader.set_content(b'{"a":"1"}')
        value_type = JsonValueType()
        result = value_type.read_value_from_reader(self.reader)
        self.assertEqual({'a': '1'}, result)

    def test_empty_string_writes_json_string(self):
        value_type = JsonValueType()
        value_type.write_value_to_writer('', self.writer)
        self.assertEqual(b'""', self.writer.content)

    def test_value_write_closes_stream(self):
        value_type = JsonValueType()
        value_type.write_value_to_writer('', self.writer)
        self.assertTrue(self.writer.closed)

    def test_value_sets_media_type_in_meta(self):
        value_type = JsonValueType()
        value_type.write_value_to_writer('', self.writer)
        self.assertEqual('application/json', self.writer.meta['media_type'])

    def test_value_writes_complete_content(self):
        value_type = JsonValueType()
        value_type.write_value_to_writer('This is a bytes string' * 1000, self.writer)
        self.assertEqual(b'"' + b'This is a bytes string' * 1000 + b'"', self.writer.content)

    def test_value_writes_dicts_as_object(self):
        value_type = JsonValueType()
        value_type.write_value_to_writer({'my': 'object'}, self.writer)
        self.assertEqual(b'{"my":"object"}', self.writer.content)

    def test_value_writes_lists_as_json_lists(self):
        value_type = JsonValueType()
        value_type.write_value_to_writer(['my', 'values'], self.writer)
        self.assertEqual(b'["my","values"]', self.writer.content)

    def test_get_specification_returns_class_and_module_name(self):
        value_type = JsonValueType()
        specification = value_type.get_specification()
        self.assertEqual('boxs.value_types:JsonValueType:', specification)

    def test_create_from_specification_can_create_from_specification(self):
        value_type = JsonValueType()
        specification = value_type.get_specification()
        recreated_value_type = ValueType.from_specification(specification)
        self.assertIsInstance(recreated_value_type, JsonValueType)

    def test_repr_returns_specification(self):
        value_type = JsonValueType()
        result = repr(value_type)
        self.assertEqual('boxs.value_types:JsonValueType:', result)

    def test_str_returns_specification(self):
        value_type = JsonValueType()
        result = str(value_type)
        self.assertEqual('boxs.value_types:JsonValueType:', result)


if __name__ == '__main__':
    unittest.main()
