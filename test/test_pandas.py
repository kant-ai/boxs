import importlib.abc
import sys
import unittest

import pandas.errors

from boxs.pandas import PandasDataFrameCsvValueType
from boxs.value_types import ValueType

from .test_value_types import DummyReader, DummyWriter


class TestPandasDataFrameCsvValueType(unittest.TestCase):

    def setUp(self):
        self.reader = DummyReader()
        self.writer = DummyWriter()

    def test_warning_is_logged_if_no_pandas_available(self):
        class ForceImportErrorFinder(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path, target=None):
                if fullname == 'pandas':
                    raise ImportError("Fake: No pandas package.")
                return
        meta_finder = ForceImportErrorFinder()
        sys.meta_path.insert(0, meta_finder)
        try:
            del sys.modules['pandas']
            del sys.modules['boxs.pandas']
            with self.assertLogs('boxs.pandas', level='WARNING') as cm:
                reloaded_module = importlib.__import__('boxs.pandas')
                self.assertEqual(
                    cm.output, [
                    'WARNING:boxs.pandas:Unable to load pandas package Fake: No pandas package., '
                    'pandas specific value types are not available.',
                ])
            self.assertFalse(hasattr(reloaded_module, 'PandasDataFrameCsvValueType'))
        finally:
            sys.meta_path.remove(meta_finder)

    def test_supports_requires_data_frame(self):
        value_type = PandasDataFrameCsvValueType()
        value = pandas.DataFrame()
        self.assertTrue(value_type.supports(value))
        self.assertFalse(value_type.supports([1, 2, 3]))

    def test_empty_reader_raises_error(self):
        value_type = PandasDataFrameCsvValueType()
        with self.assertRaisesRegex(pandas.errors.EmptyDataError, "No columns to parse from file"):
            value_type.read_value_from_reader(self.reader)

    def test_single_empty_string_returns_empty_dataframe(self):
        self.reader.set_content(b'""\n')
        value_type = PandasDataFrameCsvValueType()
        result = value_type.read_value_from_reader(self.reader)
        self.assertEqual(0, len(result))
        self.assertIsInstance(result, pandas.DataFrame)

    def test_to_result_read_closes_stream(self):
        self.reader.set_content(b'""\n')
        value_type = PandasDataFrameCsvValueType()
        result = value_type.read_value_from_reader(self.reader)
        self.assertTrue(self.reader.closed)

    def test_to_result_reads_complete_content_as_string(self):
        self.reader.set_content(b',a,b\n0,1,2\n1,2,3\n')
        value_type = PandasDataFrameCsvValueType()
        result = value_type.read_value_from_reader(self.reader)
        self.assertEqual([1, 2], result.to_dict('list')['a'])
        self.assertEqual([2, 3], result.to_dict('list')['b'])

    def test_reader_default_encoding_is_utf8(self):
        self.reader.set_content(
            b',a,b\n0,1,\xc3\xa4\xc3\xb6\xc3\xbc\xc3\x9f\n1,2,\xc3\x84\xc3\x96\xc3\x9c\n',
        )
        value_type = PandasDataFrameCsvValueType()
        result = value_type.read_value_from_reader(self.reader)
        self.assertEqual([1, 2], result.to_dict('list')['a'])
        self.assertEqual(['äöüß', 'ÄÖÜ'], result.to_dict('list')['b'])

    def test_default_encoding_can_be_specified(self):
        self.reader.set_content(
            b'\xff\xfe,\x00a\x00,\x00b\x00\n\x000\x00,\x001\x00,\x00c\x00\n\x00',
        )
        value_type = PandasDataFrameCsvValueType(default_encoding='utf-16')
        result = value_type.read_value_from_reader(self.reader)
        self.assertEqual([1], result.to_dict('list')['a'])
        self.assertEqual(['c'], result.to_dict('list')['b'])

    def test_encoding_is_used_from_meta_if_exists(self):
        self.reader.set_content(
            b'\xff\xfe,\x00a\x00,\x00b\x00\n\x000\x00,\x001\x00,\x00c\x00\n\x00',
        )
        self.reader.meta['encoding'] = 'utf-16'
        value_type = PandasDataFrameCsvValueType()
        result = value_type.read_value_from_reader(self.reader)
        self.assertEqual([1], result.to_dict('list')['a'])
        self.assertEqual(['c'], result.to_dict('list')['b'])

    def test_empty_value_writes_empty_line(self):
        value = pandas.DataFrame([])
        value_type = PandasDataFrameCsvValueType()
        value_type.write_value_to_writer(value, self.writer)
        self.assertEqual(b'""\n', self.writer.content)

    def test_value_write_closes_stream(self):
        value = pandas.DataFrame([])
        value_type = PandasDataFrameCsvValueType()
        value_type.write_value_to_writer(value, self.writer)
        self.assertTrue(self.writer.closed)

    def test_value_writes_complete_content(self):
        value = pandas.DataFrame({
            'a': [1, 2],
            'b': ['2', '3'],
        })
        value_type = PandasDataFrameCsvValueType()
        value_type.write_value_to_writer(value, self.writer)
        self.assertEqual(b',a,b\n0,1,2\n1,2,3\n', self.writer.content)

    def test_default_encoding_is_utf8(self):
        value = pandas.DataFrame({
            'a': [1, 2],
            'b': ['äöüß', 'ÄÖÜ'],
        })
        value_type = PandasDataFrameCsvValueType()
        value_type.write_value_to_writer(value, self.writer)
        self.assertEqual(
            b',a,b\n0,1,\xc3\xa4\xc3\xb6\xc3\xbc\xc3\x9f\n1,2,\xc3\x84\xc3\x96\xc3\x9c\n',
            self.writer.content
        )
        self.assertEqual('utf-8', self.writer.meta['encoding'])

    def test_encoding_can_be_specified(self):
        value = pandas.DataFrame([
            {'a': 1, 'b': 'c'},
        ])
        value_type = PandasDataFrameCsvValueType(default_encoding='utf-16')
        value_type.write_value_to_writer(value, self.writer)
        self.assertEqual(
            b'\xff\xfe,\x00a\x00,\x00b\x00\n\x000\x00,\x001\x00,\x00c\x00\n\x00',
            self.writer.content
        )
        self.assertEqual('utf-16', self.writer.meta['encoding'])

    def test_get_specification_returns_class_and_module_name(self):
        value_type = PandasDataFrameCsvValueType()
        specification = value_type.get_specification()
        self.assertEqual('boxs.pandas:PandasDataFrameCsvValueType:utf-8', specification)

    def test_create_from_specification_can_create_from_specification(self):
        value_type = PandasDataFrameCsvValueType()
        specification = value_type.get_specification()
        recreated_value_type = ValueType.from_specification(specification)
        self.assertIsInstance(recreated_value_type, PandasDataFrameCsvValueType)

    def test_repr_returns_specification(self):
        value_type = PandasDataFrameCsvValueType()
        result = repr(value_type)
        self.assertEqual('boxs.pandas:PandasDataFrameCsvValueType:utf-8', result)

    def test_can_be_recreated_with_non_default_encoding(self):
        value_type = PandasDataFrameCsvValueType(default_encoding='utf-16')
        specification = value_type.get_specification()
        recreated_value_type = ValueType.from_specification(specification)
        self.assertEqual(value_type._default_encoding, recreated_value_type._default_encoding)


if __name__ == '__main__':
    unittest.main()
