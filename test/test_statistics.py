import datetime
import io
import sys
import unittest.mock

from boxs.statistics import StatisticsTransformer


class TestStatisticsTransformer(unittest.TestCase):

    def setUp(self):
        self.transformer = StatisticsTransformer()

    def test_transformer_doesnt_change_reader(self):
        reader = 'reader'
        transformed_reader = self.transformer.transform_reader(reader)
        self.assertIs(transformed_reader, reader)

    def test_transformed_writer_counts_bytes(self):
        writer = unittest.mock.MagicMock()
        writer.meta = {}
        writer.as_stream.return_value = io.BytesIO()
        transformed_writer = self.transformer.transform_writer(writer)

        with transformed_writer.as_stream() as stream:
            stream.write(b'My content')

        self.assertEqual(10, transformed_writer.meta['size_in_bytes'])

    def test_transformed_writer_counts_lines(self):
        writer = unittest.mock.MagicMock()
        writer.meta = {}
        writer.as_stream.return_value = io.BytesIO()
        transformed_writer = self.transformer.transform_writer(writer)

        with transformed_writer.as_stream() as stream:
            stream.write(b'content\nwith\nmultiple\n\nlines\n')

        self.assertEqual(5, transformed_writer.meta['number_of_lines'])

    @unittest.skipIf(sys.version_info < (3, 7), "not supported in python < 3.7")
    def test_transformed_writer_sets_start_and_end_time_in_timestamp_format(self):
        writer = unittest.mock.MagicMock()
        writer.meta = {}
        writer.as_stream.return_value = io.BytesIO()
        transformed_writer = self.transformer.transform_writer(writer)

        with transformed_writer.as_stream() as stream:
            stream.write(b'content\nwith\nmultiple\n\nlines\n')

        self.assertIn('store_start', transformed_writer.meta)
        self.assertIn('store_end', transformed_writer.meta)
        start = datetime.datetime.fromisoformat(transformed_writer.meta['store_start'])
        end = datetime.datetime.fromisoformat(transformed_writer.meta['store_start'])
        now = datetime.datetime.now(datetime.timezone.utc)
        self.assertLess(now - start, datetime.timedelta(seconds=1))
        self.assertLess(now - end, datetime.timedelta(seconds=1))

    def test_transformed_writer_start_and_end_time_includes_utc_timezone(self):
        writer = unittest.mock.MagicMock()
        writer.meta = {}
        writer.as_stream.return_value = io.BytesIO()
        transformed_writer = self.transformer.transform_writer(writer)

        with transformed_writer.as_stream() as stream:
            stream.write(b'content\nwith\nmultiple\n\nlines\n')

        self.assertTrue(transformed_writer.meta['store_start'].endswith('+00:00'))
        self.assertTrue(transformed_writer.meta['store_end'].endswith('+00:00'))


if __name__ == '__main__':
    unittest.main()
