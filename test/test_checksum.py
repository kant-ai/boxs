import io
import unittest.mock

from boxs.checksum import ChecksumTransformer, DataChecksumMismatch
from boxs.data import DataRef
from boxs.value_types import BytesValueType
from boxs.storage import Reader


class ReaderImpl(Reader):

    def __init__(self):
        super().__init__(DataRef('my-data-id', 'box_id', '1'))
        self._meta = {}

    @property
    def info(self):
        pass

    @property
    def meta(self):
        return self._meta

    def as_stream(self):
        return io.BytesIO(b'My content')


class TestChecksumTransformer(unittest.TestCase):

    def setUp(self):
        self.transformer = ChecksumTransformer()

    def test_transformed_reader_warns_if_no_digest_algorithm_is_set_and_uses_default(self):
        reader = ReaderImpl()
        reader.meta.update({
            'checksum_digest': 'ce858b5195f68056187c143dc2023caaabb07a8c1eacf482ee222bfc481ffa0a',
            'checksum_digest_size': 32,
        })
        with self.assertLogs('boxs.checksum', level='WARNING') as cm:
            transformed_reader = self.transformer.transform_reader(reader)

            result = transformed_reader.read_value(BytesValueType())
            self.assertEqual(cm.output, [
                "WARNING:boxs.checksum:No checksum algorithm given,"
                " disabling checksum verification"
            ])

        self.assertEqual(
            b'My content',
            result,
        )

    def test_transformed_reader_warns_if_digest_algorithm_is_not_known(self):
        reader = ReaderImpl()
        reader.meta.update({
            'checksum_algorithm': 'unknown',
            'checksum_digest': 'ce858b5195f68056187c143dc2023caaabb07a8c1eacf482ee222bfc481ffa0a',
            'checksum_digest_size': 32,
        })
        with self.assertLogs('boxs.checksum', level='WARNING') as cm:
            transformed_reader = self.transformer.transform_reader(reader)

            result = transformed_reader.read_value(BytesValueType())
            self.assertEqual(cm.output, [
                "WARNING:boxs.checksum:Unknown checksum algorithm 'unknown',"
                " disabling checksum verification"
            ])

        self.assertEqual(
            b'My content',
            result,
        )

    def test_transformed_reader_adds_info_with_checksum(self):
        reader = ReaderImpl()
        reader.meta.update({
            'checksum_algorithm': 'blake2b',
            'checksum_digest': 'ce858b5195f68056187c143dc2023caaabb07a8c1eacf482ee222bfc481ffa0a',
            'checksum_digest_size': 32,
        })
        with self.assertLogs('boxs.checksum', level='INFO') as cm:
            transformed_reader = self.transformer.transform_reader(reader)

            result = transformed_reader.read_value(BytesValueType())
            self.assertEqual(cm.output, [
                'INFO:boxs.checksum:Checksum when reading data my-data-id: '
                'ce858b5195f68056187c143dc2023caaabb07a8c1eacf482ee222bfc481ffa0a'
            ])

        self.assertEqual(
            b'My content',
            result,
        )

    def test_transformed_reader_uses_digest_size_from_meta(self):
        reader = ReaderImpl()
        reader.meta.update({
            'checksum_algorithm': 'blake2b',
            'checksum_digest': 'a7b9bb7a4fb93658f8c4d4d68dfd3ef3',
            'checksum_digest_size': 16,
        })
        with self.assertLogs('boxs.checksum', level='INFO') as cm:
            transformed_reader = self.transformer.transform_reader(reader)

            result = transformed_reader.read_value(BytesValueType())
            self.assertEqual(cm.output, [
                'INFO:boxs.checksum:Checksum when reading data my-data-id: '
                'a7b9bb7a4fb93658f8c4d4d68dfd3ef3'
            ])

        self.assertEqual(
            b'My content',
            result,
        )

    def test_transformed_reader_raises_error_if_checksum_mismatch(self):
        reader = ReaderImpl()
        reader.meta.update({
            'checksum_algorithm': 'blake2b',
            'checksum_digest': 'invalid-0123456789',
            'checksum_digest_size': 12,
        })
        with self.assertRaisesRegex(
                DataChecksumMismatch,
                "Data 'my-data-id' has wrong checksum 'be68aba8b3041f8292681595',"
                " expected 'invalid-0123456789'"):
            transformed_reader = self.transformer.transform_reader(reader)
            transformed_reader.read_value(BytesValueType())

    def test_transformed_writer_calculates_checksum(self):
        writer = unittest.mock.MagicMock()
        writer.meta = {}
        writer.as_stream.return_value = io.BytesIO()
        transformed_writer = self.transformer.transform_writer(writer)

        transformed_writer.write_value(b'My content', BytesValueType())

        self.assertEqual(
            'ce858b5195f68056187c143dc2023caaabb07a8c1eacf482ee222bfc481ffa0a',
            transformed_writer.meta['checksum_digest'],
        )

    def test_transformed_writer_logs_calculated_checksum(self):
        writer = unittest.mock.MagicMock()
        writer.meta = {}
        writer.as_stream.return_value = io.BytesIO()
        writer.data_id = 'my-data-id'
        transformed_writer = self.transformer.transform_writer(writer)

        with self.assertLogs('boxs.checksum', level='INFO') as cm:
            transformed_writer.write_value(b'My content', BytesValueType())
            self.assertEqual(cm.output, [
                'INFO:boxs.checksum:Checksum when writing data my-data-id: '
                'ce858b5195f68056187c143dc2023caaabb07a8c1eacf482ee222bfc481ffa0a'
            ])

        self.assertEqual(
            'ce858b5195f68056187c143dc2023caaabb07a8c1eacf482ee222bfc481ffa0a',
            transformed_writer.meta['checksum_digest'],
        )

    def test_transformed_writer_adds_checksum_algorithm(self):
        writer = unittest.mock.MagicMock()
        writer.meta = {}
        writer.as_stream.return_value = io.BytesIO()
        transformed_writer = self.transformer.transform_writer(writer)

        transformed_writer.write_value(b'My content', BytesValueType())

        self.assertEqual(
            'blake2b',
            transformed_writer.meta['checksum_algorithm'],
        )

    def test_transformed_writer_checksum_default_size(self):
        writer = unittest.mock.MagicMock()
        writer.meta = {}
        writer.as_stream.return_value = io.BytesIO()
        transformed_writer = self.transformer.transform_writer(writer)

        transformed_writer.write_value(b'My content', BytesValueType())

        self.assertEqual(32*2, len(transformed_writer.meta['checksum_digest']))
        self.assertEqual(32, transformed_writer.meta['checksum_digest_size'])

    def test_transformed_writer_checksum_size_can_be_changed(self):
        transformer = ChecksumTransformer(digest_size=10)
        writer = unittest.mock.MagicMock()
        writer.meta = {}
        writer.as_stream.return_value = io.BytesIO()
        transformed_writer = transformer.transform_writer(writer)

        transformed_writer.write_value(b'My content', BytesValueType())

        self.assertEqual(10*2, len(transformed_writer.meta['checksum_digest']))

    def test_transformed_writer_and_reader_work_in_combination(self):
        transformer = ChecksumTransformer(digest_size=10)
        writer = unittest.mock.MagicMock()
        writer.meta = {}
        writer.as_stream.return_value = io.BytesIO()
        transformed_writer = transformer.transform_writer(writer)

        transformed_writer.write_value(b'My content', BytesValueType())

        self.assertEqual(10*2, len(transformed_writer.meta['checksum_digest']))
        self.assertEqual(transformed_writer.meta['checksum_digest'], '336e597e7437529d340c')

        reader = unittest.mock.MagicMock()
        reader.data_id = 'my-data-id'
        reader.as_stream.return_value = io.BytesIO(b'My content')

        reader.meta = writer.meta

        with self.assertLogs('boxs.checksum', level='INFO') as cm:
            transformed_reader = self.transformer.transform_reader(reader)

            result = transformed_reader.read_value(BytesValueType())
            self.assertEqual(cm.output, [
                'INFO:boxs.checksum:Checksum when reading data my-data-id: '
                '336e597e7437529d340c'
            ])

        self.assertEqual(
            b'My content',
            result,
        )


if __name__ == '__main__':
    unittest.main()
