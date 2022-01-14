import pathlib
import shutil
import tempfile
import unittest

from boxs.filesystem import FileSystemStorage


class TestFileSystemStorage(unittest.TestCase):

    def setUp(self):
        self.dir = pathlib.Path(tempfile.mkdtemp())
        self.storage = FileSystemStorage(self.dir)

    def tearDown(self):
        shutil.rmtree(self.dir)

    def test_writer_writes_data_to_file(self):
        writer = self.storage.create_writer('data-id', 'rev-id')
        with writer.as_stream() as stream:
            stream.write(b'My data')

        data_file = self.dir / 'data-id' / 'rev-id.data'
        self.assertEqual(b'My data', data_file.read_bytes())

    def test_writer_raises_collision_if_same_ids_twice(self):
        writer = self.storage.create_writer('data-id', 'rev-id')
        with writer.as_stream() as stream:
            stream.write(b'My data')

        with writer.as_stream() as stream:
            stream.write(b'My data')

        data_file = self.dir / 'data-id' / 'rev-id.data'
        self.assertEqual(b'My data', data_file.read_bytes())

    def test_writer_meta_can_be_set(self):
        writer = self.storage.create_writer('data-id', 'rev-id')
        writer.meta['my'] = 'meta'
        self.assertEqual({'my': 'meta'}, writer.meta)

    def test_exists_returns_true_for_new_data(self):
        writer = self.storage.create_writer('data-id', 'rev-id')
        with writer.as_stream() as stream:
            stream.write(b'My data')
        writer.write_info({})

        self.assertTrue(self.storage.exists('data-id', 'rev-id'))

    def test_exists_returns_false_for_non_existing_data(self):
        self.assertFalse(self.storage.exists('data-id', 'rev-id'))

    def test_reader_reads_previously_written_data(self):
        writer = self.storage.create_writer('data-id', 'rev-id')
        with writer.as_stream() as stream:
            stream.write(b'My data')

        reader = self.storage.create_reader('data-id', 'rev-id')
        with reader.as_stream() as stream:
            data = stream.read()

        self.assertEqual(b'My data', data)

    def test_reader_reads_previously_written_info(self):
        writer = self.storage.create_writer('data-id', 'rev-id')
        writer.write_info({'my': 'info'})

        reader = self.storage.create_reader('data-id', 'rev-id')

        self.assertEqual({'my': 'info'}, reader.info)

    def test_reader_caches_info(self):
        writer = self.storage.create_writer('data-id', 'rev-id')
        writer.write_info({'my': 'info'})

        reader = self.storage.create_reader('data-id', 'rev-id')
        first_info = reader.info
        second_info = reader.info
        self.assertIs(second_info, first_info)

    def test_reader_takes_meta_from_info(self):
        writer = self.storage.create_writer('data-id', 'rev-id')
        writer.write_info({'meta': {'my': 'meta'}})

        reader = self.storage.create_reader('data-id', 'rev-id')
        info = reader.info
        meta = reader.meta
        self.assertEqual(meta, info['meta'])


if __name__ == '__main__':
    unittest.main()
