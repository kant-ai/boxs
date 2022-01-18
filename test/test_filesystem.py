import datetime
import pathlib
import shutil
import tempfile
import time
import unittest

from boxs.data import DataRef
from boxs.filesystem import FileSystemStorage


class TestFileSystemStorage(unittest.TestCase):

    def setUp(self):
        self.dir = pathlib.Path(tempfile.mkdtemp())
        self.storage = FileSystemStorage(self.dir)

    def tearDown(self):
        shutil.rmtree(self.dir)

    def test_runs_can_be_listed(self):
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'run1'))
        writer.write_info({})
        time.sleep(0.1)
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'run2'))
        writer.write_info({})
        time.sleep(0.1)
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'run3'))
        writer.write_info({})

        runs = self.storage.list_runs()
        self.assertEqual('run3', runs[0].run_id)
        self.assertEqual('run2', runs[1].run_id)
        self.assertEqual('run1', runs[2].run_id)
        self.assertIsInstance(runs[0].time, datetime.datetime)
        self.assertGreater(runs[0].time, runs[1].time)
        self.assertGreater(runs[1].time, runs[2].time)

    def test_list_runs_can_be_limited(self):
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'run2'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'run3'))
        writer.write_info({})

        runs = self.storage.list_runs(limit=2)
        self.assertEqual(2, len(runs))
        self.assertEqual('run3', runs[0].run_id)
        self.assertEqual('run2', runs[1].run_id)

    def test_items_in_run_can_be_listed(self):
        writer = self.storage.create_writer(DataRef('data1', 'box-id', 'run'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(DataRef('data2', 'box-id', 'run'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(DataRef('data3', 'box-id', 'run'))
        writer.write_info({})

        items = self.storage.list_items_in_run('run')
        self.assertEqual('run', items[0].run_id)
        self.assertEqual('run', items[1].run_id)
        self.assertEqual('run', items[2].run_id)
        self.assertEqual('data1', items[0].data_id)
        self.assertEqual('data2', items[1].data_id)
        self.assertEqual('data3', items[2].data_id)
        self.assertIsInstance(items[0].time, datetime.datetime)
        self.assertLess(items[0].time, items[1].time)
        self.assertLess(items[1].time, items[2].time)

    def test_items_in_run_can_be_listed_with_their_name(self):
        writer = self.storage.create_writer(DataRef('data1', 'box-id', 'run'))
        writer.write_info({'name': 'item-name'})

        items = self.storage.list_items_in_run('run')
        self.assertEqual('item-name', items[0].name)

    def test_writer_writes_data_to_file(self):
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'rev-id'))
        with writer.as_stream() as stream:
            stream.write(b'My data')

        data_file = self.dir / 'data' / 'data-id' / 'rev-id.data'
        self.assertEqual(b'My data', data_file.read_bytes())

    def test_writer_raises_collision_if_same_ids_twice(self):
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'rev-id'))
        with writer.as_stream() as stream:
            stream.write(b'My data')

        with writer.as_stream() as stream:
            stream.write(b'My data')

        data_file = self.dir / 'data' / 'data-id' / 'rev-id.data'
        self.assertEqual(b'My data', data_file.read_bytes())

    def test_writer_meta_can_be_set(self):
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'rev-id'))
        writer.meta['my'] = 'meta'
        self.assertEqual({'my': 'meta'}, writer.meta)

    def test_exists_returns_true_for_new_data(self):
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'rev-id'))
        with writer.as_stream() as stream:
            stream.write(b'My data')
        writer.write_info({})

        self.assertTrue(self.storage.exists(DataRef('data-id', 'box-id', 'rev-id')))

    def test_exists_returns_false_for_non_existing_data(self):
        self.assertFalse(self.storage.exists(DataRef('data-id', 'box-id', 'rev-id')))

    def test_reader_reads_previously_written_data(self):
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'rev-id'))
        with writer.as_stream() as stream:
            stream.write(b'My data')

        reader = self.storage.create_reader(DataRef('data-id', 'box-id', 'rev-id'))
        with reader.as_stream() as stream:
            data = stream.read()

        self.assertEqual(b'My data', data)

    def test_reader_reads_previously_written_info(self):
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'rev-id'))
        writer.write_info({'my': 'info'})

        reader = self.storage.create_reader(DataRef('data-id', 'box-id', 'rev-id'))

        self.assertEqual({'my': 'info'}, reader.info)

    def test_reader_caches_info(self):
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'rev-id'))
        writer.write_info({'my': 'info'})

        reader = self.storage.create_reader(DataRef('data-id', 'box-id', 'rev-id'))
        first_info = reader.info
        second_info = reader.info
        self.assertIs(second_info, first_info)

    def test_reader_takes_meta_from_info(self):
        writer = self.storage.create_writer(DataRef('data-id', 'box-id', 'rev-id'))
        writer.write_info({'meta': {'my': 'meta'}})

        reader = self.storage.create_reader(DataRef('data-id', 'box-id', 'rev-id'))
        info = reader.info
        meta = reader.meta
        self.assertEqual(meta, info['meta'])


if __name__ == '__main__':
    unittest.main()
