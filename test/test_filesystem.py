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
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'run1'))
        writer.write_info('origin', [], {})
        time.sleep(0.1)
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'run2'))
        writer.write_info('origin', [], {})
        time.sleep(0.1)
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'run3'))
        writer.write_info('origin', [], {})

        runs = self.storage.list_runs('box-id')
        self.assertEqual('run3', runs[0].run_id)
        self.assertEqual('run2', runs[1].run_id)
        self.assertEqual('run1', runs[2].run_id)
        self.assertIsInstance(runs[0].time, datetime.datetime)
        self.assertGreater(runs[0].time, runs[1].time)
        self.assertGreater(runs[1].time, runs[2].time)

    def test_list_runs_can_be_limited(self):
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'run1'))
        writer.write_info('origin', [], {})
        time.sleep(0.01)
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'run2'))
        writer.write_info('origin', [], {})
        time.sleep(0.01)
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'run3'))
        writer.write_info('origin', [], {})

        runs = self.storage.list_runs('box-id', limit=2)
        self.assertEqual(2, len(runs))
        self.assertEqual('run3', runs[0].run_id)
        self.assertEqual('run2', runs[1].run_id)

    def test_items_in_run_can_be_listed(self):
        writer = self.storage.create_writer(DataRef('box-id', 'data1', 'run'))
        writer.write_info('origin', [], {})
        time.sleep(0.01)
        writer = self.storage.create_writer(DataRef('box-id', 'data2', 'run'))
        writer.write_info('origin', [], {})
        time.sleep(0.01)
        writer = self.storage.create_writer(DataRef('box-id', 'data3', 'run'))
        writer.write_info('origin', [], {})

        items = self.storage.list_items_in_run('box-id', 'run')
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
        writer = self.storage.create_writer(DataRef('box-id', 'data1', 'run'), name='item-name')
        writer.write_info('origin', [], {})

        items = self.storage.list_items_in_run('box-id', 'run')
        self.assertEqual('item-name', items[0].name)

    def test_a_run_can_be_named(self):
        writer = self.storage.create_writer(DataRef('box-id', 'data1', 'run'), name='item-name')
        writer.write_info('origin', [], {})

        run = self.storage.set_run_name('box-id', 'run', 'My run name')
        self.assertEqual('My run name', run.name)

    def test_a_run_can_be_renamed(self):
        writer = self.storage.create_writer(DataRef('box-id', 'data1', 'run'), name='item-name')
        writer.write_info('origin', [], {})

        run = self.storage.set_run_name('box-id', 'run', 'My first name')
        run = self.storage.set_run_name('box-id', 'run', 'My second name')

        self.assertEqual('My second name', run.name)

    def test_a_run_name_can_be_removed(self):
        writer = self.storage.create_writer(DataRef('box-id', 'data1', 'run'), name='item-name')
        writer.write_info('origin', [], {})

        run = self.storage.set_run_name('box-id', 'run', 'My first name')
        run = self.storage.set_run_name('box-id', 'run', None)

        self.assertIsNone(run.name)

    def test_writer_writes_data_to_file(self):
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'rev-id'))
        with writer.as_stream() as stream:
            stream.write(b'My data')

        data_file = self.dir / 'data' / 'data-id' / 'rev-id.data'
        self.assertEqual(b'My data', data_file.read_bytes())

    def test_writer_raises_collision_if_same_ids_twice(self):
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'rev-id'))
        with writer.as_stream() as stream:
            stream.write(b'My data')

        with writer.as_stream() as stream:
            stream.write(b'My data')

        data_file = self.dir / 'data' / 'data-id' / 'rev-id.data'
        self.assertEqual(b'My data', data_file.read_bytes())

    def test_writer_meta_can_be_set(self):
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'rev-id'))
        writer.meta['my'] = 'meta'
        self.assertEqual({'my': 'meta'}, writer.meta)

    def test_exists_returns_true_for_new_data(self):
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'rev-id'))
        with writer.as_stream() as stream:
            stream.write(b'My data')
        writer.write_info('origin', [], {})

        self.assertTrue(self.storage.exists(DataRef('box-id', 'data-id', 'rev-id')))

    def test_exists_returns_false_for_non_existing_data(self):
        self.assertFalse(self.storage.exists(DataRef('box-id', 'data-id', 'rev-id')))

    def test_reader_reads_previously_written_data(self):
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'rev-id'))
        with writer.as_stream() as stream:
            stream.write(b'My data')

        reader = self.storage.create_reader(DataRef('box-id', 'data-id', 'rev-id'))
        with reader.as_stream() as stream:
            data = stream.read()

        self.assertEqual(b'My data', data)

    def test_reader_reads_previously_written_info(self):
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'rev-id'))
        writer.write_info('origin', [], {})

        reader = self.storage.create_reader(DataRef('box-id', 'data-id', 'rev-id'))

        self.assertEqual({
            'meta': {},
            'name': None,
            'origin': 'origin',
            'parents': [],
            'ref': {'box_id': 'box-id', 'data_id': 'data-id', 'run_id': 'rev-id'},
            'tags': {}
        }, reader.info)

    def test_reader_caches_info(self):
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'rev-id'))
        writer.write_info('origin', [], {})

        reader = self.storage.create_reader(DataRef('box-id', 'data-id', 'rev-id'))
        first_info = reader.info
        second_info = reader.info
        self.assertIs(second_info, first_info)

    def test_reader_takes_meta_from_writer(self):
        writer = self.storage.create_writer(DataRef('box-id', 'data-id', 'rev-id'))
        writer.meta['my'] = 'meta'
        writer.write_info('origin', [], {})

        reader = self.storage.create_reader(DataRef('box-id', 'data-id', 'rev-id'))
        meta = reader.meta
        self.assertEqual({'my': 'meta'}, meta)


if __name__ == '__main__':
    unittest.main()
