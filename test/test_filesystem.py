import datetime
import pathlib
import shutil
import tempfile
import time
import unittest

from boxs.errors import BoxNotFound, DataCollision, DataNotFound, NameCollision, RunNotFound
from boxs.filesystem import FileSystemStorage
from boxs.storage import Item, ItemQuery
from boxs.value_types import BytesValueType


class TestFileSystemStorage(unittest.TestCase):

    def setUp(self):
        self.dir = pathlib.Path(tempfile.mkdtemp())
        self.storage = FileSystemStorage(self.dir)

    def tearDown(self):
        shutil.rmtree(self.dir)

    def test_runs_can_be_listed(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'run1'))
        writer.write_info({})
        time.sleep(0.1)
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'run2'))
        writer.write_info({})
        time.sleep(0.1)
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'run3'))
        writer.write_info({})

        runs = self.storage.list_runs('box-id')
        self.assertEqual('run3', runs[0].run_id)
        self.assertEqual('run2', runs[1].run_id)
        self.assertEqual('run1', runs[2].run_id)
        self.assertIsInstance(runs[0].time, datetime.datetime)
        self.assertGreater(runs[0].time, runs[1].time)
        self.assertGreater(runs[1].time, runs[2].time)

    def test_listing_runs_for_invalid_box_id_raises(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'run1'))
        writer.write_info({})

        with self.assertRaisesRegex(BoxNotFound, "Box unknown-box-id does not exist"):
            self.storage.list_runs('unknown-box-id')

    def test_list_runs_can_be_limited(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'run2'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'run3'))
        writer.write_info({})

        runs = self.storage.list_runs('box-id', limit=2)
        self.assertEqual(2, len(runs))
        self.assertEqual('run3', runs[0].run_id)
        self.assertEqual('run2', runs[1].run_id)

    def test_delete_run_removes_data_items(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'run1'))
        writer.write_value(b'My data', BytesValueType())
        writer.write_info({})

        data_file = self.dir / 'box-id' / 'data' / 'data-id' / 'run1.data'
        info_file = self.dir / 'box-id' / 'data' / 'data-id' / 'run1.info'

        self.assertTrue(data_file.exists())
        self.assertTrue(info_file.exists())

        self.storage.delete_run('box-id', 'run1')

        self.assertFalse(data_file.exists())
        self.assertFalse(info_file.exists())

    def test_delete_run_removes_unknown_run(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'run1'))
        writer.write_info({})

        with self.assertRaisesRegex(RunNotFound, "Run unknown-run does not exist in box box-id"):
            self.storage.delete_run('box-id', 'unknown-run')

    def test_list_items_with_invalid_box_id_raises(self):
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run1'))
        writer.write_info({})

        with self.assertRaisesRegex(BoxNotFound, "Box unknown-box-id does not exist"):
            query = ItemQuery('unknown-box-id::')
            self.storage.list_items(query)

    def test_list_items_with_only_box_in_query_returns_all(self):
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run2'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run2'))
        writer.write_info({})

        query = ItemQuery('box-id::')
        items = self.storage.list_items(query)
        self.assertEqual(len(items), 4)
        self.assertEqual('run1', items[0].run_id)
        self.assertEqual('data1', items[0].data_id)
        self.assertEqual('run1', items[1].run_id)
        self.assertEqual('data2', items[1].data_id)
        self.assertEqual('run2', items[2].run_id)
        self.assertEqual('data1', items[2].data_id)
        self.assertEqual('run2', items[3].run_id)
        self.assertEqual('data2', items[3].data_id)
        self.assertIsInstance(items[0].time, datetime.datetime)
        self.assertLess(items[0].time, items[1].time)
        self.assertLess(items[1].time, items[2].time)
        self.assertLess(items[2].time, items[3].time)

    def test_list_items_with_data_in_query_returns_only_matching_items(self):
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data21', 'run2'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run2'))
        writer.write_info({})

        query = ItemQuery('box-id:data2:')
        items = self.storage.list_items(query)
        self.assertEqual(len(items), 3)
        self.assertEqual('run1', items[0].run_id)
        self.assertEqual('data2', items[0].data_id)
        self.assertEqual('run2', items[1].run_id)
        self.assertEqual('data21', items[1].data_id)
        self.assertEqual('run2', items[2].run_id)
        self.assertEqual('data2', items[2].data_id)
        self.assertIsInstance(items[0].time, datetime.datetime)
        self.assertLess(items[0].time, items[1].time)
        self.assertLess(items[1].time, items[2].time)

    def test_list_items_with_data_in_query_returns_matching_names(self):
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run1'), name='train_data')
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data21', 'run2'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run2'), name='train_data')
        writer.write_info({})

        query = ItemQuery('box-id:train:')
        items = self.storage.list_items(query)
        self.assertEqual(len(items), 2)
        self.assertEqual('run1', items[0].run_id)
        self.assertEqual('data2', items[0].data_id)
        self.assertEqual('run2', items[1].run_id)
        self.assertEqual('data2', items[1].data_id)
        self.assertIsInstance(items[0].time, datetime.datetime)
        self.assertLess(items[0].time, items[1].time)

    def test_list_items_with_run_in_query_returns_only_matching_runs(self):
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data21', 'run2'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run2'))
        writer.write_info({})

        query = ItemQuery('box-id::run1')
        items = self.storage.list_items(query)
        self.assertEqual(len(items), 2)
        self.assertEqual('run1', items[0].run_id)
        self.assertEqual('data1', items[0].data_id)
        self.assertEqual('run1', items[1].run_id)
        self.assertEqual('data2', items[1].data_id)
        self.assertIsInstance(items[0].time, datetime.datetime)
        self.assertLess(items[0].time, items[1].time)

    def test_list_items_with_run_in_query_returns_only_matching_run_name(self):
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data21', 'run2'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run2'))
        writer.write_info({})
        self.storage.set_run_name('box-id', 'run1', 'test-name')

        query = ItemQuery('box-id::test')
        items = self.storage.list_items(query)
        self.assertEqual(len(items), 2)
        self.assertEqual('run1', items[0].run_id)
        self.assertEqual('data1', items[0].data_id)
        self.assertEqual('run1', items[1].run_id)
        self.assertEqual('data2', items[1].data_id)
        self.assertIsInstance(items[0].time, datetime.datetime)
        self.assertLess(items[0].time, items[1].time)

    def test_list_items_with_data_and_run_in_query(self):
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run2'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run2'))
        writer.write_info({})

        query = ItemQuery('box-id:data1:run2')
        items = self.storage.list_items(query)
        self.assertEqual(len(items), 1)
        self.assertEqual('run2', items[0].run_id)
        self.assertEqual('data1', items[0].data_id)
        self.assertIsInstance(items[0].time, datetime.datetime)

    def test_list_items_with_nothing_matches_returns_empty_list(self):
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run1'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run2'))
        writer.write_info({})
        time.sleep(0.01)
        writer = self.storage.create_writer(Item('box-id', 'data2', 'run2'))
        writer.write_info({})

        query = ItemQuery('box-id:data3:run3')
        items = self.storage.list_items(query)
        self.assertEqual(len(items), 0)

    def test_a_run_can_be_named(self):
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run'), name='item-name')
        writer.write_info({})

        run = self.storage.set_run_name('box-id', 'run', 'My run name')
        self.assertEqual('My run name', run.name)

    def test_a_run_can_be_renamed(self):
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run'), name='item-name')
        writer.write_info({})

        run = self.storage.set_run_name('box-id', 'run', 'My first name')
        run = self.storage.set_run_name('box-id', 'run', 'My second name')

        self.assertEqual('My second name', run.name)

    def test_a_run_name_can_be_removed(self):
        writer = self.storage.create_writer(Item('box-id', 'data1', 'run'), name='item-name')
        writer.write_info({})

        run = self.storage.set_run_name('box-id', 'run', 'My first name')
        run = self.storage.set_run_name('box-id', 'run', None)

        self.assertIsNone(run.name)

    def test_renaming_a_run_in_invalid_box_id_raises(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'run1'))
        writer.write_info({})

        with self.assertRaisesRegex(BoxNotFound, "Box unknown-box-id does not exist"):
            self.storage.set_run_name('unknown-box-id', 'run', 'My first name')

    def test_renaming_a_run_with_invalid_run_id_raises(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'run1'))
        writer.write_info({})

        with self.assertRaisesRegex(RunNotFound, "Run unknown-run does not exist"):
            self.storage.set_run_name('box-id', 'unknown-run', 'My first name')

    def test_writer_writes_data_to_file(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'rev-id'))
        with writer.as_stream() as stream:
            stream.write(b'My data')

        data_file = self.dir / 'box-id' / 'data' / 'data-id' / 'rev-id.data'
        self.assertEqual(b'My data', data_file.read_bytes())

    def test_writer_raises_collision_writing_data_with_same_ids_twice(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'rev-id'))
        writer.write_value(b'My data', BytesValueType())

        writer = self.storage.create_writer(Item('box-id', 'data-id', 'rev-id'))
        with self.assertRaisesRegex(DataCollision, "Data data-id from run rev-id already exists"):
            writer.write_value(b'My data', BytesValueType())

    def test_writer_raises_collision_writing_info_with_same_ids_twice(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'rev-id'))
        writer.write_info({})

        writer = self.storage.create_writer(Item('box-id', 'data-id', 'rev-id'))
        with self.assertRaisesRegex(DataCollision, "Data data-id from run rev-id already exists"):
            writer.write_info({})

    def test_writer_raises_collision_if_same_name_twice(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'rev-id'), name='a')
        writer.write_info({})

        writer = self.storage.create_writer(Item('box-id', 'data-id2', 'rev-id'), name='a')
        with self.assertRaisesRegex(NameCollision, "There already exists a data item in run rev-id with the name a"):
            writer.write_info({})

    def test_writer_meta_can_be_set(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'rev-id'))
        writer.meta['my'] = 'meta'
        self.assertEqual({'my': 'meta'}, writer.meta)

    def test_reader_reads_previously_written_data(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'rev-id'))
        with writer.as_stream() as stream:
            stream.write(b'My data')

        reader = self.storage.create_reader(Item('box-id', 'data-id', 'rev-id'))
        with reader.as_stream() as stream:
            data = stream.read()

        self.assertEqual(b'My data', data)

    def test_reader_can_return_a_file_path_to_content(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'rev-id'))
        with writer.as_stream() as stream:
            stream.write(b'My data')

        reader = self.storage.create_reader(Item('box-id', 'data-id', 'rev-id'))
        file_path = reader.as_file()
        data = file_path.read_bytes()

        self.assertEqual(b'My data', data)

    def test_reader_reads_previously_written_info(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'rev-id'))
        writer.write_info({'my': 'info'})

        reader = self.storage.create_reader(Item('box-id', 'data-id', 'rev-id'))

        self.assertEqual({'my': 'info'}, reader.info)

    def test_reader_raises_when_reading_non_existing_info(self):
        reader = self.storage.create_reader(Item('box-id', 'data-id', 'rev-id'))
        with self.assertRaisesRegex(DataNotFound, "Data data-id from run rev-id does not exist in box box-id"):
            reader.info

    def test_reader_raises_when_reading_non_existing_value(self):
        reader = self.storage.create_reader(Item('box-id', 'data-id', 'rev-id'))
        with self.assertRaisesRegex(DataNotFound, "Data data-id from run rev-id does not exist in box box-id"):
            reader.as_stream()

    def test_reader_caches_info(self):
        writer = self.storage.create_writer(Item('box-id', 'data-id', 'rev-id'))
        writer.write_info({'my': 'info'})

        reader = self.storage.create_reader(Item('box-id', 'data-id', 'rev-id'))
        first_info = reader.info
        second_info = reader.info
        self.assertIs(second_info, first_info)


if __name__ == '__main__':
    unittest.main()
