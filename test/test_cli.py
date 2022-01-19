import io
import json
import pathlib
import shutil
import tempfile
import unittest.mock

from boxs.box import Box
from boxs.box_registry import unregister_box
from boxs.cli import main
from boxs.filesystem import FileSystemStorage


class TestCli(unittest.TestCase):

    def setUp(self):
        self.dir = pathlib.Path(tempfile.mkdtemp())
        self.storage = FileSystemStorage(self.dir)
        self.box = Box('cli-box', self.storage)
        self.load_box_patcher = unittest.mock.patch('boxs.cli._load_box')
        self.load_box_mock = self.load_box_patcher.start()
        self.load_box_mock.return_value = self.box

    def tearDown(self):
        shutil.rmtree(self.dir)
        unregister_box(self.box.box_id)
        self.load_box_patcher.stop()

    def test_main_without_args_raises_error_and_exits(self):
        with self.assertRaisesRegex(SystemExit, "2"):
            main([])

    def test_main_with_help_shows_help(self):
        with unittest.mock.patch('sys.stdout', new=io.StringIO()) as fake_out:
            with self.assertRaisesRegex(SystemExit, "0"):
                main(['-h'])
            self.assertIn('usage: boxs [-h] -b BOX [-j] {list,info,name}', fake_out.getvalue())
            self.assertIn('Allows to inspect and manipulate boxes', fake_out.getvalue())
            self.assertIn('positional arguments:', fake_out.getvalue())
            self.assertIn('optional arguments:', fake_out.getvalue())

    def test_main_list_runs(self):
        self.box.store('My value', run_id='run-1')
        self.box.store('My other', run_id='run-2')
        with unittest.mock.patch('sys.stdout', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'list'])
            self.assertIn('List runs', fake_out.getvalue())
            self.assertIn('run_id|name|', fake_out.getvalue())
            self.assertIn('run-1', fake_out.getvalue())
            self.assertIn('run-2', fake_out.getvalue())

    def test_main_list_data_items_in_run(self):
        self.box.store('My value', origin='1', name='data_1', run_id='run-1')
        self.box.store('My other', origin='2', name='data_2', run_id='run-1')
        with unittest.mock.patch('sys.stdout', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'list', '-r', 'run-1'])
            self.assertIn('List run run-1', fake_out.getvalue())
            self.assertIn('data_id     |run_id| name', fake_out.getvalue())
            self.assertIn('run-1  data_1', fake_out.getvalue())
            self.assertIn('run-1  data_2', fake_out.getvalue())

    def test_main_list_data_items_in_run_in_json(self):
        self.box.store('My value', origin='1', name='data_1', run_id='run-1')
        self.box.store('My other', origin='2', name='data_2', run_id='run-1')
        with unittest.mock.patch('sys.stdout', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', '-j', 'list', '-r', 'run-1'])
            result = json.loads(fake_out.getvalue())
            self.assertEqual(len(result), 2)

    def test_main_list_data_items_with_invalid_box_prints_error(self):
        with unittest.mock.patch('sys.stderr', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'list', '-r', 'run-1'])
            self.assertIn('Error: Box cli-box does not exist in storage', fake_out.getvalue())

    def test_main_list_data_items_with_invalid_box_prints_error(self):
        self.box.store('My value', run_id='run-1')
        with unittest.mock.patch('sys.stderr', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'list', '-r', 'run-2'])
            self.assertIn('Error: No run found with run-id or name starting with run-2', fake_out.getvalue())

    def test_main_list_data_items_with_invalid_box_prints_error_in_json(self):
        self.box.store('My value', run_id='run-1')
        with unittest.mock.patch('sys.stderr', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', '-j', 'list', '-r', 'run-2'])
            self.assertIn('{"error":"No run found with run-id or name starting with run-2"}', fake_out.getvalue())

    def test_main_name_run(self):
        self.box.store('My value', run_id='run-1')
        with unittest.mock.patch('sys.stdout', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'name', '-r', 'run-1', '-n', 'my-name'])
            self.assertIn('Run name set run-1', fake_out.getvalue())
            self.assertIn('run_id| name', fake_out.getvalue())
            self.assertIn('run-1  my-name', fake_out.getvalue())

    def test_main_name_run_with_invalid_run(self):
        self.box.store('My value', run_id='run-1')
        with unittest.mock.patch('sys.stderr', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'name', '-r', 'run-2', '-n', 'my-name'])
            self.assertIn('Error: No run found with run-id or name starting with run-2', fake_out.getvalue())

    def test_main_info_run(self):
        self.box.store('My value', name='my-data', run_id='run-1')
        with unittest.mock.patch('sys.stdout', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'info', '-r', 'run-1', '-d', 'my-data'])
            self.assertIn('Info 7ae40d399daba304 run-1', fake_out.getvalue())
            self.assertIn('Property  Value', fake_out.getvalue())
            self.assertIn('name    : my-data', fake_out.getvalue())
            self.assertIn('meta    :', fake_out.getvalue())

    def test_main_info_run_with_json(self):
        self.box.store('My value', name='my-data', run_id='run-1')
        with unittest.mock.patch('sys.stdout', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', '-j', 'info', '-r', 'run-1', '-d', 'my-data'])
            result = json.loads(fake_out.getvalue())
            self.assertEqual(result['name'], 'my-data')

    def test_main_info_with_invalid_run(self):
        self.box.store('My value', name='my-data', run_id='run-1')
        with unittest.mock.patch('sys.stderr', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'info', '-r', 'run-2', '-d', 'my-data'])
            self.assertIn('Error: No run found with run-id or name starting with run-2', fake_out.getvalue())

    def test_main_info_with_invalid_data(self):
        self.box.store('My value', name='my-data', run_id='run-1')
        with unittest.mock.patch('sys.stderr', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'info', '-r', 'run-1', '-d', 'my-other-data'])
            self.assertIn('Error: No item found with data-id starting with my-other-data', fake_out.getvalue())

if __name__ == '__main__':
    unittest.main()
