import io
import json
import pathlib
import shutil
import tempfile
import unittest.mock

from boxs.box import Box
from boxs.box_registry import unregister_box
from boxs.cli import main
from boxs.errors import BoxNotDefined
from boxs.filesystem import FileSystemStorage


class TestCli(unittest.TestCase):

    def setUp(self):
        self.dir = pathlib.Path(tempfile.mkdtemp())
        self.storage = FileSystemStorage(self.dir)
        self.box = Box('cli-box', self.storage)
        self.get_box_patcher = unittest.mock.patch('boxs.cli.get_box')
        self.get_box_mock = self.get_box_patcher.start()
        self.get_box_mock.return_value = self.box
        self.path_home_patcher = unittest.mock.patch('boxs.cli.pathlib.Path.home')
        self.path_home_mock = self.path_home_patcher.start()
        self.path_home_mock.return_value = self.dir

    def tearDown(self):
        shutil.rmtree(self.dir)
        unregister_box(self.box.box_id)
        self.get_box_patcher.stop()
        self.path_home_patcher.stop()

    def test_main_without_args_shows_help(self):
        with unittest.mock.patch('sys.stdout', new=io.StringIO()) as fake_out:
            main([])
            self.assertIn('usage: boxs [-h] [-b BOX] [-i INIT_MODULE] [-j]', fake_out.getvalue())
            self.assertIn('{list,info,name,diff,export,graph}', fake_out.getvalue())
            self.assertIn('Allows to inspect and manipulate boxes', fake_out.getvalue())
            self.assertIn('positional arguments:', fake_out.getvalue())
            self.assertIn('optional arguments:', fake_out.getvalue())

    def test_main_with_help_shows_help(self):
        with unittest.mock.patch('sys.stdout', new=io.StringIO()) as fake_out:
            with self.assertRaisesRegex(SystemExit, "0"):
                main(['-h'])
            self.assertIn('usage: boxs [-h] [-b BOX] [-i INIT_MODULE] [-j]', fake_out.getvalue())
            self.assertIn('{list,info,name,diff,export,graph}', fake_out.getvalue())
            self.assertIn('Allows to inspect and manipulate boxes', fake_out.getvalue())
            self.assertIn('positional arguments:', fake_out.getvalue())
            self.assertIn('optional arguments:', fake_out.getvalue())

    def test_main_with_init_module_sets_config(self):
        with unittest.mock.patch('boxs.cli.get_config') as get_config_mock:
            main(['-i', 'my_init_module'])
            self.assertEqual('my_init_module', get_config_mock.return_value.init_module)

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
        self.get_box_mock.side_effect = BoxNotDefined('unknown-box')
        self.box.store('My value', run_id='run-1')
        with unittest.mock.patch('sys.stderr', new=io.StringIO()) as fake_out:
            main(['-b', 'unknown-box', 'list', '-r', 'run-2'])
            self.assertIn('Error: Box with box id unknown-box not defined', fake_out.getvalue())

    def test_main_list_data_items_without_default_box_prints_error(self):
        self.get_box_mock.side_effect = BoxNotDefined(None)
        self.box.store('My value', run_id='run-1')
        with unittest.mock.patch('sys.stderr', new=io.StringIO()) as fake_out:
            main(['list', '-r', 'run-2'])
            self.assertIn('Error: Box with box id None not defined', fake_out.getvalue())

    def test_main_list_data_items_with_invalid_run_prints_error(self):
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
            self.assertIn('Info fb920699f86b785a run-1', fake_out.getvalue())
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

    def test_main_diff(self):
        self.box.store('My value', name='my-data', run_id='run-1')
        self.box.store('My other', name='my-data', run_id='run-2')
        with unittest.mock.patch('sys.stdout', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'diff', 'my-data:run-1', 'my-data:run-2'])
            self.assertIn('< My value', fake_out.getvalue())
            self.assertIn('> My other', fake_out.getvalue())

    def test_main_diff_with_uris(self):
        data_ref1 = self.box.store('My value', name='my-data', run_id='run-1')
        data_ref2 = self.box.store('My other', name='my-data', run_id='run-2')
        with unittest.mock.patch('sys.stdout', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'diff', data_ref1.uri, data_ref2.uri])
            self.assertIn('< My value', fake_out.getvalue())
            self.assertIn('> My other', fake_out.getvalue())

    def test_main_diff_with_custom_diff(self):
        self.box.store('My value', name='my-data', run_id='run-1')
        self.box.store('My other', name='my-data', run_id='run-2')
        with unittest.mock.patch('boxs.cli.subprocess.run') as run_mock:
            main(['-b', 'cli-box', 'diff', '--diff-command', 'my-diff', 'my-data:run-1', 'my-data:run-2'])
            run_mock.assert_called()
            self.assertEqual('my-diff', run_mock.call_args[0][0][0])

    def test_main_diff_with_additional_diff_arguments(self):
        self.box.store('My value', name='my-data', run_id='run-1')
        self.box.store('My other', name='my-data', run_id='run-2')
        with unittest.mock.patch('boxs.cli.subprocess.run') as run_mock:
            main(['-b', 'cli-box', 'diff', 'my-data:run-1', 'my-data:run-2', '--', '--my-arg'])
            run_mock.assert_called()
            self.assertEqual('--my-arg', run_mock.call_args[0][0][7])

    def test_main_diff_without_labels(self):
        self.box.store('My value', name='my-data', run_id='run-1')
        self.box.store('My other', name='my-data', run_id='run-2')
        with unittest.mock.patch('boxs.cli.subprocess.run') as run_mock:
            main(['-b', 'cli-box', 'diff', 'my-data:run-1', 'my-data:run-2', '--without-labels'])
            run_mock.assert_called()
            self.assertNotIn('--label', run_mock.call_args[0][0])
            self.assertNotIn('my-data:run-1', run_mock.call_args[0][0])
            self.assertNotIn('my-data:run-2', run_mock.call_args[0][0])

    def test_main_diff_with_ambiguous_data(self):
        self.box.store('My value', origin='1', name='my-data', run_id='run-1')
        self.box.store('My value', origin='2', name='my-other', run_id='run-1')
        self.box.store('My other', name='my-data', run_id='run-2')
        with unittest.mock.patch('sys.stderr', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'diff', 'my-:run-1', 'my-data:run-2'])
            self.assertIn('Error: Ambiguous values to diff', fake_out.getvalue())

    def test_main_export(self):
        export_file_path = pathlib.Path(tempfile.mktemp())
        self.assertFalse(export_file_path.exists())
        self.box.store('My value', origin='1', name='my-data', run_id='run-1')
        with unittest.mock.patch('sys.stdout', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'export', 'my-:run-1', str(export_file_path)])
            self.assertIn('my-:run-1 successfully exported to', fake_out.getvalue())
        self.assertTrue(export_file_path.exists())
        export_file_path.unlink()

    def test_main_export_with_uri(self):
        export_file_path = pathlib.Path(tempfile.mktemp())
        self.assertFalse(export_file_path.exists())
        data_ref = self.box.store('My value', origin='1', name='my-data', run_id='run-1')
        with unittest.mock.patch('sys.stdout', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'export', data_ref.uri, str(export_file_path)])
            self.assertIn('boxs://cli-box/92eb5d2d4f7d499c/run-1 successfully exported to', fake_out.getvalue())
        self.assertTrue(export_file_path.exists())
        export_file_path.unlink()

    def test_main_export_with_not_existing_data(self):
        export_file_path = pathlib.Path(tempfile.mktemp())
        self.assertFalse(export_file_path.exists())
        self.box.store('My value', origin='1', name='my-data', run_id='run-1')
        with unittest.mock.patch('sys.stderr', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'export', 'my-:run-2', str(export_file_path)])
            self.assertIn('Error: No item found for my-:run-2.', fake_out.getvalue())
        self.assertFalse(export_file_path.exists())

    def test_main_export_with_multiple_data(self):
        export_file_path = pathlib.Path(tempfile.mktemp())
        self.assertFalse(export_file_path.exists())
        self.box.store('My value', origin='1', name='my-data', run_id='run-1')
        self.box.store('My value', origin='2', name='my-other', run_id='run-1')
        with unittest.mock.patch('sys.stderr', new=io.StringIO()) as fake_out:
            main(['-b', 'cli-box', 'export', 'my-:run-1', str(export_file_path)])
            self.assertIn('Error: Multiple items found for my-:run-1.', fake_out.getvalue())
        self.assertFalse(export_file_path.exists())

    def test_main_graph_to_file(self):
        graph_file_path = pathlib.Path(tempfile.mktemp())
        self.assertFalse(graph_file_path.exists())
        self.box.store('My value', origin='1', name='my-data', run_id='run-1')
        main(['-b', 'cli-box', 'graph', 'my-:run-1', str(graph_file_path)])
        self.assertTrue(graph_file_path.exists())
        graph = graph_file_path.read_text()
        self.assertIn('digraph {', graph)
        graph_file_path.unlink()

    def test_main_graph_to_stdout(self):
        self.box.store('My value', origin='1', name='my-data', run_id='run-1')
        fake_io = io.StringIO()
        fake_io.close = lambda: None
        with unittest.mock.patch('sys.stdout', new=fake_io) as fake_out:
            main(['-b', 'cli-box', 'graph', 'my-:run-1'])
            graph = fake_out.getvalue()
        self.assertIn('digraph {', graph)

    def test_main_graph_with_box_as_part_of_query(self):
        graph_file_path = pathlib.Path(tempfile.mktemp())
        self.assertFalse(graph_file_path.exists())
        self.box.store('My value', origin='1', name='my-data', run_id='run-1')
        main(['graph', 'cli-box:my-:run-1', str(graph_file_path)])
        self.assertTrue(graph_file_path.exists())
        graph = graph_file_path.read_text()
        self.assertIn('digraph {', graph)
        graph_file_path.unlink()


if __name__ == '__main__':
    unittest.main()
