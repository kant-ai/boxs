import unittest.mock

from boxs import run


class TestRunId(unittest.TestCase):

    def setUp(self):
        run._RUN_ID = None

    def test_run_id_is_automatically_defined(self):
        run_id = run.get_run_id()
        self.assertIsNotNone(run_id)

    def test_run_id_stays_the_same(self):
        first_run_id = run.get_run_id()
        second_run_id = run.get_run_id()
        self.assertEqual(first_run_id, second_run_id)

    def test_run_id_can_be_set(self):
        run.set_run_id('my-run-id')
        run_id = run.get_run_id()
        self.assertEqual(run_id, 'my-run-id')

    def test_setting_run_id_again_raises(self):
        run.set_run_id('my-first-id')
        with self.assertRaisesRegex(RuntimeError, "Run ID was already set"):
            run.set_run_id('my-run-id')


if __name__ == '__main__':
    unittest.main()
