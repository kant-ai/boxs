import importlib
import os
import sys
import unittest

import boxs.config
from boxs.config import get_config


class TestConfig(unittest.TestCase):

    def setUp(self):
        self.test_init_module = self.__module__.rsplit('.', maxsplit=1)[0]+'.box_config'

    def tearDown(self):
        boxs.config._CONFIG = None

    def test_configuration_is_automatically_created(self):
        config = get_config()
        self.assertIsNotNone(config)

    def test_configuration_is_cached(self):
        config1 = get_config()
        config2 = get_config()
        self.assertIs(config1, config2)

    def test_configuration_contains_default_box_id(self):
        config = get_config()
        self.assertTrue(hasattr(config, 'default_box'))

    def test_configuration_default_box_id_can_be_set(self):
        config = get_config()
        config.default_box = 'my-default-box'
        self.assertEqual('my-default-box', config.default_box)

    def test_configuration_default_box_id_is_initialized_from_env_variable_if_set(self):
        os.environ['BOXS_DEFAULT_BOX'] = 'my-default-box'
        reloaded_module = importlib.reload(sys.modules['boxs.config'])
        config = reloaded_module.get_config()
        self.assertEqual('my-default-box', config.default_box)
        del os.environ['BOXS_DEFAULT_BOX']

    def test_configuration_contains_init_module(self):
        config = get_config()
        self.assertTrue(hasattr(config, 'init_module'))

    def test_configuration_setting_non_existing_init_module_raises_import_error(self):
        config = get_config()
        config.initialized = True
        with self.assertRaisesRegex(ImportError, "No module named 'not_existing_module'"):
            config.init_module = 'not_existing_module'
        self.assertFalse(config.initialized)

    def test_configuration_setting_init_module_imports_the_module_when_initialized(self):
        config = get_config()
        config.initialized = True
        self.assertNotIn(self.test_init_module, sys.modules)
        config.init_module = self.test_init_module
        self.assertIn(self.test_init_module, sys.modules)
        del sys.modules[self.test_init_module]
        self.assertNotIn(self.test_init_module, sys.modules)
        config.initialized = False

    def test_configuration_init_module_is_initialized_from_env_variable_if_set_and_loaded(self):
        self.assertNotIn(self.test_init_module, sys.modules)
        os.environ['BOXS_INIT_MODULE'] = self.test_init_module
        reloaded_module = importlib.reload(sys.modules['boxs.config'])
        config = reloaded_module.get_config()
        self.assertEqual(self.test_init_module, config.init_module)
        self.assertIn(self.test_init_module, sys.modules)
        del os.environ['BOXS_INIT_MODULE']
        del sys.modules[self.test_init_module]
        self.assertNotIn(self.test_init_module, sys.modules)


if __name__ == '__main__':
    unittest.main()
