"""Configuration for Boxs"""
import importlib
import logging
import os
import pathlib


logger = logging.getLogger(__name__)


class Configuration:
    """
    Class that contains the individual config values.

    Attributes:
        default_box (str): The id of a box that should be used, no other box id is
            specified. Will be initialized from the `BOXS_DEFAULT_BOX` environment
            variable if defined, otherwise is initialized to `None`.
        home_directory (pathlib.Path): The home directory to use for boxs.
            This directory can be used for logging or other purposes, where boxs needs
            to persist data.
        init_module (str); The name of a python module, that should be automatically
            loaded at initialization time. Ideally, the loading of this module should
            trigger the definition of all boxes that are used, so that they can be
            found if needed. Setting this to a new module name will lead to an import
            of the module. Will be initialized from the `BOXS_INIT_MODULE` environment
            variable if defined, otherwise is initialized to `None`.
    """

    def __init__(self):
        self._initialized = False
        self.default_box = os.environ.get('BOXS_DEFAULT_BOX', None)
        logger.info("Using default_box %s", self.default_box)
        self.home_directory = pathlib.Path(
            os.environ.get(
                'BOXS_HOME_DIRECTORY',
                str(pathlib.Path.home() / '.boxs'),
            )
        )
        logger.info("Using home_directory %s", self.home_directory)
        self.init_module = os.environ.get('BOXS_INIT_MODULE', None)
        logger.info("Using init_module %s", self.init_module)

    @property
    def init_module(self):
        """
        Returns the name of the init_module that is used in this configuration.

        Returns:
            str: The name of the init_module that is used.
        """
        return self._init_module

    @init_module.setter
    def init_module(self, init_module):
        """
        Set the name of the init_module.

        Setting this value might lead to the module being imported, if boxs is
        properly initialized.

        Args:
            init_module (str): The name of the module to use for initialization.
        """
        self._init_module = init_module
        self._load_init_module()

    @property
    def initialized(self):
        """
        Returns if boxs is completely initialized.

        Returns:
            bool: `True` if the boxs library is initialized, otherwise `False`.
        """
        return self._initialized

    @initialized.setter
    def initialized(self, initialized):
        """
        Set the initialization status of boxs.

        Setting this value to `True` might lead to the init_module being imported, if
        `init_module` is set.

        Args:
            initialized (bool): If the library is fully initialized.
        """
        if not self._initialized and initialized:
            self._load_init_module()
        self._initialized = initialized

    def _load_init_module(self):
        if self.init_module is not None and self.initialized:
            logger.info("Import init_module %s", self.init_module)
            try:
                importlib.import_module(self.init_module)
            except ImportError as import_error:
                self.initialized = False
                raise import_error


_CONFIG = Configuration()


def get_config():
    """
    Returns the configuration.

    Returns:
         boxs.config.Configuration: The configuration.
    """
    global _CONFIG  # pylint: disable=global-statement
    if _CONFIG is None:
        logger.info("Create new configuration")
        _CONFIG = Configuration()
    return _CONFIG
