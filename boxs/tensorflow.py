"""Value type definitions for storing tensorflow specific classes"""
import importlib
import logging
import pathlib
import shutil
import tempfile

from .value_types import DirectoryValueType


logger = logging.getLogger(__name__)


class TensorflowKerasModelValueType(DirectoryValueType):
    """
    Value type for storing tensorflow keras models.

    The necessary tensorflow functions for saving and loading the model to a directory
    are dynamically loaded, so that the module can be imported WITHOUT tensorflow.
    Only if one instantiates an instance of the class, the tensorflow package must be
    available.
    """

    def __init__(self, dir_path=None, default_format='tf'):
        self._tf_models_module = importlib.import_module('tensorflow.keras.models')
        self._default_format = default_format
        super().__init__(dir_path)

    def supports(self, value):
        return False

    def write_value_to_writer(self, value, writer):
        model_dir_path = pathlib.Path(tempfile.mkdtemp())
        try:
            self._tf_models_module.save_model(
                value, filepath=model_dir_path, save_format=self._default_format
            )

            super().write_value_to_writer(model_dir_path, writer)
            writer.meta['model_format'] = self._default_format
        finally:
            shutil.rmtree(model_dir_path)

    def read_value_from_reader(self, reader):
        model_dir_path = super().read_value_from_reader(reader)
        try:
            result = self._tf_models_module.load_model(filepath=model_dir_path)
        finally:
            if self._dir_path is None:
                shutil.rmtree(model_dir_path)
        return result

    def _get_parameter_string(self):
        return self._default_format

    @classmethod
    def _from_parameter_string(cls, parameters):
        return cls(default_format=parameters)


class TensorBoardLogDirValueType(DirectoryValueType):
    """
    Value type for storing tensorbord logs.

    The necessary tensorflow functions for saving and loading the model to a directory
    are dynamically loaded, so that the module can be imported WITHOUT tensorflow.
    Only if one instantiates an instance of the class, the tensorflow package must be
    available.
    """

    def write_value_to_writer(self, value, writer):
        super().write_value_to_writer(pathlib.Path(value), writer)
        writer.meta['dir_content'] = 'tensorboard-logs'
