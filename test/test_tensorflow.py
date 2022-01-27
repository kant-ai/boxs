import io
import pathlib
import shutil
import sys
import tempfile
import unittest.mock
import zipfile

from boxs.tensorflow import TensorflowKerasModelValueType, TensorBoardLogDirValueType
from boxs.value_types import ValueType

from .test_value_types import DummyReader, DummyWriter


def load_model(filepath=None):
    return 'My model dummy'


def save_model(value, filepath=None, save_format=None):
    model_file_path = filepath / ('my-model.'+save_format)
    model_file_path.write_text('My model dummy')


class TestTensorflowKerasModelValueType(unittest.TestCase):

    def setUp(self):
        self.import_patcher = unittest.mock.patch('boxs.tensorflow.importlib.import_module')
        self.import_mock = self.import_patcher.start()
        self.import_mock.return_value = sys.modules[__name__]
        self.reader = DummyReader()
        self.writer = DummyWriter()

    def tearDown(self):
        self.import_patcher.stop()

    def test_import_error_is_raised_if_no_tensorflow_available(self):
        self.import_mock.side_effect = ImportError("No dummy tensorflow")
        with self.assertRaisesRegex(ImportError, "No dummy tensorflow"):
            TensorflowKerasModelValueType()

    def test_supports_nothing(self):
        value_type = TensorflowKerasModelValueType()
        self.assertFalse(value_type.supports('model'))
        self.assertFalse(value_type.supports(1))
        self.assertFalse(value_type.supports(list()))
        self.assertFalse(value_type.supports(dict()))

    def test_archive_is_extracted_and_then_model_returned(self):
        archive_path = pathlib.Path(__file__).parent / 'empty.zip'
        self.reader.stream = io.FileIO(archive_path)
        value_type = TensorflowKerasModelValueType()
        model = value_type.read_value_from_reader(self.reader)
        self.assertEqual('My model dummy', model)

    def test_given_directory_is_not_deleted(self):
        target_directory = pathlib.Path(tempfile.mkdtemp())
        archive_path = pathlib.Path(__file__).parent / 'empty.zip'
        self.reader.stream = io.FileIO(archive_path)
        value_type = TensorflowKerasModelValueType(dir_path=target_directory)
        value_type.read_value_from_reader(self.reader)
        self.assertTrue(target_directory.exists())
        shutil.rmtree(target_directory)

    def test_value_writes_model(self):
        value_type = TensorflowKerasModelValueType()
        value_type.write_value_to_writer(None, self.writer)

        zip_file = zipfile.ZipFile(self.writer.reset())
        self.assertEqual(1, len(zip_file.namelist()))
        self.assertEqual(b"My model dummy", zip_file.read('my-model.tf'))

    def test_value_writes_model_with_configured_format(self):
        value_type = TensorflowKerasModelValueType(default_format='h5')
        value_type.write_value_to_writer(None, self.writer)

        zip_file = zipfile.ZipFile(self.writer.reset())
        self.assertEqual(1, len(zip_file.namelist()))
        self.assertEqual(b"My model dummy", zip_file.read('my-model.h5'))

    def test_value_sets_model_format_in_meta(self):
        value_type = TensorflowKerasModelValueType()
        value_type.write_value_to_writer(None, self.writer)

        self.assertEqual('tf', self.writer.meta['model_format'])

    def test_get_specification_returns_class_and_module_name(self):
        value_type = TensorflowKerasModelValueType()
        specification = value_type.get_specification()
        self.assertEqual('boxs.tensorflow:TensorflowKerasModelValueType:tf', specification)

    def test_create_from_specification_can_create_from_specification(self):
        value_type = TensorflowKerasModelValueType()
        specification = value_type.get_specification()
        recreated_value_type = ValueType.from_specification(specification)
        self.assertIsInstance(recreated_value_type, TensorflowKerasModelValueType)

    def test_repr_returns_specification(self):
        value_type = TensorflowKerasModelValueType()
        result = repr(value_type)
        self.assertEqual('boxs.tensorflow:TensorflowKerasModelValueType:tf', result)

    def test_can_be_recreated_with_non_default_format(self):
        value_type = TensorflowKerasModelValueType(default_format='h5')
        specification = value_type.get_specification()
        recreated_value_type = ValueType.from_specification(specification)
        self.assertEqual(value_type._default_format, recreated_value_type._default_format)


class TestTensorBoardLogDirValueType(unittest.TestCase):

    def setUp(self):
        self.reader = DummyReader()
        self.writer = DummyWriter()
        self.dir_path = pathlib.Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.dir_path)

    def test_write_adds_dir_content_meta_data(self):
        value_type = TensorBoardLogDirValueType()
        value_type.write_value_to_writer(self.dir_path, self.writer)

        self.assertEqual({'dir_content': 'tensorboard-logs'}, self.writer.meta)


if __name__ == '__main__':
    unittest.main()
