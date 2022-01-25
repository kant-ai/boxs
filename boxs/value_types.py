"""Types for reading and writing of different value types"""
import abc
import importlib
import io
import json
import logging
import pathlib
import shutil
import tempfile
import zipfile

logger = logging.getLogger(__name__)


class ValueType(abc.ABC):
    """
    Base class for implementing the type depending reading and writing of values to
    and from Readers and Writers.
    """

    def __init__(self):
        self._logger = logging.getLogger(str(self.__class__))

    def supports(self, value):  # pylint: disable=unused-argument,no-self-use
        """
        Returns if the value type can be used for reading a writing the given value.

        This method is used to determine if a value can be read and written by a value
        type. It is only necessary, if the value type should be picked up
        automatically. If it is only used explicitly, no check is performed.

        Args:
            value (Any): The value for which the value type should be checked.

        Returns:
            bool: `True` if the value type supports this value, otherwise `False`.
                The default implementation just returns `False`.
        """
        return False

    @abc.abstractmethod
    def write_value_to_writer(self, value, writer):
        """
        Write the given value to the writer.

        This method needs to be implemented by the specific value type implementations
        that take care of the necessary type conversions.

        Args:
            value (Any): The value that should be written.
            writer (boxs.storage.Writer): The writer into which the value should be
                written.
        """

    @abc.abstractmethod
    def read_value_from_reader(self, reader):
        """
        Read a value from the reader.

        This method needs to be implemented by the specific value type implementations
        that take care of the necessary type conversions.

        Args:
            reader (boxs.storage.Reader): The reader from which the value should be
                read.

        Returns:
            Any: The value that was read from the reader.
        """

    def get_specification(self):
        """
        Returns a string that specifies this ValueType.

        Returns:
            str: The specification that can be used for recreating this specific
                ValueType.
        """
        module_name = self.__class__.__module__
        class_name = self.__class__.__qualname__
        parameter_string = self._get_parameter_string()
        return ':'.join([module_name, class_name, parameter_string])

    @classmethod
    def from_specification(cls, specification):
        """
        Create a new ValueType instance from its specification string.

        Args:
            specification (str): The specification string that specifies the ValueType
                thate should be instantiated.


        Returns:
            ValueType: The specified ValueType instance.
        """
        logger.debug("Recreating value type from specification %s", specification)
        module_name, class_name, parameter_string = specification.split(':', maxsplit=2)
        module = importlib.import_module(module_name)
        class_ = getattr(module, class_name)
        value_type = class_._from_parameter_string(  # pylint: disable=protected-access
            parameter_string,
        )
        return value_type

    def _get_parameter_string(self):  # pylint: disable=no-self-use
        """
        Return a string encoding the ValueType specific parameters.

        This method needs to be overridden by subclasses, that use parameters.

        Returns:
            str: The string containing the parameters.
        """
        return ''

    @classmethod
    def _from_parameter_string(cls, parameters):  # pylint: disable=unused-argument
        """
        Return a new instance of a specific ValueType from its parameter string.

        This method needs to be overridden by subclasses, that use parameters.

        Returns:
            ValueType: The specified ValueType instance.
        """
        return cls()

    def __repr__(self):
        return self.get_specification()

    def __str__(self):
        return self.get_specification()


class BytesValueType(ValueType):
    """
    A ValueType for reading and writing bytes/bytearray values.
    """

    def supports(self, value):
        return isinstance(value, (bytes, bytearray))

    def write_value_to_writer(self, value, writer):
        source_stream = io.BytesIO(value)
        with writer.as_stream() as destination_stream:
            shutil.copyfileobj(source_stream, destination_stream)

    def read_value_from_reader(self, reader):
        with reader.as_stream() as stream:
            return stream.read()


class DirectoryValueType(ValueType):
    """
    A ValueType for reading and writing directories.

    The values have to be instances of `pathlib.Path` and must point to an existing
    directory. Everything within this directory is then added to a new zip archive,
    that is written to the storage.
    """

    def __init__(self, dir_path=None):
        self._dir_path = dir_path
        super().__init__()

    def supports(self, value):
        return isinstance(value, pathlib.Path) and value.exists() and value.is_dir()

    def write_value_to_writer(self, value, writer):
        def _add_directory(root, directory, _zip_file):
            for path in directory.iterdir():
                if path.is_file():
                    _zip_file.write(path, arcname=path.relative_to(root))
                if path.is_dir():
                    _add_directory(root, path, _zip_file)

        with writer.as_stream() as destination_stream, zipfile.ZipFile(
            destination_stream, mode='w'
        ) as zip_file:
            _add_directory(value, value, zip_file)

    def read_value_from_reader(self, reader):
        dir_path = self._dir_path
        if self._dir_path is None:
            dir_path = tempfile.mkdtemp()
        dir_path = pathlib.Path(dir_path)
        self._logger.debug("Directory will be stored in %s", dir_path)
        with reader.as_stream() as read_stream, zipfile.ZipFile(
            read_stream, 'r'
        ) as zip_file:
            for zip_info in zip_file.infolist():
                target_path = dir_path / zip_info.filename
                self._logger.debug(
                    "Extracting %s to %s", zip_info.filename, target_path
                )
                zip_file.extract(zip_info, target_path)
        return dir_path


class FileValueType(ValueType):
    """
    A ValueType for reading and writing files.

    The values have to be instances of `pathlib.Path`.
    """

    def __init__(self, file_path=None):
        self._file_path = file_path
        super().__init__()

    def supports(self, value):
        return isinstance(value, pathlib.Path) and value.exists() and value.is_file()

    def write_value_to_writer(self, value, writer):
        with value.open('rb') as file_reader, writer.as_stream() as destination_stream:
            shutil.copyfileobj(file_reader, destination_stream)

    def read_value_from_reader(self, reader):
        if hasattr(reader, 'as_file'):
            self._logger.debug("Reader has as_file()")
            if self._file_path:
                self._logger.debug("Copying file directly")
                shutil.copyfile(str(reader.as_file()), str(self._file_path))
                return self._file_path
            return reader.as_file()
        file_path = self._file_path
        if self._file_path is None:
            file_path = tempfile.mktemp()
        file_path = pathlib.Path(file_path)
        with reader.as_stream() as read_stream, io.FileIO(
            file_path, 'w'
        ) as file_stream:
            self._logger.debug("Writing file from stream")
            shutil.copyfileobj(read_stream, file_stream)
        return file_path


class JsonValueType(ValueType):
    """
    ValueType for storing values as JSON.
    """

    def supports(self, value):
        return isinstance(value, (dict, list))

    def write_value_to_writer(self, value, writer):
        writer.meta['media_type'] = 'application/json'
        with writer.as_stream() as destination_stream, io.TextIOWrapper(
            destination_stream
        ) as text_writer:
            json.dump(value, text_writer, sort_keys=True, separators=(',', ':'))

    def read_value_from_reader(self, reader):
        with reader.as_stream() as stream:
            return json.load(stream)


class StreamValueType(ValueType):
    """
    A ValueType for reading and writing from and to a stream.
    """

    def supports(self, value):
        return isinstance(value, io.IOBase)

    def write_value_to_writer(self, value, writer):
        with writer.as_stream() as destination_stream:
            shutil.copyfileobj(value, destination_stream)

    def read_value_from_reader(self, reader):
        return reader.as_stream()


class StringValueType(ValueType):
    """
    A ValueType for reading and writing string values.

    The ValueType can use different encodings via its constructor argument, but
    defaults to 'utf-8'.
    """

    def __init__(self, default_encoding='utf-8'):
        self._default_encoding = default_encoding
        super().__init__()

    def supports(self, value):
        return isinstance(value, str)

    def write_value_to_writer(self, value, writer):
        source_stream = io.BytesIO(value.encode(self._default_encoding))
        writer.meta['encoding'] = self._default_encoding
        with writer.as_stream() as destination_stream:
            shutil.copyfileobj(source_stream, destination_stream)

    def read_value_from_reader(self, reader):
        encoding = reader.meta.get('encoding', self._default_encoding)
        self._logger.debug("Reading string with encoding %s", encoding)
        with reader.as_stream() as stream, io.TextIOWrapper(
            stream, encoding=encoding
        ) as text_reader:
            return text_reader.read()

    def _get_parameter_string(self):
        return self._default_encoding

    @classmethod
    def _from_parameter_string(cls, parameters):
        return cls(default_encoding=parameters)
