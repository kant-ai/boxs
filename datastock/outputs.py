"""LoadDataFunctions"""
import io
import json
import pathlib
import shutil
import tempfile


def as_bytes():
    """
    Returns a function to load data as bytes.

    Returns:
        datastock.storage.LoadDataFunction: A function which loads data and returns
            it as `bytes`.
    """

    def _read(reader):
        with reader.as_stream() as stream:
            return stream.read()

    return _read


def as_file(file_path=None):
    """
    Returns a function to load data and store it as a file.

    Args:
        file_path (Union[str,pathlib.Path]): A path to a file in which the data should
            be stored. The directory in which the file will be created has to exist.
            Defaults to `None`, in which case a temporary file will be returned. The
            caller is responsible for deleting the file after it is no longer used.

    Returns:
        datastock.storage.LoadDataFunction: A function which loads data, stores it
            into a file and returns its file path.
    """
    if file_path is None:
        file_path = tempfile.mktemp()
    file_path = pathlib.Path(file_path)

    def _read(reader):
        with reader.as_stream() as read_stream, io.FileIO(
            file_path, 'w'
        ) as file_stream:
            shutil.copyfileobj(read_stream, file_stream)
        return file_path

    return _read


def as_stream():
    """
    Returns a function to load data as a stream.

    Returns:
        datastock.storage.LoadDataFunction: A function which loads data and returns
            a stream from which it can be read.
    """

    def _read(reader):
        return reader.as_stream()

    return _read


def as_string(default_encoding='utf-8'):
    """
    Returns a function to load data as a string.

    Args:
        default_encoding (str): The name of the encoding to use when decoding the
            string. Defaults to `utf-8`.

    Returns:
        datastock.storage.LoadDataFunction: A function which loads data and returns
            it as a string.
    """

    def _read(reader):
        encoding = reader.meta.get('encoding', default_encoding)
        with reader.as_stream() as stream, io.TextIOWrapper(
            stream, encoding=encoding
        ) as text_reader:
            return text_reader.read()

    return _read


def as_json_value():
    """
    Returns a function to load data as a JSON value.

    Returns:
        datastock.storage.LoadDataFunction: A function which loads data, parses it as
            JSON and returns the contained value.
    """

    def _read(reader):
        with reader.as_stream() as stream:
            return json.load(stream)

    return _read
