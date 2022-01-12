"""Functions for writing input data"""
import io
import json
import shutil


def from_bytes(byte_buffer):
    """
    Input for writing `bytes` to a `datastock.Writer`.

    Args:
        byte_buffer (bytes): The bytes which should be used as input data.

    Returns:
        Callable: A callable which takes a `Writer` as single argument and writes the
            bytes to the writer.
    """

    def _write(writer):
        source_stream = io.BytesIO(byte_buffer)
        with writer.as_stream() as destination_stream:
            shutil.copyfileobj(source_stream, destination_stream)

    return _write


def from_file(file_path):
    """
    Input for writing the content of a file to a `datastock.Writer`.

    Args:
        file_path (pathlib.Path): The path of a file whose content should be written.

    Returns:
        Callable: A callable which takes a `Writer` as single argument and writes the
            file content to the writer.
    """

    def _write(writer):
        with file_path.open(
            'rb'
        ) as file_reader, writer.as_stream() as destination_stream:
            shutil.copyfileobj(file_reader, destination_stream)
        writer.run_id = str(file_path.stat().st_mtime)

    return _write


def from_stream(input_stream):
    """
    Input for writing the content of a stream to a `datastock.Writer`.

    Args:
        input_stream (io.RawIOBase): The stream whose content should be written.

    Returns:
        Callable: A callable which takes a `Writer` as single argument and writes the
            stream content to the writer.
    """

    def _write(writer):
        with writer.as_stream() as destination_stream:
            shutil.copyfileobj(input_stream, destination_stream)

    return _write


def from_string(string, encoding='utf-8'):
    """
    Input for writing a string to a `datastock.Writer`.

    Args:
        string (str): The string which should be written.
        encoding (str): The encoding which should be used for encoding the string.
            Defaults to 'utf-8'.

    Returns:
        Callable: A callable which takes a `Writer` as single argument and writes the
            string content in the specified encoding to the writer.
    """

    def _write(writer):
        source_stream = io.BytesIO(string.encode(encoding))
        writer.meta['encoding'] = encoding
        with writer.as_stream() as destination_stream:
            shutil.copyfileobj(source_stream, destination_stream)

    return _write


def from_value_as_json(value):
    """
    Input for writing a value in JSON format to a `datastock.Writer`.

    Additionally, it sets the `media_type' meta-data value to `application/json'.

    Args:
        value (Any): The value which should be written. This has to be serializable
            using the standard JSON encoder/decoder in python.

    Returns:
        Callable: A callable which takes a `Writer` as single argument and writes the
            value JSON content to the writer.
    """

    def _write(writer):
        writer.meta['media_type'] = 'application/json'
        with writer.as_stream() as destination_stream, io.TextIOWrapper(
            destination_stream
        ) as text_writer:
            json.dump(value, text_writer, sort_keys=True, separators=(',', ':'))

    return _write
