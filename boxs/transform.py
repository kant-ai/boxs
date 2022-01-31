"""Transforming data items"""
from .storage import Reader, Writer


class Transformer:
    # pylint: disable=no-self-use
    """
    Base class for transformers

    Transformers allow modifying content and meta-data of a DataItem during store and
    load by wrapping the writer and reader that are used for accessing them from the
    storage. This can be useful for e.g. adding new meta-data, filtering content or
    implementing encryption.
    """

    def transform_writer(self, writer):
        """
        Transform a given writer.

        Args:
            writer (boxs.storage.Writer): Writer object that is used for writing
                new data content and meta-data.

        Returns:
            boxs.storage.Writer: A modified writer that will be used instead.
        """
        return writer

    def transform_reader(self, reader):
        """
        Transform a given reader.

        Args:
            reader (boxs.storage.Reader): Reader object that is used for reading
                data content and meta-data.

        Returns:
            boxs.storage.Reader: A modified reader that will be used instead.
        """
        return reader


class DelegatingReader(Reader):
    """
    Reader class that delegates all calls to a wrapped reader.
    """

    def __init__(self, delegate):
        """
        Create a new DelegatingReader.

        Args:
            delegate (boxs.storage.Reader): The reader to which all calls are
                delegated.
        """
        super().__init__(delegate.item)
        self.delegate = delegate

    @property
    def info(self):
        return self.delegate.info

    @property
    def meta(self):
        return self.delegate.meta

    def read_value(self, value_type):
        return self.delegate.read_value(value_type)

    def as_stream(self):
        return self.delegate.as_stream()


class DelegatingWriter(Writer):
    """
    Writer that delegates all call to a wrapped writer.
    """

    def __init__(self, delegate):
        self.delegate = delegate
        super().__init__(delegate.item, delegate.name, delegate.tags)

    @property
    def meta(self):
        return self.delegate.meta

    def write_value(self, value, value_type):
        self.delegate.write_value(value, value_type)

    def write_info(self, info):
        return self.delegate.write_info(info)

    def as_stream(self):
        return self.delegate.as_stream()
