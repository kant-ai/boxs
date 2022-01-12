"""Interface to backend storage"""
import abc
import typing


class Storage(abc.ABC):
    """
    Backend that allows a stock to store and load data in arbitrary storage locations.

    This abstract base class defines the interface, that is used by `Stock` to store
    and load data. The data items between `Stock` and `Storage` are always identified
    by their `data_id` and `run_id`. The functionality to store data is provided by
    the `Writer` object, that is created by the `create_writer()` method. Similarly,
    loading data is implemented in a separate `Reader` object that is created by
    `create_reader()`.
    """

    @abc.abstractmethod
    def exists(self, data_id, run_id):
        """
        Checks is a specific data already exists.

        Args:
            data_id (str): The `data_id` of the data to check for.
            run_id (str): The `run_id` of the data to check for.

        Returns:
            bool: `True` if the referenced data already exists, otherwise `False`.
        """

    @abc.abstractmethod
    def create_reader(self, data_id, run_id):
        """
        Creates a `Reader` instance, that allows to load existing data.

        Args:
            data_id (str): The `data_id` of the data that should be loaded.
            run_id (str): The `run_id` of the data that should be loaded.

        Returns:
            datastock.storage.Reader: The reader that will load the data from the
                storage.
        """

    @abc.abstractmethod
    def create_writer(self, data_id, run_id):
        """
        Creates a `Writer` instance, that allows to store new data.

        Args:
            data_id (str): The `data_id` of the new data.
            run_id (str): The `run_id` of the new data.

        Returns:
            datastock.storage.Writer: The writer that will write the data into the
                storage.
        """


class Reader(abc.ABC):
    """
    Base class for the storage specific reader implementations.
    """

    def __init__(self, data_id, run_id):
        """
        Creates a `Reader` instance, that allows to load existing data.

        Args:
            data_id (str): The `data_id` of the data that should be loaded.
            run_id (str): The `run_id` of the data that should be loaded.
        """
        self._data_id = data_id
        self._run_id = run_id

    @property
    def data_id(self):
        """The data_id of the data that this reader can read."""
        return self._data_id

    @property
    def run_id(self):
        """The run_id of the data that this reader can read."""
        return self._run_id

    def read_content(self, data_output):
        """
        Read the content and return it.

        Args:
            data_output (LoadDataFunction): The function that allows to load data from
                a reader.

        Returns:
            Any: The returned value from the `data_output`.
        """
        return data_output(self)

    @property
    @abc.abstractmethod
    def info(self):
        """Dictionary containing information about the data."""

    @property
    @abc.abstractmethod
    def meta(self):
        """Dictionary containing the meta-data about the data."""

    @abc.abstractmethod
    def as_stream(self):
        """
        Return a stream from which the data content can be read.

        Returns:
            io.RawIOBase: A stream instance from which the data can be read.
        """


class DelegatingReader(Reader):
    """
    Reader class that delegates all calls to a wrapped reader.
    """

    def __init__(self, delegate):
        """
        Create a new DelegatingReader.

        Args:
            delegate (datastock.storage.Reader): The reader to which all calls are
                delegated.
        """
        super().__init__(delegate.data_id, delegate.run_id)
        self.delegate = delegate

    @property
    def data_id(self):
        return self.delegate.data_id

    @property
    def run_id(self):
        return self.delegate.run_id

    @property
    def info(self):
        return self.delegate.info

    @property
    def meta(self):
        return self.delegate.meta

    def read_content(self, data_output):
        return self.delegate.read_content(data_output)

    def as_stream(self):
        return self.delegate.as_stream()


class Writer(abc.ABC):
    """
    Base class for the storage specific writer implementations.
    """

    def __init__(self, data_id, run_id):
        """
        Creates a `Writer` instance, that allows to store new data.

        Args:
            data_id (str): The `data_id` of the new data.
            run_id (str): The `run_id` of the new data.
        """
        self._data_id = data_id
        self._run_id = run_id

    @property
    def data_id(self):
        """Returns the data_id of the DataItem which this writer writes to."""
        return self._data_id

    @property
    def run_id(self):
        """Returns the run_id of the DataItem which this writer writes to."""
        return self._run_id

    @property
    @abc.abstractmethod
    def meta(self):
        """
        Returns a dictionary which contains meta-data of the DataItem.

        This allows either StoreDataFunctions or Transformers to add additional
        meta-data for the data item.
        """

    def write_content(self, data_input):
        """
        Write the data content to the storage.

        Args:
            data_input (StoreDataFunction): Function that writes the data to this
                writer.
        """
        data_input(self)

    @abc.abstractmethod
    def write_info(self, info):
        """
        Write the info for the data item to the storage.

        Args:
            info (Dict[str,Any]): Info for the data item.
        """

    @abc.abstractmethod
    def as_stream(self):
        """
        Return a stream to which the data content should be written.

        This method is being used by the StoreDataFunction to actually transfer the
        data.

        Returns:
            io.RawIOBase: The binary io-stream.
        """


class DelegatingWriter(Writer):
    """
    Writer that delegates all call to a wrapped writer.
    """

    def __init__(self, delegate):
        self.delegate = delegate
        super().__init__(delegate.data_id, delegate.run_id)

    @property
    def data_id(self):
        return self.delegate.data_id

    @property
    def run_id(self):
        return self.delegate.run_id

    @property
    def meta(self):
        return self.delegate.meta

    def write_content(self, data_input):
        self.delegate.write_content(data_input)

    def write_info(self, info):
        self.delegate.write_info(info)

    def as_stream(self):
        return self.delegate.as_stream()


LoadDataFunction = typing.Callable[[Reader], typing.Any]
StoreDataFunction = typing.Callable[[Writer], None]


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
            writer (datastock.storage.Writer): Writer object that is used for writing
                new data content and meta-data.

        Returns:
            datastock.storage.Writer: A modified writer that will be used instead.
        """
        return writer

    def transform_reader(self, reader):
        """
        Transform a given reader.

        Args:
            reader (datastock.storage.Reader): Reader object that is used for reading
                data content and meta-data.

        Returns:
            datastock.storage.Reader: A modified reader that will be used instead.
        """
        return reader
