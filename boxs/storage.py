"""Interface to backend storage"""
import abc
import collections


Run = collections.namedtuple('Run', 'run_id name time')
Item = collections.namedtuple('Item', 'box_id data_id run_id name time')


class ItemQuery:
    """
    Query object that allows to query a Storage for items.

    The query is build from a string with up to 3 components separated by ':'.
    The individual components are the <box-id>:<data-id>:<run-id>.
    A query doesn't have to contain all components, but it needs to contain at least
    one with its trailing ':'.

    All components are treated as prefixes, so one doesn't have to write the full ids.


    Examples:
        # Query all items in a specific run
        >>> ItemQuery('my-run-id')
        # or with written separators
        >>> ItemQuery('::my-run-id')

        # Query all items with the same data-id in all runs
        >>> ItemQuery('my-data-id:')

        # Query all items with the same data-id in specific runs with a shared prefix
        >>> ItemQuery('my-data-id:my-run')
        # for multiple runs like e.g. my-run-1 and my-run-2

        # Query everything in a specific box:
        >>> ItemQuery('box-id::')

    Attributes:
        box (Optional[str]): The optional box id.
        data (Optional[str]): The optional prefix for data ids or names.
        run (Optional[str]): The optional prefix for run ids or names.
    """

    def __init__(self, string):
        parts = list(reversed(string.strip().rsplit(':')))
        self.run = parts[0] or None
        if len(parts) > 1:
            self.data = parts[1] or None
        else:
            self.data = None
        if len(parts) > 2:
            self.box = parts[2] or None
        else:
            self.box = None
        if len(parts) > 3:
            raise ValueError("Invalid query, must be in format '<box>:<data>:<run>'.")
        if self.run is None and self.data is None and self.box is None:
            raise ValueError("Neither, box, data or run is specified.")

    def __str__(self):
        return ':'.join([self.box or '', self.data or '', self.run or ''])


class Storage(abc.ABC):
    """
    Backend that allows a box to store and load data in arbitrary storage locations.

    This abstract base class defines the interface, that is used by `Box` to store
    and load data. The data items between `Box` and `Storage` are always identified
    by their `data_id` and `run_id`. The functionality to store data is provided by
    the `Writer` object, that is created by the `create_writer()` method. Similarly,
    loading data is implemented in a separate `Reader` object that is created by
    `create_reader()`.
    """

    @abc.abstractmethod
    def list_runs(self, box_id, limit=None):
        """
        List the runs within a box stored in this storage.

        The runs should be returned in descending order of their start time.

        Args:
            box_id (str): `box_id` of the box in which to look for runs.
            limit (Optional[int]): Limits the returned runs to maximum `limit` number.
                Defaults to `None` in which case all runs are returned.

        Returns:
            List[box.storage.Run]: The runs.
        """

    @abc.abstractmethod
    def list_items_in_run(self, box_id, run_id):
        """
        List all items that were created in a run.

        The runs should be returned in descending order of their start time.

        Args:
            box_id (str): `box_id` of the box in which to look for.
            run_id (str): Run id of the run for which all items should be returned.

        Returns:
            List[box.storage.Item]: The runs.
        """

    @abc.abstractmethod
    def list_items(self, item_query):
        """
        List all items that match a given query.

        The item query can contain parts of box id, run id or run name and data id or
        data name. If a query value is not set (`== None`) it is not used as a filter
        criteria.

        Args:
            item_query (boxs.storage.ItemQuery): The query which defines which items
                should be listed.

        Returns:
            List[box.storage.Item]: The runs.
        """

    @abc.abstractmethod
    def set_run_name(self, box_id, run_id, name):
        """
        Set the name of a run.

        The name can be updated and removed by providing `None`.

        Args;
            box_id (str): `box_id` of the box in which the run is stored.
            run_id (str): Run id of the run which should be named.
            name (Optional[str]): New name of the run. If `None`, an existing name
                will be removed.

        Returns:
            box.storage.Run: The run with its new name.
        """

    @abc.abstractmethod
    def exists(self, data_ref):
        """
        Checks is a specific data already exists.

        Args:
            data_ref (boxs.data.DataRef): The reference to the data that should be
                checked if it exists.

        Returns:
            bool: `True` if the referenced data already exists, otherwise `False`.
        """

    @abc.abstractmethod
    def create_reader(self, data_ref):
        """
        Creates a `Reader` instance, that allows to load existing data.

        Args:
            data_ref (boxs.data.DataRef): The reference to the data that should be
                read.

        Returns:
            boxs.storage.Reader: The reader that will load the data from the
                storage.
        """

    @abc.abstractmethod
    def create_writer(self, data_ref, name=None, tags=None):
        """
        Creates a `Writer` instance, that allows to store new data.

        Args:
            data_ref (boxs.data.DataRef): The reference to the new data item.
            name (str): An optional name, that can be used for referring to this item
                within the run. Defaults to `None`.
            tags (Dict[str,str]): A dictionary containing tags that can be used for
                grouping multiple items together. Defaults to an empty dictionary.

        Returns:
            boxs.storage.Writer: The writer that will write the data into the
                storage.
        """


class Reader(abc.ABC):
    """
    Base class for the storage specific reader implementations.
    """

    def __init__(self, data_ref):
        """
        Creates a `Reader` instance, that allows to load existing data.

        Args:
            data_ref (boxs.data.DataRef): The `data_ref` of the data that should be
                loaded.
        """
        self._data_ref = data_ref

    @property
    def data_ref(self):
        """The data_ref of the data that this reader can read."""
        return self._data_ref

    def read_value(self, value_type):
        """
        Read the value and return it.

        Args:
            value_type (boxs.value_types.ValueType): The value type that reads the
                value from the reader and converts it to the correct type.

        Returns:
            Any: The returned value from the `value_type`.
        """
        return value_type.read_value_from_reader(self)

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
            delegate (boxs.storage.Reader): The reader to which all calls are
                delegated.
        """
        super().__init__(delegate.data_ref)
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


class Writer(abc.ABC):
    """
    Base class for the storage specific writer implementations.
    """

    def __init__(self, data_ref, name, tags):
        """
        Creates a `Writer` instance, that allows to store new data.

        Args:
            data_ref (boxs.data.DataRef): The `data_ref` of the new data.
        """
        self._data_ref = data_ref
        self._name = name
        self._tags = tags

    @property
    def data_ref(self):
        """Returns the data_ref of the DataItem which this writer writes to."""
        return self._data_ref

    @property
    def name(self):
        """Returns the name of the new data item."""
        return self._name

    @property
    def tags(self):
        """Returns the tags of the new data item."""
        return self._tags

    @property
    @abc.abstractmethod
    def meta(self):
        """
        Returns a dictionary which contains meta-data of the DataItem.

        This allows either StoreDataFunctions or Transformers to add additional
        meta-data for the data item.
        """

    def write_value(self, value, value_type):
        """
        Write the data content to the storage.

        Args:
            value (Any): The value that should be written to the writer.
            value_type (boxs.value_types.ValueType): The value type that takes care
                of actually writing the value and converting it to the correct type.
        """
        value_type.write_value_to_writer(value, self)

    @abc.abstractmethod
    def write_info(self, origin, parents, meta):
        """
        Write the info for the data item to the storage.

        Args:
            origin (str): The origin of the data.
            parents (tuple[DataInfo]): The infos about the parent data items from
                which this new data item was derived.
            meta (Dict[str,Any]): Meta-data about the new data item.

        Returns:
            boxs.data.DataInfo: The data info about the new data item.
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
        super().__init__(delegate.data_ref, delegate.name, delegate.tags)

    @property
    def meta(self):
        return self.delegate.meta

    def write_value(self, value, value_type):
        self.delegate.write_value(value, value_type)

    def write_info(self, origin, parents, meta):
        return self.delegate.write_info(origin, parents, meta)

    def as_stream(self):
        return self.delegate.as_stream()


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
