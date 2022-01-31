"""Interface to backend storage"""
import abc
import collections


class Run(collections.namedtuple('Run', 'box_id run_id name time')):
    """
    A class representing a run.
    """

    __slots__ = ()

    def __str__(self):
        return f"Run({self.box_id}/{self.run_id})"

    def __eq__(self, o):
        return (self.box_id, self.run_id) == (o.box_id, o.run_id)

    def __hash__(self):
        return hash((self.box_id, self.run_id))


Run.__new__.__defaults__ = (None, None)  # type: ignore


class Item(collections.namedtuple('Item', 'box_id data_id run_id name time')):
    """
    A class representing a data item.
    """

    __slots__ = ()

    def __str__(self):
        return f"Item(boxs://{self.box_id}/{self.data_id}/{self.run_id})"


Item.__new__.__defaults__ = (None, None)  # type: ignore


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

    @classmethod
    def from_fields(cls, box=None, data=None, run=None):
        """
        Create an ItemQuery from the individual fields of the query.

        Args:
            box (Optional[str]): The search string for boxes. Defaults to `None`
                matching all boxes.
            data (Optional[str]): The search string for data items. Defaults to `None`
                matching all data items.
            run (Optional[str]): The search string for run. Defaults to `None`
                matching all runs.

        Returns:
            ItemQuery: The new item query with the given search fields.
        """
        return ItemQuery(':'.join([box or '', data or '', run or '']))

    def __str__(self):
        return ':'.join([self.box or '', self.data or '', self.run or ''])


class Storage(abc.ABC):
    """
    Backend that allows a box to store and load data in arbitrary storage locations.

    This abstract base class defines the interface, that is used by `Box` to store
    and load data. The data items between `Box` and `Storage` are always identified
    by their `box_id`, `data_id` and `run_id`. The functionality to store data is
    provided by the `Writer` object, that is created by the `create_writer()` method.
    Similarly, loading data is implemented in a separate `Reader` object that is
    created by `create_reader()`.
    """

    @abc.abstractmethod
    def create_reader(self, item):
        """
        Creates a `Reader` instance, that allows to load existing data.

        Args:
            item (boxs.storage.Item): The item that should be read.

        Returns:
            boxs.storage.Reader: The reader that will load the data from the
                storage.
        """

    @abc.abstractmethod
    def create_writer(self, item, name=None, tags=None):
        """
        Creates a `Writer` instance, that allows to store new data.

        Args:
            item (boxs.storage.Item): The new data item.
            name (str): An optional name, that can be used for referring to this item
                within the run. Defaults to `None`.
            tags (Dict[str,str]): A dictionary containing tags that can be used for
                grouping multiple items together. Defaults to an empty dictionary.

        Returns:
            boxs.storage.Writer: The writer that will write the data into the
                storage.
        """

    @abc.abstractmethod
    def list_runs(self, box_id, limit=None, name_filter=None):
        """
        List the runs within a box stored in this storage.

        The runs should be returned in descending order of their start time.

        Args:
            box_id (str): `box_id` of the box in which to look for runs.
            limit (Optional[int]): Limits the returned runs to maximum `limit` number.
                Defaults to `None` in which case all runs are returned.
            name_filter (Optional[str]): If set, only include runs which have names
                that have the filter as prefix. Defaults to `None` in which case all
                runs are returned.

        Returns:
            List[box.storage.Run]: The runs.
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
    def delete_run(self, box_id, run_id):
        """
        Delete all the data of the specified run.

        Args;
            box_id (str): `box_id` of the box in which the run is stored.
            run_id (str): Run id of the run which should be deleted.
        """


class Reader(abc.ABC):
    """
    Base class for the storage specific reader implementations.
    """

    def __init__(self, item):
        """
        Creates a `Reader` instance, that allows to load existing data.

        Args:
            item (boxs.storage.Item): The `item` with the data that should be
                loaded.
        """
        self._item = item

    @property
    def item(self):
        """The item of the data that this reader can read."""
        return self._item

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
    def meta(self):
        """Dictionary containing the meta-data about the data."""
        return self.info['meta']

    @abc.abstractmethod
    def as_stream(self):
        """
        Return a stream from which the data content can be read.

        Returns:
            io.RawIOBase: A stream instance from which the data can be read.
        """


class Writer(abc.ABC):
    """
    Base class for the storage specific writer implementations.
    """

    def __init__(self, item, name, tags):
        """
        Creates a `Writer` instance, that allows to store new data.

        Args:
            item (boxs.storage.Item): The new item.
        """
        self._item = item
        self._name = name
        self._tags = tags
        self._meta = {}

    @property
    def item(self):
        """Returns the item which this writer writes to."""
        return self._item

    @property
    def name(self):
        """Returns the name of the new data item."""
        return self._name

    @property
    def tags(self):
        """Returns the tags of the new data item."""
        return self._tags

    @property
    def meta(self):
        """
        Returns a dictionary which contains meta-data of the item.

        This allows either ValueTypes or Transformers to add additional
        meta-data for the data item.
        """
        return self._meta

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
    def write_info(self, info):
        """
        Write the info for the data item to the storage.

        Args:
            info (Dict[str,Any]): The information about the new data item.

        Raises:
            boxs.errors.DataCollision: If a data item with the same ids already
                exists.
        """

    @abc.abstractmethod
    def as_stream(self):
        """
        Return a stream to which the data content should be written.

        This method can be used by the ValueType to actually transfer the data.

        Returns:
            io.RawIOBase: The binary io-stream.

        Raises:
            boxs.errors.DataCollision: If a data item with the same ids already
                exists.
        """
