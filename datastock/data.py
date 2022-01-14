"""Classes representing data items and references"""
import urllib.parse

from .api import info, load


class DataRef:
    """
    Reference to a DataInfo.
    """

    __slots__ = [
        'data_id',
        'run_id',
        'stock_id',
        '_info',
    ]

    def __init__(self, data_id, stock_id, run_id):
        self.data_id = data_id
        self.stock_id = stock_id
        self.run_id = run_id
        self._info = None

    def value_info(self):
        """
        Returns information about this reference.

        Returns:
            Dict[str,str]: A dict containing information about this reference.
        """
        value_info = {
            'data_id': self.data_id,
            'run_id': self.run_id,
            'stock_id': self.stock_id,
        }
        return value_info

    @classmethod
    def from_value_info(cls, value_info):
        """
        Recreate a DataRef from its value_info.

        Args:
            value_info (Dict[str,str]): A dictionary containing the ids.

        Returns:
            datatstock.data.DataRef: The DataRef referencing the data.

        Raises:
            KeyError: If necessary attributes are missing from the `value_info`.
        """
        data_id = value_info['data_id']
        run_id = value_info['run_id']
        storage_id = value_info['stock_id']
        data = DataRef(data_id, storage_id, run_id)
        return data

    @property
    def uri(self):
        """Return the URI of the data item referenced."""
        return f'stock://{self.stock_id}/{self.data_id}/{self.run_id}'

    @classmethod
    def from_uri(cls, uri):
        """
        Recreate a DataRef from a URI.

        Args:
            uri (str): URI in the format 'stock://<stock-id>/<data-id>/<run-id>'.

        Returns:
            DataRef: The DataRef referencing the data.

        Raises:
            ValueError: If the URI doesn't follow the expected format.
        """
        url_parts = urllib.parse.urlparse(uri)
        if url_parts.scheme != 'stock':
            raise ValueError("Invalid scheme")
        data_id, run_id = url_parts.path[1:].split('/', 1)
        storage_id = url_parts.hostname
        data = DataRef(data_id, storage_id, run_id)
        return data

    @property
    def info(self):
        """
        Returns the info object describing the referenced data item.

        Returns:
             datatstock.data.DataInfo: The info about the data item referenced.
        """
        if self._info is None:
            self._info = info(self)
        return self._info

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        return (
            self.stock_id == other.stock_id
            and self.data_id == other.data_id
            and self.run_id == other.run_id
        )

    def __hash__(self):
        return hash((self.stock_id, self.data_id, self.run_id))


class DataInfo:
    """
    Class representing a stored data item.

    Attributes:
        ref (datastock.data.DataRef): Reference to this item.
        origin (str): The origin of the data.
        parents (Tuple[datastock.data.DataItem]): A tuple containing other data items
            from which this item was derived.
        name (Optional[str]): A string that can be used to refer to this item by an
            user. Defaults to `None`.
        tags (Dict[str,str]): A dictionary containing string keys and values, that can
            be used for grouping multiple items together. Defaults to an empty dict.
        meta (Dict[str,Any]): A dictionary containing meta-data. This meta-data can
            have arbitrary values as long as they can be serialized to JSON. Defaults
            to an empty dict.

    """

    __slots__ = [
        'ref',
        'origin',
        'name',
        'parents',
        'tags',
        'meta',
    ]

    def __init__(
        self,
        ref,
        origin,
        parents=tuple(),
        name=None,
        tags=None,
        meta=None,
    ):  # pylint: disable=too-many-arguments
        self.ref = ref
        self.origin = origin
        self.parents = parents
        self.name = name
        self.tags = tags or {}
        self.meta = meta or {}

    @property
    def data_id(self):
        """Returns the data_id."""
        return self.ref.data_id

    @property
    def stock_id(self):
        """Returns the stock_id."""
        return self.ref.stock_id

    @property
    def run_id(self):
        """Returns the run_id."""
        return self.ref.run_id

    def load(self, data_output):
        """
        Load the content of the data item.

        Args:
            data_output (Callable[datastock.storage.Reader]): A callable that takes a
                single `Reader` argument, reads the data and returns it.

        Returns:
            Any: The loaded data.

        Raises:
            datastock.errors.StockNotDefined: If the data is stored in an unknown stock.
            datastock.errors.DataNotFound: If no data with the specific ids are stored
                in the referenced stock.
        """
        return load(data_output, self)

    def value_info(self):
        """
        Returns information about this data item.

        Returns:
            Dict[str,str]: A dict containing information about this reference.
        """
        value_info = {
            'ref': self.ref.value_info(),
            'origin': self.origin,
            'name': self.name,
            'tags': self.tags,
            'parents': [parent.value_info() for parent in self.parents],
            'meta': self.meta,
        }
        return value_info

    @classmethod
    def from_value_info(cls, value_info):
        """
        Recreate a DataInfo from its value_info.

        Args:
            value_info (Dict[str,str]): A dictionary containing the info.

        Returns:
            aatatstock.data.DataInfo: The information about the data item.

        Raises:
            KeyError: If necessary attributes are missing from the `value_info`.
        """
        data_ref = DataRef.from_value_info(value_info['ref'])
        origin = value_info['origin']
        name = value_info['name']
        tags = value_info['tags']
        meta = value_info['meta']
        parents = tuple(
            DataInfo.from_value_info(parent_info)
            for parent_info in value_info['parents']
        )
        return DataInfo(
            data_ref,
            origin,
            parents,
            name=name,
            tags=tags,
            meta=meta,
        )
