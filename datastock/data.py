"""Classes representing data items and references"""
import urllib.parse


class DataRef:
    """
    Reference to a DataItem.
    """

    __slots__ = [
        'data_id',
        'run_id',
        'stock_id',
        'uri',
    ]

    def __init__(self, data_id, stock_id, run_id):
        self.data_id = data_id
        self.stock_id = stock_id
        self.run_id = run_id
        self.uri = self._create_uri()

    def _create_uri(self):
        return f'stock://{self.stock_id}/{self.data_id}/{self.run_id}'

    def info(self):
        """
        Returns information about this reference.

        Returns:
            Dict[str,str]: A dict containing information about this reference.
        """
        info = {
            'data_id': self.data_id,
            'run_id': self.run_id,
            'stock_id': self.stock_id,
        }
        return info

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


class DataItem:
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

    def info(self):
        """
        Returns information about this data item.

        Returns:
            Dict[str,str]: A dict containing information about this reference.
        """
        info = {
            'ref': self.ref.info(),
            'origin': self.origin,
            'name': self.name,
            'tags': self.tags,
            'parents': [parent.info() for parent in self.parents],
            'meta': self.meta,
        }
        return info
