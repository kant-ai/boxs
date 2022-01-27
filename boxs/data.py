"""Classes representing data items and references"""
import urllib.parse

from .api import info, load


class DataRef:
    """
    Reference to a DataInfo.
    """

    __slots__ = [
        'box_id',
        'data_id',
        'run_id',
        '_info',
    ]

    def __init__(self, box_id, data_id, run_id):
        self.box_id = box_id
        self.data_id = data_id
        self.run_id = run_id
        self._info = None

    def value_info(self):
        """
        Returns information about this reference.

        Returns:
            Dict[str,str]: A dict containing information about this reference.
        """
        value_info = {
            'box_id': self.box_id,
            'data_id': self.data_id,
            'run_id': self.run_id,
        }
        return value_info

    @classmethod
    def from_value_info(cls, value_info):
        """
        Recreate a DataRef from its value_info.

        Args:
            value_info (Dict[str,str]): A dictionary containing the ids.

        Returns:
            boxs.data.DataRef: The DataRef referencing the data.

        Raises:
            KeyError: If necessary attributes are missing from the `value_info`.
        """
        box_id = value_info['box_id']
        data_id = value_info['data_id']
        run_id = value_info['run_id']
        data = DataRef(box_id, data_id, run_id)
        return data

    @property
    def uri(self):
        """Return the URI of the data item referenced."""
        return f'boxs://{self.box_id}/{self.data_id}/{self.run_id}'

    @classmethod
    def from_uri(cls, uri):
        """
        Recreate a DataRef from a URI.

        Args:
            uri (str): URI in the format 'box://<box-id>/<data-id>/<run-id>'.

        Returns:
            DataRef: The DataRef referencing the data.

        Raises:
            ValueError: If the URI doesn't follow the expected format.
        """
        url_parts = urllib.parse.urlparse(uri)
        if url_parts.scheme != 'boxs':
            raise ValueError("Invalid scheme")
        box_id = url_parts.hostname
        data_id, run_id = url_parts.path[1:].split('/', 1)
        data = DataRef(box_id, data_id, run_id)
        return data

    @classmethod
    def from_item(cls, item):
        """
        Recreate a DataRef from an Item.

        Args:
            item (boxs.storage.Item): The item which describes the data we want to
                refer to.

        Returns:
            DataRef: The DataRef referencing the data.
        """
        return DataRef(item.box_id, item.data_id, item.run_id)

    @property
    def info(self):
        """
        Returns the info object describing the referenced data item.

        Returns:
             boxs.data.DataInfo: The info about the data item referenced.
        """
        if self._info is None:
            self._info = info(self)
        return self._info

    def load(self, value_type=None):
        """
        Load the content of the data item.

        Args:
            value_type (boxs.value_types.ValueType): The value type to use when
                loading the data. Defaults to `None`, in which case the same value
                type will be used that was used when the data was initially stored.

        Returns:
            Any: The loaded data.

        Raises:
            boxs.errors.BoxNotDefined: If the data is stored in an unknown box.
            boxs.errors.DataNotFound: If no data with the specific ids are stored
                in the referenced box.
        """
        return self.info.load(value_type=value_type)

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        return (
            self.box_id == other.box_id
            and self.data_id == other.data_id
            and self.run_id == other.run_id
        )

    def __hash__(self):
        return hash((self.box_id, self.data_id, self.run_id))

    def __str__(self):
        return self.uri


class DataInfo:
    """
    Class representing a stored data item.

    Attributes:
        ref (boxs.data.DataRef): Reference to this item.
        origin (str): The origin of the data.
        parents (Tuple[boxs.data.DataItem]): A tuple containing other data items
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
    def box_id(self):
        """Returns the box_id."""
        return self.ref.box_id

    @property
    def run_id(self):
        """Returns the run_id."""
        return self.ref.run_id

    @property
    def uri(self):
        """Returns the uri."""
        return self.ref.uri

    @property
    def info(self):
        """Returns the info. This is to be compatible with DataRef"""
        return self

    def load(self, value_type=None):
        """
        Load the content of the data item.

        Args:
            value_type (boxs.value_types.ValueType): The value type to use when
                loading the data. Defaults to `None`, in which case the same value
                type will be used that was used when the data was initially stored.

        Returns:
            Any: The loaded data.

        Raises:
            boxs.errors.BoxNotDefined: If the data is stored in an unknown box.
            boxs.errors.DataNotFound: If no data with the specific ids are stored
                in the referenced box.
        """
        return load(self, value_type=value_type)

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
            boxs.data.DataInfo: The information about the data item.

        Raises:
            KeyError: If necessary attributes are missing from the `value_info`.
        """
        if 'ref' not in value_info:
            return DataRef.from_value_info(value_info)

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

    def __str__(self):
        return self.uri
