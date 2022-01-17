"""Boxes to store items in"""
import hashlib

from .box_registry import register_box
from .data import DataInfo, DataRef
from .errors import DataCollision, DataNotFound, MissingValueType
from .origin import ORIGIN_FROM_FUNCTION_NAME, determine_origin
from .run import get_run_id
from .value_types import (
    BytesValueType,
    FileValueType,
    StreamValueType,
    StringValueType,
    ValueType,
)


def calculate_data_id(origin, parent_ids=tuple()):
    """
    Derive a data_id from origin and parent_ids

    Args:
        origin (str): The origin of the data.
        parent_ids (tuple[str]): A tuple of data_ids of "parent" data, that this data
            is derived from.

    Returns:
         str: The data_id.
    """
    id_origin_data = ':'.join(
        [
            origin,
        ]
        + sorted(parent_ids)
    )
    return hashlib.blake2b(id_origin_data.encode('utf-8'), digest_size=8).hexdigest()


class Box:
    """Box that allows to store and load data.

    Attributes:
        box_id (str): The id that uniquely identifies this Box.
        storage (boxs.storage.Storage): The storage that actually writes and
            reads the data.
        transformers (boxs.storage.Transformer): A tuple with transformers, that
            add additional meta-data and transform the data stored and loaded.
    """

    def __init__(self, box_id, storage, *transformers):
        self.box_id = box_id
        self.storage = storage
        self.transformers = transformers
        self.value_types = [
            BytesValueType(),
            StreamValueType(),
            StringValueType(),
            FileValueType(),
        ]
        register_box(self)

    def store(
        self,
        value,
        *parents,
        origin=ORIGIN_FROM_FUNCTION_NAME,
        name=None,
        tags=None,
        meta=None,
        value_type=None,
        run_id=None,
    ):
        """
        Store new data in this box.

        Args:
            value (Any): A value that should be stored.
            *parents (boxs.data.DataInfo): Parent data instances, that this data
                depends on.
            origin (Union[str,Callable]): A string or callable returning a string,
                that is used as an origin for deriving the data's id. Defaults to a
                callable, that takes the name of the function, from which `store` is
                being called as origin.
            name (str): An optional user-defined name, that can be used for looking up
                data manually.
            tags (Dict[str,str]): A dictionary of tags that can be used for grouping
                multiple data together. Keys and values have to be strings.
            meta (Dict[str, Any]): Additional meta-data about this data. This can be
                used for arbitrary information that might be useful, e.g. information
                about type or format of the data, timestamps, user info etc.
            value_type (boxs.value_types.ValueType): The value_type to use for writing
                this value to the storage. Defaults to `None` in which case a suitable
                value type is taken from the list of predefined values types.
            run_id (str): The id of the run when the data was stored.

        Returns:
            boxs.data.DataInfo: Data instance that contains information about the
                data and allows referring to it.
        """
        if tags is None:
            tags = {}
        if meta is None:
            meta = {}
        origin = determine_origin(origin)
        data_id = calculate_data_id(
            origin, parent_ids=tuple(p.data_id for p in parents)
        )

        if run_id is None:
            run_id = get_run_id()

        if self.storage.exists(data_id, run_id):
            raise DataCollision(self.box_id, data_id, run_id)

        ref = DataRef(data_id, self.box_id, run_id)

        writer = self.storage.create_writer(data_id, run_id)
        for transformer in self.transformers:
            writer = transformer.transform_writer(writer)

        if value_type is None:
            for configured_value_type in self.value_types:
                if configured_value_type.supports(value):
                    value_type = configured_value_type

        if value_type is None:
            raise MissingValueType(value)

        value_type.write_value_to_writer(value, writer)

        data = DataInfo(
            ref,
            origin=origin,
            parents=parents,
            name=name,
            tags=tags,
            meta=meta,
        )
        writer.write_info(data.value_info())
        return data

    def load(self, data_ref, value_type=None):
        """
        Load data from the box.

        Args:
            data_ref (Union[boxs.data.DataRef,boxs.data.DataInfo]): Data reference
                that points to the data content to be loaded.
            value_type (boxs.value_types.ValueType): The value type to use when
                loading the data. Defaults to `None`, in which case the same value
                type will be used that was used when the data was initially stored.

        Returns:
            Any: The loaded data.

        Raises:
            boxs.errors.DataNotFound: If no data with the specific ids are stored
                in this box.
            ValueError: If the data refers to a different box by its box_id.
        """
        if data_ref.box_id != self.box_id:
            raise ValueError("Data references different box id")

        info = data_ref.info

        if value_type is None:
            value_type_specification = info.meta['value_type']
            value_type = ValueType.from_specification(value_type_specification)

        reader = self.storage.create_reader(data_ref.data_id, data_ref.run_id)

        for transformer in reversed(self.transformers):
            reader = transformer.transform_reader(reader)

        return reader.read_value(value_type)

    def info(self, data_ref):
        """
        Load info from the box.

        Args:
            data_ref (boxs.data.DataRef): Data reference that points to the data
                whose info is requested.

        Returns:
            boxs.data.DataInfo: The info about the data.

        Raises:
            boxs.errors.DataNotFound: If no data with the specific ids are stored
                in this box.
            ValueError: If the data refers to a different box by its box_id.
        """
        if data_ref.box_id != self.box_id:
            raise ValueError("Data references different box id")
        if not self.storage.exists(data_ref.data_id, data_ref.run_id):
            raise DataNotFound(self.box_id, data_ref.data_id, data_ref.run_id)

        reader = self.storage.create_reader(data_ref.data_id, data_ref.run_id)
        return reader.info
