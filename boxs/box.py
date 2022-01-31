"""Boxes to store items in"""
import hashlib
import logging

from .box_registry import register_box
from .data import DataInfo, DataRef
from .errors import MissingValueType
from .origin import ORIGIN_FROM_FUNCTION_NAME, determine_origin
from .run import get_run_id
from .value_types import (
    BytesValueType,
    FileValueType,
    JsonValueType,
    StreamValueType,
    StringValueType,
    ValueType,
)


logger = logging.getLogger(__name__)


def calculate_data_id(origin, parent_ids=tuple(), name=None):
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
            name or '',
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
            JsonValueType(),
        ]
        register_box(self)

    def add_value_type(self, value_type):
        """
        Add a new value type.

        The value type is added at the beginning of the list, so that it takes
        precedence over the already added value types.

        Args:
            value_type (boxs.value_types.ValueType): The new value type to add.
        """
        self.value_types.insert(0, value_type)

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
            *parents (Union[boxs.data.DataInfo, boxs.data.DataRef]): Parent data refs,
                that this data depends on.
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
        else:
            meta = dict(meta)
        origin = determine_origin(origin, name=name, tags=tags, level=3)
        logger.info("Storing value in box %s with origin %s", self.box_id, origin)
        parent_ids = tuple(p.data_id for p in parents)
        data_id = calculate_data_id(origin, parent_ids=parent_ids, name=name)
        logger.debug(
            "Calculate data_id %s from origin %s with parents %s",
            data_id,
            origin,
            parent_ids,
        )
        if run_id is None:
            run_id = get_run_id()

        ref = DataRef(self.box_id, data_id, run_id)

        writer = self.storage.create_writer(ref, name, tags)
        logger.debug("Created writer %s for data %s", writer, ref)

        writer = self._apply_transformers_to_writer(writer)

        if value_type is None:
            value_type = self._find_suitable_value_type(value)

        if value_type is None:
            raise MissingValueType(value)

        logger.debug(
            "Write value for data %s with value type %s",
            ref.uri,
            value_type.get_specification(),
        )
        writer.write_value(value, value_type)

        meta['value_type'] = value_type.get_specification()
        meta = dict(meta)
        meta.update(writer.meta)
        data_info = DataInfo(
            DataRef.from_item(writer.item),
            origin=origin,
            parents=parents,
            name=name,
            tags=tags,
            meta=meta,
        )

        logger.debug("Write info for data %s", ref.uri)
        writer.write_info(data_info.value_info())

        return data_info

    def _find_suitable_value_type(self, value):
        value_type = None
        for configured_value_type in self.value_types:
            if configured_value_type.supports(value):
                value_type = configured_value_type
                logger.debug(
                    "Automatically chose value type %s",
                    value_type.get_specification(),
                )
        return value_type

    def _apply_transformers_to_writer(self, writer):
        for transformer in self.transformers:
            logger.debug("Applying transformer %s", transformer)
            writer = transformer.transform_writer(writer)
        return writer

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

        logger.info("Loading value %s from box %s", data_ref.uri, self.box_id)

        info = data_ref.info

        if value_type is None:
            value_type = self._get_value_type_from_meta_data(info)

        reader = self.storage.create_reader(data_ref)
        logger.debug("Created reader %s for data %s", reader, data_ref)

        reader = self._apply_transformers_to_reader(reader)

        logger.debug(
            "Read value from data %s with value type %s",
            data_ref.uri,
            value_type.get_specification(),
        )
        return reader.read_value(value_type)

    @staticmethod
    def _get_value_type_from_meta_data(info):
        value_type_specification = info.meta['value_type']
        value_type = ValueType.from_specification(value_type_specification)
        logger.debug(
            "Use value type %s taken from meta-data",
            value_type.get_specification(),
        )
        return value_type

    def _apply_transformers_to_reader(self, reader):
        for transformer in reversed(self.transformers):
            logger.debug("Applying transformer %s", transformer)
            reader = transformer.transform_reader(reader)
        return reader

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

        logger.info("Getting info for value %s from box %s", data_ref.uri, self.box_id)
        reader = self.storage.create_reader(data_ref)

        logger.debug("Created reader %s for data %s", reader, data_ref)
        return DataInfo.from_value_info(reader.info)
