"""API to be used by users"""
import logging

from .box_registry import get_box
from .config import get_config
from .origin import determine_origin, ORIGIN_FROM_FUNCTION_NAME


logger = logging.getLogger(__name__)


def store(
    value,
    *parents,
    name=None,
    origin=ORIGIN_FROM_FUNCTION_NAME,
    tags=None,
    meta=None,
    value_type=None,
    run_id=None,
    box=None
):
    """
    Store new data in this box.

    Args:
        value (Any): A value that should be stored.
        *parents (Union[boxs.data.DataInfo,boxs.data.DataRef]): Parent data refs,
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
        run_id (str): The id of the run when the data was stored. Defaults to the
            current global run_id (see `get_run_id()`).
        box (Union[str,boxs.box.Box]): The box in which the data should be stored.
            The box can be either given as Box instance, or by its `box_id`.

    Returns:
        boxs.data.DataInfo: Data instance that contains information about the
            data and allows referring to it.

    Raises:
        ValueError: If no box or no origin was provided.
        boxs.errors.BoxNotDefined: If no box with the given box id is
            defined.
    """
    if box is None:
        box = get_config().default_box
        logger.debug("No box defined, using default_box %s from config", box)
    if box is None:
        raise ValueError("'box' must be set.")
    if isinstance(box, str):
        box = get_box(box)
    origin = determine_origin(origin, name=name, tags=tags, level=3)
    return box.store(
        value,
        *parents,
        name=name,
        origin=origin,
        tags=tags,
        meta=meta,
        value_type=value_type,
        run_id=run_id
    )


def load(data, value_type=None):
    """
    Load the content of the data item.

    Args:
        data (Union[boxs.data.DataRef,boxs.data.DataInfo]): DataInfo or
            DataRef that points to the data that should be loaded.
        value_type (boxs.value_types.ValueType): The value type to use when
            loading the data. Defaults to `None`, in which case the same value
            type will be used that was used when the data was initially stored.
    Returns:
        Any: The loaded data.

    Raises:
        boxs.errors.BoxNotDefined: If the data is stored in an unknown box.
        boxs.errors.DataNotFound: If no data with the specific ids are stored in the
            referenced box.
    """
    box_id = data.box_id
    box = get_box(box_id)
    logger.debug("Loading value %s from box %s", data.uri, box.box_id)
    return box.load(data, value_type=value_type)


def info(data_ref):
    """
    Load info from a reference to an item.

    Args:
        data_ref (boxs.data.DataRef): Data reference that points to the data
            whose info is requested.

    Returns:
        boxs.data.DataInfo: The info about the data.

    Raises:
        boxs.errors.BoxNotDefined: If the data is stored in an unknown box.
        boxs.errors.DataNotFound: If no data with the specific ids are stored in this
            box.
    """
    box_id = data_ref.box_id
    box = get_box(box_id)
    logger.debug("Getting info about value %s from box %s", data_ref.uri, box.box_id)
    return box.info(data_ref)
