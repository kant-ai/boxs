"""API to be used by users"""
from .origin import determine_origin, ORIGIN_FROM_FUNCTION_NAME
from .box_registry import get_box


def store(
    data_input,
    *parents,
    name=None,
    origin=ORIGIN_FROM_FUNCTION_NAME,
    tags=None,
    run_id=None,
    meta=None,
    box=None
):
    """
    Store new data in this box.

    Args:
        data_input (Callable[boxs.storage.Writer]): A callable that takes a
            single `Writer` argument.
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
        raise ValueError("'box' must be set.")
    if isinstance(box, str):
        box = get_box(box)
    origin = determine_origin(origin)
    return box.store(
        data_input,
        *parents,
        name=name,
        origin=origin,
        tags=tags,
        meta=meta,
        run_id=run_id
    )


def load(data_output, data):
    """
    Load the content of the data item.

    Args:
        data_output (Callable[boxs.storage.Reader]): A callable that takes a
            single `Reader` argument, reads the data and returns it.
        data (Union[boxs.data.DataRef,boxs.data.DataInfo]): DataInfo or
            DataRef that points to the data that should be loaded.

    Returns:
        Any: The loaded data.

    Raises:
        boxs.errors.BoxNotDefined: If the data is stored in an unknown box.
        boxs.errors.DataNotFound: If no data with the specific ids are stored in the
            referenced box.
    """
    box_id = data.box_id
    box = get_box(box_id)
    return box.load(data_output, data)


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
    return box.info(data_ref)
