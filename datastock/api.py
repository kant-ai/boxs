"""API to be used by users"""
from .origin import determine_origin, ORIGIN_FROM_FUNCTION_NAME
from .stock import get_stock


def store(
    data_input,
    *parents,
    name=None,
    origin=ORIGIN_FROM_FUNCTION_NAME,
    tags=None,
    run_id=None,
    meta=None,
    stock=None
):
    """
    Store new data in this stock.

    Args:
        data_input (Callable[datastock.storage.Writer]): A callable that takes a
            single `Writer` argument.
        *parents (datastock.data.DataItem): Parent data instances, that this data
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
        stock (Union[str,datastock.stock.Stock]): The stock in which the data should
            be stored. The stock can be either given as Stock instance, or by its
            `stock_id`.

    Returns:
        datastock.data.DataItem: Data instance that contains information about the
            data and allows referring to it.

    Raises:
        ValueError: If no stock or no origin was provided.
        datastock.errors.StockNotDefined: If no stock with the given stock id is
            defined.
    """
    if stock is None:
        raise ValueError("'stock' must be set.")
    if isinstance(stock, str):
        stock = get_stock(stock)
    origin = determine_origin(origin)
    return stock.store(
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
    Load data..

    Args:
        data_output (Callable[datastock.storage.Reader]): A callable that takes a
            single `Reader` argument, reads the data and returns it.
        data (Union[datastock.data.DataRef,datastock.data.DataItem]): DataItem or
            DataRef that points to the data that should be loaded.

    Returns:
        Any: The loaded data.

    Raises:
        datastock.errors.StockNotDefined: If the data is stored in an unknown stock.
        datastock.errors.DataNotFound: If no data with the specific ids are stored
            in this stock.
        ValueError: If the data refers to a different stock by its stock_id.
    """
    stock_id = data.stock_id
    stock = get_stock(stock_id)
    return stock.load(data_output, data)
