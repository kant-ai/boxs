"""Functionality for stocks"""
import hashlib

from .data import DataItem, DataRef
from .errors import DataCollision, DataNotFound, StockAlreadyDefined, StockNotDefined
from .origin import ORIGIN_FROM_FUNCTION_NAME, determine_origin
from .run import get_run_id


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


_STOCK_REGISTRY = {}


def _register_stock(stock):
    stock_id = stock.stock_id
    if stock_id in _STOCK_REGISTRY:
        raise StockAlreadyDefined(stock_id)
    _STOCK_REGISTRY[stock.stock_id] = stock


def _unregister_stock(stock_id):
    del _STOCK_REGISTRY[stock_id]


def get_stock(stock_id):
    """
    Return the stock with the given stock_id.

    Args:
        stock_id (str): The id of the stock that should be returned.

    Returns:
        datastock.stock.Stock: The stock with the given `sotck_id`.

    Raises:
        datastock.errors.StockNotDefined: If no stock with the given id is defined.
    """
    if stock_id not in _STOCK_REGISTRY:
        raise StockNotDefined(stock_id)
    return _STOCK_REGISTRY[stock_id]


class Stock:
    """Stocks that allows to store and load data.

    Attributes:
        stock_id (str): The id that uniquely identifies this stock.
        storage (datastock.storage.Storage): The storage that actually writes and
            reads the data.
        transformers (datastock.storage.Transformer): A tuple with transformers, that
            add additional meta-data and transform the data stored and loaded.
    """

    def __init__(self, stock_id, storage, *transformers):
        self.stock_id = stock_id
        self.storage = storage
        self.transformers = transformers
        _register_stock(self)

    def store(
        self,
        data_input,
        *parents,
        origin=ORIGIN_FROM_FUNCTION_NAME,
        name=None,
        tags=None,
        meta=None,
        run_id=None,
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
            run_id (str): The id of the run when the data was stored.

        Returns:
            datastock.data.DataItem: Data instance that contains information about the
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
            raise DataCollision(self.stock_id, data_id, run_id)

        ref = DataRef(data_id, self.stock_id, run_id)

        writer = self.storage.create_writer(data_id, run_id)
        for transformer in self.transformers:
            writer = transformer.transform_writer(writer)

        writer.write_content(data_input)

        data = DataItem(
            ref,
            origin=origin,
            parents=parents,
            name=name,
            tags=tags,
            meta=meta,
        )
        writer.write_info(data.info())
        return data

    def load(self, data_output, data_ref):
        """
        Load data from the stock.

        Args:
            data_output (Callable[datastock.storage.Reader]): A callable that takes a
                single `Reader` argument, reads the data and returns it.
            data_ref (datastock.data.DataRef): Data reference that points to the data
                to be loaded.

        Returns:
            Any: The loaded data.

        Raises:
            datastock.errors.DataNotFound: If no data with the specific ids are stored
                in this stock.
            ValueError: If the data refers to a different stock by its stock_id.
        """
        if data_ref.stock_id != self.stock_id:
            raise ValueError("Data references different stock id")
        if not self.storage.exists(data_ref.data_id, data_ref.run_id):
            raise DataNotFound(self.stock_id, data_ref.data_id, data_ref.run_id)

        reader = self.storage.create_reader(data_ref.data_id, data_ref.run_id)
        for transformer in reversed(self.transformers):
            reader = transformer.transform_reader(reader)

        return reader.read_content(data_output)
