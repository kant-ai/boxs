"""Registry of stocks"""
from .errors import StockAlreadyDefined, StockNotDefined


_STOCK_REGISTRY = {}


def register_stock(stock):
    """
    Registers a new stock.

    Args:
        stock (datastock.stock.Stock): The  stock that should be registered.

    Raises:
        datastock.errors.StockAlreadyDefined: If a stock with the same id is already
            registered.
    """
    stock_id = stock.stock_id
    if stock_id in _STOCK_REGISTRY:
        raise StockAlreadyDefined(stock_id)
    _STOCK_REGISTRY[stock.stock_id] = stock


def unregister_stock(stock_id):
    """
    Unregisters the stock with the given stock_id.

    Args:
        stock_id (str): The id of the stock that should be removed.

    Raises:
        datastock.errors.StockNotDefined: If no stock with the given id is defined.
    """
    if stock_id not in _STOCK_REGISTRY:
        raise StockNotDefined(stock_id)
    del _STOCK_REGISTRY[stock_id]


def get_stock(stock_id):
    """
    Return the stock with the given stock_id.

    Args:
        stock_id (str): The id of the stock that should be returned.

    Returns:
        datastock.stock.Stock: The stock with the given `stock_id`.

    Raises:
        datastock.errors.StockNotDefined: If no stock with the given id is defined.
    """
    if stock_id not in _STOCK_REGISTRY:
        raise StockNotDefined(stock_id)
    return _STOCK_REGISTRY[stock_id]
