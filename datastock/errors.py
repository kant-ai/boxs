"""Errors in datastock"""


class DatastockError(Exception):
    """Base class for all datastock specific errors"""


class DataError(DatastockError):
    """Base class for all datastock specific errors related to data"""


class DataCollision(DataError):
    """
    Error that is raised if a newly created data item already exists.

    Attributes:
        stock_id (str): The id of the stock containing the data item.
        data_id (str): The id of the data item.
        run_id (str): The id of the run when the data was created.
    """

    def __init__(self, stock_id, data_id, run_id):
        self.stock_id = stock_id
        self.data_id = data_id
        self.run_id = run_id
        super().__init__(
            f"Data {self.data_id} from run {self.run_id} "
            f"already exists in stock {self.stock_id}"
        )


class DataNotFound(DataError):
    """
    Error that is raised if a data item can't be found.

    Attributes:
        stock_id (str): The id of the stock which should contain the data item.
        data_id (str): The id of the data item.
        run_id (str): The id of the run when the data was created.
    """

    def __init__(self, stock_id, data_id, run_id):
        self.stock_id = stock_id
        self.data_id = data_id
        self.run_id = run_id
        super().__init__(
            f"Data {self.data_id} from run {self.run_id} "
            f"does not exist in stock {self.stock_id}"
        )


class StockError(DatastockError):
    """Base class for all datastock specific errors related to stocks"""


class StockAlreadyDefined(StockError):
    """
    Error that is raised if multiple stocks are defined using the same stock id.

    Attributes:
        stock_id (str): The id of the stock.
    """

    def __init__(self, stock_id):
        self.stock_id = stock_id
        super().__init__(f"Stock with stock id {self.stock_id} already defined")


class StockNotDefined(StockError):
    """
    Error that is raised if a stock id refers to a non-defined stock.

    Attributes:
        stock_id (str): The id of the stock.
    """

    def __init__(self, stock_id):
        self.stock_id = stock_id
        super().__init__(f"Stock with stock id {self.stock_id} not defined")
