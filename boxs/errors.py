"""Errors in boxs"""


class BoxsError(Exception):
    """Base class for all boxs specific errors"""


class DataError(BoxsError):
    """Base class for all boxs specific errors related to data"""


class DataCollision(DataError):
    """
    Error that is raised if a newly created data item already exists.

    Attributes:
        box_id (str): The id of the box containing the data item.
        data_id (str): The id of the data item.
        run_id (str): The id of the run when the data was created.
    """

    def __init__(self, box_id, data_id, run_id):
        self.box_id = box_id
        self.data_id = data_id
        self.run_id = run_id
        super().__init__(
            f"Data {self.data_id} from run {self.run_id} "
            f"already exists in box {self.box_id}"
        )


class DataNotFound(DataError):
    """
    Error that is raised if a data item can't be found.

    Attributes:
        box_id (str): The id of the box which should contain the data item.
        data_id (str): The id of the data item.
        run_id (str): The id of the run when the data was created.
    """

    def __init__(self, box_id, data_id, run_id):
        self.box_id = box_id
        self.data_id = data_id
        self.run_id = run_id
        super().__init__(
            f"Data {self.data_id} from run {self.run_id} "
            f"does not exist in box {self.box_id}"
        )


class BoxError(BoxsError):
    """Base class for all errors related to boxes"""


class BoxAlreadyDefined(BoxError):
    """
    Error that is raised if multiple boxes are defined using the same box id.

    Attributes:
        box_id (str): The id of the box.
    """

    def __init__(self, box_id):
        self.box_id = box_id
        super().__init__(f"Box with box id {self.box_id} already defined")


class BoxNotDefined(BoxError):
    """
    Error that is raised if a box id refers to a non-defined box.

    Attributes:
        box_id (str): The id of the box.
    """

    def __init__(self, box_id):
        self.box_id = box_id
        super().__init__(f"Box with box id {self.box_id} not defined")
