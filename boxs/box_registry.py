"""Registry of boxes"""
from .config import get_config
from .errors import BoxAlreadyDefined, BoxNotDefined


_BOX_REGISTRY = {}


def register_box(box):
    """
    Registers a new box.

    Args:
        box (boxs.box.Box): The box that should be registered.

    Raises:
        boxs.errors.BoxAlreadyDefined: If a box with the same id is already
            registered.
    """
    box_id = box.box_id
    if box_id in _BOX_REGISTRY:
        raise BoxAlreadyDefined(box_id)
    _BOX_REGISTRY[box.box_id] = box


def unregister_box(box_id):
    """
    Unregisters the box with the given box_id.

    Args:
        box_id (str): The id of the box that should be removed.

    Raises:
        boxs.errors.BoxNotDefined: If no box with the given id is defined.
    """
    if box_id not in _BOX_REGISTRY:
        raise BoxNotDefined(box_id)
    del _BOX_REGISTRY[box_id]


def get_box(box_id=None):
    """
    Return the box with the given box_id.

    Args:
        box_id (Optional[str]): The id of the box that should be returned. Defaults
            to `None` in which case the default box is taken from the config and
            returned.

    Returns:
        boxs.box.Box: The box with the given `box_id`.

    Raises:
        boxs.errors.BoxNotDefined: If no box with the given id is defined.
    """
    box_id = box_id or get_config().default_box
    if box_id not in _BOX_REGISTRY:
        raise BoxNotDefined(box_id)
    return _BOX_REGISTRY[box_id]
